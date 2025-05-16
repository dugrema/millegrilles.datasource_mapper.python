import asyncio
import datetime
import logging
import json
import gzip
import math
import os
import pathlib
import tempfile
import zlib

from typing import Optional, Union

from millegrilles_datasourcemapper.DataStructures import ProcessJob
from millegrilles_datasourcemapper.FeedDataProcessor import select_data_processor
from millegrilles_datasourcemapper.Util import decode_base64_nopad
from millegrilles_messages.chiffrage.DechiffrageUtils import dechiffrer_reponse, dechiffrer_bytes_secrete
from millegrilles_messages.messages import Constantes
from millegrilles_datasourcemapper.Context import DatasourceMapperContext
from millegrilles_messages.messages.MessagesModule import MessageWrapper


class FeedViewProcessor:

    def __init__(self, context: DatasourceMapperContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__process_queue: asyncio.Queue[Optional[ProcessJob]] = asyncio.Queue(maxsize=1)
        self.__workers: list[FeedViewProcessorWorker] = list()

        self.__staging_feeds_path = pathlib.Path(f'{self.__context.configuration.dir_data}/feeds')
        self.__feed_data_downloader = FeedDataDownloader(context, self.__staging_feeds_path)

    async def run(self):
        async with asyncio.TaskGroup() as group:
            group.create_task(self.__stop_thread())
            # Create all worker threads
            for w in self.__workers:
                group.create_task(w.run())

    async def __stop_thread(self):
        await self.__context.wait()
        for w in self.__workers:
            await w.cancel()
            # Unblock waiters
            try:
                self.__process_queue.put_nowait(None)
            except asyncio.QueueFull:
                pass

    async def setup(self):
        self.__staging_feeds_path.mkdir(parents=True, exist_ok=True)
        num_workers = 2
        for i in range(0, num_workers):
            self.__workers.append(FeedViewProcessorWorker(self.__context, self.__feed_data_downloader, self.__process_queue, worker_id=str(i)))

    async def add_to_queue(self, message: MessageWrapper, reset_staging=False):
        job = ProcessJob(message)
        if reset_staging:
            job.reset = True
        self.__process_queue.put_nowait(job)

    async def add_updates_to_queue(self, message: MessageWrapper):
        job = ProcessJob(message)
        try:
            self.__process_queue.put_nowait(job)
        except asyncio.QueueFull:
            # Queue full, ignore this trigger event
            return



class FeedDataDownloader:
    """
    Downloads and decrypts feed data into a staging area.
    """

    def __init__(self, context: DatasourceMapperContext, staging_path: pathlib.Path, threads=1):
        self.__context = context
        self.__staging_path = staging_path

        self.__semaphore = asyncio.BoundedSemaphore(threads)
        self.__current_feedview_downloads: dict[str, asyncio.Event] = dict()

    async def download_feed_data(self, job: ProcessJob):
        feed_id = job.feed['feed_id']
        feed_view_id = job.view['feed_view_id']

        producer = await self.__context.get_producer()

        job.data_file_path = pathlib.Path(f'{self.__staging_path}/feedview_{feed_view_id}.jsonl.gz')
        staging_file_info_path = pathlib.Path(f'{self.__staging_path}/feedview_{feed_view_id}_info.json')

        # Load staging state
        staging_file_info = dict()
        if job.reset:
            # Reset the local staging area
            try:
                os.unlink(job.data_file_path)
            except FileNotFoundError:
                pass
            try:
                os.unlink(staging_file_info_path)
            except FileNotFoundError:
                pass
        else:
            try:
                with open(staging_file_info_path) as fp:
                    staging_file_info = json.load(fp)
            except FileNotFoundError:
                pass

        try:
            download_event = self.__current_feedview_downloads[feed_view_id]
            # Download being done in a different thread, do not block other processes trying to download simultaneously
            await download_event.wait()
            # Let the process through, will revalidate that all the most recent data was received
            # or retry if other process failed.
        except KeyError:
            pass

        # Limit the number of simultaneous downloads
        async with self.__semaphore:
            # Fetch all records from start date
            skip = 0
            limit = 50

            self.__current_feedview_downloads[feed_view_id] = asyncio.Event()
            download_event = self.__current_feedview_downloads[feed_view_id]
            try:
                start_date = staging_file_info['most_recent_date']  # Epoch in milliseconds
                most_recent_date: Optional[datetime.datetime] = datetime.datetime.fromtimestamp(start_date / 1000)
            except KeyError:
                start_date = 0
                most_recent_date = None

            if start_date > 0:
                open_mode = 'at'  # Append to file
            else:
                open_mode = 'wt'  # New file or overwrite

            with gzip.open(job.data_file_path, open_mode) as output_file:
                try:
                    while self.__context.stopping is False:
                        response = await producer.request(
                            {"feed_id": feed_id, "feed_view_id": feed_view_id, "batch_start": start_date, "limit": limit, "skip": skip},
                            "DataCollector", "getFeedData", Constantes.SECURITE_PROTEGE)

                        if response.parsed['ok'] is not True:
                            raise FeedDownloadException(
                                f'Error received when fetching next batch (code: {response.parsed.get('code')}: {response.parsed.get('err')})')

                        items: list = response.parsed['items']
                        if len(items) == 0:
                            break  # Done

                        decrypted_keys_message = dechiffrer_reponse(self.__context.signing_key, response.parsed['keys'])
                        keys: dict[str, bytes] = dict()
                        for key in decrypted_keys_message['cles']:
                            keys[key['cle_id']] = decode_base64_nopad(key['cle_secrete_base64'])

                        skip += len(items)  # For next batch
                        for item in items:
                            # Download and save in staging area
                            save_date = item['save_date'] / 1000  # To seconds
                            save_date_ts = datetime.datetime.fromtimestamp(save_date)
                            await self.download_data_item_file(job, item, keys, output_file)
                            if most_recent_date is None or most_recent_date < save_date_ts:
                                most_recent_date = save_date_ts

                        staging_file_info['most_recent_date'] = math.floor(most_recent_date.timestamp()*1000)

                        with open(staging_file_info_path, 'wt') as fp:
                            json.dump(staging_file_info, fp)
                except asyncio.TimeoutError:
                    raise FeedDownloadException('Timeout on getFeedData')
                finally:
                    # Releasse waiting threads
                    download_event.set()
                    del self.__current_feedview_downloads[feed_view_id]

    async def download_data_item_file(self, job: ProcessJob, item: dict, keys: dict[str, bytes], output_file) -> (dict, Optional[dict]):
        fuuid = item['data_fuuid']
        with tempfile.TemporaryFile('wb+') as temp_file:
            await self.__context.file_handler.download_file(fuuid, temp_file)
            temp_file.seek(0)
            content = await asyncio.to_thread(zlib.decompress, temp_file.read())
            content = content.decode('utf-8')
            file_content = json.loads(content)

        # Decrypt "encrypted_data", "encrypted_files_map"
        encrypted_data = file_content['encrypted_data']
        encrypted_files_map = file_content.get('encrypted_files_map')

        data_key_id = encrypted_data['cle_id']
        data_key = keys[data_key_id]
        decrypted_data = dechiffrer_bytes_secrete(data_key, encrypted_data)
        output_content: dict[str, Union[str, dict]] = {'data': decrypted_data.decode('utf-8')}

        if encrypted_files_map:
            files_map = dict()
            files_list = file_content.get('files')

            files_key_id = encrypted_files_map['cle_id']
            files_key = keys[files_key_id]
            decrypted_files_map_str = dechiffrer_bytes_secrete(files_key, encrypted_files_map).decode('utf-8')
            decrypted_files_map = json.loads(decrypted_files_map_str)
            for (file_path, fuuid) in decrypted_files_map.items():
                files_map[file_path] = {'fuuid': fuuid}
                file_detail = [f for f in files_list if f['fuuid'] == fuuid].pop()
                files_map[file_path].update(file_detail)

            # if files_list:
            #     for file in files_list:
            #         files_map[file['fuuid']] = file

            output_content['files'] = files_map

        await asyncio.to_thread(json.dump, output_content, output_file)
        await asyncio.to_thread(output_file.write, '\n')  # Separator for JSONL


class FeedViewProcessorWorker:

    def __init__(self, context: DatasourceMapperContext, data_downloader: FeedDataDownloader, work_queue: asyncio.Queue, worker_id: str):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__data_downloader = data_downloader
        self.__work_queue = work_queue
        self.__worker_id = worker_id

        self.__current_task: Optional[asyncio.Task] = None
        # self.__current_job: Optional[ProcessJob] = None

    async def run(self):
        while self.__context.stopping is False:
            # Get next job
            job = await self.__work_queue.get()
            if self.__context.stopping is True:
                return  # Stopping

            # Run processing task
            self.__current_task = asyncio.create_task(self.run_job(job))
            await self.__current_task
            # self.__current_job = None
            self.__current_task = None

    async def run_job(self, job: ProcessJob):
        # job = self.__current_job
        if job is None:
            self.__logger.error("No job to process")
            return

        self.__logger.debug(f"{self.__worker_id} Starting job")

        # Load feed/view metadata
        # Potential to get multiple jobs back (one per feed view)
        try:
            jobs = await self.get_feed_view_information(job)
        except FeedPreparationException:
            self.__logger.exception("Error preparing feed")
            return

        for job in jobs:
            # Download data to staging
            try:
                await self.__data_downloader.download_feed_data(job)
            except FeedDownloadException:
                self.__logger.exception("Error when downloading feed data")
                return

            # Process data and upload to database
            data_processor = select_data_processor(self.__context, job)
            await data_processor.process()

            # Cleanup - removing data but not the staging info file (allows incremental updates)
            if job.data_file_path:
                try:
                    os.unlink(job.data_file_path)
                except FileNotFoundError:
                    pass

        self.__logger.debug(f"{self.__worker_id} Finishing job")

    async def get_feed_view_information(self, job: ProcessJob) -> list[ProcessJob]:
        # job = self.__current_job
        feed_id = job.message.parsed['feed_id']
        try:
            feed_view_ids = job.message.parsed['feed_view_ids']
        except KeyError:
            try:
                feed_view_ids = [job.message.parsed['feed_view_id']]
            except KeyError:
                feed_view_ids = None  # This will return all active feed views

        producer = await self.__context.get_producer()

        feed_response = await producer.request({
            'feed_id': feed_id,
            'feed_view_ids': feed_view_ids,
            'active_only': True,
        }, "DataCollector", "getFeedViews", Constantes.SECURITE_PRIVE)
        try:
            if feed_response.parsed['ok'] is not True:
                raise FeedPreparationException(f"Error loading feed view information (code: {feed_response.parsed.get('code')}): {feed_response.parsed.get('err')}")
        except (ValueError, KeyError, IndexError):
            raise FeedPreparationException("Invalid reponse on loading feed view information")
        except asyncio.TimeoutError:
            raise FeedPreparationException("Timeout when loading feed view information")

        try:
            # Inject feed view information into the job
            job.feed = feed_response.parsed['feed']
            jobs = list()

            for view in feed_response.parsed['views']:
                if view.get('active') is False:
                    continue  # Skip

                view_job = job.copy()
                view_job.view = view

                # Decrypt the view key
                view_job.encryption_key_id = view_job.view['encrypted_data']['cle_id']
                decrypted_keys_message = dechiffrer_reponse(self.__context.signing_key, feed_response.parsed['keys'])
                decrypted_keys = decrypted_keys_message['cles']
                decrypted_key_info = [k for k in decrypted_keys if k['cle_id'] == view_job.encryption_key_id].pop()
                view_job.encryption_key_str = decrypted_key_info['cle_secrete_base64']
                view_job.encryption_key = decode_base64_nopad(view_job.encryption_key_str)

                # Decrypt feed view information to test key
                decrypted_view = dechiffrer_bytes_secrete(view_job.encryption_key, view_job.view['encrypted_data'])
                view_job.decrypted_view_information = json.loads(decrypted_view)
                jobs.append(view_job)

            return jobs
        except (IndexError, KeyError, ValueError) as e:
            raise FeedPreparationException("Error preparing job", e)

    async def cancel(self):
        if self.__current_task:
            self.__current_task.cancel()


class FeedPreparationException(Exception):
    pass

class FeedDownloadException(Exception):
    pass
