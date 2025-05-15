import asyncio
import logging
import json

from typing import Optional

from millegrilles_datasourcemapper.Util import decode_base64_nopad
from millegrilles_messages.chiffrage.DechiffrageUtils import dechiffrer_document, dechiffrer_reponse, \
    dechiffrer_bytes_secrete
from millegrilles_messages.messages import Constantes
from millegrilles_datasourcemapper.Context import DatasourceMapperContext
from millegrilles_messages.messages.MessagesModule import MessageWrapper


class ProcessJob:

    def __init__(self, message: MessageWrapper):
        self.message = message
        self.feed: Optional[dict] = None
        self.view: Optional[dict] = None
        self.decrypted_view_information: Optional[dict] = None
        self.encryption_key_id: Optional[str] = None
        self.encryption_key_str: Optional[str] = None
        self.encryption_key: Optional[bytes] = None

class FeedViewProcessor:

    def __init__(self, context: DatasourceMapperContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__process_queue: asyncio.Queue[Optional[ProcessJob]] = asyncio.Queue(maxsize=1)
        self.__workers: list[FeedViewProcessorWorker] = list()

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
        num_workers = 2
        for i in range(0, num_workers):
            self.__workers.append(FeedViewProcessorWorker(self.__context, self.__process_queue, worker_id=str(i)))

    async def add_to_queue(self, message: MessageWrapper):
        job = ProcessJob(message)
        self.__process_queue.put_nowait(job)


class FeedViewProcessorWorker:

    def __init__(self, context: DatasourceMapperContext, work_queue: asyncio.Queue, worker_id: str):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__work_queue = work_queue
        self.__worker_id = worker_id

        self.__current_task: Optional[asyncio.Task] = None
        self.__current_job: Optional[ProcessJob] = None

    async def run(self):
        while self.__context.stopping is False:
            # Get next job
            self.__current_job = await self.__work_queue.get()
            if self.__context.stopping is True:
                return  # Stopping

            # Run processing task
            self.__current_task = asyncio.create_task(self.run_job())
            await self.__current_task
            self.__current_job = None
            self.__current_task = None

    async def run_job(self):
        job = self.__current_job
        if job is None:
            self.__logger.error("No job to process")
            return

        self.__logger.debug(f"{self.__worker_id} Starting job")
        try:
            await self.get_feed_view_information()
        except FeedPreparationException:
            self.__logger.exception("Error preparing feed")
            return

        self.__logger.debug(f"{self.__worker_id} Finishing job")

    async def get_feed_view_information(self):
        job = self.__current_job
        feed_id = job.message.parsed['feed_id']
        feed_view_id = job.message.parsed['feed_view_id']
        producer = await self.__context.get_producer()

        feed_response = await producer.request({'feed_id': feed_id, 'feed_view_id': feed_view_id}, "DataCollector", "getFeedViews", Constantes.SECURITE_PRIVE)
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
            job.view = feed_response.parsed['views'][0]

            # Decrypt the view key
            job.encryption_key_id = job.view['encrypted_data']['cle_id']
            decrypted_keys_message = dechiffrer_reponse(self.__context.signing_key, feed_response.parsed['keys'])
            decrypted_keys = decrypted_keys_message['cles']
            decrypted_key_info = [k for k in decrypted_keys if k['cle_id'] == job.encryption_key_id].pop()
            job.encryption_key_str = decrypted_key_info['cle_secrete_base64']
            job.encryption_key = decode_base64_nopad(job.encryption_key_str)

            # Decrypt feed view information to test key
            decrypted_view = dechiffrer_bytes_secrete(job.encryption_key, job.view['encrypted_data'])
            job.decrypted_view_information = json.loads(decrypted_view)
        except (IndexError, KeyError, ValueError) as e:
            raise FeedPreparationException("Error preparing job", e)

    async def download_feed_data(self):
        """ Downloads all the feed data to a staging aread """

        pass

    async def cancel(self):
        if self.__current_task:
            self.__current_task.cancel()


class FeedDataDownloader:
    """
    Downloads and decrypts feed data into a staging area.
    """

    def __init__(self, context: DatasourceMapperContext):
        self.__context = context

class FeedPreparationException(Exception):
    pass

class FeedDownloadException(Exception):
    pass
