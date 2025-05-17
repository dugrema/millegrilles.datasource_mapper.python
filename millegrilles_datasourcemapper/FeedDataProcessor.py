import asyncio
import logging
import json
import gzip

from millegrilles_datasourcemapper.DataParserGoogleTrends import parse_google_trends
from millegrilles_messages.chiffrage.Mgs4 import chiffrer_mgs4_bytes_secrete
from millegrilles_messages.messages import Constantes
from millegrilles_datasourcemapper.Context import DatasourceMapperContext
from millegrilles_datasourcemapper.DataParserUtilities import DatedItemData, hash_to_id, GroupedDatedItemData
from millegrilles_datasourcemapper.FeedViewProcessor import ProcessJob

class FeedDataItem:

    def __init__(self, data: str, files: dict):
        self.data = data
        self.files = files

    @staticmethod
    def from_str(line: str):
        value = json.loads(line)
        data = value['data']
        files = value.get('files')
        return FeedDataItem(data, files)


class FeedViewDataProcessor:

    def __init__(self, context: DatasourceMapperContext, job: ProcessJob):
        self._context = context
        self._job = job

    async def read_data_items(self):
        with gzip.open(self._job.data_file_path, 'rt') as fp:
            while True:
                line = await asyncio.to_thread(fp.readline, 1024 * 1024 * 16)  # Max 16 mb per record
                if len(line) == 0:
                    return
                yield FeedDataItem.from_str(line)

    async def process(self):
        raise NotImplementedError('must implement')

class FeedViewDataProcessorPythonCustom(FeedViewDataProcessor):

    def __init__(self, context: DatasourceMapperContext, job: ProcessJob):
        super().__init__(context, job)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    async def process(self):
        self.__logger.debug("Processing data")
        count_item = 0
        count_sub_item = 0

        batch: list[dict] = list()
        async for data_item in self.read_data_items():
            count_item += 1
            async for parsed_item in parse_google_trends(data_item.data):
                count_sub_item += 1
                prepared_item = await self.produce_data_item(data_item, parsed_item)
                batch.append(prepared_item)
                if len(batch) >= 20:
                    truncate = False
                    if count_item == 1 and self._job.reset:
                        # Truncate on first batch only
                        truncate = True
                    await self.send_batch(batch, truncate)
                    batch.clear()

        if len(batch) > 0:
            await self.send_batch(batch, False)

        self.__logger.info(f"Parsed through {count_item} data items and {count_sub_item} sub-items for feed_view {self._job.view['feed_view_id']}")

    async def produce_data_item(self, feed_item: FeedDataItem, item: DatedItemData):
        job = self._job
        cleartext = item.get_cleartext()
        encrypted_data = chiffrer_mgs4_bytes_secrete(job.encryption_key, json.dumps(cleartext))[1]
        encrypted_data['cle_id'] = job.encryption_key_id

        data_item = {
            'data_id': item.data_id,
            'feed_id': job.feed['feed_id'],
            'feed_view_id': job.view['feed_view_id'],
            'encrypted_data': encrypted_data,
            'pub_date': item.date * 1000,   # Pub date in millisecs
        }

        if isinstance(item, GroupedDatedItemData):
            data_item['group_id'] = hash_to_id(item.group)

        # Check if we need to link a picture
        try:
            picture = [p[0] for p in item.associated_urls.items() if p[1] == 'picture'].pop()
        except (TypeError, IndexError):
            picture = None

        if picture and feed_item.files:
            try:
                picture_info = feed_item.files[picture]
                data_item['files'] = [{
                    'fuuid': picture_info['fuuid'],
                    'decryption': {
                        'cle_id': picture_info['cle_id'],
                        'format': picture_info['format'],
                        'nonce': picture_info['nonce'],
                        'compression': picture_info.get('compression'),
                    },
                }]
            except (IndexError, KeyError):
                pass  # No match

        return data_item

    async def send_batch(self, batch: list, truncate: False):
        producer = await self._context.get_producer()
        command = {
            'feed_view_id': self._job.view['feed_view_id'],
            'feed_id': self._job.feed['feed_id'],
            'data': batch,
            'truncate': truncate,
            'deduplicate': False,
        }
        response = await producer.command(command, 'DataCollector', 'insertViewData', Constantes.SECURITE_PROTEGE)
        if response.parsed['ok'] is not True:
            raise Exception(f'Error saving batch: {response.parsed.get('err')}')


def select_data_processor(context: DatasourceMapperContext, job: ProcessJob) -> FeedViewDataProcessor:
    feed_type = job.feed['feed_type']
    if feed_type == 'web.scraper.python_custom':
        return FeedViewDataProcessorPythonCustom(context, job)
    else:
        raise Exception('Feed type not supported')
