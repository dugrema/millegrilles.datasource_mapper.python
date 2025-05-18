import asyncio
import logging
import json
import gzip

from typing import AsyncIterable, Optional

from millegrilles_datasourcemapper.mappers.WIPMapper import parse as wip_parser
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
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
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
        self.__logger.debug("Processing data")
        count_item = 0
        count_sub_item = 0

        # Truncate on first batch only
        truncate = self._job.reset

        batch: list[dict] = list()
        async for data_item in self.read_data_items():
            count_item += 1
            async for parsed_item in self.parse_data_items(data_item.data):
                count_sub_item += 1
                prepared_item = await self.produce_data_item(data_item, parsed_item)
                batch.append(prepared_item)
                if len(batch) >= 20:
                    await self.send_batch(batch, truncate)
                    truncate = False  # Reset truncation to keep batches
                    batch.clear()

        if len(batch) > 0:
            await self.send_batch(batch, truncate)

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
        except (AttributeError, TypeError, IndexError):
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

    async def send_batch(self, batch: list[DatedItemData], truncate: False):
        # Detect the type of data
        item = batch[0]
        action = 'insertViewData'
        # if isinstance(item, GroupedDatedItemData):
        #     action = 'insertViewGroupedDated'
        # elif isinstance(item, DatedItemData):
        #     action = 'insertViewDated'
        # else:
        #     raise TypeError("Data type %s not supported" % item.__class__.__name__)

        producer = await self._context.get_producer()
        command = {
            'feed_view_id': self._job.view['feed_view_id'],
            'feed_id': self._job.feed['feed_id'],
            'data': batch,
            'truncate': truncate,
            'deduplicate': False,
        }
        response = await producer.command(command, 'DataCollector', action, Constantes.SECURITE_PROTEGE)
        if response.parsed['ok'] is not True:
            raise Exception(f'Error saving batch: {response.parsed.get('err')}')

    async def parse_data_items(self, feed_data_item: str) -> AsyncIterable[DatedItemData]:
        raise NotImplementedError('must implement')


class FeedViewDataProcessorWIP(FeedViewDataProcessor):

    def __init__(self, context: DatasourceMapperContext, job: ProcessJob):
        super().__init__(context, job)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    async def parse_data_items(self, feed_data_item: str) -> AsyncIterable[DatedItemData]:
        async for item in wip_parser(feed_data_item):
            yield item


class FeedViewDataProcessorPythonCustom(FeedViewDataProcessor):

    def __init__(self, context: DatasourceMapperContext, job: ProcessJob):
        super().__init__(context, job)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__processing_method: Optional = None

    async def parse_data_items(self, feed_data_item: str):
        custom_process: str = self._job.view['mapping_code']

        try:
            self.__processing_method = compile(custom_process, '<string>', 'exec')
        except KeyError:
            self.__processing_method = None
        except Exception as e:
            self.__logger.exception("Error parsing custom process")
            self.__processing_method = None
            raise e

        values = {}  # Local context
        exec(self.__processing_method, values)
        async for item in  values['parse'](feed_data_item):
            yield item


def select_data_processor(context: DatasourceMapperContext, job: ProcessJob) -> FeedViewDataProcessor:
    feed_type = job.feed['feed_type']

    if feed_type == 'web.scraper.python_custom':
        mapping_code = job.view.get('mapping_code')
        if mapping_code is not None and mapping_code != '':
            return FeedViewDataProcessorPythonCustom(context, job)
        else:
            return FeedViewDataProcessorWIP(context, job)
    else:
        raise Exception('Feed type not supported')
