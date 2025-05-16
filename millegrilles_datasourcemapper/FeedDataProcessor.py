import asyncio
import logging
import json
import gzip

from millegrilles_messages.messages import Constantes
from millegrilles_datasourcemapper.Context import DatasourceMapperContext
from millegrilles_datasourcemapper.DataParserSamples import parse_google_trends, ScrapedGoogleTrendsNewsItem
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
        count = 0

        batch: list[dict] = list()
        async for data_item in self.read_data_items():
            count += 1
            async for parsed_item in parse_google_trends(data_item.data):
                prepared_item = parsed_item.produce_data(self._job, data_item.files)
                batch.append(prepared_item)
                if len(batch) >= 20:
                    truncate = False
                    if count == 1 and self._job.reset:
                        # Truncate on first batch only
                        truncate = True
                    await self.send_batch(batch, truncate)
                    batch.clear()

        if len(batch) > 0:
            await self.send_batch(batch, False)

        self.__logger.debug(f"Parsed through {count} data items")

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
