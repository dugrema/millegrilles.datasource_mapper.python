import asyncio
import logging
import json
import gzip

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

    def __init__(self, job: ProcessJob):
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

    def __init__(self, job: ProcessJob):
        super().__init__(job)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    async def process(self):
        self.__logger.debug("Processing data")
        count = 0
        async for item in self.read_data_items():
            pass
            count += 1
        self.__logger.debug(f"Parsed through {count} data items")


def select_data_processor(job: ProcessJob) -> FeedViewDataProcessor:
    feed_type = job.feed['feed_type']
    if feed_type == 'web.scraper.python_custom':
        return FeedViewDataProcessorPythonCustom(job)
    else:
        raise Exception('Feed type not supported')

