import logging

from millegrilles_datasourcemapper.FeedViewProcessor import ProcessJob


class FeedViewDataProcessor:

    def __init__(self, job: ProcessJob):
        self._job = job

    async def process(self):
        raise NotImplementedError('must implement')

class FeedViewDataProcessorPythonCustom(FeedViewDataProcessor):

    def __init__(self, job: ProcessJob):
        super().__init__(job)
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    async def process(self):
        self.__logger.debug("Processing data")


def select_data_processor(job: ProcessJob) -> FeedViewDataProcessor:
    feed_type = job.feed['feed_type']
    if feed_type == 'web.scraper.python_custom':
        return FeedViewDataProcessorPythonCustom(job)
    else:
        Exception('Feed type not supported')

