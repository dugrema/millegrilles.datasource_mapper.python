import asyncio
import logging

from asyncio import TaskGroup
from typing import Optional, TypedDict

from millegrilles_datasourcemapper.FeedViewProcessor import FeedViewProcessor
from millegrilles_datasourcemapper.Context import DatasourceMapperContext
from millegrilles_messages.messages.MessagesModule import MessageWrapper


class DecryptedKeyDict(TypedDict):
    cle_id: str
    cle_secrete_base64: str


class DecryptedKey:

    def __init__(self, key_id: str, secret_key: bytes):
        self.key_id = key_id
        self.secret_key = secret_key


class DatasourceManager:

    def __init__(self, context: DatasourceMapperContext, feed_view_processor: FeedViewProcessor):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__feed_view_processor = feed_view_processor

        # Feed semaphore to limit the number of scrapers running at the same time
        self.__feed_semaphore = asyncio.BoundedSemaphore(1)

        self.__group: Optional[TaskGroup] = None

    async def run(self):
        async with TaskGroup() as group:
            self.__group = group
            group.create_task(self.__maintain_datasources_thread())

    async def __maintain_datasources_thread(self):
        while self.__context.stopping is False:
            await self.__maintain_staging()
            await self.__context.wait(300)

    async def __maintain_staging(self):
        self.__logger.warning("TODO - maintain staging")

    async def process_feed_view(self, message: MessageWrapper, reset_staging=False):
        try:
            await self.__feed_view_processor.add_to_queue(message, reset_staging=reset_staging)
            return {'ok': True}
        except asyncio.QueueFull:
            return {'ok': False, 'code': 1, 'err': 'Processing queue full'}
