import asyncio
import logging

from typing import Optional

from millegrilles_datasourcemapper.Context import DatasourceMapperContext
from millegrilles_messages.messages.MessagesModule import MessageWrapper


class ProcessJob:

    def __init__(self, message: MessageWrapper):
        self.message = message


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

    async def run(self):
        while self.__context.stopping is False:
            # Get next job
            job = await self.__work_queue.get()
            if self.__context.stopping is True:
                return  # Stopping

            # Run processing task
            self.__current_task = asyncio.create_task(self.run_job(job))
            await self.__current_task
            self.__current_task = None

    async def run_job(self, job: ProcessJob):
        self.__logger.debug(f"{self.__worker_id} Starting job")
        await asyncio.sleep(5)
        self.__logger.debug(f"{self.__worker_id} Finishing job")

    async def cancel(self):
        if self.__current_task:
            self.__current_task.cancel()


class FeedDataDownloader:
    """
    Downloads and decrypts feed data into a staging area.
    """

    def __init__(self, context: DatasourceMapperContext):
        self.__context = context

