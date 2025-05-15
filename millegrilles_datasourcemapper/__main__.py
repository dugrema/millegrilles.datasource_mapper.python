import asyncio
import logging
from asyncio import TaskGroup
from collections.abc import Awaitable
from concurrent.futures.thread import ThreadPoolExecutor

from millegrilles_datasourcemapper.BusMessageHandler import BusMessageHandler
from millegrilles_datasourcemapper.DataSourceManager import DatasourceManager
from millegrilles_datasourcemapper.FeedViewProcessor import FeedViewProcessor
from millegrilles_messages.bus.BusContext import StopListener, ForceTerminateExecution
from millegrilles_messages.bus.PikaConnector import MilleGrillesPikaConnector

from millegrilles_datasourcemapper.Configuration import DatasourceMapperConfiguration
from millegrilles_datasourcemapper.Context import DatasourceMapperContext
from millegrilles_datasourcemapper.AttachedFileHelper import AttachedFileHelper

LOGGER = logging.getLogger(__name__)


async def force_terminate_task_group():
    """Used to force termination of a task group."""
    raise ForceTerminateExecution()


async def main():
    config = DatasourceMapperConfiguration.load()
    context = DatasourceMapperContext(config)

    LOGGER.setLevel(logging.INFO)
    LOGGER.info("Starting")

    # Wire classes together, gets awaitables to run
    coros = await wiring(context)

    try:
        # Use taskgroup to run all threads
        async with TaskGroup() as group:
            for coro in coros:
                group.create_task(coro)

            # Create a listener that fires a task to cancel all other tasks
            async def stop_group():
                group.create_task(force_terminate_task_group())
            stop_listener = StopListener(stop_group)
            context.register_stop_listener(stop_listener)

    except* (ForceTerminateExecution, asyncio.CancelledError):
        pass  # Result of the termination task


async def wiring(context: DatasourceMapperContext) -> list[Awaitable]:
    # Some threads get used to handle sync events for the duration of the execution. Ensure there are enough.
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=10))

    # Create instances
    bus_connector = MilleGrillesPikaConnector(context)
    context.bus_connector = bus_connector
    feed_view_processor = FeedViewProcessor(context)
    feed_manager = DatasourceManager(context, feed_view_processor)
    bus_handler = BusMessageHandler(context, feed_manager)
    attached_file_helper = AttachedFileHelper(context)

    # Additional wiring
    context.file_handler = attached_file_helper

    # Setup
    await bus_handler.setup()
    await feed_view_processor.setup()

    # Create tasks
    coros = [
        context.run(),
        bus_connector.run(),
        feed_manager.run(),
        attached_file_helper.run(),
        feed_view_processor.run(),
    ]

    return coros


if __name__ == '__main__':
    asyncio.run(main())
    LOGGER.info("Stopped")
