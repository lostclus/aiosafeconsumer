import asyncio
import logging
import time
from typing import Generic

from .processor import DataProcessor, DataProcessorSettings
from .source import DataSource, DataSourceSettings
from .types import DataType
from .worker import Worker, WorkerSettings

log = logging.getLogger(__name__)


class ConsumerWorkerSettings(Generic[DataType], WorkerSettings):
    source_class: type[DataSource[DataType]]
    source_settings: DataSourceSettings
    processor_class: type[DataProcessor[DataType]]
    processor_settings: DataProcessorSettings


class ConsumerWorker(Worker):
    settings: ConsumerWorkerSettings

    def __init__(self, settings: ConsumerWorkerSettings):
        super().__init__(settings)
        self.source = self.settings.source_class(self.settings.source_settings)
        self.processor = self.settings.processor_class(self.settings.processor_settings)

    async def run(self, burst: bool = False) -> None:
        self.setup_logging_context()

        log.debug(f"Enter {self}")
        generator = self.source.read()
        try:
            while True:
                try:
                    batch = await anext(generator)
                except StopAsyncIteration:
                    log.debug(f"Source {self.source} generator exist")
                    break
                log.info(f"Got batch of {len(batch)} items from {self.source}")

                start_time = time.monotonic()
                await self.processor.process(batch)
                total_time = time.monotonic() - start_time

                log.info(
                    f"{len(batch)} items was processed by {self.processor} in "
                    f"{total_time:.03f} seconds",
                    extra={
                        "items_processed": len(batch),
                        "processing_time": total_time,
                    },
                )

                await self.source.commit(batch)
                await asyncio.sleep(0)

                if burst:
                    log.debug("Performed one cycle in burst mode")
                    break
        finally:
            log.debug(f"Exit {self}")
            await generator.aclose()
