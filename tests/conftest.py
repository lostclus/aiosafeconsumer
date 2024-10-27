import asyncio
import itertools
import logging
import logging.config
import os
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest

from aiosafeconsumer import (
    ConsumerWorker,
    ConsumerWorkerSettings,
    DataProcessor,
    DataProcessorSettings,
    DataSource,
    DataSourceSettings,
    DataTransformer,
    DataTransformerSettings,
)

if not os.getenv("KAFKA_BOOTSTRAP_SERVERS") and not os.getenv("REDIS_URL"):
    collect_ignore = ["real/kafka_redis"]


_LOGGING = {
    "version": 1,
    "disable_existing_loggers": True,
    "filters": {
        "context_injecting": {
            "()": "aiosafeconsumer.logging.ContextInjectingFilter",
        },
        "is_worker_context": {
            "()": "aiosafeconsumer.logging.IsWorkerContextFilter",
        },
        "is_not_worker_context": {
            "()": "aiosafeconsumer.logging.IsWorkerContextFilter",
            "invert": True,
        },
    },
    "formatters": {
        "common": {
            "format": "[%(levelname)s/%(name)s] %(message)s",
        },
        "worker": {
            "()": "aiosafeconsumer.logging.ExtraFieldsFormatter",
            "fmt": "[%(levelname)s/%(worker_type)s-%(worker_id)s] %(message)s",
        },
    },
    "handlers": {
        "common": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "common",
            "filters": ["is_not_worker_context"],
        },
        "worker": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "worker",
            "filters": ["is_worker_context", "context_injecting"],
        },
    },
    "loggers": {
        "aiosafeconsumer": {
            "level": "DEBUG",
            "handlers": ["common", "worker"],
            "propagate": True,
        },
    },
}


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("aiosafeconsumer")
    group.addoption(
        "--with-custom-logging",
        action="store_true",
        dest="with_custom_logging",
        default=False,
        help="Use ContextInjectingFilter and ExtraFieldsFormatter",
    )


def pytest_configure(config: pytest.Config) -> None:
    with_custom_logging = config.getoption("with_custom_logging")
    if with_custom_logging:
        logging.config.dictConfig(_LOGGING)


class StrSourceSettings(DataSourceSettings):
    stop_on: int | None = None


class StrSource(DataSource[str]):
    settings: StrSourceSettings

    BATCHES = [
        ["one", "two", "three", "fore", "five"],
        ["six", "seven", "eight", "nine", "ten"],
    ]

    is_resource_allocated = False

    async def read(self) -> AsyncGenerator[list[str], None]:
        self.is_resource_allocated = True
        count = 0
        try:
            for batch in itertools.cycle(self.BATCHES):
                yield batch
                count += 1
                if self.settings.stop_on is not None and count == self.settings.stop_on:
                    break
        finally:
            self.is_resource_allocated = False


class StrProcessor(DataProcessor[str]):
    storage: list[str]

    def __init__(self, settings: DataProcessorSettings | None = None) -> None:
        super().__init__(settings)
        self.storage = []

    async def process(self, batch: list[str]) -> None:
        self.storage.extend(batch)
        await asyncio.sleep(0.1)
        if len(self.storage) >= 1000:
            raise Exception("Too many data")


class IntProcessor(DataProcessor[int]):
    storage: list[int]

    def __init__(self, settings: DataProcessorSettings | None = None) -> None:
        super().__init__(settings)
        self.storage = []

    async def process(self, batch: list[int]) -> None:
        self.storage.extend(batch)
        await asyncio.sleep(0.1)
        if len(self.storage) >= 1000:
            raise Exception("Too many data")


@dataclass
class StrToIntTransformerSettings(DataTransformerSettings[str, int]):
    pass


class StrToIntTransformer(DataTransformer[str, int]):
    _MAPPING = {
        "one": 1,
        "two": 2,
        "three": 3,
        "fore": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
    }

    async def transform(self, batch: list[str]) -> list[int]:
        return [self._MAPPING[x] for x in batch]


@dataclass
class StrConsumerSettings(ConsumerWorkerSettings[str]):
    pass


@dataclass
class StrConsumerWithTansformerSettings(ConsumerWorkerSettings[str]):
    pass


@pytest.fixture
def source_settings() -> StrSourceSettings:
    return StrSourceSettings()


@pytest.fixture
def source(source_settings: StrSourceSettings) -> StrSource:
    return StrSource(source_settings)


@pytest.fixture
def consumer_settings(source_settings: StrSourceSettings) -> StrConsumerSettings:
    return StrConsumerSettings(
        source_class=StrSource,
        source_settings=source_settings,
        processor_class=StrProcessor,
        processor_settings=DataProcessorSettings(),
    )


@pytest.fixture
def transformer_settings() -> StrToIntTransformerSettings:
    return StrToIntTransformerSettings(
        target_processor_class=IntProcessor,
        target_processor_settings=DataProcessorSettings(),
    )


@pytest.fixture
def consumer_with_transformer_settings(
    source_settings: StrSourceSettings,
    transformer_settings: StrToIntTransformerSettings,
) -> StrConsumerWithTansformerSettings:
    return StrConsumerWithTansformerSettings(
        source_class=StrSource,
        source_settings=source_settings,
        processor_class=StrToIntTransformer,
        processor_settings=transformer_settings,
    )


@pytest.fixture
def processor() -> StrProcessor:
    return StrProcessor()


@pytest.fixture
def transformer(
    transformer_settings: StrToIntTransformerSettings,
) -> StrToIntTransformer:
    return StrToIntTransformer(transformer_settings)


@pytest.fixture
def consumer(consumer_settings: StrConsumerSettings) -> ConsumerWorker:
    consumer = ConsumerWorker(consumer_settings)
    return consumer


@pytest.fixture
def consumer_with_transformer(
    consumer_with_transformer_settings: StrConsumerWithTansformerSettings,
) -> ConsumerWorker:
    consumer = ConsumerWorker(consumer_with_transformer_settings)
    return consumer
