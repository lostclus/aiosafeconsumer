import asyncio
import itertools
import logging
import logging.config
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

_LOGGING = {
    "version": 1,
    "disable_existing_loggers": True,
    "filters": {
        "context_injecting": {
            "()": "aiosafeconsumer.logging.ContextInjectingFilter",
        },
    },
    "formatters": {
        "simple": {
            "()": "aiosafeconsumer.logging.ExtraFieldsFormatter",
            "fmt": "%(worker_type)s[%(worker_id)s]: %(message)s",
        },
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "filters": ["context_injecting"],
        },
    },
    "root": {
        "level": "DEBUG",
        "handlers": ["console"],
        "propagate": True,
    },
    "loggers": {
        "aiosafeconsumer": {
            "level": "DEBUG",
            "handlers": ["console"],
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


class StrSource(DataSource[str]):
    BATCHES = [
        ["one", "two", "three", "fore", "five"],
        ["six", "seven", "eight", "nine", "ten"],
    ]

    is_resource_allocated = False

    async def read(self) -> AsyncGenerator[list[str], None]:
        self.is_resource_allocated = True
        try:
            for batch in itertools.cycle(self.BATCHES):
                yield batch
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
    target_processor_class = IntProcessor
    target_processor_settings = DataProcessorSettings()


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
    source_class = StrSource
    source_settings = DataSourceSettings()
    processor_class = StrProcessor
    processor_settings = DataProcessorSettings()


@dataclass
class StrConsumerWithTansformerSettings(ConsumerWorkerSettings[str]):
    source_class = StrSource
    source_settings = DataSourceSettings()
    processor_class = StrToIntTransformer
    processor_settings = StrToIntTransformerSettings()


@pytest.fixture
def source() -> StrSource:
    return StrSource()


@pytest.fixture
def processor() -> StrProcessor:
    return StrProcessor()


@pytest.fixture
def transformer() -> StrToIntTransformer:
    settings = StrToIntTransformerSettings()
    return StrToIntTransformer(settings)


@pytest.fixture
def consumer() -> ConsumerWorker:
    settings = StrConsumerSettings()
    consumer = ConsumerWorker(settings)
    return consumer


@pytest.fixture
def consumer_with_transformer() -> ConsumerWorker:
    settings = StrConsumerWithTansformerSettings()
    consumer = ConsumerWorker(settings)
    return consumer
