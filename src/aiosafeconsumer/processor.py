import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic

from .types import DataType, TargetDataType

log = logging.getLogger(__name__)


@dataclass
class DataProcessorSettings:
    pass


class DataProcessor(Generic[DataType], ABC):
    def __init__(self, settings: DataProcessorSettings | None = None):
        self.settings = settings or DataProcessorSettings()

    def __str__(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    async def process(self, batch: list[DataType]) -> None:  # pragma: no cover
        pass


@dataclass
class DataTransformerSettings(Generic[DataType, TargetDataType], DataProcessorSettings):
    target_processor_class: type[DataProcessor[TargetDataType]]
    target_processor_settings: DataProcessorSettings


class DataTransformer(Generic[DataType, TargetDataType], DataProcessor[DataType], ABC):
    def __init__(self, settings: DataTransformerSettings) -> None:
        super().__init__(settings=settings)
        self.target_processor = settings.target_processor_class(
            settings.target_processor_settings
        )

    async def process(self, batch: list[DataType]) -> None:
        new_batch = await self.transform(batch)
        log.info(
            f"{len(batch)} items was transformed by {self} to {len(new_batch)} items"
        )
        await self.target_processor.process(new_batch)

    @abstractmethod
    async def transform(
        self, batch: list[DataType]
    ) -> list[TargetDataType]:  # pragma: no cover
        pass
