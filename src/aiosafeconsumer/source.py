from abc import ABC
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Generic

from .types import DataType


@dataclass
class DataSourceSettings:
    pass


class DataSource(Generic[DataType], ABC):
    """
    Abstract class to represent source of data.
    """

    def __init__(self, settings: DataSourceSettings | None = None):
        self.settings = settings or DataSourceSettings()

    def __str__(self) -> str:
        return self.__class__.__name__

    async def read(self) -> AsyncGenerator[list[DataType], None]:
        """
        Returns async generator to read batches of items.
        """
        if False:
            batch: list[DataType] = []
            yield batch
        raise NotImplementedError

    async def commit(self, batch: list[DataType]) -> None:
        """
        This method is called from worker just after batch of items was processed.

        """
        pass
