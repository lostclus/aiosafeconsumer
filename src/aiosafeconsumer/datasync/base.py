from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic

from ..processor import DataProcessor, DataProcessorSettings
from ..types import DataType
from .types import EnumerateIDsRecord, EventType, ObjectID, Version


@dataclass
class DataWriterSettings(Generic[DataType], DataProcessorSettings):
    version_getter: Callable[[DataType], Version]
    event_type_getter: Callable[[DataType], EventType]
    id_getter: Callable[[DataType], ObjectID]
    enum_getter: Callable[[DataType], EnumerateIDsRecord]


class DataWriter(Generic[DataType], DataProcessor[DataType], ABC):
    settings: DataWriterSettings
