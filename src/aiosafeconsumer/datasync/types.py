from datetime import datetime
from enum import Enum
from typing import NamedTuple, TypeAlias


class EventType(str, Enum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    REFRESH = "refresh"
    ENUMERATE = "enumerate"


Version: TypeAlias = int | str | datetime
ObjectID: TypeAlias = int | str


class EnumerateIDsChunk(NamedTuple):
    chunk_index: int
    total_chunks: int
    session: str


class EnumerateIDsRecord(NamedTuple):
    ids: list[ObjectID]
    chunk: EnumerateIDsChunk | None = None
