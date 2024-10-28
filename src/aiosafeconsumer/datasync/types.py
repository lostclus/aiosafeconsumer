from datetime import datetime
from enum import Enum
from typing import NamedTuple, TypeAlias


class EventType(str, Enum):
    CREATE = "create"  # object was createed
    UPDATE = "update"  # object was updated
    DELETE = "delete"  # object was deleted
    REFRESH = "refresh"  # nothing happened to the object, just refresh data
    ENUMERATE = "enumerate"  # enumerate IDs of existing objects
    EOS = "eos"  # end of stream


Version: TypeAlias = int | datetime
ObjectID: TypeAlias = int | str


class EnumerateIDsChunk(NamedTuple):
    chunk_index: int
    total_chunks: int
    session: str


class EnumerateIDsRecord(NamedTuple):
    ids: list[ObjectID]
    chunk: EnumerateIDsChunk | None = None
