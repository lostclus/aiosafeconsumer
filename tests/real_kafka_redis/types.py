from typing import NamedTuple

from aiosafeconsumer.datasync import EventType


class UserRecord(NamedTuple):
    ev_time: str
    ev_type: EventType
    ev_source: str
    id: int
    email: str
    is_active: bool


class UserDeleteRecord(NamedTuple):
    ev_time: str
    ev_type: EventType
    ev_source: str
    id: int


class UserEnumerateRecord(NamedTuple):
    ev_time: str
    ev_type: EventType
    ev_source: str
    ids: list[int]


User = UserRecord | UserDeleteRecord | UserEnumerateRecord
