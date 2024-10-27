from datetime import datetime
from decimal import Decimal
from typing import NamedTuple

from aiosafeconsumer.datasync import EventType


class UserRecord(NamedTuple):
    ev_time: datetime
    ev_type: EventType
    ev_source: str
    id: int
    email: str
    score: Decimal
    is_active: bool


class UserDeleteRecord(NamedTuple):
    ev_time: datetime
    ev_type: EventType
    ev_source: str
    id: int


class UserEnumerateRecord(NamedTuple):
    ev_time: datetime
    ev_type: EventType
    ev_source: str
    ids: list[int]


class UserEOSRecord(NamedTuple):
    ev_time: datetime
    ev_type: EventType
    ev_source: str


User = UserRecord | UserDeleteRecord | UserEnumerateRecord | UserEOSRecord
