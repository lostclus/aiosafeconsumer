from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from aiosafeconsumer.datasync import EventType

from .deserializers import json_to_namedtuple_deserializer
from .types import UserDeleteRecord, UserEnumerateRecord, UserEOSRecord, UserRecord


@pytest.fixture()
def deserializer() -> Callable:
    return json_to_namedtuple_deserializer(
        record_class=UserRecord,
        delete_record_class=UserDeleteRecord,
        enumerate_record_class=UserEnumerateRecord,
        eos_record_class=UserEOSRecord,
    )


def test_json_to_namedtuple_deserializer_user_record(deserializer: Callable) -> None:
    record = deserializer(
        b'{"_time": "2024-01-01T00:00:00+00:00", "_type": "create",'
        b' "_source": "test", "id": 1, "email": "test@example.com",'
        b' "score": "1.23", "is_active": true}'
    )

    assert record == UserRecord(
        ev_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ev_type=EventType.CREATE,
        ev_source="test",
        id=1,
        email="test@example.com",
        score=Decimal("1.23"),
        is_active=True,
    )


def test_json_to_namedtuple_deserializer_user_delete_record(
    deserializer: Callable,
) -> None:
    record = deserializer(
        b'{"_time": "2024-01-01T00:00:00+00:00", "_type": "delete",'
        b' "_source": "test", "id": 1}'
    )

    assert record == UserDeleteRecord(
        ev_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ev_type=EventType.DELETE,
        ev_source="test",
        id=1,
    )


def test_json_to_namedtuple_deserializer_user_enumerate_record(
    deserializer: Callable,
) -> None:
    record = deserializer(
        b'{"_time": "2024-01-01T00:00:00+00:00", "_type": "enumerate",'
        b' "_source": "test", "ids": [1, 2, 3]}'
    )

    assert record == UserEnumerateRecord(
        ev_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ev_type=EventType.ENUMERATE,
        ev_source="test",
        ids=[1, 2, 3],
    )


def test_json_to_namedtuple_deserializer_user_eos_record(
    deserializer: Callable,
) -> None:
    record = deserializer(
        b'{"_time": "2024-01-01T00:00:00+00:00", "_type": "eos", "_source": "test"}'
    )

    assert record == UserEOSRecord(
        ev_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ev_type=EventType.EOS,
        ev_source="test",
    )
