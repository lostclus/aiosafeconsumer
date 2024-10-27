from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from aiosafeconsumer.datasync import EventType

from .serializers import namedtuple_to_json_serializer
from .types import UserDeleteRecord, UserEnumerateRecord, UserEOSRecord, UserRecord


@pytest.fixture()
def serializer() -> Callable:
    return namedtuple_to_json_serializer()


def test_namedtuple_to_json_serializer_user_record(serializer: Callable) -> None:
    record = UserRecord(
        ev_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ev_type=EventType.CREATE,
        ev_source="test",
        id=1,
        email="test@example.com",
        score=Decimal("1.23"),
        is_active=True,
    )

    value = serializer(record)

    assert value == (
        b'{"_time": "2024-01-01T00:00:00+00:00", "_type": "create",'
        b' "_source": "test", "id": 1, "email": "test@example.com",'
        b' "score": "1.23", "is_active": true}'
    )


def test_namedtuple_to_json_serializer_user_delete_record(serializer: Callable) -> None:
    record = UserDeleteRecord(
        ev_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ev_type=EventType.DELETE,
        ev_source="test",
        id=1,
    )

    value = serializer(record)

    assert value == (
        b'{"_time": "2024-01-01T00:00:00+00:00", "_type": "delete",'
        b' "_source": "test", "id": 1}'
    )


def test_namedtuple_to_json_serializer_user_enumerate_record(
    serializer: Callable,
) -> None:
    record = UserEnumerateRecord(
        ev_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ev_type=EventType.ENUMERATE,
        ev_source="test",
        ids=[1, 2, 3],
    )

    value = serializer(record)

    assert value == (
        b'{"_time": "2024-01-01T00:00:00+00:00", "_type": "enumerate",'
        b' "_source": "test", "ids": [1, 2, 3],'
        b' "chunk_session": null, "chunk_index": null, "total_chunks": null}'
    )


def test_namedtuple_to_json_serializer_user_eos_record(
    serializer: Callable,
) -> None:
    record = UserEOSRecord(
        ev_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ev_type=EventType.EOS,
        ev_source="test",
    )

    value = serializer(record)

    assert value == (
        b'{"_time": "2024-01-01T00:00:00+00:00", "_type": "eos", "_source": "test"}'
    )
