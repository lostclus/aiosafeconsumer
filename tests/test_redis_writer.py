import json
from dataclasses import dataclass
from datetime import timedelta
from typing import NamedTuple, cast
from unittest import mock

import pytest
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline

from aiosafeconsumer.datasync import EnumerateIDsRecord, EventType, ObjectID, Version
from aiosafeconsumer.datasync.redis import RedisWriter, RedisWriterSettings


class ItemRecord(NamedTuple):
    ev_type: EventType
    version: int
    id: int
    data: str


class ItemDeleteRecord(NamedTuple):
    ev_type: EventType
    version: int
    id: int


class ItemEnumerateRecord(NamedTuple):
    ev_type: EventType
    version: int
    ids: list[int]


Item = ItemRecord | ItemDeleteRecord | ItemEnumerateRecord


@dataclass
class Settings(RedisWriterSettings[Item]):
    pass


class Writer(RedisWriter[Item]):
    settings: Settings


@pytest.fixture
def redis_mock() -> Redis:
    versions: dict[bytes, bytes] = {}
    db: dict[bytes, bytes] = {}
    expire_db: dict[bytes, timedelta] = {}

    async def hgetall(key: bytes) -> dict[bytes, bytes]:
        if key == b"items":
            return versions.copy()
        return {}

    def hdel(name: bytes, *keys: bytes) -> None:
        if name == b"items":
            for k in keys:
                versions.pop(k, None)

    def hset(name: bytes, mapping: dict[bytes, bytes]) -> None:
        if name == b"items":
            versions.update(mapping)

    def mset(mapping: dict[bytes, bytes]) -> None:
        db.update(mapping)

    def delete(*keys: bytes) -> None:
        for key in keys:
            db.pop(key, None)

    def expire(key: bytes, ex: timedelta) -> None:
        expire_db[key] = ex

    redis = mock.MagicMock(spec=Redis)
    redis.hgetall.side_effect = hgetall

    pipe = mock.MagicMock(spec=Pipeline)
    redis.pipeline.return_value.__aenter__.return_value = pipe

    pipe.delete.side_effect = delete
    pipe.expire.side_effect = expire
    pipe.hdel.side_effect = hdel
    pipe.hset.side_effect = hset
    pipe.mset.side_effect = mset

    redis._test_versions = versions
    redis._test_db = db
    redis._test_expire_db = expire_db

    return redis


@pytest.fixture
def settings(redis_mock: Redis) -> Settings:
    ENCODING = "utf-8"

    def get_redis() -> Redis:
        return redis_mock

    def version_getter(item: Item) -> Version:
        return item.version

    def record_serializer(item: Item) -> bytes:
        return json.dumps(item._asdict()).encode(ENCODING)

    def event_type_getter(item: Item) -> EventType:
        return item.ev_type

    def id_getter(item: Item) -> ObjectID:
        assert isinstance(item, ItemRecord) or isinstance(item, ItemDeleteRecord)
        return item.id

    def enum_getter(item: Item) -> EnumerateIDsRecord:
        assert isinstance(item, ItemEnumerateRecord)
        return EnumerateIDsRecord(ids=cast(list[ObjectID], item.ids))

    def version_serializer(ver: Version) -> bytes:
        return str(ver).encode()

    def version_deserializer(val: bytes) -> Version:
        return int(val.decode())

    settings = Settings(
        redis=get_redis,
        version_getter=version_getter,
        version_serializer=version_serializer,
        version_deserializer=version_deserializer,
        record_serializer=record_serializer,
        key_prefix="item:",
        versions_key="items",
        event_type_getter=event_type_getter,
        id_getter=id_getter,
        enum_getter=enum_getter,
    )
    return settings


@pytest.mark.asyncio
async def test_redis_writer_initial_empty(
    redis_mock: Redis, settings: Settings
) -> None:
    items: list[Item] = [
        ItemRecord(ev_type=EventType.CREATE, version=1, id=1, data="1v1"),
        ItemRecord(ev_type=EventType.UPDATE, version=2, id=2, data="2v2"),
        ItemRecord(ev_type=EventType.REFRESH, version=1, id=3, data="3v1"),
    ]

    writer = Writer(settings)
    await writer.process(items)

    assert redis_mock._test_versions == {  # type: ignore
        b"1": b"1",
        b"2": b"2",
        b"3": b"1",
    }
    assert redis_mock._test_db == {  # type: ignore
        b"item:1": b'{"ev_type": "create", "version": 1, "id": 1, "data": "1v1"}',
        b"item:2": b'{"ev_type": "update", "version": 2, "id": 2, "data": "2v2"}',
        b"item:3": b'{"ev_type": "refresh", "version": 1, "id": 3, "data": "3v1"}',
    }
    assert redis_mock._test_expire_db == {  # type: ignore
        b"item:1": timedelta(hours=30),
        b"item:2": timedelta(hours=30),
        b"item:3": timedelta(hours=30),
        b"items": timedelta(hours=24),
    }


@pytest.mark.asyncio
async def test_redis_writer_upsert(redis_mock: Redis, settings: Settings) -> None:
    redis_mock._test_versions.update(  # type: ignore
        {
            b"1": b"0",
            b"2": b"2",
            b"3": b"1",
            b"4": b"0",
        },
    )
    redis_mock._test_db.update(  # type:ignore
        {
            b"item:1": b"",
            b"item:2": b"",
            b"item:3": b"",
            b"item:4": b"",
        },
    )

    items: list[Item] = [
        ItemRecord(ev_type=EventType.CREATE, version=1, id=1, data="1v1"),
        ItemRecord(ev_type=EventType.UPDATE, version=2, id=2, data="2v2"),
        ItemRecord(ev_type=EventType.REFRESH, version=2, id=3, data="3v2"),
        ItemRecord(ev_type=EventType.DELETE, version=1, id=4, data=""),
    ]

    writer = Writer(settings)
    await writer.process(items)

    assert redis_mock._test_versions == {  # type: ignore
        b"1": b"1",
        b"2": b"2",
        b"3": b"2",
    }
    assert redis_mock._test_db == {  # type: ignore
        b"item:1": b'{"ev_type": "create", "version": 1, "id": 1, "data": "1v1"}',
        b"item:2": b"",
        b"item:3": b'{"ev_type": "refresh", "version": 2, "id": 3, "data": "3v2"}',
    }
    assert redis_mock._test_expire_db == {  # type: ignore
        b"item:1": timedelta(hours=30),
        b"item:3": timedelta(hours=30),
        b"items": timedelta(hours=24),
    }


@pytest.mark.asyncio
async def test_redis_writer_enumerate(redis_mock: Redis, settings: Settings) -> None:
    redis_mock._test_versions.update(  # type: ignore
        {
            b"1": b"0",
            b"2": b"2",
            b"3": b"1",
            b"4": b"0",
        },
    )
    redis_mock._test_db.update(  # type:ignore
        {
            b"item:1": b"",
            b"item:2": b"",
            b"item:3": b"",
            b"item:4": b"",
        },
    )

    items: list[Item] = [
        ItemEnumerateRecord(ev_type=EventType.ENUMERATE, version=1, ids=[1, 3]),
    ]

    writer = Writer(settings)
    await writer.process(items)

    assert redis_mock._test_versions == {  # type: ignore
        b"1": b"0",
        b"3": b"1",
    }
    assert redis_mock._test_db == {  # type: ignore
        b"item:1": b"",
        b"item:3": b"",
    }
    assert redis_mock._test_expire_db == {  # type: ignore
        b"items": timedelta(hours=24),
    }
