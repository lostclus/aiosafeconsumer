import json
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import timedelta
from fnmatch import fnmatch
from typing import NamedTuple, cast
from unittest import mock

import pytest
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline

from aiosafeconsumer.datasync import (
    EnumerateIDsChunk,
    EnumerateIDsRecord,
    EventType,
    ObjectID,
    Version,
)
from aiosafeconsumer.datasync.redis import RedisWriter, RedisWriterSettings

KEY_PREFIX = "item:"
VERSIONS_KEY = "items"
ENUM_CHUNKS_PREFIX = "items_chunk:"


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
    chunk_session: str | None = None
    chunk_index: int | None = None
    total_chunks: int | None = None


class ItemEOSRecord(NamedTuple):
    ev_type: EventType
    version: int


Item = ItemRecord | ItemDeleteRecord | ItemEnumerateRecord | ItemEOSRecord


@dataclass
class Settings(RedisWriterSettings[Item]):
    pass


class Writer(RedisWriter[Item]):
    settings: Settings


@pytest.fixture
def redis_mock() -> Redis:
    versions: dict[bytes, bytes] = {}
    objects: dict[bytes, bytes] = {}
    expire: dict[bytes, timedelta] = {}
    enum_chunks: dict[bytes, bytes] = {}

    async def hgetall(key: bytes) -> dict[bytes, bytes]:
        if key == VERSIONS_KEY.encode():
            return versions.copy()
        return {}

    def hdel(name: bytes, *keys: bytes) -> None:
        if name == VERSIONS_KEY.encode():
            for k in keys:
                versions.pop(k, None)

    def hset(name: bytes, mapping: dict[bytes, bytes]) -> None:
        if name == VERSIONS_KEY.encode():
            versions.update(mapping)

    def mset(mapping: dict[bytes, bytes]) -> None:
        new_objects: dict[bytes, bytes] = {}
        new_enum_chunks: dict[bytes, bytes] = {}

        for key, value in mapping.items():
            if key.decode().startswith(ENUM_CHUNKS_PREFIX):
                new_enum_chunks[key] = value
            else:
                new_objects[key] = value
        objects.update(new_objects)
        enum_chunks.update(new_enum_chunks)

    def delete(*keys: bytes) -> None:
        for key in keys:
            key_str = key.decode()
            if key_str.startswith(ENUM_CHUNKS_PREFIX):
                enum_chunks.pop(key, None)
            else:
                objects.pop(key, None)
            expire.pop(key, None)

    def expire_(key: bytes, ex: timedelta) -> None:
        expire[key] = ex

    async def get(key: bytes) -> bytes | None:
        if key.decode().startswith(ENUM_CHUNKS_PREFIX):
            return enum_chunks.get(key)
        return objects.get(key)

    async def set_(key: bytes, value: bytes, ex: timedelta | None = None) -> None:
        if key.decode().startswith(ENUM_CHUNKS_PREFIX):
            enum_chunks[key] = value
        else:
            objects[key] = value
        if ex:
            expire[key] = ex

    async def scan_iter(match: bytes) -> AsyncGenerator[bytes]:
        match_str = match.decode()
        if match_str.startswith(ENUM_CHUNKS_PREFIX):
            for key in enum_chunks.keys():
                key_str = key.decode()
                if fnmatch(key_str, match_str):
                    yield key
        elif match_str.startswith(KEY_PREFIX):
            for key in objects.keys():
                key_str = key.decode()
                if fnmatch(key_str, match_str):
                    yield key

    redis = mock.MagicMock(spec=Redis)
    redis.hgetall.side_effect = hgetall
    redis.get.side_effect = get
    redis.set.side_effect = set_
    redis.scan_iter.side_effect = scan_iter

    pipe = mock.MagicMock(spec=Pipeline)
    redis.pipeline.return_value.__aenter__.return_value = pipe

    pipe.delete.side_effect = delete
    pipe.expire.side_effect = expire_
    pipe.hdel.side_effect = hdel
    pipe.hset.side_effect = hset
    pipe.mset.side_effect = mset

    redis._test_versions = versions
    redis._test_objects = objects
    redis._test_expire = expire

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
        ids = cast(list[ObjectID], item.ids)
        chunk: EnumerateIDsChunk | None = None
        if item.chunk_session:
            assert item.chunk_index is not None
            assert item.total_chunks is not None
            chunk = EnumerateIDsChunk(
                chunk_index=item.chunk_index,
                total_chunks=item.total_chunks,
                session=item.chunk_session,
            )
        return EnumerateIDsRecord(ids=ids, chunk=chunk)

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
        key_prefix=KEY_PREFIX,
        versions_key=VERSIONS_KEY,
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
    assert redis_mock._test_objects == {  # type: ignore
        b"item:1": b'{"ev_type": "create", "version": 1, "id": 1, "data": "1v1"}',
        b"item:2": b'{"ev_type": "update", "version": 2, "id": 2, "data": "2v2"}',
        b"item:3": b'{"ev_type": "refresh", "version": 1, "id": 3, "data": "3v1"}',
    }
    assert redis_mock._test_expire == {  # type: ignore
        b"item:1": timedelta(hours=30),
        b"item:2": timedelta(hours=30),
        b"item:3": timedelta(hours=30),
        b"items": timedelta(hours=24),
    }


@pytest.mark.asyncio
async def test_redis_writer_upsert(redis_mock: Redis, settings: Settings) -> None:
    redis_mock._test_versions.update(  # type: ignore
        {
            b"2": b"3",
            b"3": b"1",
            b"4": b"0",
            b"5": b"2",
        },
    )
    redis_mock._test_objects.update(  # type:ignore
        {
            b"item:2": b"",
            b"item:3": b"",
            b"item:4": b"",
            b"item:5": b"",
        },
    )

    items: list[Item] = [
        ItemRecord(ev_type=EventType.CREATE, version=1, id=1, data="1v1"),
        ItemRecord(ev_type=EventType.UPDATE, version=2, id=2, data="2v2"),
        ItemRecord(ev_type=EventType.REFRESH, version=2, id=3, data="3v2"),
        ItemDeleteRecord(ev_type=EventType.DELETE, version=1, id=4),
        ItemDeleteRecord(ev_type=EventType.DELETE, version=1, id=5),
    ]

    writer = Writer(settings)
    await writer.process(items)

    assert redis_mock._test_versions == {  # type: ignore
        b"1": b"1",
        b"2": b"3",
        b"3": b"2",
        b"5": b"2",
    }
    assert redis_mock._test_objects == {  # type: ignore
        b"item:1": b'{"ev_type": "create", "version": 1, "id": 1, "data": "1v1"}',
        b"item:2": b"",
        b"item:3": b'{"ev_type": "refresh", "version": 2, "id": 3, "data": "3v2"}',
        b"item:5": b"",
    }
    assert redis_mock._test_expire == {  # type: ignore
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
    redis_mock._test_objects.update(  # type:ignore
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
        b"2": b"2",
        b"3": b"1",
    }
    assert redis_mock._test_objects == {  # type: ignore
        b"item:1": b"",
        b"item:2": b"",
        b"item:3": b"",
    }
    assert redis_mock._test_expire == {  # type: ignore
        b"items": timedelta(hours=24),
    }


@pytest.mark.asyncio
async def test_redis_writer_enumerate_with_chunks(
    redis_mock: Redis, settings: Settings
) -> None:
    redis_mock._test_versions.update(  # type: ignore
        {
            b"1": b"0",
            b"2": b"0",
            b"3": b"0",
            b"4": b"0",
            b"5": b"0",
            b"6": b"0",
        },
    )
    redis_mock._test_objects.update(  # type:ignore
        {
            b"item:1": b"",
            b"item:2": b"",
            b"item:3": b"",
            b"item:4": b"",
            b"item:5": b"",
            b"item:6": b"",
        },
    )

    writer = Writer(settings)

    items: list[Item] = [
        ItemEnumerateRecord(
            ev_type=EventType.ENUMERATE,
            version=1,
            ids=[1, 3],
            chunk_session="session",
            chunk_index=0,
            total_chunks=2,
        ),
    ]
    await writer.process(items)

    assert redis_mock._test_versions == {  # type: ignore
        b"1": b"0",
        b"2": b"0",
        b"3": b"0",
        b"4": b"0",
        b"5": b"0",
        b"6": b"0",
    }
    assert redis_mock._test_objects == {  # type: ignore
        b"item:1": b"",
        b"item:2": b"",
        b"item:3": b"",
        b"item:4": b"",
        b"item:5": b"",
        b"item:6": b"",
    }
    assert redis_mock._test_expire == {  # type: ignore
        b"items": timedelta(hours=24),
        b"items_chunk:session:0": timedelta(hours=24),
    }

    items = [
        ItemEnumerateRecord(
            ev_type=EventType.ENUMERATE,
            version=1,
            ids=[4, 6],
            chunk_session="session",
            chunk_index=1,
            total_chunks=2,
        ),
    ]
    await writer.process(items)

    assert redis_mock._test_versions == {  # type: ignore
        b"1": b"0",
        b"3": b"0",
        b"4": b"0",
        b"6": b"0",
    }
    assert redis_mock._test_objects == {  # type: ignore
        b"item:1": b"",
        b"item:3": b"",
        b"item:4": b"",
        b"item:6": b"",
    }
    assert redis_mock._test_expire == {  # type: ignore
        b"items": timedelta(hours=24),
    }
