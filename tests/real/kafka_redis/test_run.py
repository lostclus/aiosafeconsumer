import asyncio
import pickle
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import cast

import pytest
from aiokafka import AIOKafkaProducer  # type: ignore
from redis.asyncio import Redis

from aiosafeconsumer import WorkerPool, WorkerPoolSettings
from aiosafeconsumer.datasync import EventType

from ..types import UserRecord


@pytest.fixture
def ev_time() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


@pytest.fixture
def users(ev_time: datetime) -> list[UserRecord]:
    ev_type = EventType.REFRESH
    ev_source = "test"
    return [
        UserRecord(
            ev_time=ev_time,
            ev_type=ev_type,
            ev_source=ev_source,
            id=1,
            email="user1@example.com",
            score=Decimal("1.23"),
            is_active=True,
        ),
        UserRecord(
            ev_time=ev_time,
            ev_type=ev_type,
            ev_source=ev_source,
            id=2,
            email="user2@example.com",
            score=Decimal("4.56"),
            is_active=False,
        ),
    ]


def encode_version(dt: datetime) -> bytes:
    return str(int(dt.timestamp())).encode()


def encode_id(id: int) -> bytes:
    return str(id).encode()


@pytest.mark.asyncio
async def test_on_empty_db(
    worker_pool_settings: WorkerPoolSettings,
    producer: AIOKafkaProducer,
    users: list[UserRecord],
    redis: Redis,
    ev_time: datetime,
) -> None:

    pool = WorkerPool(worker_pool_settings, burst=True)
    task = asyncio.create_task(pool.run())

    await asyncio.sleep(0.1)
    await producer.start()
    try:
        for user in users:
            await producer.send("users", user)
    finally:
        await producer.flush()
        await producer.stop()

    await task

    versions = await redis.hgetall(b"users")
    assert versions == {
        encode_id(1): encode_version(ev_time),
        encode_id(2): encode_version(ev_time),
    }

    users_in_redis = [
        pickle.loads(cast(bytes, await redis.get(b"user:" + user_id)))
        for user_id in versions.keys()
    ]

    assert users_in_redis == users


@pytest.mark.asyncio
async def test_update(
    worker_pool_settings: WorkerPoolSettings,
    producer: AIOKafkaProducer,
    users: list[UserRecord],
    redis: Redis,
    ev_time: datetime,
) -> None:
    initial_users: list[UserRecord] = []
    for record in users:
        if record.id == 1:
            record = record._replace(ev_time=ev_time + timedelta(minutes=1))
        elif record.id == 2:
            record = record._replace(ev_time=ev_time - timedelta(minutes=1))
        initial_users.append(record)

    versions = {
        encode_id(record.id): encode_version(record.ev_time) for record in initial_users
    }
    await redis.hset(b"users", mapping=versions)  # type: ignore
    for record in initial_users:
        await redis.set(b"user:" + encode_id(record.id), pickle.dumps(record))

    pool = WorkerPool(worker_pool_settings, burst=True)
    task = asyncio.create_task(pool.run())

    await asyncio.sleep(0.1)
    await producer.start()
    try:
        for user in users:
            await producer.send("users", user)
    finally:
        await producer.flush()
        await producer.stop()

    await task

    versions = await redis.hgetall(b"users")
    assert versions == {
        encode_id(1): encode_version(ev_time + timedelta(minutes=1)),
        encode_id(2): encode_version(ev_time),
    }

    users_in_redis = [
        pickle.loads(cast(bytes, await redis.get(b"user:" + user_id)))
        for user_id in versions.keys()
    ]

    assert users_in_redis[0] == initial_users[0]
    assert users_in_redis[1] == users[1]
