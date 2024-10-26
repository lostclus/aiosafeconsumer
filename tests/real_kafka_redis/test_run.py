import asyncio
import pickle
from typing import cast

import pytest
from aiokafka import AIOKafkaProducer  # type: ignore
from redis.asyncio import Redis

from aiosafeconsumer import WorkerPool, WorkerPoolSettings
from aiosafeconsumer.datasync import EventType

from .types import UserRecord


@pytest.fixture
def users() -> list[UserRecord]:
    ev_time = "2024-01-01T00:00:00"
    ev_type = EventType.REFRESH
    ev_source = "test"
    return [
        UserRecord(
            ev_time=ev_time,
            ev_type=ev_type,
            ev_source=ev_source,
            id=1,
            email="user1@example.com",
            is_active=True,
        ),
        UserRecord(
            ev_time=ev_time,
            ev_type=ev_type,
            ev_source=ev_source,
            id=2,
            email="user2@example.com",
            is_active=False,
        ),
    ]


@pytest.mark.asyncio
async def test(
    worker_pool_settings: WorkerPoolSettings,
    producer: AIOKafkaProducer,
    users: list[UserRecord],
    redis: Redis,
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

    versions = await redis.hgetall("users")
    assert versions == {
        b"1": b"1704067200",
        b"2": b"1704067200",
    }

    users_in_redis = [
        pickle.loads(cast(bytes, await redis.get(b"user:" + user_id)))
        for user_id in versions.keys()
    ]

    assert users_in_redis == users
