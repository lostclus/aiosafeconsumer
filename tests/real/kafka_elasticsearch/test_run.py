import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from aiokafka import AIOKafkaProducer  # type: ignore
from elasticsearch import AsyncElasticsearch

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


@pytest.mark.asyncio
async def test_on_empty_db(
    worker_pool_settings: WorkerPoolSettings,
    producer: AIOKafkaProducer,
    users: list[UserRecord],
    elasticsearch: AsyncElasticsearch,
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

    results = await elasticsearch.search(
        index="users",
        query={
            "match_all": {},
        },
    )

    assert results["hits"]["hits"] == [
        {
            "_id": "1",
            "_index": "users",
            "_score": 1.0,
            "_source": {
                "email": "user1@example.com",
                "ev_source": "test",
                "ev_time": ev_time.isoformat(),
                "ev_type": "refresh",
                "id": 1,
                "is_active": True,
                "score": 1.23,
            },
        },
        {
            "_id": "2",
            "_index": "users",
            "_score": 1.0,
            "_source": {
                "email": "user2@example.com",
                "ev_source": "test",
                "ev_time": ev_time.isoformat(),
                "ev_type": "refresh",
                "id": 2,
                "is_active": False,
                "score": 4.56,
            },
        },
    ]


@pytest.mark.asyncio
async def test_update(
    worker_pool_settings: WorkerPoolSettings,
    producer: AIOKafkaProducer,
    users: list[UserRecord],
    elasticsearch: AsyncElasticsearch,
    ev_time: datetime,
) -> None:
    initial_users: list[UserRecord] = []
    for record in users:
        if record.id == 1:
            record = record._replace(ev_time=ev_time + timedelta(minutes=1))
        elif record.id == 2:
            record = record._replace(ev_time=ev_time - timedelta(minutes=1))
        initial_users.append(record)

    for record in initial_users:
        await elasticsearch.index(
            index="users",
            id=str(record.id),
            version=int(record.ev_time.timestamp()),
            version_type="external",
            document=record._asdict(),
        )

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

    results = await elasticsearch.search(
        index="users",
        query={
            "match_all": {},
        },
    )

    assert results["hits"]["hits"] == [
        {
            "_id": "1",
            "_index": "users",
            "_score": 1.0,
            "_source": {
                "email": "user1@example.com",
                "ev_source": "test",
                "ev_time": (ev_time + timedelta(minutes=1)).isoformat(),
                "ev_type": "refresh",
                "id": 1,
                "is_active": True,
                "score": 1.23,
            },
        },
        {
            "_id": "2",
            "_index": "users",
            "_score": 1.0,
            "_source": {
                "email": "user2@example.com",
                "ev_source": "test",
                "ev_time": ev_time.isoformat(),
                "ev_type": "refresh",
                "id": 2,
                "is_active": False,
                "score": 4.56,
            },
        },
    ]
