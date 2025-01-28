import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from aiokafka import AIOKafkaProducer  # type: ignore
from camel_converter import dict_to_camel
from pymongo import AsyncMongoClient

from aiosafeconsumer import WorkerPool, WorkerPoolSettings
from aiosafeconsumer.datasync import EventType

from ..types import UserEOSRecord, UserRecord


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
    mongodb: AsyncMongoClient,
    mongodb_database: str,
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

    coll_client = mongodb[mongodb_database]["users"]
    results = [x async for x in coll_client.find()]

    assert results == [
        {
            "_id": "1",
            "email": "user1@example.com",
            "evSource": "test",
            "evTime": ev_time,
            "evType": "refresh",
            "id": 1,
            "isActive": True,
            "score": Decimal("1.23"),
        },
        {
            "_id": "2",
            "email": "user2@example.com",
            "evSource": "test",
            "evTime": ev_time,
            "evType": "refresh",
            "id": 2,
            "isActive": False,
            "score": Decimal("4.56"),
        },
    ]


@pytest.mark.asyncio
async def test_update(
    worker_pool_settings: WorkerPoolSettings,
    producer: AIOKafkaProducer,
    users: list[UserRecord],
    mongodb: AsyncMongoClient,
    mongodb_database: str,
    ev_time: datetime,
) -> None:
    coll_client = mongodb[mongodb_database]["users"]

    initial_users: list[UserRecord] = []
    for record in users:
        if record.id == 1:
            record = record._replace(ev_time=ev_time + timedelta(minutes=1))
        elif record.id == 2:
            record = record._replace(ev_time=ev_time - timedelta(minutes=1))
        initial_users.append(record)

    for record in initial_users:
        await coll_client.insert_one(
            {"_id": str(record.id), **dict_to_camel(record._asdict())}
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

    results = [x async for x in coll_client.find()]

    assert results == [
        {
            "_id": "1",
            "email": "user1@example.com",
            "evSource": "test",
            "evTime": ev_time + timedelta(minutes=1),
            "evType": "refresh",
            "id": 1,
            "isActive": True,
            "score": Decimal("1.23"),
        },
        {
            "_id": "2",
            "email": "user2@example.com",
            "evSource": "test",
            "evTime": ev_time,
            "evType": "refresh",
            "id": 2,
            "isActive": False,
            "score": Decimal("4.56"),
        },
    ]


@pytest.mark.asyncio
async def test_eos(
    worker_pool_settings: WorkerPoolSettings,
    producer: AIOKafkaProducer,
    users: list[UserRecord],
    mongodb: AsyncMongoClient,
    mongodb_database: str,
    ev_time: datetime,
) -> None:
    coll_client = mongodb[mongodb_database]["users"]

    initial_users: list[UserRecord] = []
    for record in users:
        if record.id == 1:
            record = record._replace(
                id=11,
                ev_time=ev_time - timedelta(minutes=1),
            )
        elif record.id == 2:
            record = record._replace(
                id=12,
                ev_time=ev_time - timedelta(minutes=1),
            )
        initial_users.append(record)

    for record in initial_users:
        await coll_client.insert_one(
            {"_id": str(record.id), **dict_to_camel(record._asdict())}
        )

    pool = WorkerPool(worker_pool_settings, burst=True)
    task = asyncio.create_task(pool.run())

    await asyncio.sleep(0.1)
    await producer.start()
    try:
        for user in users:
            await producer.send("users", user)
        eos_record = UserEOSRecord(
            ev_time=ev_time,
            ev_type=EventType.EOS,
            ev_source="test",
        )
        await producer.send("users", eos_record)
    finally:
        await producer.flush()
        await producer.stop()

    await task

    results = [x async for x in coll_client.find()]

    assert results == [
        {
            "_id": "1",
            "email": "user1@example.com",
            "evSource": "test",
            "evTime": ev_time,
            "evType": "refresh",
            "id": 1,
            "isActive": True,
            "score": Decimal("1.23"),
        },
        {
            "_id": "2",
            "email": "user2@example.com",
            "evSource": "test",
            "evTime": ev_time,
            "evType": "refresh",
            "id": 2,
            "isActive": False,
            "score": Decimal("4.56"),
        },
    ]
