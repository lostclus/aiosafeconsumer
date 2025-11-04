import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from aiokafka import AIOKafkaProducer  # type: ignore
from asyncpg import Pool

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
async def test_on_empty_table(
    worker_pool_settings: WorkerPoolSettings,
    producer: AIOKafkaProducer,
    users: list[UserRecord],
    postgres_pool: Pool,
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

    async with postgres_pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM "user" ORDER BY id')
    users_in_postgres = [UserRecord(**row) for row in rows]

    assert users_in_postgres == users


@pytest.mark.asyncio
async def test_update(
    worker_pool_settings: WorkerPoolSettings,
    producer: AIOKafkaProducer,
    users: list[UserRecord],
    postgres_pool: Pool,
    ev_time: datetime,
) -> None:
    initial_users: list[UserRecord] = []
    for record in users:
        if record.id == 1:
            record = record._replace(ev_time=ev_time + timedelta(minutes=1))
        elif record.id == 2:
            record = record._replace(ev_time=ev_time - timedelta(minutes=1))
        initial_users.append(record)

    async with postgres_pool.acquire() as conn:
        await conn.copy_records_to_table(
            "user", records=initial_users, columns=UserRecord._fields
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

    async with postgres_pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM "user" ORDER BY id')
    users_in_postgres = [UserRecord(**row) for row in rows]

    assert users_in_postgres[0] == initial_users[0]
    assert users_in_postgres[1] == users[1]


@pytest.mark.asyncio
async def test_eos(
    worker_pool_settings: WorkerPoolSettings,
    producer: AIOKafkaProducer,
    users: list[UserRecord],
    postgres_pool: Pool,
    ev_time: datetime,
) -> None:
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

    async with postgres_pool.acquire() as conn:
        await conn.copy_records_to_table(
            "user", records=initial_users, columns=UserRecord._fields
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

    async with postgres_pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM "user" ORDER BY id')
    users_in_postgres = [UserRecord(**row) for row in rows]

    assert users_in_postgres == users
