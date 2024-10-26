import os
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaProducer  # type: ignore
from redis.asyncio import ConnectionPool, Redis

from aiosafeconsumer import WorkerDef, WorkerPoolSettings

from .deserializers import json_to_namedtuple_deserializer
from .processors import UsersWriter, UsersWriterSettings
from .serializers import namedtuple_to_json_serializer
from .sources import UsersSource, UsersSourceSettings
from .types import UserDeleteRecord, UserEnumerateRecord, UserRecord
from .workers import UsersWorker, UsersWorkerSettings

REDIS_URL: str = os.getenv("REDIS_URL", "")
KAFKA_BOOTSTRAP_SERVERS: list[str] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").split(",")


@pytest_asyncio.fixture
async def redis_pool() -> AsyncGenerator[ConnectionPool]:
    pool: ConnectionPool = ConnectionPool.from_url(REDIS_URL)
    try:
        yield pool
    finally:
        redis = Redis(connection_pool=pool)
        await redis.flushdb()
        await pool.disconnect()


@pytest.fixture
def redis(redis_pool: ConnectionPool) -> Redis:
    return Redis(connection_pool=redis_pool)


@pytest_asyncio.fixture
async def producer() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=namedtuple_to_json_serializer(),
    )
    return producer


@pytest.fixture
def worker_pool_settings(redis: Redis) -> WorkerPoolSettings:
    pool_settings = WorkerPoolSettings(
        workers=[
            WorkerDef(
                worker_class=UsersWorker,
                worker_settings=UsersWorkerSettings(
                    source_class=UsersSource,
                    source_settings=UsersSourceSettings(
                        topics=["users"],
                        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                        group_id="sync_users",
                        value_deserializer=json_to_namedtuple_deserializer(
                            UserRecord,
                            UserDeleteRecord,
                            UserEnumerateRecord,
                        ),
                        getmany_timeout=timedelta(seconds=0.1),
                        kwargs={
                            "auto_offset_reset": "earliest",
                            "fetch_max_wait_ms": 100,
                        },
                    ),
                    processor_class=UsersWriter,
                    processor_settings=UsersWriterSettings(
                        redis=lambda: redis,
                        key_prefix="user:",
                        versions_key="users",
                    ),
                ),
            ),
        ],
    )
    return pool_settings
