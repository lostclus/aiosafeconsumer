import os
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaProducer  # type: ignore
from redis.asyncio import ConnectionPool, Redis

from .serializers import namedtuple_to_json_serializer


@pytest.fixture
def redis_url() -> str:
    return os.getenv("REDIS_URL", "")


@pytest.fixture
def kafka_bootstrap_servers() -> list[str]:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").split(",")


@pytest_asyncio.fixture
async def redis_pool(redis_url: str) -> AsyncGenerator[ConnectionPool]:
    pool: ConnectionPool = ConnectionPool.from_url(redis_url)
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
async def producer(kafka_bootstrap_servers: list[str]) -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=namedtuple_to_json_serializer(),
    )
    return producer
