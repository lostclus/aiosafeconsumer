from unittest import mock

import pytest
from redis.asyncio import Redis

from aiosafeconsumer.redis import RedisStreamSource, RedisStreamSourceSettings


class StrRedisStreamSourceSettings(RedisStreamSourceSettings[str]):
    pass


class StrRedisStreamSource(RedisStreamSource[str]):
    settings: StrRedisStreamSourceSettings


@pytest.fixture
def redis_mock() -> Redis:
    redis = mock.MagicMock(spec=Redis)
    redis.xgroup_create = mock.AsyncMock()
    redis.xgroup_createconsumer = mock.AsyncMock()
    redis.xgroup_delconsumer = mock.AsyncMock()
    redis.xreadgroup = mock.AsyncMock()
    return redis


@pytest.fixture
def redis_source_settings(redis_mock: Redis) -> RedisStreamSourceSettings:
    def data_deserializer(data: bytes) -> str:
        return data.decode()

    settings = StrRedisStreamSourceSettings(
        redis=lambda: redis_mock,
        streams=["test-stream"],
        data_deserializer=data_deserializer,
    )
    return settings


@pytest.fixture
def redis_source(redis_source_settings: RedisStreamSourceSettings) -> RedisStreamSource:
    source = StrRedisStreamSource(redis_source_settings)
    return source


@pytest.fixture
def data() -> list[str]:
    return [str(i) for i in range(20)]


@pytest.fixture
def redis_stream_data(redis_mock: mock.Mock, data: list[str]) -> None:
    redis_mock.xreadgroup.return_value = [
        (
            "test-stream",
            [(None, {b"data": value.encode()}) for value in data],
        )
    ]


@pytest.mark.asyncio
async def test_redis_source_read(
    redis_stream_data: None, data: list[str], redis_source: StrRedisStreamSource
) -> None:
    generator = redis_source.read()
    batch = await anext(generator)
    await generator.aclose()

    assert batch == data
