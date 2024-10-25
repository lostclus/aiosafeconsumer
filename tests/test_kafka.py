from collections.abc import Generator
from datetime import timedelta
from unittest import mock

import pytest
from aiokafka import ConsumerRecord, TopicPartition  # type: ignore
from aiokafka.errors import KafkaConnectionError, KafkaError  # type: ignore

from aiosafeconsumer.kafka import (
    InformativeRebalanceListener,
    KafkaSource,
    KafkaSourceSettings,
)


class StrKafkaSourceSettings(KafkaSourceSettings[str]):
    pass


class StrKafkaSource(KafkaSource[str]):
    settings: StrKafkaSourceSettings


@pytest.fixture
def kafka_source_settings() -> KafkaSourceSettings:
    def value_deserializer(value: bytes) -> str:
        return value.decode()

    settings = StrKafkaSourceSettings(
        topics=["test-topic"],
        bootstrap_servers=["localhost:9092"],
        value_deserializer=value_deserializer,
        max_consumer_errors=1,
        sleep_on_consumer_error=timedelta(seconds=0.1),
    )
    return settings


@pytest.fixture
def kafka_source(kafka_source_settings: KafkaSourceSettings) -> KafkaSource:
    source = StrKafkaSource(kafka_source_settings)
    return source


@pytest.fixture
def data() -> list[str]:
    return [str(i) for i in range(20)]


@pytest.fixture
def kafka_consumer_mock() -> Generator[mock.Mock]:
    with mock.patch("aiosafeconsumer.kafka.AIOKafkaConsumer") as consumer_class_m:
        consumer_m = consumer_class_m.return_value
        consumer_m.start = mock.AsyncMock()
        consumer_m.stop = mock.AsyncMock()
        consumer_m.getmany = mock.AsyncMock()
        consumer_m.commit = mock.AsyncMock()
        consumer_m._coordinator = mock.AsyncMock()

        yield consumer_m


@pytest.fixture
def kafka_consumer_data(kafka_consumer_mock: mock.Mock, data: list[str]) -> None:
    kafka_consumer_mock.getmany.return_value = {
        TopicPartition("test-topic", 0): [
            ConsumerRecord(
                **{
                    **{f: None for f in ConsumerRecord.__dataclass_fields__.keys()},
                    "topic": "test-topic",
                    "partition": 0,
                    "offset": 0,
                    "value": value,
                },
            )
            for value in data
        ],
    }


@pytest.mark.asyncio
async def test_kafka_source_read(
    kafka_consumer_data: None, data: list[str], kafka_source: StrKafkaSource
) -> None:
    generator = kafka_source.read()
    batch = await anext(generator)
    await generator.aclose()

    assert batch == data


@pytest.mark.asyncio
async def test_kafka_source_read_with_start_errors(
    kafka_consumer_mock: mock.Mock,
    kafka_consumer_data: None,
    data: list[str],
    kafka_source: StrKafkaSource,
) -> None:
    async def start_se() -> None:
        if kafka_consumer_mock.start.call_count < 3:
            raise KafkaConnectionError("Test error")

    kafka_consumer_mock.start.side_effect = start_se
    generator = kafka_source.read()
    batch = await anext(generator)
    await generator.aclose()

    assert batch == data
    assert kafka_consumer_mock.stop.call_count == 1
    assert kafka_consumer_mock.start.call_count == 3


@pytest.mark.asyncio
async def test_kafka_source_read_with_commit_errors(
    kafka_consumer_mock: mock.Mock,
    kafka_consumer_data: None,
    data: list[str],
    kafka_source: StrKafkaSource,
) -> None:
    async def commit_se(offsets: dict[TopicPartition, int]) -> None:
        if kafka_consumer_mock.commit.call_count < 3:
            raise KafkaError("Test error")

    kafka_consumer_mock.commit.side_effect = commit_se
    generator = kafka_source.read()
    batch = await anext(generator)
    await kafka_source.commit(batch)
    await anext(generator)
    await kafka_source.commit(batch)
    await anext(generator)
    await kafka_source.commit(batch)
    await anext(generator)
    await generator.aclose()

    assert batch == data
    assert kafka_consumer_mock.commit.call_count == 3


@pytest.mark.asyncio
async def test_kafka_source_str(kafka_source: StrKafkaSource) -> None:
    assert str(kafka_source) == "StrKafkaSource"


@pytest.mark.asyncio
async def test_informative_rebalance_listener() -> None:
    listener = InformativeRebalanceListener()
    assert listener.revoked == set()
    assert listener.assigned == set()

    await listener.on_partitions_revoked([TopicPartition("test-topic", 0)])
    await listener.on_partitions_assigned([TopicPartition("test-topic", 1)])

    assert listener.revoked == {TopicPartition("test-topic", 0)}
    assert listener.assigned == {TopicPartition("test-topic", 1)}
