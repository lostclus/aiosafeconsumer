import os

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaProducer  # type: ignore

from .serializers import namedtuple_to_json_serializer


@pytest.fixture
def kafka_bootstrap_servers() -> list[str]:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "").split(",")


@pytest_asyncio.fixture
async def producer(kafka_bootstrap_servers: list[str]) -> AIOKafkaProducer:
    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=namedtuple_to_json_serializer(),
    )
    return producer
