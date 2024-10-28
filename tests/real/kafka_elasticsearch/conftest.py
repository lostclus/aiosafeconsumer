import os
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any

import pytest
import pytest_asyncio
from elasticsearch import AsyncElasticsearch

from aiosafeconsumer import WorkerDef, WorkerPoolSettings

from ..deserializers import json_to_namedtuple_deserializer
from ..sources import UsersKafkaSource, UsersKafkaSourceSettings
from ..types import UserDeleteRecord, UserEnumerateRecord, UserRecord
from ..workers import UsersWorker, UsersWorkerSettings
from .processors import UsersElasticsearchWriter, UsersElasticsearchWriterSettings


@pytest.fixture
def elasticsearch_url() -> str:
    return os.getenv("ELASTICSEARCH_URL", "")


@pytest.fixture
def elasticsearch_indices() -> dict[str, dict[str, Any]]:
    return {
        "users": {
            "mappings": {
                "properties": {
                    "ev_time": {"type": "date"},
                    "ev_type": {"type": "keyword"},
                    "ev_source": {"type": "keyword"},
                    "id": {"type": "integer"},
                    "email": {"type": "keyword"},
                    "score": {"type": "float"},
                    "is_active": {"type": "boolean"},
                },
            },
        },
    }


@pytest_asyncio.fixture
async def elasticsearch(
    elasticsearch_url: str,
    elasticsearch_indices: dict[str, dict[str, Any]],
) -> AsyncGenerator[AsyncElasticsearch]:
    es = AsyncElasticsearch(elasticsearch_url)
    try:
        for index_name, index_def in elasticsearch_indices.items():
            await es.indices.create(index=index_name, **index_def)
        yield es
    finally:
        for index_name in elasticsearch_indices.keys():
            await es.indices.delete(index=index_name, ignore_unavailable=True)
        await es.close()


@pytest.fixture
def worker_pool_settings(
    elasticsearch: AsyncElasticsearch,
    kafka_bootstrap_servers: list[str],
) -> WorkerPoolSettings:
    pool_settings = WorkerPoolSettings(
        workers=[
            WorkerDef(
                worker_class=UsersWorker,
                worker_settings=UsersWorkerSettings(
                    source_class=UsersKafkaSource,
                    source_settings=UsersKafkaSourceSettings(
                        topics=["users"],
                        bootstrap_servers=kafka_bootstrap_servers,
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
                    processor_class=UsersElasticsearchWriter,
                    processor_settings=UsersElasticsearchWriterSettings(
                        elasticsearch=lambda: elasticsearch,
                        index="users",
                        version_field="ev_time",
                        wait_for_completion=True,
                    ),
                ),
            ),
        ],
    )
    return pool_settings
