from datetime import timedelta

import pytest
from redis.asyncio import Redis

from aiosafeconsumer import WorkerDef, WorkerPoolSettings

from ..deserializers import json_to_namedtuple_deserializer
from ..processors import UsersRedisWriter, UsersRedisWriterSettings
from ..sources import UsersKafkaSource, UsersKafkaSourceSettings
from ..types import UserDeleteRecord, UserEnumerateRecord, UserRecord
from ..workers import UsersWorker, UsersWorkerSettings


@pytest.fixture
def worker_pool_settings(
    redis: Redis,
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
                    processor_class=UsersRedisWriter,
                    processor_settings=UsersRedisWriterSettings(
                        redis=lambda: redis,
                        key_prefix="user:",
                        versions_key="users",
                    ),
                ),
            ),
        ],
    )
    return pool_settings
