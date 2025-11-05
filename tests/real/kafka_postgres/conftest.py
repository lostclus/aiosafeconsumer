import os
from collections.abc import AsyncGenerator
from datetime import timedelta

import pytest
import pytest_asyncio
from asyncpg import Pool, create_pool

from aiosafeconsumer import WorkerDef, WorkerPoolSettings

from ..deserializers import json_to_namedtuple_deserializer
from ..sources import UsersKafkaSource, UsersKafkaSourceSettings
from ..types import UserDeleteRecord, UserEnumerateRecord, UserEOSRecord, UserRecord
from ..workers import UsersWorker, UsersWorkerSettings
from .processors import UsersPostgresWriter, UsersPostgresWriterSettings


@pytest.fixture
def postgres_url() -> str:
    return os.getenv("POSTGRES_URL", "")


@pytest_asyncio.fixture
async def postgres_pool(postgres_url: str) -> AsyncGenerator[Pool]:
    pool: Pool = await create_pool(postgres_url)
    async with pool.acquire() as conn:
        await conn.execute('DROP TABLE IF EXISTS "user"')
        await conn.execute(
            """
            CREATE TABLE "user" (
                ev_time timestamptz,
                ev_type varchar,
                ev_source varchar,
                id int PRIMARY KEY,
                email varchar,
                score decimal,
                is_active boolean
            )
            """
        )
    yield pool


@pytest.fixture
def worker_pool_settings(
    postgres_pool: Pool,
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
                            UserEOSRecord,
                        ),
                        getmany_timeout=timedelta(seconds=0.1),
                        kwargs={
                            "auto_offset_reset": "earliest",
                            "fetch_max_wait_ms": 100,
                        },
                    ),
                    processor_class=UsersPostgresWriter,
                    processor_settings=UsersPostgresWriterSettings(
                        connection_pool=lambda: postgres_pool,
                        table="user",
                        fields=UserRecord._fields,
                        id_fields=["id"],
                        id_sql_type="int",
                        version_field="ev_time",
                        process_eos=True,
                        # soft_delete_field="is_active",
                        # soft_delete_value=False,
                    ),
                ),
            ),
        ],
    )
    return pool_settings
