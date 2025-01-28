import os
from collections.abc import AsyncGenerator
from datetime import timedelta
from typing import Any

import pymongo
import pytest
import pytest_asyncio
from bson.codec_options import TypeRegistry

from aiosafeconsumer import WorkerDef, WorkerPoolSettings
from aiosafeconsumer.datasync.mongo import DecimalCodec

from ..deserializers import json_to_namedtuple_deserializer
from ..sources import UsersKafkaSource, UsersKafkaSourceSettings
from ..types import UserDeleteRecord, UserEnumerateRecord, UserEOSRecord, UserRecord
from ..workers import UsersWorker, UsersWorkerSettings
from .processors import UsersMongoDBWriter, UsersMongoDBWriterSettings


@pytest.fixture
def mongodb_uri() -> str:
    return os.getenv("MONGODB_URI", "")


@pytest.fixture
def mongodb_database() -> str:
    return "test"


@pytest.fixture
def mongodb_collections() -> dict[str, dict[str, Any]]:
    return {
        "users": {
            "indexes": [
                {
                    "keys": [
                        ("evTime", pymongo.ASCENDING),
                    ],
                },
            ],
        },
    }


@pytest.fixture
def mongodb_type_registry() -> TypeRegistry:
    return TypeRegistry([DecimalCodec()])


@pytest.fixture
def mongodb_client(
    mongodb_uri: str, mongodb_type_registry: TypeRegistry
) -> pymongo.AsyncMongoClient:
    return pymongo.AsyncMongoClient(
        mongodb_uri,
        tz_aware=True,
        type_registry=mongodb_type_registry,
        w=0,
    )


@pytest_asyncio.fixture
async def mongodb(
    mongodb_client: pymongo.AsyncMongoClient,
    mongodb_database: str,
    mongodb_collections: dict[str, dict[str, Any]],
) -> AsyncGenerator[pymongo.AsyncMongoClient]:
    db = mongodb_database
    try:
        for coll, coll_def in mongodb_collections.items():
            coll_client = mongodb_client[db][coll]
            indexes = coll_def.get("indexes") or []
            for index_def in indexes:
                keys = index_def.get("keys")
                if keys:
                    await coll_client.create_index(keys)

        yield mongodb_client
    finally:
        await mongodb_client.drop_database(db)


@pytest.fixture
def worker_pool_settings(
    mongodb: pymongo.AsyncMongoClient,
    mongodb_database: str,
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
                    processor_class=UsersMongoDBWriter,
                    processor_settings=UsersMongoDBWriterSettings(
                        mongo_client=lambda: mongodb,
                        database=mongodb_database,
                        collection="users",
                        version_field="evTime",
                        process_eos=True,
                    ),
                ),
            ),
        ],
    )
    return pool_settings
