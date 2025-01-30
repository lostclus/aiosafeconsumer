from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, NamedTuple, cast
from unittest import mock

import pymongo
import pytest

from aiosafeconsumer.datasync import (
    EnumerateIDsChunk,
    EnumerateIDsRecord,
    EventType,
    ObjectID,
    Version,
)
from aiosafeconsumer.datasync.mongo import (
    Document,
    MongoDBWriter,
    MongoDBWriterSettings,
    Operation,
)


class ItemRecord(NamedTuple):
    ev_type: EventType
    version: int
    id: int
    data: str


class ItemDeleteRecord(NamedTuple):
    ev_type: EventType
    version: int
    id: int


class ItemEnumerateRecord(NamedTuple):
    ev_type: EventType
    version: int
    ids: list[int]
    chunk_session: str | None = None
    chunk_index: int | None = None
    total_chunks: int | None = None


class ItemEOSRecord(NamedTuple):
    ev_type: EventType
    version: int


Item = ItemRecord | ItemDeleteRecord | ItemEnumerateRecord | ItemEOSRecord


@dataclass
class Settings(MongoDBWriterSettings[Item]):
    pass


class Writer(MongoDBWriter[Item]):
    settings: Settings


@pytest.fixture
def mongodb_mock() -> Generator[pymongo.AsyncMongoClient]:
    documents: dict[str, Document] = {}
    version_field = "version"

    async def bulk_write(
        operations: list[Operation],
        **kwargs: Any,
    ) -> None:
        for op in operations:
            if isinstance(op, pymongo.ReplaceOne):
                doc = op._doc
                obj_id = doc["_id"]
                cur_ver = documents.get(obj_id, {}).get(version_field, -1)
                if cur_ver >= op._filter[version_field]["$lt"]:
                    continue
                documents[obj_id] = cast(Document, doc)
            elif isinstance(op, pymongo.DeleteOne):
                obj_id = op._filter["_id"]
                cur_ver = documents.get(obj_id, {}).get(version_field, -1)
                if cur_ver >= op._filter[version_field]["$lt"]:
                    continue
                documents.pop(obj_id, None)

    async def delete_many(filter_: dict[str, Any]) -> Any:
        ver_filter = filter_.get(version_field, {}).get("$lt")
        id_filter = filter_.get("_id")

        result = mock.MagicMock()
        result.acknowledged = True
        result.deleted_count = 0

        if ver_filter and id_filter:
            enum_ids = set(str(x) for x in id_filter.get("$not", {}).get("$in", []))
            if enum_ids:
                for _id, doc in list(documents.items()):
                    if _id not in enum_ids and doc[version_field] < ver_filter:
                        documents.pop(_id, None)
                        result.deleted_count += 1
        elif ver_filter:
            for _id, doc in list(documents.items()):
                if doc[version_field] < ver_filter:
                    documents.pop(_id, None)
                    result.deleted_count += 1

        return result

    mongo_m = mock.MagicMock(spec=pymongo.AsyncMongoClient)
    coll_m = mongo_m.__getitem__.return_value.__getitem__.return_value
    coll_m.bulk_write.side_effect = bulk_write
    coll_m.delete_many.side_effect = delete_many
    mongo_m._test_documents = documents

    yield mongo_m


@pytest.fixture
def settings(mongodb_mock: pymongo.AsyncMongoClient) -> Settings:
    def get_mongo() -> pymongo.AsyncMongoClient:
        return mongodb_mock

    def version_getter(item: Item) -> Version:
        return item.version

    def record_serializer(item: Item) -> Document:
        return item._asdict()

    def event_type_getter(item: Item) -> EventType:
        return item.ev_type

    def id_getter(item: Item) -> ObjectID:
        assert isinstance(item, ItemRecord) or isinstance(item, ItemDeleteRecord)
        return item.id

    def enum_getter(item: Item) -> EnumerateIDsRecord:
        assert isinstance(item, ItemEnumerateRecord)
        ids = cast(list[ObjectID], item.ids)
        chunk: EnumerateIDsChunk | None = None
        if item.chunk_session:
            assert item.chunk_index is not None
            assert item.total_chunks is not None
            chunk = EnumerateIDsChunk(
                chunk_index=item.chunk_index,
                total_chunks=item.total_chunks,
                session=item.chunk_session,
            )
        return EnumerateIDsRecord(ids=ids, chunk=chunk)

    settings = Settings(
        mongo_client=get_mongo,
        database="test",
        collection="items",
        version_field="version",
        version_getter=version_getter,
        record_serializer=record_serializer,
        event_type_getter=event_type_getter,
        id_getter=id_getter,
        enum_getter=enum_getter,
    )
    return settings


@pytest.mark.asyncio
async def test_mongodb_writer_initial_empty(
    mongodb_mock: pymongo.AsyncMongoClient, settings: Settings
) -> None:
    items: list[Item] = [
        ItemRecord(ev_type=EventType.CREATE, version=1, id=1, data="1v1"),
        ItemRecord(ev_type=EventType.UPDATE, version=2, id=2, data="2v2"),
        ItemRecord(ev_type=EventType.REFRESH, version=1, id=3, data="3v1"),
    ]

    writer = Writer(settings)
    await writer.process(items)

    assert mongodb_mock._test_documents == {
        "1": {"ev_type": "create", "version": 1, "id": 1, "_id": "1", "data": "1v1"},
        "2": {"ev_type": "update", "version": 2, "id": 2, "_id": "2", "data": "2v2"},
        "3": {"ev_type": "refresh", "version": 1, "id": 3, "_id": "3", "data": "3v1"},
    }


@pytest.mark.asyncio
async def test_mongodb_writer_upsert(
    mongodb_mock: pymongo.AsyncMongoClient, settings: Settings
) -> None:
    mongodb_mock._test_documents.update(
        {
            "2": {"version": 3},
            "3": {"version": 1},
            "4": {"version": 0},
            "5": {"version": 2},
        },
    )

    items: list[Item] = [
        ItemRecord(ev_type=EventType.CREATE, version=1, id=1, data="1v1"),
        ItemRecord(ev_type=EventType.UPDATE, version=2, id=2, data="2v2"),
        ItemRecord(ev_type=EventType.REFRESH, version=2, id=3, data="3v2"),
        ItemDeleteRecord(ev_type=EventType.DELETE, version=1, id=4),
        ItemDeleteRecord(ev_type=EventType.DELETE, version=1, id=5),
    ]

    writer = Writer(settings)
    await writer.process(items)

    assert mongodb_mock._test_documents == {
        "1": {"ev_type": "create", "version": 1, "id": 1, "_id": "1", "data": "1v1"},
        "2": {"version": 3},
        "3": {"ev_type": "refresh", "version": 2, "id": 3, "_id": "3", "data": "3v2"},
        "5": {"version": 2},
    }


@pytest.mark.asyncio
async def test_mongodb_writer_enumerate(
    mongodb_mock: pymongo.AsyncMongoClient, settings: Settings
) -> None:
    mongodb_mock._test_documents.update(
        {
            "1": {"version": 0},
            "2": {"version": 2},
            "3": {"version": 1},
            "4": {"version": 0},
        },
    )

    items: list[Item] = [
        ItemEnumerateRecord(ev_type=EventType.ENUMERATE, version=1, ids=[1, 3]),
    ]

    writer = Writer(settings)
    await writer.process(items)

    assert mongodb_mock._test_documents == {
        "1": {"version": 0},
        "2": {"version": 2},
        "3": {"version": 1},
    }


@pytest.mark.asyncio
async def test_mongodb_writer_eos(
    mongodb_mock: pymongo.AsyncMongoClient, settings: Settings
) -> None:
    settings.process_eos = True

    mongodb_mock._test_documents.update(
        {
            "1": {"version": 0},
            "2": {"version": 2},
            "3": {"version": 1},
            "4": {"version": 0},
        },
    )

    items: list[Item] = [
        ItemEOSRecord(ev_type=EventType.EOS, version=1),
    ]

    writer = Writer(settings)
    await writer.process(items)

    assert mongodb_mock._test_documents == {
        "2": {"version": 2},
        "3": {"version": 1},
    }
