from collections.abc import AsyncGenerator, Generator
from dataclasses import dataclass
from typing import Any, NamedTuple, cast
from unittest import mock

import pytest
from elasticsearch import AsyncElasticsearch

from aiosafeconsumer.datasync import (
    EnumerateIDsChunk,
    EnumerateIDsRecord,
    EventType,
    ObjectID,
    Version,
)
from aiosafeconsumer.datasync.elasticsearch import (
    Action,
    Document,
    ElasticsearchWriter,
    ElasticsearchWriterSettings,
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
class Settings(ElasticsearchWriterSettings[Item]):
    pass


class Writer(ElasticsearchWriter[Item]):
    settings: Settings


@pytest.fixture
def elasticsearch_mock() -> Generator[AsyncElasticsearch]:
    documents: dict[str, Document] = {}
    versions: dict[str, int] = {}

    async def async_bulk(
        es: AsyncElasticsearch,
        actions: AsyncGenerator[Action],
        pipeline: str | None,
        raise_on_error: bool,
    ) -> None:
        async for act in actions:
            cur_ver = versions.get(act["_id"], -1)
            if cur_ver >= act["_version"]:
                continue

            if act["_op_type"] == "delete":
                documents.pop(act["_id"], None)
                versions.pop(act["_id"], None)
            elif act["_op_type"] == "index":
                documents[act["_id"]] = act["_source"]
                versions[act["_id"]] = act["_version"]

    async def delete_by_query(
        index: str,
        query: dict[str, Any],
        conflicts: str,
        refresh: bool,
        wait_for_completion: bool,
    ) -> None:
        filter_ = query.get("bool", {}).get("filter", {})
        if len(filter_) == 2:
            f1, f2 = filter_
            enum_ids = set(
                str(x) for x in f1.get("bool", {}).get("must_not", {}).get("ids", [])
            )
            enum_ver = f2.get("range", {}).get("ev_time", {}).get("lt")
            if enum_ids and enum_ver:
                for _id, ver in list(versions.items()):
                    if _id not in enum_ids and ver < enum_ver:
                        documents.pop(_id, None)
                        versions.pop(_id, None)
        elif len(filter_) == 1:
            (f1,) = filter_
            eos_ver = f1.get("range", {}).get("ev_time", {}).get("lt")
            if eos_ver:
                for _id, ver in list(versions.items()):
                    if ver < eos_ver:
                        documents.pop(_id, None)
                        versions.pop(_id, None)

    es_m = mock.MagicMock(spec=AsyncElasticsearch)
    es_m.delete_by_query.side_effect = delete_by_query

    es_m._test_documents = documents
    es_m._test_versions = versions

    with mock.patch(
        "aiosafeconsumer.datasync.elasticsearch.async_bulk"
    ) as async_bulk_m:
        async_bulk_m.side_effect = async_bulk
        yield es_m


@pytest.fixture
def settings(elasticsearch_mock: AsyncElasticsearch) -> Settings:
    def get_elasticsearch() -> AsyncElasticsearch:
        return elasticsearch_mock

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
        elasticsearch=get_elasticsearch,
        index="items",
        version_field="ev_time",
        version_getter=version_getter,
        record_serializer=record_serializer,
        event_type_getter=event_type_getter,
        id_getter=id_getter,
        enum_getter=enum_getter,
    )
    return settings


@pytest.mark.asyncio
async def test_redis_writer_initial_empty(
    elasticsearch_mock: AsyncElasticsearch, settings: Settings
) -> None:
    items: list[Item] = [
        ItemRecord(ev_type=EventType.CREATE, version=1, id=1, data="1v1"),
        ItemRecord(ev_type=EventType.UPDATE, version=2, id=2, data="2v2"),
        ItemRecord(ev_type=EventType.REFRESH, version=1, id=3, data="3v1"),
    ]

    writer = Writer(settings)
    await writer.process(items)

    assert elasticsearch_mock._test_versions == {  # type: ignore
        "1": 1,
        "2": 2,
        "3": 1,
    }

    assert elasticsearch_mock._test_documents == {  # type: ignore
        "1": {"ev_type": "create", "version": 1, "id": 1, "data": "1v1"},
        "2": {"ev_type": "update", "version": 2, "id": 2, "data": "2v2"},
        "3": {"ev_type": "refresh", "version": 1, "id": 3, "data": "3v1"},
    }


@pytest.mark.asyncio
async def test_redis_writer_upsert(
    elasticsearch_mock: AsyncElasticsearch, settings: Settings
) -> None:
    elasticsearch_mock._test_versions.update(  # type: ignore
        {
            "2": 3,
            "3": 1,
            "4": 0,
            "5": 2,
        },
    )
    elasticsearch_mock._test_documents.update(  # type:ignore
        {
            "2": {},
            "3": {},
            "4": {},
            "5": {},
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

    assert elasticsearch_mock._test_versions == {  # type: ignore
        "1": 1,
        "2": 3,
        "3": 2,
        "5": 2,
    }
    assert elasticsearch_mock._test_documents == {  # type: ignore
        "1": {"ev_type": "create", "version": 1, "id": 1, "data": "1v1"},
        "2": {},
        "3": {"ev_type": "refresh", "version": 2, "id": 3, "data": "3v2"},
        "5": {},
    }


@pytest.mark.asyncio
async def test_redis_writer_enumerate(
    elasticsearch_mock: AsyncElasticsearch, settings: Settings
) -> None:
    elasticsearch_mock._test_versions.update(  # type: ignore
        {
            "1": 0,
            "2": 2,
            "3": 1,
            "4": 0,
        },
    )
    elasticsearch_mock._test_documents.update(  # type:ignore
        {
            "1": {},
            "2": {},
            "3": {},
            "4": {},
        },
    )

    items: list[Item] = [
        ItemEnumerateRecord(ev_type=EventType.ENUMERATE, version=1, ids=[1, 3]),
    ]

    writer = Writer(settings)
    await writer.process(items)

    assert elasticsearch_mock._test_versions == {  # type: ignore
        "1": 0,
        "2": 2,
        "3": 1,
    }
    assert elasticsearch_mock._test_documents == {  # type: ignore
        "1": {},
        "2": {},
        "3": {},
    }


@pytest.mark.asyncio
async def test_redis_writer_eos(
    elasticsearch_mock: AsyncElasticsearch, settings: Settings
) -> None:
    settings.process_eos = True

    elasticsearch_mock._test_versions.update(  # type: ignore
        {
            "1": 0,
            "2": 2,
            "3": 1,
            "4": 0,
        },
    )
    elasticsearch_mock._test_documents.update(  # type:ignore
        {
            "1": {},
            "2": {},
            "3": {},
            "4": {},
        },
    )

    items: list[Item] = [
        ItemEOSRecord(ev_type=EventType.EOS, version=1),
    ]

    writer = Writer(settings)
    await writer.process(items)

    assert elasticsearch_mock._test_versions == {  # type: ignore
        "2": 2,
        "3": 1,
    }
    assert elasticsearch_mock._test_documents == {  # type: ignore
        "2": {},
        "3": {},
    }
