import logging
from collections import defaultdict
from collections.abc import AsyncGenerator, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Any, Generic, TypeAlias

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import BulkIndexError, async_bulk

from ..types import DataType
from .base import DataWriter, DataWriterSettings
from .types import EventType

log = logging.getLogger(__name__)

Document: TypeAlias = Mapping[str, Any]
Action: TypeAlias = dict[str, Any]


@dataclass
class DeleteByQuery:
    index: str
    query: Mapping[str, Any]


@dataclass
class ElasticsearchWriterSettings(Generic[DataType], DataWriterSettings[DataType]):
    elasticsearch: Callable[[], AsyncElasticsearch]
    index: str
    version_field: str
    record_serializer: Callable[[DataType], Document]
    process_eos: bool = False
    wait_for_completion: bool = False


class ElasticsearchWriter(Generic[DataType], DataWriter[DataType]):
    settings: ElasticsearchWriterSettings

    @cached_property
    def _es(self) -> AsyncElasticsearch:
        return self.settings.elasticsearch()

    def _es_index(self, record: DataType) -> str:
        return self.settings.index

    def _es_pipeline(self) -> str | None:
        return None

    def _es_id(self, record: DataType) -> str:
        return str(self.settings.id_getter(record))

    def _es_document(self, record: DataType) -> Document:
        return self.settings.record_serializer(record)

    def _es_version(self, record: DataType) -> int:
        version = self.settings.version_getter(record)
        if isinstance(version, datetime):
            version = int(version.timestamp())
        return version

    def _es_routing(self, record: DataType) -> str | None:
        return None

    def _action(self, event_type: EventType, record: DataType) -> Action:
        _index = self._es_index(record)
        _id = self._es_id(record)
        _version = self._es_version(record)
        _routing = self._es_routing(record)

        if event_type == EventType.DELETE:
            _op_type = "delete"
            _source = None
        else:
            _op_type = "index"
            _source = self._es_document(record)

        action = {
            "_op_type": _op_type,
            "_index": _index,
            "_id": _id,
            "_source": _source,
            "_version": _version,
            "_version_type": "external",
            "_routing": _routing,
        }
        return action

    def _enum_query(self, record: DataType) -> DeleteByQuery:
        index = self._es_index(record)
        rec_ver = self.settings.version_getter(record)
        enum_rec = self.settings.enum_getter(record)

        assert enum_rec.chunk is None, "Chunks is not implemented yet"

        query = {
            "bool": {
                "filter": [
                    {
                        "bool": {
                            "must_not": {
                                "ids": enum_rec.ids,
                            },
                        },
                    },
                    {
                        "range": {
                            self.settings.version_field: {
                                "lt": rec_ver,
                            },
                        },
                    },
                ],
            },
        }
        return DeleteByQuery(index=index, query=query)

    def _eos_query(self, record: DataType) -> DeleteByQuery:
        index = self._es_index(record)
        rec_ver = self.settings.version_getter(record)
        query = {
            "bool": {
                "filter": [
                    {
                        "range": {
                            self.settings.version_field: {
                                "lt": rec_ver,
                            },
                        },
                    },
                ],
            },
        }
        return DeleteByQuery(index=index, query=query)

    async def _bulk_actions(self, actions: AsyncGenerator[Action]) -> None:
        pipeline = self._es_pipeline()

        try:
            await async_bulk(self._es, actions, pipeline=pipeline, raise_on_error=True)
        except BulkIndexError as error:
            errors_summary: defaultdict[str, int] = defaultdict(int)
            errors_samples: dict[str, Any] = {}
            for err in error.errors:
                if err.get("index"):
                    error_type = "index.%s" % (
                        err["index"].get("error", {}).get("type") or "unknown"
                    )
                elif err.get("delete"):
                    error_type = "delete.%s" % (
                        err["delete"].get("error", {}).get("type")
                        or err["delete"].get("result")
                        or "unknown"
                    )
                else:
                    error_type = "unknown"
                errors_summary[error_type] += 1
                errors_samples.setdefault(error_type, err)
            ignore_error_types = {
                "delete.not_found",
                "delete.version_conflict_engine_exception",
                "index.version_conflict_engine_exception",
            }
            if set(errors_summary.keys()) - ignore_error_types:
                log.error(
                    "Error(s) summery while inserting data to ElasticSearch:"
                    f" {dict(errors_summary)}",
                )
                for error_type, err in errors_samples.items():
                    log.error(
                        "Error while inserting data to ElasticSearch:"
                        f" {error_type}: {err}",
                    )

    async def _delete_by_query(self, query: DeleteByQuery) -> None:
        await self._es.delete_by_query(
            index=query.index,
            query=query.query,
            conflicts="proceed",
            refresh=True,
            wait_for_completion=self.settings.wait_for_completion,
        )
        if self.settings.wait_for_completion:
            await self._es.indices.refresh(index=query.index)

    async def process(self, batch: list[DataType]) -> None:
        indices: set[str] = set()

        async def _generator() -> AsyncGenerator[Action]:
            for record in batch:
                event_type = self.settings.event_type_getter(record)
                query: DeleteByQuery | None = None

                if event_type == EventType.ENUMERATE:
                    query = self._enum_query(record)
                elif event_type == EventType.EOS:
                    if self.settings.process_eos:
                        query = self._eos_query(record)
                else:
                    action = self._action(event_type, record)
                    indices.add(action["_index"])
                    yield action

                if query:
                    await self._delete_by_query(query)

        actions = _generator()
        await self._bulk_actions(actions)
        if self.settings.wait_for_completion:
            for index in indices:
                await self._es.indices.refresh(index=index)
