import logging
from collections import defaultdict
from collections.abc import Callable, MutableMapping
from dataclasses import dataclass
from decimal import Decimal
from functools import cached_property
from typing import Any, Generic, TypeAlias

import pymongo
from bson.codec_options import TypeCodec
from bson.decimal128 import Decimal128

from ..types import DataType
from .base import DataWriter, DataWriterSettings
from .types import EnumerateIDsRecord, EventType, Version

log = logging.getLogger(__name__)

Document: TypeAlias = MutableMapping[str, Any]
Operation: TypeAlias = pymongo.ReplaceOne[Document] | pymongo.DeleteOne


@dataclass
class MongoDBWriterSettings(Generic[DataType], DataWriterSettings[DataType]):
    mongo_client: Callable[[], pymongo.AsyncMongoClient]
    database: str
    collection: str
    version_field: str
    record_serializer: Callable[[DataType], Document]
    process_eos: bool = False


class MongoDBWriter(Generic[DataType], DataWriter[DataType]):
    settings: MongoDBWriterSettings

    @cached_property
    def _client(self) -> pymongo.AsyncMongoClient:
        return self.settings.mongo_client()

    def _database(self, record: DataType) -> str:
        return self.settings.database

    def _collection(self, record: DataType) -> str:
        return self.settings.collection

    def _id(self, record: DataType) -> str:
        return str(self.settings.id_getter(record))

    def _document(self, record: DataType) -> Document:
        return self.settings.record_serializer(record)

    def _version(self, record: DataType) -> Version:
        version = self.settings.version_getter(record)
        return version

    def _operation(
        self, event_type: EventType, rec_ver: Version, record: DataType
    ) -> tuple[str, str, Operation]:
        database = self._database(record)
        collection = self._collection(record)
        version_field = self.settings.version_field
        doc = self._document(record)

        _id = doc.get("_id")
        if _id is None:
            _id = self._id(record)
            doc["_id"] = _id

        op: Operation
        if event_type != EventType.DELETE:
            op = pymongo.ReplaceOne(
                {"_id": _id, version_field: {"$lt": rec_ver}},
                doc,
                upsert=True,
            )
        else:
            op = pymongo.DeleteOne({"_id": _id, version_field: {"$lt": rec_ver}})

        return database, collection, op

    def _enum_delete_filter(
        self, rec_ver: Version, enum_rec: EnumerateIDsRecord
    ) -> dict:
        assert (
            enum_rec.chunk is None
        ), "Enumerate messages with chunks is not implemented"
        version_field = self.settings.version_field
        return {
            "_id": {
                "$not": {"$in": enum_rec.ids},
            },
            version_field: {"$lt": rec_ver},
        }

    def _eos_delete_filter(self, rec_ver: Version) -> dict:
        version_field = self.settings.version_field
        return {
            version_field: {"$lt": rec_ver},
        }

    async def process(self, batch: list[DataType]) -> None:
        collections: set[tuple[str, str]] = set()
        operations: defaultdict[tuple[str, str], list[Operation]] = defaultdict(list)

        for record in batch:
            event_type = self.settings.event_type_getter(record)
            rec_ver = self.settings.version_getter(record)
            delete_filter: dict | None = None

            if event_type == EventType.ENUMERATE:
                enum_rec = self.settings.enum_getter(record)
                if enum_rec.chunk:
                    log.warning("Enumerate messages with chunks is not implemented")
                else:
                    log.debug(
                        f"Accept enumerate message of version {rec_ver}"
                        f" with {len(enum_rec.ids)} object IDs"
                    )
                    delete_filter = self._enum_delete_filter(rec_ver, enum_rec)
            elif event_type == EventType.EOS:
                if self.settings.process_eos:
                    log.debug(f"Accept end of stream message of version {rec_ver}")
                    delete_filter = self._eos_delete_filter(rec_ver)
            else:
                db, coll, op = self._operation(event_type, rec_ver, record)
                collections.add((db, coll))
                operations[(db, coll)].append(op)

            if delete_filter:
                db = self._database(record)
                coll = self._collection(record)
                coll_client = self._client[db][coll]
                result = await coll_client.delete_many(delete_filter)
                if result.acknowledged:
                    log.info("Deleted %d old records", result.deleted_count)
                else:
                    log.info("Deleting old records in backgroud")

        for (db, coll), op_list in operations.items():
            coll_client = self._client[db][coll]
            await coll_client.bulk_write(op_list, ordered=False)


class DecimalCodec(TypeCodec):
    python_type = Decimal
    bson_type = Decimal128

    def transform_python(self, value: Decimal) -> Decimal128:
        return Decimal128(value)

    def transform_bson(self, value: Decimal128) -> Decimal:
        return value.to_decimal()
