import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Generic

from redis.asyncio import Redis

from ..types import DataType
from .base import DataWriter, DataWriterSettings
from .types import EnumerateIDsRecord, EventType, ObjectID, Version

log = logging.getLogger(__name__)


@dataclass
class RedisWriterSettings(Generic[DataType], DataWriterSettings[DataType]):
    redis: Callable[[], Redis]
    version_serializer: Callable[[Version], bytes]
    version_deserializer: Callable[[bytes], Version]
    record_serializer: Callable[[DataType], bytes]
    key_prefix: str
    versions_key: str
    versions_expire: timedelta = timedelta(hours=24)
    record_expire: timedelta = timedelta(hours=30)


class RedisWriter(Generic[DataType], DataWriter[DataType]):
    settings: RedisWriterSettings

    def _obj_key(self, obj_id: ObjectID) -> bytes:
        return f"{self.settings.key_prefix}{obj_id}".encode()

    def _obj_version_key(self, obj_id: ObjectID) -> bytes:
        return str(obj_id).encode()

    def _obj_version_key_to_obj_key(self, val: bytes) -> bytes:
        key_prefix = self.settings.key_prefix.encode()
        return key_prefix + val

    def _versions_key(self) -> bytes:
        return self.settings.versions_key.encode()

    async def process(self, batch: list[DataType]) -> None:
        settings = self.settings
        redis = settings.redis()

        enum_records: dict[Version, list[EnumerateIDsRecord]] = {}
        upsert_records: dict[ObjectID, DataType] = {}
        upsert_versions: dict[ObjectID, Version] = {}

        versions = await redis.hgetall(self._versions_key())
        log.debug(f"Loaded {len(versions)} object versions from Redis")

        for record in batch:
            event_type = settings.event_type_getter(record)
            rec_ver = settings.version_getter(record)

            if event_type == EventType.ENUMERATE:
                enum_rec = settings.enum_getter(record)
                enum_records.setdefault(rec_ver, [])
                enum_records[rec_ver].append(enum_rec)
            else:
                obj_id = settings.id_getter(record)
                cur_ver = upsert_versions.get(obj_id)
                if cur_ver is None:
                    cur_ver = settings.version_deserializer(
                        versions.get(self._obj_version_key(obj_id), b"0")
                    )

                if rec_ver > cur_ver:  # type: ignore
                    upsert_records[obj_id] = record
                    upsert_versions[obj_id] = rec_ver

        update_records: dict[bytes, bytes] = {}
        update_versions: dict[bytes, bytes] = {}
        del_versions: list[bytes] = []
        del_records: list[bytes] = []

        for obj_id, record in upsert_records.items():
            event_type = settings.event_type_getter(record)
            ver_key = self._obj_version_key(obj_id)
            key = self._obj_key(obj_id)
            if event_type == EventType.DELETE:
                del_versions.append(ver_key)
                del_records.append(key)
                continue
            value = settings.record_serializer(record)
            update_records[key] = value
            ver_value = settings.version_serializer(upsert_versions[obj_id])
            update_versions[ver_key] = ver_value

        if enum_records:
            # TODO: implement sessions with chunks of IDs.
            # Now just use last non chunked record.
            use_enum_rec: EnumerateIDsRecord | None = None
            for rec_ver in reversed(sorted(enum_records.keys())):
                for rec in enum_records[rec_ver]:
                    if rec.chunk is None:
                        use_enum_rec = rec
                        break
                if use_enum_rec is not None:
                    break

            if use_enum_rec is not None:
                cur_ids: set[bytes] = set(
                    list(versions.keys())
                    + [
                        self._obj_version_key(obj_id)
                        for obj_id in upsert_versions.keys()
                    ]
                )
                enum_ids: set[bytes] = set(
                    self._obj_version_key(obj_id) for obj_id in use_enum_rec.ids
                )
                # print("*** cur_ids", cur_ids)
                # print("*** enum_ids", enum_ids)
                ids_to_delete: set[bytes] = cur_ids - enum_ids
                log.debug(
                    f"Accept enumerate message with {len(enum_ids)} object IDs,"
                    f" {len(ids_to_delete)} records will be deleted"
                )
                del_records.extend(
                    [self._obj_version_key_to_obj_key(v) for v in ids_to_delete]
                )
                del_versions.extend(ids_to_delete)

        async with redis.pipeline(transaction=True) as pipe:
            if update_records:
                pipe.mset(update_records)  # type: ignore
            for key in update_records.keys():
                pipe.expire(key, settings.record_expire)
            if del_records:
                pipe.delete(*del_records)
            if del_versions:
                pipe.hdel(self._versions_key(), *del_versions)
            if update_versions:
                pipe.hset(self._versions_key(), mapping=update_versions)  # type: ignore
            pipe.expire(self._versions_key(), settings.versions_expire)
            await pipe.execute()

        log.debug(
            f"{len(update_records)} records was added/updated and {len(del_records)}"
            " records was deleted"
        )
