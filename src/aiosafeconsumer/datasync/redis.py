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
    version_serializer: Callable[[Version], str]
    version_deserializer: Callable[[str], Version]
    record_serializer: Callable[[DataType], bytes]
    key_prefix: str
    versions_key: str
    versions_expire: timedelta = timedelta(hours=24)
    record_expire: timedelta = timedelta(hours=30)


class RedisWriter(Generic[DataType], DataWriter[DataType]):
    settings: RedisWriterSettings

    async def process(self, batch: list[DataType]) -> None:
        settings = self.settings
        redis = settings.redis()

        enum_records: dict[Version, list[EnumerateIDsRecord]] = {}
        upsert_records: dict[ObjectID, DataType] = {}
        upsert_versions: dict[ObjectID, Version] = {}

        versions = await redis.hgetall(settings.versions_key)
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
                        versions.get(str(obj_id), "0")
                    )

                if rec_ver > cur_ver:  # type: ignore
                    upsert_records[obj_id] = record
                    upsert_versions[obj_id] = rec_ver

        update_records: dict[str, bytes] = {}
        update_versions: dict[str, str] = {}
        del_versions: list[str] = []
        del_records: list[str] = []

        for obj_id, record in upsert_records.items():
            event_type = settings.event_type_getter(record)
            key = f"{settings.key_prefix}{obj_id}"
            if event_type == EventType.DELETE:
                del_versions.append(str(obj_id))
                del_records.append(key)
                continue
            value = settings.record_serializer(record)
            update_records[key] = value
            ver_key = str(obj_id)
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
                cur_ids = set(
                    [str(v) for v in versions.keys()]
                    + [str(v) for v in upsert_versions.keys()]
                )
                enum_ids = set([str(v) for v in use_enum_rec.ids])
                del_records.extend(
                    [f"{settings.key_prefix}{obj_id}" for obj_id in cur_ids - enum_ids]
                )
                del_versions.extend([str(obj_id) for obj_id in cur_ids - enum_ids])

        async with redis.pipeline(transaction=True) as pipe:
            if update_records:
                pipe.mset(update_records)  # type: ignore
            for key in update_records.keys():
                pipe.expire(key, settings.record_expire)
            if del_records:
                pipe.delete(*del_records)
            if del_versions:
                pipe.hdel(settings.versions_key, *del_versions)
            if update_versions:
                pipe.hset(
                    settings.versions_key, mapping=update_versions  # type: ignore
                )
            pipe.expire(settings.versions_key, settings.versions_expire)
            await pipe.execute()

        log.debug(
            f"{len(update_records)} was updated and {len(del_records)} was deleted"
            " from Redis"
        )
