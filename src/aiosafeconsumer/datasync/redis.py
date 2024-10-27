import logging
import pickle
from collections.abc import AsyncGenerator, Callable, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Generic, TypeAlias, cast, no_type_check

from redis.asyncio import Redis

from ..types import DataType
from .base import DataWriter, DataWriterSettings
from .types import EnumerateIDsChunk, EnumerateIDsRecord, EventType, ObjectID, Version

log = logging.getLogger(__name__)


StrID: TypeAlias = str


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

    def _obj_key(self, obj_id: StrID) -> bytes:
        return f"{self.settings.key_prefix}{obj_id}".encode()

    def _obj_version_key(self, obj_id: StrID) -> bytes:
        return obj_id.encode()

    def _versions_key(self) -> bytes:
        return self.settings.versions_key.encode()

    def _enum_chunk_key(self, chunk: EnumerateIDsChunk) -> bytes:
        key = f"{self.settings.versions_key}_chunk:{chunk.session}:{chunk.chunk_index}"
        return key.encode()

    def _enum_chunk_match_pattern(self, session: str) -> bytes:
        key = f"{self.settings.versions_key}_chunk:{session}:*"
        return key.encode()

    @no_type_check
    def _compare_versions(self, a: Version, b: Version) -> int:
        assert type(a) is type(b)
        if a < b:
            return -1
        if a > b:
            return 1
        return 0

    async def _get_versions(self, redis: Redis) -> dict[StrID, Version]:
        raw_versions = await redis.hgetall(self._versions_key())

        versions: dict[StrID, Version] = {
            raw_id.decode(): self.settings.version_deserializer(raw_ver)
            for raw_id, raw_ver in raw_versions.items()
        }
        return versions

    async def _save_enum_chunk(
        self, redis: Redis, chunk: EnumerateIDsChunk, ids: Sequence[ObjectID]
    ) -> None:
        key = self._enum_chunk_key(chunk)
        ids_set = set(str(id_) for id_ in ids)
        value = pickle.dumps(ids_set)
        await redis.set(key, value, ex=self.settings.versions_expire)

    async def _iter_enum_chunks(
        self, redis: Redis, session: str
    ) -> AsyncGenerator[tuple[int, set[StrID]]]:
        match = self._enum_chunk_match_pattern(session)
        async for key in redis.scan_iter(match):
            chunk_index = int(key.decode().rsplit(":")[-1])
            value = await redis.get(key)
            if value:
                ids = cast(set[StrID], pickle.loads(value))
                yield (chunk_index, ids)

    async def process(self, batch: list[DataType]) -> None:
        settings = self.settings
        redis = settings.redis()

        enum_records: dict[Version, list[EnumerateIDsRecord]] = {}
        upsert_records: dict[StrID, DataType] = {}
        upsert_versions: dict[StrID, Version] = {}

        versions = await self._get_versions(redis)
        log.debug(f"Loaded {len(versions)} object versions from Redis")

        for record in batch:
            event_type = settings.event_type_getter(record)
            rec_ver = settings.version_getter(record)

            if event_type == EventType.ENUMERATE:
                enum_rec = settings.enum_getter(record)
                enum_records.setdefault(rec_ver, [])
                enum_records[rec_ver].append(enum_rec)
            else:
                obj_id = str(settings.id_getter(record))
                cur_ver = upsert_versions.get(obj_id, versions.get(obj_id))
                if cur_ver is None or self._compare_versions(rec_ver, cur_ver) > 0:
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

        enum_to_process: list[tuple[Version, set[StrID]]] = []

        for rec_ver, rec_list in enum_records.items():
            for rec in rec_list:
                if rec.chunk is None:
                    enum_ids: set[StrID] = set(str(id_) for id_ in rec.ids)
                    enum_to_process.append((rec_ver, enum_ids))
                    continue

                chunks_in_db: set[int] = set()
                ids_in_db: set[StrID] = set()

                async for chunk_index, ids in self._iter_enum_chunks(
                    redis, rec.chunk.session
                ):
                    chunks_in_db.add(chunk_index)
                    ids_in_db |= ids

                complete_chunks = set(range(rec.chunk.total_chunks))
                if chunks_in_db | {rec.chunk.chunk_index} == complete_chunks:
                    enum_ids = ids_in_db | set(str(id_) for id_ in rec.ids)
                    enum_to_process.append((rec_ver, enum_ids))
                    for chunk_index in complete_chunks:
                        del_records.append(
                            self._enum_chunk_key(
                                EnumerateIDsChunk(
                                    session=rec.chunk.session,
                                    chunk_index=chunk_index,
                                    total_chunks=rec.chunk.total_chunks,
                                )
                            )
                        )
                else:
                    await self._save_enum_chunk(redis, rec.chunk, rec.ids)

        for enum_ver, enum_ids in enum_to_process:
            cur_ids: set[StrID] = set(
                obj_id
                for obj_id, rec_ver in versions.items()
                if self._compare_versions(rec_ver, enum_ver) < 0
            ) | set(
                obj_id
                for obj_id, rec_ver in upsert_versions.items()
                if self._compare_versions(rec_ver, enum_ver) < 0
            )
            ids_to_delete: set[StrID] = cur_ids - enum_ids
            log.debug(
                f"Accept enumerate message with {len(enum_ids)} object IDs,"
                f" {len(ids_to_delete)} records will be deleted"
            )
            del_records.extend([self._obj_key(obj_id) for obj_id in ids_to_delete])
            del_versions.extend(
                [self._obj_version_key(obj_id) for obj_id in ids_to_delete]
            )

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
