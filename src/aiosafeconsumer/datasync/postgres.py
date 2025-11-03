import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, TypeAlias, no_type_check
from uuid import uuid4

from asyncpg import Pool
from asyncpg.pool import PoolConnectionProxy

from aiosafeconsumer.datasync import Version

from ..types import DataType
from .base import DataWriter, DataWriterSettings
from .types import EnumerateIDsRecord, EventType

log = logging.getLogger(__name__)

TupleID: TypeAlias = tuple


class LockError(Exception):
    pass


@dataclass
class PostgresWriterSettings(Generic[DataType], DataWriterSettings[DataType]):
    connection_pool: Callable[[], Pool]
    record_serializer: Callable[[DataType], dict]
    table: str
    fields: list[str]
    id_fields: list[str]
    id_sql_type: str
    version_field: str
    enum_chunks_table: str | None = None
    process_eos: bool = False


class PostgresWriter(Generic[DataType], DataWriter[DataType]):
    settings: PostgresWriterSettings

    def _id_tuple(self, row: dict) -> TupleID:
        return tuple(row[field] for field in self.settings.id_fields)

    def _id_fields_sql(self, table: str) -> str:
        fields_sql = ",".join(f'"{table}"."{field}"' for field in self.settings.id_fields)
        return fields_sql

    def _id_row_sql(self, table: str) -> str:
        fields_sql = ",".join(f'"{table}"."{field}"' for field in self.settings.id_fields)
        if len(self.settings.id_fields) == 1:
            return fields_sql
        return f"({fields_sql})"

    def _ids_to_arg(self, ids: list[tuple]) -> list:
        if len(self.settings.id_fields) == 1:
            return [id for id, in ids]
        return ids

    @no_type_check
    def _compare_versions(self, a: Version, b: Version) -> int:
        assert type(a) is type(b)
        if a < b:
            return -1
        if a > b:
            return 1
        return 0

    async def upsert_with_lock(
        self, conn: PoolConnectionProxy, rows: list[dict]
    ) -> None:
        settings = self.settings
        table = settings.table
        fields = settings.fields
        version_field = settings.version_field
        id_fields = settings.id_fields

        tmp_suffix = uuid4().hex
        tmp_table = f"{table}_{tmp_suffix}"
        fields_sql = ",".join([f'"{field}"' for field in fields])

        set_fields_sql = ",".join(
            [f'{field} = tmp."{field}"' for field in fields if field not in id_fields]
        )

        fail_count = 0
        while True:
            async with conn.transaction():
                await conn.execute(
                    f"""
                    CREATE TEMPORARY TABLE "{tmp_table}"
                    (LIKE "{table}" EXCLUDING INDEXES EXCLUDING CONSTRAINTS)
                    ON COMMIT DROP
                    """
                )
                await conn.copy_records_to_table(
                    tmp_table,
                    records=[tuple(row[field] for field in fields) for row in rows],
                    columns=fields,
                )

                await conn.execute(
                    f"""
                    WITH outdated AS (
                       SELECT {self._id_fields_sql("tmp")}
                       FROM "{tmp_table}" AS tmp
                       JOIN "{table}" AS current
                           ON {self._id_row_sql("current")} = {self._id_row_sql("tmp")}
                               AND current."{version_field}" > tmp."{version_field}"
                    )
                    DELETE FROM "{tmp_table}"
                    WHERE
                        {self._id_row_sql(tmp_table)}
                        IN (SELECT {self._id_fields_sql("outdated")} FROM outdated)
                    """
                )
                await conn.execute(
                    f"""
                    WITH new AS (
                        DELETE FROM "{tmp_table}"
                        WHERE
                            {self._id_row_sql(tmp_table)}
                            NOT IN (SELECT {self._id_fields_sql(table)} FROM "{table}")
                        RETURNING {fields_sql}
                    )
                    INSERT INTO "{table}" ({fields_sql})
                    SELECT {fields_sql} FROM new
                    """
                )

                result = await conn.fetch(
                    f"""
                    SELECT {self._id_fields_sql(tmp_table)} FROM "{tmp_table}"
                    """
                )
                ids_left: list[tuple] = [tuple(rec.values()) for rec in result]

                if not ids_left:
                    break

                status = await conn.execute(
                    f"""
                    WITH locked AS (
                        SELECT {self._id_fields_sql(table)}
                        FROM "{table}"
                        WHERE {self._id_row_sql(table)} = any($1::{settings.id_sql_type}[])
                        FOR UPDATE SKIP LOCKED
                    ), deleted AS (
                        DELETE FROM {tmp_table}
                        WHERE
                            {self._id_row_sql(tmp_table)}
                            IN (SELECT {self._id_fields_sql("locked")} FROM locked)
                        RETURNING {fields_sql}
                    )
                    UPDATE "{table}" AS current
                    SET {set_fields_sql}
                    FROM deleted AS tmp
                    WHERE {self._id_row_sql("current")} = {self._id_row_sql("tmp")}
                    """,
                    self._ids_to_arg(ids_left),
                )
                rows_count = int(status.split()[-1])

                if not rows_count:
                    fail_count += 1
                    if fail_count > 5:
                        log.error(
                            "failed to lock %d rows in the database", len(ids_left)
                        )
                        raise LockError()
                    await asyncio.sleep(1)
                elif rows_count == len(ids_left):
                    break
                else:
                    result = await conn.fetch(
                        f"""
                        SELECT {self._id_fields_sql(tmp_table)} FROM {tmp_table}
                        """
                    )
                    ids_left = [tuple(rec.values()) for rec in result]
                    rows = [row for row in rows if self._id_tuple(row) in ids_left]

    async def process(self, batch: list[DataType]) -> None:
        settings = self.settings
        pool = settings.connection_pool()

        enum_records: dict[Version, list[EnumerateIDsRecord]] = {}
        eos_versions: set[Version] = set()
        upsert_rows: dict[TupleID, dict] = {}
        upsert_versions: dict[TupleID, Version] = {}

        for record in batch:
            event_type = settings.event_type_getter(record)
            rec_ver = settings.version_getter(record)

            if event_type == EventType.ENUMERATE:
                enum_rec = settings.enum_getter(record)
                enum_records.setdefault(rec_ver, [])
                enum_records[rec_ver].append(enum_rec)
            elif event_type == EventType.EOS:
                if self.settings.process_eos:
                    eos_versions.add(rec_ver)
            else:
                row = settings.record_serializer(record)
                obj_id = self._id_tuple(row)
                cur_ver = upsert_versions.get(obj_id)
                if cur_ver is None or self._compare_versions(rec_ver, cur_ver) > 0:
                    upsert_rows[obj_id] = row
                    upsert_versions[obj_id] = rec_ver

        async with pool.acquire() as conn:
            if upsert_rows:
                await self.upsert_with_lock(conn, list(upsert_rows.values()))
