import asyncio
import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Generic, TypeAlias, no_type_check
from uuid import uuid4

from asyncpg import Connection
from asyncpg.pool import PoolConnectionProxy

from aiosafeconsumer.datasync import Version

from ..types import DataType
from .base import DataWriter, DataWriterSettings
from .types import EnumerateIDsRecord, EventType

log = logging.getLogger(__name__)

AsyncPGConnection: TypeAlias = Connection | PoolConnectionProxy
TupleID: TypeAlias = tuple


class LockError(Exception):
    pass


@dataclass
class PostgresWriterSettings(Generic[DataType], DataWriterSettings[DataType]):
    connection_manager: Callable[[], AbstractAsyncContextManager[AsyncPGConnection]]
    record_serializer: Callable[[DataType], dict]
    table: str
    fields: list[str]
    id_fields: list[str]
    id_sql_type: str
    version_field: str
    soft_delete_field: str | None = None
    soft_delete_value: bool = True
    enum_chunks_table: str | None = None
    process_eos: bool = False
    lock_attempts: int = 5
    lock_fail_delay: timedelta = timedelta(seconds=1)


class PostgresWriter(Generic[DataType], DataWriter[DataType]):
    settings: PostgresWriterSettings

    def _id_tuple(self, row: dict) -> TupleID:
        return tuple(row[field] for field in self.settings.id_fields)

    def _id_fields_sql(self, table: str) -> str:
        fields_sql = ",".join(
            f'"{table}"."{field}"' for field in self.settings.id_fields
        )
        return fields_sql

    def _id_row_sql(self, table: str) -> str:
        fields_sql = ",".join(
            f'"{table}"."{field}"' for field in self.settings.id_fields
        )
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

    async def upsert_with_lock(self, conn: AsyncPGConnection, rows: list[dict]) -> None:
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
                        WHERE
                            {self._id_row_sql(table)}
                            = any($1::{settings.id_sql_type}[])
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
                    if fail_count > settings.lock_attempts:
                        log.error(
                            "failed to lock %d rows in the database", len(ids_left)
                        )
                        raise LockError()
                    await asyncio.sleep(settings.lock_fail_delay.total_seconds())
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

    async def delete_with_lock(
        self, conn: AsyncPGConnection, before_version: Version
    ) -> None:
        settings = self.settings
        table = settings.table
        version_field = settings.version_field
        soft_delete_field = settings.soft_delete_field
        soft_delete_value = settings.soft_delete_value

        deleted_count = 0
        fail_count = 0
        while True:
            async with conn.transaction():

                args: list[Any] = [before_version]
                if soft_delete_field:
                    not_deleted_sql = f'"{soft_delete_field}" != $2'
                    args.append(soft_delete_value)
                else:
                    not_deleted_sql = "(true)"

                result = await conn.fetch(
                    f"""
                    SELECT {self._id_fields_sql(table)}
                    FROM "{table}"
                    WHERE "{version_field}" < $1 AND {not_deleted_sql}
                    """,
                    *args,
                )
                ids_left: list[tuple] = [tuple(rec.values()) for rec in result]

                if not ids_left:
                    break

                args = [self._ids_to_arg(ids_left)]
                if soft_delete_field:
                    delete_sql = f"""
                        UPDATE "{table}"
                        SET {soft_delete_field} = $2,
                            {version_field} = $3
                    """
                    args.extend([soft_delete_value, before_version])
                else:
                    delete_sql = f"""
                        DELETE FROM "{table}"
                    """

                status = await conn.execute(
                    f"""
                    WITH locked AS (
                        SELECT {self._id_fields_sql(table)}
                        FROM "{table}"
                        WHERE
                            {self._id_row_sql(table)}
                            = any($1::{settings.id_sql_type}[])
                        FOR UPDATE SKIP LOCKED
                    )
                    {delete_sql}
                    WHERE
                        {self._id_row_sql(table)}
                        IN (SELECT {self._id_fields_sql("locked")} FROM locked)
                    """,
                    *args,
                )
                rows_count = int(status.split()[-1])
                deleted_count += rows_count

                if not rows_count:
                    fail_count += 1
                    if fail_count > settings.lock_attempts:
                        log.error("Failed to lock %d rows in database", len(ids_left))
                        raise LockError()
                    await asyncio.sleep(settings.lock_fail_delay.total_seconds())
                elif rows_count == len(ids_left):
                    break

    async def process(self, batch: list[DataType]) -> None:
        settings = self.settings

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

        async with settings.connection_manager() as conn:
            if upsert_rows:
                await self.upsert_with_lock(conn, list(upsert_rows.values()))
            if eos_versions:
                max_ver = max(eos_versions)
                await self.delete_with_lock(conn, max_ver)
