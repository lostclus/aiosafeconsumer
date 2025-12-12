import asyncio
import dataclasses
import logging
from collections.abc import AsyncGenerator, Callable, Generator
from datetime import timedelta
from functools import cached_property
from typing import Any, Generic, cast
from uuid import uuid4

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .source import DataSource, DataSourceSettings
from .types import DataType

log = logging.getLogger(__name__)

XReadGroupResponse = list[tuple[Any, list[tuple[Any, dict[bytes, bytes]]]]]


@dataclasses.dataclass
class RedisStreamSourceSettings(Generic[DataType], DataSourceSettings):
    redis: Callable[[], Redis]
    streams: list[str]
    data_deserializer: Callable[[bytes], DataType]
    data_field: str = "data"
    group_name: str | None = None
    group_start_message_id: str = "0"
    mkstream: bool = True
    consumer_name: str = "{group_name}:{uuid}"
    max_records: int | None = None
    timeout: timedelta = timedelta(seconds=3)


class RedisStreamSource(Generic[DataType], DataSource[DataType]):
    settings: RedisStreamSourceSettings[DataType]

    def __init__(self, settings: RedisStreamSourceSettings[DataType]) -> None:
        super().__init__(settings)
        self.commit_event = asyncio.Event()
        self._consumer_name = None

    def __str__(self) -> str:
        return self.__class__.__name__

    @cached_property
    def _redis(self) -> Redis:
        return self.settings.redis()

    @cached_property
    def _group_name(self) -> str:
        return self.settings.group_name or self.__class__.__name__

    async def _create_groups(self) -> None:
        async def create_one_group(stream_name: str) -> None:
            try:
                await self._redis.xgroup_create(
                    stream_name,
                    self._group_name,
                    id=self.settings.group_start_message_id,
                    mkstream=self.settings.mkstream,
                )
            except ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    raise

        await asyncio.gather(
            *[create_one_group(stream_name) for stream_name in self.settings.streams]
        )

    async def _create_consumer(self) -> str:
        consumer_name = self.settings.consumer_name.format(
            group_name=self._group_name, uuid=uuid4()
        )
        await asyncio.gather(
            *[
                self._redis.xgroup_createconsumer(
                    stream_name,
                    self._group_name,
                    consumer_name,
                )
                for stream_name in self.settings.streams
            ]
        )
        return consumer_name

    async def _delete_consumer(self, consumer_name: str) -> None:
        await asyncio.gather(
            *[
                self._redis.xgroup_delconsumer(
                    stream_name,
                    self._group_name,
                    consumer_name,
                )
                for stream_name in self.settings.streams
            ]
        )

    async def read(self) -> AsyncGenerator[list[DataType], None]:
        async def read_panding(consumer_name: str) -> XReadGroupResponse:
            response = await self._redis.xreadgroup(
                self._group_name,
                consumer_name,
                {stream_name: "0" for stream_name in self.settings.streams},
                count=self.settings.max_records,
                block=0,
            )
            return cast(XReadGroupResponse, response)

        async def read_new(consumer_name: str) -> XReadGroupResponse:
            response = await self._redis.xreadgroup(
                self._group_name,
                consumer_name,
                {stream_name: ">" for stream_name in self.settings.streams},
                count=self.settings.max_records,
                block=int(self.settings.timeout.total_seconds() * 1000),
            )
            return cast(XReadGroupResponse, response)

        def has_messages(consumed_data: XReadGroupResponse) -> bool:
            for _, messages in consumed_data:
                if messages:
                    return True
            return False

        def iter_messages(
            consumed_data: XReadGroupResponse,
        ) -> Generator[dict[bytes, bytes]]:
            for _, messages in consumed_data:
                for _, msg_data in messages:
                    yield msg_data

        data_field = self.settings.data_field.encode()
        await self._create_groups()
        consumer_name = await self._create_consumer()

        try:
            while True:
                consumed_data = await read_panding(consumer_name)
                if not has_messages(consumed_data):
                    consumed_data = await read_new(consumer_name)

                batch = []
                for msg_data in iter_messages(consumed_data):
                    data = self.settings.data_deserializer(msg_data[data_field])
                    batch.append(data)
                if not batch:
                    continue

                yield batch

                log.debug("Waiting for commit event")
                await self.commit_event.wait()

                commit_count = 0
                for stream_name, messages in consumed_data:
                    for message_id, _ in messages:
                        await self._redis.xack(
                            stream_name, self._group_name, message_id
                        )
                        commit_count += 1

                log.debug(f"{commit_count} messages commited")
                self.commit_event.clear()
        finally:
            await self._delete_consumer(consumer_name)

    async def commit(self, batch: list[DataType]) -> None:
        self.commit_event.set()
