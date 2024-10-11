import asyncio
import dataclasses
import logging
import random
from collections.abc import AsyncGenerator, Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic

if not TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener, TopicPartition
    from aiokafka.errors import KafkaConnectionError, KafkaError
else:
    from .kafka_stubs import (
        AIOKafkaConsumer,
        ConsumerRebalanceListener,
        KafkaConnectionError,
        KafkaError,
        TopicPartition,
    )

from .source import DataSource, DataSourceSettings
from .types import DataType

log = logging.getLogger(__name__)

CONSUMER_DEFAULTS = {
    "auto_offset_reset": "latest",
    "max_partition_fetch_bytes": 256 * 1024,
    "fetch_min_bytes": 32 * 1024,
    "fetch_max_wait_ms": 2500,
    "session_timeout_ms": 10000,
    "max_poll_interval_ms": 300000,
}


class InformativeRebalanceListener(ConsumerRebalanceListener):
    revoked: set[TopicPartition]
    assigned: set[TopicPartition]

    def __init__(self) -> None:
        self.revoked = set()
        self.assigned = set()

    async def on_partitions_revoked(self, revoked: list[TopicPartition]) -> None:
        self.revoked = set(revoked)

    async def on_partitions_assigned(self, assigned: list[TopicPartition]) -> None:
        self.assigned = set(assigned)


@dataclasses.dataclass
class KafkaSourceSettings(Generic[DataType], DataSourceSettings):
    topics: list[str]
    bootstrap_servers: list[str]
    value_deserializer: Callable[[bytes], DataType]
    enable_auto_commit: bool = False
    max_read_partitions: int | None = None
    getmany_max_records: int | None = None
    getmany_timeout: timedelta = timedelta(seconds=10)
    max_consumer_errors: int = 100
    sleep_on_consumer_error: timedelta = timedelta(seconds=5)
    kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)


class KafkaSource(Generic[DataType], DataSource[DataType]):
    settings: KafkaSourceSettings[DataType]

    def __init__(self, settings: KafkaSourceSettings[DataType]):
        super().__init__(settings)
        self.commit_event = asyncio.Event()

    def __str__(self) -> str:
        return self.__class__.__name__

    def get_topics(self) -> list[str]:
        return self.settings.topics

    def get_bootstrap_servers(self) -> list[str]:
        servers = list(self.settings.bootstrap_servers)
        random.shuffle(servers)
        return servers

    def get_default_group_id(self) -> str:
        return self.__class__.__name__

    def create_kafka_consumer(self) -> AIOKafkaConsumer:
        kwargs = {
            **CONSUMER_DEFAULTS,
            "bootstrap_servers": self.get_bootstrap_servers(),
            "enable_auto_commit": self.settings.enable_auto_commit,
            "group_id": self.get_default_group_id(),
            "value_deserializer": self.settings.value_deserializer,
            **self.settings.kwargs,
        }
        return AIOKafkaConsumer(**kwargs)

    def create_listener(self) -> InformativeRebalanceListener:
        return InformativeRebalanceListener()

    async def read(self) -> AsyncGenerator[list[DataType], None]:
        try:
            consumer = None
            listener = self.create_listener()
            consumer_errors_count = 0

            while True:

                if (
                    consumer_errors_count > self.settings.max_consumer_errors
                    and consumer is not None
                ):
                    await consumer.stop()
                    consumer = None
                    consumer_errors_count = 0
                    log.error("To many consumer errors, restarting consumer")

                while consumer is None:
                    consumer = self.create_kafka_consumer()
                    try:
                        consumer.subscribe(self.get_topics(), listener=listener)
                        await consumer.start()
                    except KafkaConnectionError as error:
                        log.error(f"Kafka error: {error}")
                        consumer = None
                        await asyncio.sleep(
                            self.settings.sleep_on_consumer_error.total_seconds()
                        )

                listener.revoked = set()
                consumed_data = await consumer.getmany(
                    timeout_ms=self.settings.getmany_timeout.total_seconds() * 1000,
                    max_records=self.settings.getmany_max_records,
                )
                consumed_count = sum(len(x) for x in consumed_data.values())
                if not consumed_count:
                    continue

                skipped_tps = set()
                consumed_tps = set()

                for tp, messages in consumed_data.items():
                    if tp in listener.revoked:
                        continue

                    if (
                        self.settings.max_partitions
                        and len(consumed_tps) >= self.settings.max_partitions
                    ):
                        skipped_tps.add(tp)
                        continue

                    consumed_tps.add(tp)

                    for msg in messages:
                        yield msg.value

                if listener.revoked:
                    count = sum(
                        len(consumed_data.get(tp, [])) for tp in listener.revoked
                    )
                    log.debug(
                        f"Will not process {count} messages in {len(listener.revoked)}"
                        " partitions because partitions assignment is revoked"
                    )
                if skipped_tps:
                    count = sum(len(consumed_data.get(tp, [])) for tp in skipped_tps)
                    log.debug(
                        f"Will not process {count} messages in {len(skipped_tps)}"
                        " partitions because max read partitions limit achieved"
                    )

                if not self.settings.enable_auto_commit:
                    await self.commit_event.wait()

                    no_commit_tps = listener.revoked | skipped_tps
                    for tp, messages in consumed_data.items():
                        if tp not in no_commit_tps:
                            try:
                                await consumer.commit({tp: messages[-1].offset + 1})
                            except KafkaError as error:
                                self.log_error(f"Kafka error: {error}")
                                consumer_errors_count += 1

        finally:
            if consumer is not None:
                await consumer.stop()

    async def commit(self, batch: list[DataType]) -> None:
        self.commit_event.set()
