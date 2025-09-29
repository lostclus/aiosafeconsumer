from typing import Any


class TopicPartition:
    pass


class Message:
    offset: int
    value: Any


class ConsumerRebalanceListener:
    async def on_partitions_revoked(self, revoked: list[TopicPartition]) -> None:
        pass

    async def on_partitions_assigned(self, assigned: list[TopicPartition]) -> None:
        pass


class AIOKafkaConsumer:
    def subscribe(
        self, topics: list[str], listener: ConsumerRebalanceListener | None = None
    ) -> None:
        pass

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def getmany(  # type: ignore
        self,
        timeout_ms: float,
        max_records: int | None,
    ) -> dict[TopicPartition, list[Message]]:
        pass

    async def commit(self, offsets: dict[TopicPartition, int]) -> None:
        pass


class KafkaError(Exception):
    pass


class KafkaConnectionError(KafkaError):
    pass
