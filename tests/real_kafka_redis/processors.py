import pickle
from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast

from aiosafeconsumer.datasync import EnumerateIDsRecord, EventType, ObjectID, Version
from aiosafeconsumer.datasync.redis import RedisWriter, RedisWriterSettings

from .types import User, UserDeleteRecord, UserEnumerateRecord, UserRecord


@dataclass
class UsersWriterSettings(RedisWriterSettings[User]):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            version_getter=self._version_getter,
            event_type_getter=self._event_type_getter,
            id_getter=self._id_getter,
            enum_getter=self._enum_getter,
            version_serializer=self._version_serializer,
            version_deserializer=self._version_deserializer,
            record_serializer=self._record_serializer,
            **kwargs,
        )

    @staticmethod
    def _version_getter(item: User) -> Version:
        return int(datetime.fromisoformat(item.ev_time).timestamp())

    @staticmethod
    def _record_serializer(item: User) -> bytes:
        return pickle.dumps(item)

    @staticmethod
    def _event_type_getter(item: User) -> EventType:
        return item.ev_type

    @staticmethod
    def _id_getter(item: User) -> ObjectID:
        assert isinstance(item, UserRecord) or isinstance(item, UserDeleteRecord)
        return item.id

    @staticmethod
    def _enum_getter(item: User) -> EnumerateIDsRecord:
        assert isinstance(item, UserEnumerateRecord)
        return EnumerateIDsRecord(ids=cast(list[ObjectID], item.ids))

    @staticmethod
    def _version_serializer(ver: Version) -> bytes:
        return str(ver).encode()

    @staticmethod
    def _version_deserializer(val: bytes) -> Version:
        return int(val.decode())


class UsersWriter(RedisWriter[User]):
    pass
