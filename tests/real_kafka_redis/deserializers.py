import json
from collections.abc import Callable
from typing import NamedTuple, TypeVar

from aiosafeconsumer.datasync import EventType

from .constants import ENCODING, FIELD_TRANSLATION

_FIELDS_TR = dict(FIELD_TRANSLATION)

RecordT = TypeVar("RecordT", bound=NamedTuple)
DeleteRecordT = TypeVar("DeleteRecordT", bound=NamedTuple)
EnumerateRecordT = TypeVar("EnumerateRecordT", bound=NamedTuple)


def json_to_namedtuple_deserializer(
    record_class: type[RecordT],
    delete_record_class: type[DeleteRecordT] | None = None,
    enumerate_record_class: type[EnumerateRecordT] | None = None,
    encoding: str = ENCODING,
) -> Callable[[bytes], RecordT | DeleteRecordT | EnumerateRecordT]:
    def deserializer(value: bytes) -> RecordT | DeleteRecordT | EnumerateRecordT:
        payload = json.loads(value.decode(encoding))
        ev_type = EventType(payload["_type"])

        class_: type[NamedTuple] = record_class
        if ev_type == EventType.DELETE:
            assert delete_record_class is not None
            class_ = delete_record_class
        elif ev_type == EventType.ENUMERATE:
            assert enumerate_record_class is not None
            class_ = enumerate_record_class

        fields = set(f for f in class_._fields)
        transformed = [(_FIELDS_TR.get(k, k), v) for k, v in payload.items()]
        data = {k: v for k, v in transformed if k in fields}
        return class_(**data)  # type: ignore

    return deserializer
