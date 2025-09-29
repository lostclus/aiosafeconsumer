import json
import types
import typing
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import NamedTuple, TypeVar

from aiosafeconsumer.datasync import EventType

from .constants import ENCODING, FIELD_TRANSLATION

_FIELDS_TR = dict(FIELD_TRANSLATION)

RecordT = TypeVar("RecordT", bound=NamedTuple)
DeleteRecordT = TypeVar("DeleteRecordT", bound=NamedTuple)
EnumerateRecordT = TypeVar("EnumerateRecordT", bound=NamedTuple)
EOSRecordT = TypeVar("EOSRecordT", bound=NamedTuple)


def json_to_namedtuple_deserializer(
    record_class: type[RecordT],
    delete_record_class: type[DeleteRecordT] | None = None,
    enumerate_record_class: type[EnumerateRecordT] | None = None,
    eos_record_class: type[EOSRecordT] | None = None,
    encoding: str = ENCODING,
) -> Callable[[bytes], RecordT | DeleteRecordT | EnumerateRecordT | EOSRecordT]:
    def deserializer(
        value: bytes,
    ) -> RecordT | DeleteRecordT | EnumerateRecordT | EOSRecordT:
        payload = json.loads(value.decode(encoding))
        ev_type = EventType(payload["_type"])

        class_: type[NamedTuple] = record_class
        if ev_type == EventType.DELETE:
            assert delete_record_class is not None
            class_ = delete_record_class
        elif ev_type == EventType.ENUMERATE:
            assert enumerate_record_class is not None
            class_ = enumerate_record_class
        elif ev_type == EventType.EOS:
            assert eos_record_class is not None
            class_ = eos_record_class

        fields = set(f for f in class_._fields)
        transformed = [(_FIELDS_TR.get(k, k), v) for k, v in payload.items()]
        data = {k: v for k, v in transformed if k in fields}

        for f in class_._fields:
            f_type = class_.__annotations__[f]
            if typing.get_origin(f_type) is types.UnionType:  # type: ignore
                f_type_args = typing.get_args(f_type)
                f_type = f_type_args[0]

            if f in data:
                v = data[f]
            elif f in class_._field_defaults:
                v = class_._field_defaults[f]
            else:
                raise ValueError(f"No required field: {f}")

            if f_type is datetime:
                if type(v) is int:
                    data[f] = datetime.fromtimestamp(v)
                elif type(v) is str:
                    data[f] = datetime.fromisoformat(v)
            if f_type is Decimal:
                if type(v) is str or type(v) is int:
                    data[f] = Decimal(v)
            elif f_type is EventType:
                if type(v) is str:
                    data[f] = EventType(v)

        return class_(**data)  # type: ignore

    return deserializer
