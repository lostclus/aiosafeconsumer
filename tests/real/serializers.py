import json
import types
import typing
from collections.abc import Callable
from datetime import datetime
from decimal import Decimal
from typing import NamedTuple

from .constants import ENCODING, FIELD_TRANSLATION

_FIELDS_TR = dict([(obj_f, bus_f) for bus_f, obj_f in FIELD_TRANSLATION])


def namedtuple_to_json_serializer(
    encoding: str = ENCODING,
    ensure_ascii: bool = False,
) -> Callable[[NamedTuple], bytes]:
    def serializer(obj: NamedTuple) -> bytes:
        payload = obj._asdict()

        for f in obj._fields:
            f_type = obj.__annotations__[f]
            if typing.get_origin(f_type) is types.UnionType:
                f_type = typing.get_args(f_type)[0]

            v = payload[f]
            if f_type is datetime:
                if isinstance(v, datetime):
                    payload[f] = v.isoformat()
            if f_type is Decimal:
                if isinstance(v, Decimal):
                    payload[f] = str(v)

        transformed = {_FIELDS_TR.get(k, k): v for k, v in payload.items()}
        return json.dumps(transformed, ensure_ascii=ensure_ascii).encode(encoding)

    return serializer
