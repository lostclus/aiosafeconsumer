import json
from collections.abc import Sequence
from logging import Filter, Formatter, LogRecord
from typing import Any, cast

from .logging_context import worker_id_context, worker_type_context

CONTEXT_FIELDS = {
    "worker_type": worker_type_context,
    "worker_id": worker_id_context,
}

OTHER_FIELDS = ["items_processed", "processing_time"]


class ContextInjectingFilter(Filter):
    def filter(self, record: LogRecord) -> bool:
        for field, var in CONTEXT_FIELDS.items():
            if not hasattr(record, field):
                value = var.get()
                if value is not None:
                    setattr(record, field, value)
        return True


class IsWorkerContextFilter(Filter):
    def __init__(self, invert: bool = False) -> None:
        super().__init__()
        self.invert = invert

    def filter(self, record: LogRecord) -> bool:
        result = (
            worker_type_context.get() is not None
            and worker_id_context.get() is not None
        )
        if self.invert:
            result = not result
        return result


class ExtraFieldsFormatter(Formatter):
    fields: Sequence[str]
    json_encoder = json.JSONEncoder

    def __init__(self, *args: Any, **kwargs: Any):
        self.fields = cast(
            Sequence[str],
            kwargs.pop("fields", list(CONTEXT_FIELDS.keys()) + OTHER_FIELDS),
        )
        super().__init__(**kwargs)

    def format(self, record: LogRecord) -> str:
        extra = {
            field: getattr(record, field)
            for field in self.fields
            if hasattr(record, field)
        }
        text = super().format(record)
        if extra:
            text += " " + json.dumps(extra, cls=self.json_encoder, ensure_ascii=False)
        return text
