from contextvars import ContextVar

VAR_PREFIX = "aiosafeconsumer."

worker_type_context: ContextVar[str | None] = ContextVar(
    f"{VAR_PREFIX}worker_type",
    default=None,
)
worker_id_context: ContextVar[str | None] = ContextVar(
    f"{VAR_PREFIX}worker_id",
    default=None,
)
