from contextvars import ContextVar

VAR_PREFIX = "aiosafeconsumer."

worker_type_context: ContextVar[str] = ContextVar(
    f"{VAR_PREFIX}worker_type",
    default="worker",
)
worker_id_context: ContextVar[str] = ContextVar(
    f"{VAR_PREFIX}worker_id",
    default="1",
)
