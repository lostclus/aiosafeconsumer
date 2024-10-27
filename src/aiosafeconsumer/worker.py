import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from .logging_context import worker_id_context, worker_type_context


@dataclass
class WorkerSettings:
    pass


class Worker(ABC):
    settings: WorkerSettings
    worker_type: str
    worker_id: str

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if not hasattr(cls, "worker_type"):
            cls.worker_type = f"{cls.__module__}.{cls.__name__}"

    def __init__(self, settings: WorkerSettings | None) -> None:
        self.settings = settings or WorkerSettings()
        self.worker_id = str(uuid.uuid4())[:8]

    def __str__(self) -> str:
        return f"{self.worker_type}-{self.worker_id}"

    def setup_logging_context(self) -> None:
        if self.worker_type:
            worker_type_context.set(self.worker_type)
        if self.worker_id:
            worker_id_context.set(self.worker_id)

    @abstractmethod
    async def run(self, burst: bool = False) -> None:  # pragma: no cover
        pass
