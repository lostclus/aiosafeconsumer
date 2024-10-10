from abc import ABC
from dataclasses import dataclass
from typing import Any

from .logging_context import worker_id_context, worker_type_context

_workers_count = 0


@dataclass
class WorkerSettings:
    pass


class Worker(ABC):
    settings: WorkerSettings
    worker_type: str
    worker_id: str

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.worker_type = cls.__name__

    def __init__(self, settings: WorkerSettings | None) -> None:
        global _workers_count

        _workers_count += 1

        self.settings = settings or WorkerSettings()
        self.worker_id = str(_workers_count)

    def __str__(self) -> str:
        return f"{self.worker_type}[{self.worker_id}]"

    def setup_logging_context(self) -> None:
        if self.worker_type:
            worker_type_context.set(self.worker_type)
        if self.worker_id:
            worker_id_context.set(self.worker_id)

    async def run(self, burst: bool = False) -> None:
        raise NotImplementedError
