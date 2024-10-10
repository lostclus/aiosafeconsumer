import asyncio
import logging
from collections.abc import Sequence
from dataclasses import dataclass

from .worker import Worker, WorkerSettings

log = logging.getLogger(__name__)

DEFAULT_GROUP = "default"


@dataclass
class WorkerDef:
    worker_class: type[Worker]
    worker_settings: WorkerSettings
    worker_group: str = DEFAULT_GROUP
    concurrency: int = 1


@dataclass
class WorkerPoolSettings:
    workers: Sequence[WorkerDef]


class WorkerPool:
    settings: WorkerPoolSettings
    worker_defs: dict[str, WorkerDef]

    def __init__(
        self,
        settings: WorkerPoolSettings,
        include_groups: Sequence[str] | None = None,
        exclude_types: Sequence[str] | None = None,
        burst: bool = False,
    ) -> None:
        self.settings = settings
        self.include_groups = include_groups
        self.exclude_types = exclude_types
        self.burst = burst
        self.worker_defs = {}
        self._interrupt = asyncio.Event()

    def _filter_workers(self) -> None:
        self.worker_defs = {}

        for worker_def in self.settings.workers:
            if (
                self.include_groups is not None
                and worker_def.worker_group not in self.include_groups
            ):
                log.debug(
                    f"Skipping {worker_def} because worker group not in include groups"
                )
                continue

            worker_type = worker_def.worker_class.worker_type

            if self.exclude_types is not None and worker_type in self.exclude_types:
                log.debug(f"Skipping {worker_def} because worker type in exclude types")
                continue

            assert worker_type not in self.worker_defs
            self.worker_defs[worker_type] = worker_def

    async def _run_workers(self) -> None:
        pool: dict[str, list[asyncio.Task | None]] = {}
        run_count: dict[str, list[int]] = {}

        for worker_type, worker_def in self.worker_defs.items():
            pool[worker_type] = [None] * worker_def.concurrency
            run_count[worker_type] = [0] * worker_def.concurrency

        while True:
            has_done_task = False
            has_run_task = False

            for worker_type, worker_def in self.worker_defs.items():
                concurrency = len(pool[worker_type])
                for worker_num in range(concurrency):
                    task = pool[worker_type][worker_num]
                    if task is None:
                        continue

                    if task.done():
                        error = task.exception()
                        if error is not None:
                            log.error(
                                "Error in worker %s[#%d]: %s",
                                worker_type,
                                worker_num,
                                str(error),
                                exc_info=error,
                            )
                            # sentry_sdk.capture_exception(error)
                        else:
                            if not self.burst:
                                log.error(
                                    "Worker %s[#%d] was exited unexpectedly",
                                    worker_type,
                                    worker_num,
                                )
                        pool[worker_type][worker_num] = None
                        has_done_task = True
                    else:
                        has_run_task = True

            if has_done_task and not self.burst:
                await asyncio.sleep(10)

            for worker_type, worker_def in self.worker_defs.items():
                concurrency = len(pool[worker_type])
                for worker_num in range(concurrency):
                    task = pool[worker_type][worker_num]

                    if self._interrupt.is_set():
                        if task is not None:
                            log.info(f"Terminating worker {worker_type}[#{worker_num}]")
                            task.cancel()
                    else:
                        if task is None:
                            no_start = (
                                self.burst and run_count[worker_type][worker_num] > 0
                            )
                            if not no_start:
                                log.info(
                                    f"Starting worker {worker_type}[#{worker_num}]"
                                )
                                kw = {}
                                if self.burst:
                                    kw["burst"] = True
                                task = asyncio.create_task(
                                    worker_def.worker_class(
                                        worker_def.worker_settings
                                    ).run(**kw)
                                )
                                pool[worker_type][worker_num] = task
                                run_count[worker_type][worker_num] += 1
                                has_run_task = True

            if self.burst and not has_run_task:
                self._interrupt.set()

            all_tasks = [
                task for task in sum(list(pool.values()), []) if task is not None
            ]

            if self._interrupt.is_set():
                await asyncio.gather(*all_tasks, return_exceptions=True)
                break

            await asyncio.wait(
                all_tasks + [asyncio.create_task(self._interrupt.wait())],
                return_when=asyncio.FIRST_COMPLETED,
            )

    async def run(self) -> None:
        self._filter_workers()
        await self._run_workers()

    def terminate(self) -> None:
        self._interrupt.set()
