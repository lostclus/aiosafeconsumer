import asyncio
from asyncio.exceptions import CancelledError
from dataclasses import dataclass

import pytest

from aiosafeconsumer import (
    Worker,
    WorkerDef,
    WorkerPool,
    WorkerPoolSettings,
    WorkerSettings,
)


@dataclass
class AllWorkerSettings(WorkerSettings):
    counter: dict[str, int]


class BaseWorker(Worker):
    settings: AllWorkerSettings

    async def run(self, burst: bool = False) -> None:
        counter = self.settings.counter
        counter.setdefault(self.worker_type, 0)

        while True:
            counter[self.worker_type] += 1

            await asyncio.sleep(0.01)
            if burst:
                break


class Worker1(BaseWorker):
    pass


class Worker2(BaseWorker):
    pass


class Worker3(BaseWorker):
    pass


@pytest.fixture
def counter() -> dict[str, int]:
    return {}


@pytest.fixture
def worker_pool_settings(counter: dict[str, int]) -> WorkerPoolSettings:
    workers = [
        WorkerDef(
            worker_class=Worker1,
            worker_settings=AllWorkerSettings(counter=counter),
        ),
        WorkerDef(
            worker_class=Worker2,
            worker_settings=AllWorkerSettings(counter=counter),
        ),
        WorkerDef(
            worker_class=Worker3,
            worker_settings=AllWorkerSettings(counter=counter),
        ),
    ]
    return WorkerPoolSettings(workers=workers)


@pytest.mark.asyncio
async def test_worker_pool_run(
    counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    pool = WorkerPool(worker_pool_settings)

    assert counter == {}

    task = asyncio.create_task(pool.run())
    await asyncio.sleep(0.1)
    task.cancel()

    with pytest.raises(CancelledError):
        await task

    assert counter["Worker1"] > 1
    assert counter["Worker2"] > 1
    assert counter["Worker3"] > 1


@pytest.mark.asyncio
async def test_worker_pool_run_burst(
    counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    assert counter == {}

    pool = WorkerPool(worker_pool_settings, burst=True)
    await pool.run()

    assert counter == {
        "Worker1": 1,
        "Worker2": 1,
        "Worker3": 1,
    }
