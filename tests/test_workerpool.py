import asyncio
from asyncio.exceptions import CancelledError
from dataclasses import dataclass
from datetime import timedelta
from typing import cast
from unittest import mock

import pytest

from aiosafeconsumer import (
    Worker,
    WorkerDef,
    WorkerPool,
    WorkerPoolSettings,
    WorkerSettings,
)
from aiosafeconsumer.run import run as cli_run


@dataclass
class AllWorkerSettings(WorkerSettings):
    counter: dict[str, int]
    start_counter: dict[str, int]
    error_on: int | None = None
    exit_on: int | None = None


class BaseWorker(Worker):
    settings: AllWorkerSettings

    async def run(self, burst: bool = False) -> None:
        counter = self.settings.counter
        counter.setdefault(self.worker_type, 0)

        start_counter = self.settings.start_counter
        start_counter.setdefault(self.worker_type, 0)
        start_counter[self.worker_type] += 1

        while True:
            counter[self.worker_type] += 1
            cnt = counter[self.worker_type]

            if self.settings.error_on and cnt == self.settings.error_on:
                raise Exception("Test error")
            if self.settings.exit_on and cnt == self.settings.exit_on:
                return

            await asyncio.sleep(0.001)
            if burst:
                break


class Worker1(BaseWorker):
    worker_type = "worker1"


class Worker2(BaseWorker):
    worker_type = "worker2"


class Worker3(BaseWorker):
    worker_type = "worker3"


@pytest.fixture
def counter() -> dict[str, int]:
    return {}


@pytest.fixture
def start_counter() -> dict[str, int]:
    return {}


@pytest.fixture
def worker_pool_settings(
    counter: dict[str, int], start_counter: dict[str, int]
) -> WorkerPoolSettings:
    workers = [
        WorkerDef(
            worker_class=Worker1,
            worker_settings=AllWorkerSettings(
                counter=counter,
                start_counter=start_counter,
            ),
        ),
        WorkerDef(
            worker_class=Worker2,
            worker_settings=AllWorkerSettings(
                counter=counter,
                start_counter=start_counter,
            ),
        ),
        WorkerDef(
            worker_class=Worker3,
            worker_settings=AllWorkerSettings(
                counter=counter,
                start_counter=start_counter,
            ),
            worker_group="worker3",
        ),
    ]
    return WorkerPoolSettings(workers=workers)


@pytest.mark.asyncio
async def test_worker_pool_run(
    counter: dict[str, int],
    start_counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    pool = WorkerPool(worker_pool_settings)

    assert counter == {}

    task = asyncio.create_task(pool.run())
    await asyncio.sleep(0.1)
    task.cancel()

    with pytest.raises(CancelledError):
        await task

    assert counter["worker1"] > 10
    assert counter["worker2"] > 10
    assert counter["worker3"] > 10

    assert start_counter["worker1"] == 1
    assert start_counter["worker2"] == 1
    assert start_counter["worker3"] == 1


@pytest.mark.asyncio
async def test_worker_pool_run_with_concurrency(
    counter: dict[str, int],
    start_counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    pool = WorkerPool(worker_pool_settings, override_concurrency=4)

    assert counter == {}

    task = asyncio.create_task(pool.run())
    await asyncio.sleep(0.1)
    task.cancel()

    with pytest.raises(CancelledError):
        await task

    assert counter["worker1"] > 10
    assert counter["worker2"] > 10
    assert counter["worker3"] > 10

    assert start_counter["worker1"] == 4
    assert start_counter["worker2"] == 4
    assert start_counter["worker3"] == 4


@pytest.mark.asyncio
async def test_worker_pool_run_with_error(
    counter: dict[str, int],
    start_counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    worker_settings = cast(
        AllWorkerSettings, worker_pool_settings.workers[0].worker_settings
    )
    worker_settings.error_on = 2
    pool = WorkerPool(worker_pool_settings, sleep_on_task_exit=timedelta(seconds=0.001))

    assert counter == {}

    task = asyncio.create_task(pool.run())
    await asyncio.sleep(0.1)
    task.cancel()

    with pytest.raises(CancelledError):
        await task

    assert counter["worker1"] > 10
    assert counter["worker2"] > 10
    assert counter["worker3"] > 10

    assert start_counter["worker1"] == 2
    assert start_counter["worker2"] == 1
    assert start_counter["worker3"] == 1


@pytest.mark.asyncio
async def test_worker_pool_run_with_exit(
    counter: dict[str, int],
    start_counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    worker_settings = cast(
        AllWorkerSettings, worker_pool_settings.workers[0].worker_settings
    )
    worker_settings.exit_on = 2
    pool = WorkerPool(worker_pool_settings, sleep_on_task_exit=timedelta(seconds=0.001))

    assert counter == {}

    task = asyncio.create_task(pool.run())
    await asyncio.sleep(0.1)
    task.cancel()

    with pytest.raises(CancelledError):
        await task

    assert counter["worker1"] > 10
    assert counter["worker2"] > 10
    assert counter["worker3"] > 10

    assert start_counter["worker1"] == 2
    assert start_counter["worker2"] == 1
    assert start_counter["worker3"] == 1


@pytest.mark.asyncio
async def test_worker_pool_run_terminate(
    counter: dict[str, int],
    start_counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    pool = WorkerPool(worker_pool_settings)

    assert counter == {}

    task = asyncio.create_task(pool.run())
    await asyncio.sleep(0.1)
    pool.terminate()
    await task

    assert counter["worker1"] > 10
    assert counter["worker2"] > 10
    assert counter["worker3"] > 10

    assert start_counter["worker1"] == 1
    assert start_counter["worker2"] == 1
    assert start_counter["worker3"] == 1


@pytest.mark.asyncio
async def test_worker_pool_run_burst(
    counter: dict[str, int],
    start_counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    assert counter == {}

    pool = WorkerPool(worker_pool_settings, burst=True)
    await pool.run()

    assert counter == {
        "worker1": 1,
        "worker2": 1,
        "worker3": 1,
    }

    assert start_counter["worker1"] == 1
    assert start_counter["worker2"] == 1
    assert start_counter["worker3"] == 1


def test_cli_list_tasks(worker_pool_settings: WorkerPoolSettings) -> None:
    with mock.patch("aiosafeconsumer.run.print") as print_m:
        cli_run(
            ["test", "--list-tasks"],
            pool_class=WorkerPool,
            pool_settings=worker_pool_settings,
        )

        assert print_m.mock_calls == [
            mock.call("worker1"),
            mock.call("worker2"),
            mock.call("worker3"),
        ]


def test_cli_list_groups(worker_pool_settings: WorkerPoolSettings) -> None:
    with mock.patch("aiosafeconsumer.run.print") as print_m:
        cli_run(
            ["test", "--list-groups"],
            pool_class=WorkerPool,
            pool_settings=worker_pool_settings,
        )

        assert print_m.mock_calls == [
            mock.call("default"),
            mock.call("worker3"),
        ]


def test_cli_run(
    counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    cli_run(
        ["test", "--burst"],
        pool_class=WorkerPool,
        pool_settings=worker_pool_settings,
    )

    assert counter == {
        "worker1": 1,
        "worker2": 1,
        "worker3": 1,
    }


def test_cli_run_task(
    counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    cli_run(
        ["test", "--burst", "--task=worker1"],
        pool_class=WorkerPool,
        pool_settings=worker_pool_settings,
    )

    assert counter == {
        "worker1": 1,
    }


def test_cli_run_exclude_task(
    counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    cli_run(
        ["test", "--burst", "--exclude-task=worker1"],
        pool_class=WorkerPool,
        pool_settings=worker_pool_settings,
    )

    assert counter == {
        "worker2": 1,
        "worker3": 1,
    }


def test_cli_run_group(
    counter: dict[str, int],
    worker_pool_settings: WorkerPoolSettings,
) -> None:
    cli_run(
        ["test", "--burst", "--group=worker3"],
        pool_class=WorkerPool,
        pool_settings=worker_pool_settings,
    )

    assert counter == {
        "worker3": 1,
    }
