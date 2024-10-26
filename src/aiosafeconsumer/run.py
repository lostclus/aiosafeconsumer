import argparse
import asyncio
import functools
import logging
import multiprocessing
import signal
import sys
import threading
import time
from datetime import datetime
from importlib import import_module
from typing import Any

from croniter import croniter

from .workerpool import WorkerPool, WorkerPoolSettings

log = logging.getLogger(__name__)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="aiosafeconsumer worker")
    parser.add_argument(
        "--concurrency",
        "-c",
        metavar="NUMBER",
        type=int,
        default=None,
        help="Override workers concurrency",
    )
    parser.add_argument(
        "--processes",
        "-p",
        metavar="NUMBER",
        type=int,
        default=1,
        help="Run NUMBER of child processes",
    )
    parser.add_argument(
        "--task",
        "-t",
        metavar="TASK_TYPE",
        dest="only_workers",
        action="append",
        help="Run only specified tasks. To see available tasks use --list-tasks",
    )
    parser.add_argument(
        "--exclude-task",
        "-e",
        metavar="TASK_TYPE",
        dest="exclude_workers",
        action="append",
        help="Exclude tasks",
    )
    parser.add_argument(
        "--list-tasks",
        dest="list_tasks",
        action="store_true",
        help="List available tasks",
    )
    parser.add_argument(
        "--group",
        "-g",
        metavar="TASK_GROUP",
        dest="include_groups",
        action="append",
        help=(
            "Run only tasks in specified groups. To see available groups use"
            " --list-groups"
        ),
    )
    parser.add_argument(
        "--list-groups",
        dest="list_groups",
        action="store_true",
        help="List available groups",
    )
    parser.add_argument(
        "--burst",
        dest="burst",
        action="store_true",
        default=False,
        help="Run one iteration and exit",
    )
    parser.add_argument(
        "--restart-schedule",
        metavar="CRONTAB",
        dest="restart_schedule",
        default=None,
        help="Restart processes using cron-like schedule",
    )
    parser.add_argument(
        "--pool-class",
        metavar="CLASS",
        dest="pool_class",
        default=None,
        help="WorkerPool class",
    )
    parser.add_argument(
        "--pool-settings",
        metavar="CLASS",
        dest="pool_settings",
        default=None,
        help="WorkerPoolSettings class",
    )
    return parser


def _import_name(pathname: str) -> Any:
    module_name, name = pathname.rsplit(".", 1)
    module = import_module(module_name)
    value = getattr(module, name)
    return value


def _create_pool(
    pool_class: type[WorkerPool],
    pool_settings: WorkerPoolSettings,
    args: argparse.Namespace,
) -> WorkerPool:
    pool = pool_class(
        pool_settings,
        only_workers=args.only_workers,
        exclude_workers=args.exclude_workers,
        include_groups=args.include_groups,
        override_concurrency=args.concurrency,
        burst=args.burst,
    )
    return pool


def _run_sp(
    pool_class: type[WorkerPool],
    pool_settings: WorkerPoolSettings,
    args: argparse.Namespace,
    process_id: int | None = None,
) -> None:
    def handle_signal(sig_number: int, pool: WorkerPool) -> None:
        sig = signal.Signals(sig_number)
        log.info(
            f"terminating worker process #{(process_id or 0) + 1} by {sig.name}...",
        )
        pool.terminate()

    async def shutdown(loop: asyncio.AbstractEventLoop, pool: WorkerPool) -> None:
        log.info("waiting for all tasks complete...")
        pool.terminate()
        tasks = [
            task for task in asyncio.all_tasks() if task is not asyncio.current_task()
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    # logging.config.dictConfig(config.logging)
    pool = _create_pool(pool_class, pool_settings, args)
    loop = asyncio.new_event_loop()
    loop.add_signal_handler(
        signal.SIGINT, functools.partial(handle_signal, signal.SIGINT, pool)
    )
    loop.add_signal_handler(
        signal.SIGTERM, functools.partial(handle_signal, signal.SIGTERM, pool)
    )

    try:
        loop.run_until_complete(pool.run())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(shutdown(loop, pool))
    loop.close()


def _run_mp(
    pool_class: type[WorkerPool],
    pool_settings: WorkerPoolSettings,
    args: argparse.Namespace,
) -> None:
    def create_process(
        args: argparse.Namespace, process_id: int
    ) -> multiprocessing.Process:
        proc = multiprocessing.Process(
            target=_run_sp,
            args=[args, process_id],
            name=f"process-{process_id + 1}",
            daemon=True,
        )
        return proc

    def handle_signal(sig_number: int, stack: Any) -> None:
        sig = signal.Signals(sig_number)
        log.info(f"Terminating master process by {sig.name}")
        terminate.set()

    # logging.config.dictConfig(config.logging)

    if args.restart_schedule:
        restart_iter = croniter(args.restart_schedule)
        restart_at = restart_iter.get_next()
        restart_at_str = datetime.utcfromtimestamp(restart_at).isoformat()
        log.debug("Will restart at {restart_at_str} UTC")
    else:
        restart_iter = None
        restart_at = None

    terminate = threading.Event()
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    children = [
        create_process(args, process_id) for process_id in range(args.processes)
    ]

    for proc in children:
        proc.start()

    try:
        while not terminate.is_set():
            is_restart = restart_at and restart_at <= time.time()
            if is_restart:
                for process_id, proc in enumerate(children):
                    log.info(f"Restarting process #{process_id + 1} by schedule")
                    proc.terminate()
                for proc in children:
                    proc.join()

            for process_id, proc in enumerate(children):
                if not proc.is_alive():
                    if not is_restart:
                        log.info(f"Process %{process_id + 1} was died, restarting")
                    proc.close()
                    new_proc = create_process(args, process_id)
                    new_proc.start()
                    children[process_id] = new_proc

            if is_restart and restart_iter:
                while restart_at <= time.time():
                    restart_at = restart_iter.get_next()
                restart_at_str = datetime.utcfromtimestamp(restart_at).isoformat()
                log.debug(f"Will restart at {restart_at_str} UTC")
            terminate.wait(1)
    finally:
        log.info("Terminating all child processes")
        for proc in children:
            proc.terminate()
        for proc in children:
            proc.join()
            proc.close()


def _list_tasks(pool_settings: WorkerPoolSettings) -> None:
    tasks: set[str] = set()
    for worker_def in pool_settings.workers:
        tasks.add(worker_def.worker_type)
    for task in sorted(tasks):
        print(task)


def _list_groups(pool_settings: WorkerPoolSettings) -> None:
    groups: set[str] = set()
    for worker_def in pool_settings.workers:
        groups.add(worker_def.worker_group)
    for group in sorted(groups):
        print(group)


def run(
    argv: list[str] | None = None,
    pool_class: type[WorkerPool] | None = None,
    pool_settings: WorkerPoolSettings | None = None,
) -> None:
    parser = _parser()
    args = parser.parse_args(argv and argv[1:] or [])

    if args.pool_class:
        value = _import_name(args.pool_class)
        assert issubclass(value, WorkerPool)
        pool_class = value

    if args.pool_settings:
        value = _import_name(args.pool_settings)
        if callable(value):
            value = value()
        assert isinstance(value, WorkerPoolSettings)
        pool_settings = value

    assert (
        pool_class
    ), "The pool_class is required. Pass it as function argument or cli argument"
    assert (
        pool_settings
    ), "The pool_settings is required. Pass it as function argument or cli argument"

    if args.list_tasks:
        _list_tasks(pool_settings)
    elif args.list_groups:
        _list_groups(pool_settings)
    else:
        if args.processes > 1 or args.restart_schedule:
            _run_mp(pool_class, pool_settings, args)
        else:
            _run_sp(pool_class, pool_settings, args)


if __name__ == "__main__":
    run(argv=sys.argv, pool_class=WorkerPool)
