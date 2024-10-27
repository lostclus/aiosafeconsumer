from dataclasses import dataclass

from aiosafeconsumer import ConsumerWorker, ConsumerWorkerSettings

from .types import User


@dataclass
class UsersWorkerSettings(ConsumerWorkerSettings[User]):
    pass


class UsersWorker(ConsumerWorker):
    worker_type = "sync_users"
