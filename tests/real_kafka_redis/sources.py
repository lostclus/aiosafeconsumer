from aiosafeconsumer.kafka import KafkaSource, KafkaSourceSettings

from .types import User


class UsersSourceSettings(KafkaSourceSettings[User]):
    pass


class UsersSource(KafkaSource[User]):
    settings: UsersSourceSettings
