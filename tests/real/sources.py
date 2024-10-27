from aiosafeconsumer.kafka import KafkaSource, KafkaSourceSettings

from .types import User


class UsersKafkaSourceSettings(KafkaSourceSettings[User]):
    pass


class UsersKafkaSource(KafkaSource[User]):
    settings: UsersKafkaSourceSettings
