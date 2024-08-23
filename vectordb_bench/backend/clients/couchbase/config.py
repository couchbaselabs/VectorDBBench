from pydantic import SecretStr

from ..api import DBCaseConfig, DBConfig


class CouchbaseConfig(DBConfig):
    host: str = "localhost"
    bucket: str = "bucket-1"
    username: SecretStr = SecretStr("Administrator")
    password: SecretStr = SecretStr("password")

    def to_dict(self) -> dict:
        return {
            "host": self.host,
            "bucket": self.bucket,
            "username": self.username.get_secret_value(),
            "password": self.password.get_secret_value(),
        }


class CouchbaseCapellaConfig(DBCaseConfig):
    pass