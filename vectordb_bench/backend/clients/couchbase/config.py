from pydantic import SecretStr

from ..api import DBCaseConfig, DBConfig


class CouchbaseConfig(DBConfig):
    host: str = "localhost"
    bucket: str = "bucket-1"
    username: SecretStr = SecretStr("Administrator")
    password: SecretStr = SecretStr("password")
    ssl_mode: str = "none"

    def to_dict(self) -> dict:
        return {
            "host": self.host,
            "bucket": self.bucket,
            "username": self.username.get_secret_value(),
            "password": self.password.get_secret_value(),
            "ssl_mode": self.ssl_mode,
        }


class CouchbaseCapellaConfig(DBCaseConfig):
    pass