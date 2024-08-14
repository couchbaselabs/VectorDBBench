from enum import Enum

from pydantic import SecretStr

from vectordb_bench.base import BaseModel

from ..api import DBCaseConfig, DBConfig, MetricType


class CouchbaseConfig(DBConfig):
    host: str = "localhost"
    bucket: str = "bucket-1"
    username: SecretStr = SecretStr("Administrator")
    password: SecretStr = SecretStr("password")
    ssl_mode: str = "none"
    index_type: str = "FTS"

    def to_dict(self) -> dict:
        return {
            "host": self.host,
            "bucket": self.bucket,
            "username": self.username.get_secret_value(),
            "password": self.password.get_secret_value(),
            "ssl_mode": self.ssl_mode,
            "index_type": self.index_type.upper(),
        }


class CouchbaseIndexType(str, Enum):
    FTS = "FTS"
    CVI = "CVI"
    BHVI = "BHVI"


class CouchbaseIndexConfig(BaseModel, DBCaseConfig):
    metric_type: MetricType = MetricType.L2
    is_gsi_index: bool = False

    def parse_metric(self) -> str:
        if self.metric_type == MetricType.L2:
            return "l2_norm"
        elif self.metric_type == MetricType.IP:
            return "dot"
        return "cosine"

    def index_param(self) -> dict:
        return {}

    def search_param(self) -> dict:
        return {}


class CouchbaseFTSIndexConfig(CouchbaseIndexConfig):
    index_partitions: int = 20
    max_partitions: int = 52

    def plan_params(self):
        return {
            "indexPartitions": self.index_partitions,
            "maxPartitionsPerPIndex": self.max_partitions,
        }

    def search_index_params(self, dim: int):
        return {
            "doc_config": {
                "mode": "type_field",
                "type_field": "type",
            },
            "store": {
                "indexType": "scorch",
                "segmentVersion": 16,
            },
            "mapping": {
                "default_type": "_default",
                "default_analyzer": "standard",
                "default_datetime_parser": "dateTimeOptional",
                "default_field": "_all",
                "store_dynamic": False,
                "index_dynamic": True,
                "type_field": "_type",
                "default_mapping": {
                    "dynamic": False,
                    "enabled": True,
                    "properties": {
                        "emb": {
                            "dynamic": False,
                            "enabled": True,
                            "fields": [
                                {
                                    "dims": dim,
                                    "index": True,
                                    "name": "emb",
                                    "similarity": "l2_norm",
                                    "type": "vector",
                                }
                            ],
                        }
                    },
                },
            },
        }


class CouchbaseGSICVIndexConfig(CouchbaseIndexConfig):
    is_gsi_index: bool = True
    # GSI configuration
    nprobes: int = 1
    train_list: int = 10000
    description: str = "IVF,SQ8"

    def parse_metric(self) -> str:
        return self.metric_type.value

    def index_param(self, dim: int = 128) -> dict:
        return {
            "dimension": dim,
            "description": self.description,
            "similarity": "L2",
        }


class CouchbaseGSIBHIndexConfig(CouchbaseGSICVIndexConfig):
    pass


_couchbase_index_config = {
    CouchbaseIndexType.FTS: CouchbaseFTSIndexConfig,
    CouchbaseIndexType.CVI: CouchbaseGSICVIndexConfig,
    CouchbaseIndexType.BHVI: CouchbaseGSIBHIndexConfig,
}
