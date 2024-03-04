import logging
from contextlib import contextmanager
from datetime import timedelta
from functools import cache
from time import sleep

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import SearchIndexNotFoundException
from couchbase.management.buckets import BucketManager
from couchbase.management.search import SearchIndex
from couchbase.options import ClusterOptions, SearchOptions, UpsertMultiOptions
from couchbase.search import MatchAllQuery, SearchRequest
from couchbase.vector_search import VectorQuery, VectorSearch

from vectordb_bench.backend.clients.api import DBCaseConfig

from ..api import VectorDB

log = logging.getLogger(__name__)

@cache
def get_cluster(conn_string, username, password):
    auth = PasswordAuthenticator(username, password)
    cluster_options = ClusterOptions(auth)
    cluster = Cluster(conn_string, cluster_options)
    cluster.wait_until_ready(timedelta(seconds=10))
    return cluster


class Couchbase(VectorDB):
    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: DBCaseConfig | None,
        drop_old: bool = False,
        **kwargs,
    ) -> None:
        self.db_config = db_config
        self.db_case_config = db_case_config
        self.dim = dim
        host = db_config.get("host")
        self.username = db_config.get("username")
        self.password = db_config.get("password")

        cb_proto = ""
        if "://" not in host:
            cb_proto = "couchbase://"

        self.connection_string = "{}{}".format(cb_proto, host)
        self.bucket = db_config.get("bucket")
        self.batch_size = 100  # TODO
        self.docs_count = 0

        self.index_name = f"{self.bucket}_vector_index"

        if drop_old:
            self._drop_or_flush_old()

    @contextmanager
    def init(self):
        # Nothing todo here as we cant store or return cluster/collection object due to the way the runners uses the db object.
        # The serial runner uses multiprocces and these objects cant be pickled.
        yield {}

    def insert_embeddings(
        self, embeddings: list[list[float]], metadata: list[int], **kwargs
    ) -> tuple[int, Exception]:
        assert len(embeddings) == len(metadata)
        self.docs_count += len(embeddings)
        insert_count = 0
        try:
            upsert_options = UpsertMultiOptions(
                return_exceptions=False, timeout=timedelta(seconds=5)
            )
            coll = self._get_cluster().bucket(self.bucket).default_collection()
            for start_offset in range(0, len(embeddings), self.batch_size):
                end_offset = start_offset + self.batch_size
                emb_batch = embeddings[start_offset:end_offset]
                meta_batch = metadata[start_offset:end_offset]
                batch_data = {
                    f"{meta}": {"id": meta, "emb": emb}
                    for emb, meta in zip(emb_batch, meta_batch, strict=False)
                }
                coll.upsert_multi(batch_data, upsert_options)
                insert_count += self.batch_size
        except Exception as e:
            log.debug(f"Couchbase: {e}")
            return insert_count, e

        return insert_count, None

    def search_embedding(
        self, query: list[float], k: int = 100, filters: dict | None = None
    ) -> list[int]:
        rows = [0]
        try:
            search_req = SearchRequest.create(MatchAllQuery()).with_vector_search(
                VectorSearch.from_vector_query(
                    VectorQuery("emb", query, num_candidates=k)
                )
            )
            search_iter = self._get_cluster().search(
                self.index_name, search_req, SearchOptions(limit=k)
            )
            rows = [int(row.id) for row in search_iter.rows()]
        except Exception as e:
            log.debug(f"Couchbase: {e}")

        return rows

    def optimize(self):
        log.info("Couchbase: waiting for docs processed by the index")
        self._wait_for_index_processed_docs()

    def ready_to_load(self):
        pass

    # --------Couchbase helpers------

    @cache  # noqa
    def _get_cluster(self):
        """Helper for creating a cluster connection."""
        auth = PasswordAuthenticator(self.username, self.password)
        cluster_options = ClusterOptions(auth)
        cluster = Cluster(self.connection_string, cluster_options)
        cluster.wait_until_ready(timedelta(seconds=10))
        return cluster

    def _create_search_index(self):
        index = SearchIndex(
            name=self.index_name,
            source_name=self.bucket,
            params=self._get_search_index_params(),
            plan_params=self._get_search_index_plan_params(),
            source_params={},
        )

        index_manager = self._get_cluster().search_indexes()
        index_manager.upsert_index(index)
        log.debug("Couchbase: Index created")

    def _wait_for_index_processed_docs(self):
        index_manager = self._get_cluster().search_indexes()
        while True:
            indexed_docs = index_manager.get_indexed_documents_count(self.index_name)
            log.debug(f"Indexed docs {indexed_docs}")
            if indexed_docs >= self.docs_count:
                return
            sleep(30)

    def _drop_or_flush_old(self):
        log.debug("Couchbase: Droping bucket and recreating it")
        cluster = self._get_cluster()
        bucket_settings = None
        try:
            # Drop index if already exists
            index_manager = cluster.search_indexes()
            try:
                index_manager.drop_index(self.index_name)
            except SearchIndexNotFoundException:
                pass

            manager = BucketManager(cluster.connection)
            # Flush or recreate the bucket
            bucket_settings = manager.get_bucket(self.bucket)
            if bucket_settings.flush_enabled:
                manager.flush_bucket(self.bucket)
            else:
                # Create bucket with the same settings if already exists
                log.debug(f"Bucket settings: {bucket_settings}")
                manager.drop_bucket(self.bucket)
                sleep(10)
                manager.create_bucket(bucket_settings)

            sleep(15)
            self._create_search_index()
        except Exception as e:
            log.warn(f"Couchbase: {e}")
            raise Exception(e) from None

    def _get_search_index_params(self):
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
                                    "dims": self.dim,
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

    def _get_search_index_plan_params(self):
        return {
            "indexPartitions": 20,
            "maxPartitionsPerPIndex": 52,
        }
