import concurrent.futures
import logging
from contextlib import contextmanager
from datetime import timedelta
from functools import cache
from time import sleep

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.diagnostics import ServiceType
from couchbase.exceptions import (CouchbaseException, QueryErrorContext,
                                  QueryIndexNotFoundException,
                                  SearchErrorContext,
                                  SearchIndexNotFoundException)
from couchbase.management.buckets import BucketManager
from couchbase.management.search import SearchIndex
from couchbase.options import (ClusterOptions, QueryOptions, SearchOptions,
                               WaitUntilReadyOptions)
from couchbase.search import MatchNoneQuery, SearchRequest
from couchbase.vector_search import VectorQuery, VectorSearch

from ..api import VectorDB
from .config import (CouchbaseFTSIndexConfig, CouchbaseGSICVIndexConfig,
                     CouchbaseIndexConfig)

log = logging.getLogger(__name__)

class Couchbase(VectorDB):
    def __new__(
        cls,
        dim: int,
        db_config: dict,
        db_case_config: CouchbaseIndexConfig | None,
        drop_old: bool = False,
        **kwargs,
    ):
        if db_case_config.is_gsi_index:
            return GSICouchbaseClient(
                dim, db_config, db_case_config, drop_old, **kwargs
            )
        return FTSCouchbaseClient(dim, db_config, db_case_config, drop_old, **kwargs)


class CouchbaseClient(VectorDB):
    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: CouchbaseIndexConfig | None,
        drop_old: bool = False,
        **kwargs,
    ) -> None:
        self.db_config = db_config
        self.db_case_config = db_case_config
        self.dim = dim
        self.index_type = db_config.get("index_type")
        host = db_config.get("host")
        self.username = db_config.get("username")
        self.password = db_config.get("password")
        ssl_mode = db_config.get("ssl_mode")
        self.is_capella = ssl_mode == "capella"

        cb_proto = ""
        if ssl_mode in ("tls", "capella", "n2n") and "://" not in host:
            cb_proto = "couchbases://"
        elif "://" not in host:
            cb_proto = "couchbase://"

        params = ""
        if cb_proto.startswith("couchbases:") or host.startswith("couchbases:"):
            params = "?ssl=no_verify"

        self.connection_string = f"{cb_proto}{host}{params}"
        self.bucket = db_config.get("bucket")
        self.index_name = f"{self.bucket}_vector_index"

        log.debug(f"{db_case_config=}")

        self.cpu_count = self._get_cpu_count()
        if drop_old:
            self._drop_or_flush_old()

    @contextmanager
    def init(self):
        # Nothing todo here as we cant store or return cluster/collection object due to the way the runners uses the db object.
        # The runner uses multiprocces and these objects cant be pickled.
        yield {}

    def insert_embeddings(
        self, embeddings: list[list[float]], metadata: list[int], **kwargs
    ) -> tuple[int, Exception]:
        data_len = len(metadata)
        batch_size = round(data_len / self.cpu_count)
        batches = [
            (
                embeddings[start_offset : start_offset + batch_size],
                metadata[start_offset : start_offset + batch_size],
            )
            for start_offset in range(0, data_len, batch_size)
        ]
        log.debug(f"Using {len(batches)} batches of {batch_size=}")
        with concurrent.futures.ProcessPoolExecutor() as executor:
            executor.map(self.upsert_batch, batches)

        return data_len, None

    def upsert_batch(self, batch: tuple[list[list[float]], list[int]]):
        coll = self._get_cluster().bucket(self.bucket).default_collection()
        for emb, id in zip(*batch, strict=False):
            try:
                coll.upsert(f"{id}", {"id": id, "emb": emb})
            except CouchbaseException as e:
                logging.root.warn(e.message)

    def ready_to_load(self):
        pass

    def optimize(self):
        log.info(f"Creating {self.index_type} index")
        self.create_index()
        log.info(f"Waiting for index '{self.index_name}' to be ready")
        self.wait_for_index()

    @cache  # noqa
    def _get_cluster(self):
        """Helper for creating a cluster connection."""
        auth = PasswordAuthenticator(self.username, self.password)
        cluster_options = ClusterOptions(auth)
        if self.is_capella:
            cluster_options.apply_profile("wan_development")

        cluster = Cluster(self.connection_string, cluster_options)

        cluster.wait_until_ready(
            timedelta(seconds=30), WaitUntilReadyOptions(service_types=self.services)
        )
        return cluster

    async def _get_bucket_async(self):
        """Helper for creating an async cluster connection."""
        from acouchbase.cluster import Cluster
        auth = PasswordAuthenticator(self.username, self.password)
        cluster_options = ClusterOptions(auth)
        if self.is_capella:
            cluster_options.apply_profile("wan_development")

        cluster = Cluster(self.connection_string, cluster_options)
        bucket_ = cluster.bucket(self.bucket)
        await bucket_.on_connect()
        return bucket_

    def _drop_or_flush_old(self):
        cluster = self._get_cluster()
        bucket_settings = None
        try:
            # Drop index if one already exists
            log.debug("Droping index (if exists)")
            self.drop_index(cluster)

            # Due to permission, we may not be able to perform these operations through SDK
            if not self.is_capella:
                manager = BucketManager(cluster.connection)
                # Flush or recreate the bucket
                bucket_settings = manager.get_bucket(self.bucket)
                if bucket_settings.flush_enabled:
                    log.debug("Flushing bucket")
                    manager.flush_bucket(self.bucket)
                else:
                    # Create bucket with the same settings if already exists
                    log.debug(f"Dropping and recreating bucket with {bucket_settings=}")
                    manager.drop_bucket(self.bucket)
                    sleep(10)
                    manager.create_bucket(bucket_settings)

                sleep(15)
        except CouchbaseException as e:
            log.warn(e.message)
            log.debug(e)
            raise Exception(e.message) from None

    def drop_index(self, cluster: Cluster):
        # Drop index depend on the specific index type
        pass

    def create_index(self):
        pass

    def wait_for_index(self):
        pass

    def _get_cpu_count(self) -> int:
        """Get 70% of the current machine CPU."""
        try:
            import multiprocessing as mp

            return max(2, int(mp.cpu_count() * 0.7))
        except Exception:
            return 2

class FTSCouchbaseClient(CouchbaseClient):
    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: CouchbaseFTSIndexConfig | None,
        drop_old: bool = False,
        **kwargs,
    ) -> None:
        self.services = [
            ServiceType.KeyValue,
            ServiceType.Search,
        ]
        super().__init__(dim, db_config, db_case_config, drop_old, **kwargs)

    def search_embedding(
        self, query: list[float], k: int = 100, filters: dict | None = None
    ) -> list[int]:
        rows = [0]
        try:
            search_req = SearchRequest.create(MatchNoneQuery()).with_vector_search(
                VectorSearch.from_vector_query(
                    VectorQuery("emb", query, num_candidates=k)
                )
            )
            search_iter = self._get_cluster().search(
                self.index_name, search_req, SearchOptions(limit=k)
            )
            rows = [int(row.id) for row in search_iter.rows()]
        except CouchbaseException as e:
            log.warn(e.message)
            if isinstance(e.error_context, SearchErrorContext):
                log.debug(e.error_context.response_body)

        return rows

    def create_index(self):
        index = SearchIndex(
            name=self.index_name,
            source_name=self.bucket,
            params=self.db_case_config.search_index_params(self.dim),
            plan_params=self.db_case_config.plan_params(),
            source_params={},
        )
        try:
            index_manager = self._get_cluster().search_indexes()
            index_manager.upsert_index(index)
            log.debug("Index created")
        except CouchbaseException as e:
            log.warn(f"{e.message} for {index=}")

    def wait_for_index(self):
        index_manager = self._get_cluster().search_indexes()
        current_count = 0
        limit_hit = 0
        while True:
            try:
                indexed_docs = index_manager.get_indexed_documents_count(
                    self.index_name
                )
                log.debug(f"Index status: {indexed_docs=}")
                if indexed_docs > 0 and current_count == indexed_docs:
                    # Consider the index is ready if the indexed docs havent changed since last 2 checks
                    limit_hit += 1
                    if limit_hit == 2:
                        return
                else:
                    limit_hit = 0
                current_count = indexed_docs
            except CouchbaseException as e:
                log.warn(e.message)
            sleep(10)

    def drop_index(self, cluster: Cluster):
        try:
            index_manager = cluster.search_indexes()
            index_manager.drop_index(self.index_name)
        except SearchIndexNotFoundException:
            pass


class GSICouchbaseClient(CouchbaseClient):
    def __init__(
        self,
        dim: int,
        db_config: dict,
        db_case_config: CouchbaseGSICVIndexConfig | None,
        drop_old: bool = False,
        **kwargs,
    ) -> None:
        self.services = [ServiceType.KeyValue, ServiceType.Query]
        super().__init__(dim, db_config, db_case_config, drop_old, **kwargs)
        self.nprobes = db_case_config.nprobes
        self.similarity = db_case_config.similarity

    def search_embedding(
        self, query: list[float], k: int = 100, filters: dict | None = None
    ) -> list[int]:
        rows = [0]
        options = QueryOptions(timeout=timedelta(minutes=5))
        # Filters are in the form of filters={'metadata': '>=5000', 'id': 5000}
        where_clause = f"WHERE id {filters.get('metadata')}" if filters else ""
        try:
            select_query = f"SELECT meta().id from `{self.bucket}` {where_clause} ORDER BY ANN(emb, {query}, '{self.similarity}', {self.nprobes}) LIMIT {k};"
            query_result = self._get_cluster().query(select_query, options).execute()
            rows = [int(row.get("id", 0)) for row in query_result]
        except CouchbaseException as e:
            log.warn(e.message)
            if isinstance(e.error_context, QueryErrorContext):
                log.debug(e.error_context.response_body)

        return rows

    def wait_for_index(self):
        index_manager = self._get_cluster().query_indexes()
        while True:
            indexes = index_manager.get_all_indexes(self.bucket)
            if len(indexes):
                state = indexes[0].state
                log.debug(f"Index {state=}")
                if state == "online":
                    break
            sleep(10)
        sleep(300)  # extra 5min sleep
        log.debug("Index created")

    def create_index(self):
        create_index_query = self._get_create_index_statement()
        log.debug(f"Creating index: {create_index_query}")
        cluster = self._get_cluster()
        try:
            cluster.query(
                create_index_query, QueryOptions(timeout=timedelta(seconds=180))
            ).execute()
        except CouchbaseException as e:
            # Possibly a timeout, just continue and wait for the index to be ready
            log.debug(e)

    def drop_index(self, cluster: Cluster):
        try:
            index_manager = cluster.query_indexes()
            index_manager.drop_index(self.bucket, self.index_name)
        except QueryIndexNotFoundException:
            pass
        except CouchbaseException as e:
            log.debug(e)

    def _get_create_index_statement(self) -> str:
        index_params = self.db_case_config.index_param(self.dim)
        prefix = ""
        fields = "emb VECTOR"
        if self.index_type == "BHIVE":
            prefix = "VECTOR"
        if self.

        return f"CREATE {prefix} INDEX `{self.index_name}` ON `{self.bucket}`({fields}) USING GSI WITH {index_params}"
