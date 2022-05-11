#!/usr/bin/env python
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, Future
import functools
from typing import Union, TYPE_CHECKING
import elasticapm
import time
import os

from datemath import dm

from assemblyline.common.isotime import epoch_to_iso, now_as_iso
from assemblyline_core.server_base import ServerBase
from assemblyline.common import forge
from assemblyline.common.metrics import MetricsFactory
from assemblyline.filestore import FileStore
from assemblyline.odm.messages.expiry_heartbeat import Metrics

if TYPE_CHECKING:
    from assemblyline.datastore.collection import ESCollection


ARCHIVE_SIZE = 5000
EXPIRY_SIZE = 10000
DAY_SECONDS = 24 * 60 * 60
WORKERS = int(os.environ.get('EXPIRY_WORKERS', '10'))
MAX_TASKS = 20


class ExpiryManager(ServerBase):
    def __init__(self, force_ilm=False):
        self.config = forge.get_config()
        if force_ilm:
            self.config.datastore.ilm.enabled = True

        super().__init__('assemblyline.expiry', shutdown_timeout=self.config.core.expiry.sleep_time + 5)
        self.datastore = forge.get_datastore(config=self.config, archive_access=True)
        self.hot_datastore = forge.get_datastore(config=self.config, archive_access=False)
        self.filestore = forge.get_filestore(config=self.config)
        self.cachestore = FileStore(*self.config.filestore.cache)
        self.expirable_collections: list[ESCollection] = []
        self.archiveable_collections: list[ESCollection] = []
        self.counter = MetricsFactory('expiry', Metrics)
        self.counter_archive = MetricsFactory('archive', Metrics)

        if self.config.datastore.ilm.enabled:
            self.fs_hashmap = {
                'file': self.archive_filestore_delete,
                'cached_file': self.archive_cachestore_delete
            }
        else:
            self.fs_hashmap = {
                'file': self.filestore_delete,
                'cached_file': self.cachestore_delete
            }

        for name, definition in self.datastore.ds.get_models().items():
            if hasattr(definition, 'archive_ts'):
                self.archiveable_collections.append(getattr(self.datastore, name))
            if hasattr(definition, 'expiry_ts'):
                self.expirable_collections.append(getattr(self.datastore, name))

        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="expiry")
        else:
            self.apm_client = None

    def stop(self):
        if self.counter:
            self.counter.stop()

        if self.apm_client:
            elasticapm.uninstrument()
        super().stop()

    def log_errors(self, function):
        @functools.wraps(function)
        def _func(*args, **kwargs):
            try:
                function(*args, **kwargs)
            except Exception:
                self.log.exception("Error in expiry worker")
        return _func

    def filestore_delete(self, sha256, _):
        self.filestore.delete(sha256)

    def archive_filestore_delete(self, sha256, expiry_time):
        # If we are working with an archive, their may be a hot entry that expires later.
        doc = self.hot_datastore.file.get_if_exists(sha256, as_obj=False)
        if doc and doc['expiry_ts'] > expiry_time:
            return
        self.filestore.delete(sha256)

    def cachestore_delete(self, sha256, _expiry_time):
        self.filestore.delete(sha256)

    def archive_cachestore_delete(self, sha256, expiry_time):
        doc = self.hot_datastore.cached_file.get_if_exists(sha256, as_obj=False)
        if doc and doc['expiry_ts'] > expiry_time:
            return
        self.cachestore.delete(sha256)

    def _finish_delete(self, collection, delete_query, number_to_delete, tasks: list[Future]):
        for future in tasks:
            future.result()
        self.log.info(f'    Deleted associated files from the '
                      f'{"cachestore" if "cache" in collection.name else "filestore"}...')
        self._simple_delete(collection, delete_query, number_to_delete)

    def _simple_delete(self, collection, delete_query, number_to_delete):
        self.heartbeat()
        collection.delete_by_query(delete_query, workers=self.config.core.expiry.workers)
        self.counter.increment(f'{collection.name}', increment_by=number_to_delete)
        self.log.info(f"    Deleted {number_to_delete} items from the datastore...")

    def run_expiry_once(self, pool: ThreadPoolExecutor):
        now = now_as_iso()
        reached_max = False

        # Expire data
        for collection in self.expirable_collections:
            # Call heartbeat pre-dated by 5 minutes. If a collection takes more than
            # 5 minutes to expire, this container could be seen as unhealthy. The down
            # side is if it is stuck on something it will be more than 5 minutes before
            # the container is restarted.
            self.heartbeat(int(time.time() + 5 * 60))

            # Start of expiry transaction
            if self.apm_client:
                self.apm_client.begin_transaction("Delete expired documents")

            if self.config.core.expiry.batch_delete:
                final_date = dm(f"{now}||-{self.config.core.expiry.delay}h/d").float_timestamp
            else:
                final_date = dm(f"{now}||-{self.config.core.expiry.delay}h").float_timestamp
            final_date_string = epoch_to_iso(final_date)

            # Break down the expiry window into smaller chunks of data
            unchecked_chunks: list[tuple[float, float]] = [(0, final_date)]
            ready_chunks: dict[tuple[float, float], int] = {}
            while unchecked_chunks and len(ready_chunks) < MAX_TASKS:
                start, end = unchecked_chunks.pop(0)
                chunk_size = self._count_expired(collection, start, end)

                # Empty chunks are fine
                if chunk_size == 0:
                    continue

                # We found a small enough chunk to run on
                if chunk_size < EXPIRY_SIZE:
                    ready_chunks[(start, end)] = chunk_size
                    continue

                # Break this chunk into parts
                if start == 0:
                    middle = end - DAY_SECONDS
                else:
                    middle = (end + start)/2
                unchecked_chunks.append((middle, end))
                unchecked_chunks.append((start, middle))

            # If there are still chunks we haven't checked, then we know there is more data
            if unchecked_chunks:
                reached_max = True

            for (start, end), number_to_delete in ready_chunks.items():
                # We assume that no records are ever inserted such that their expiry_ts is in the past.
                # We also assume that the `end` dates are also in the past.
                # As long as these two things are true, the set returned by this query should be consistent.
                # The one race condition is that a record might be refreshed while the file
                # blob would be deleted anyway, leaving a file record with no filestore object
                self.log.info(f"Processing collection: {collection.name}")
                delete_query = f"expiry_ts:[{epoch_to_iso(start) if start > 0 else '*'} TO {epoch_to_iso(end)}}}"

                # check if we are dealing with an index that needs file cleanup
                if self.config.core.expiry.delete_storage and collection.name in self.fs_hashmap:
                    # Delete associated files
                    delete_tasks: list[Future] = []
                    for item in collection.search(delete_query, fl='id', use_archive=True, as_obj=False)['items']:
                        delete_tasks.append(pool.submit(self.fs_hashmap[collection.name],
                                                        item['id'], final_date_string))

                    # Proceed with deletion, but only after all the scheduled deletes for this
                    self.log.info(f"Scheduled {len(delete_tasks)}/{number_to_delete} "
                                  f"files to be removed for: {collection.name}")
                    pool.submit(self.log_errors(self._finish_delete), collection,
                                delete_query, number_to_delete, delete_tasks)

                else:
                    # Proceed with deletion
                    pool.submit(self.log_errors(self._simple_delete),
                                collection, delete_query, number_to_delete)

            # End of expiry transaction
            if self.apm_client:
                self.apm_client.end_transaction(collection.name, 'deleted')

        return reached_max

    def run_archive_once(self, pool: ThreadPoolExecutor):
        reached_max = False
        if not self.config.datastore.ilm.enabled:
            return reached_max

        # Start of expiry transaction
        if self.apm_client:
            self.apm_client.begin_transaction("Archive older documents")

        # Collect a set of jobs which archive all the collections in
        # managable sized queries. Breaking up the query is helpful
        # where each of our archive calls are actually a reindex
        # followed by a delete, which we would like to both complete
        # relatively close togeather.
        for collection in self.archiveable_collections:
            self.heartbeat(int(time.time() + 60))
            reached_max |= self._archive_collection(collection, pool)

        # End of expiry transaction
        if self.apm_client:
            self.apm_client.end_transaction(result='archived')

        return reached_max

    def _find_archive_start(self, container: ESCollection):
        """
        Moving backwards one day at a time get a rough idea where archiveable material
        in the datastore starts.
        """
        now = time.time()
        offset = 1
        while True:
            count = self._count_archivable(container, "*", now - offset * DAY_SECONDS)
            if count == 0:
                return now - offset * DAY_SECONDS
            offset += 1

    def _count_expired(self, container: ESCollection, start: Union[float, str], end: float) -> int:
        """Count how many items need to be erased in the given window."""
        if start == 0:
            start = '*'
        if isinstance(start, (float, int)):
            start = epoch_to_iso(start)
        query = f'expiry_ts:[{start} TO {epoch_to_iso(end)}}}'
        return container.search(query, rows=0, as_obj=False, use_archive=True, track_total_hits=EXPIRY_SIZE)['total']

    def _count_archivable(self, container: ESCollection, start: Union[float, str], end: float) -> int:
        """Count how many items need to be archived in the given window."""
        if isinstance(start, (float, int)):
            start = epoch_to_iso(start)
        query = f'archive_ts:[{start} TO {epoch_to_iso(end)}}}'
        return container.search(query, rows=0, as_obj=False, use_archive=False, track_total_hits=ARCHIVE_SIZE)['total']

    def _archive_collection(self, collection: ESCollection, pool: ThreadPoolExecutor) -> bool:
        # Start with archiving everything up until now
        chunks: list[tuple[float, float]] = [(self._find_archive_start(collection), time.time())]
        futures: list[Future] = []

        while len(chunks) > 0 and len(futures) < MAX_TASKS:
            # Take the next chunk, and figure out how many records it covers
            start, end = chunks.pop(0)
            count = self._count_archivable(collection, start, end)

            # Chunks that are fully archived are fine to skip
            if count == 0:
                continue

            # If the chunk is bigger than the number we intend to archive
            # in a single call break it into parts
            if count >= ARCHIVE_SIZE:
                middle = (start + end)/2
                chunks.append((start, middle))
                chunks.append((middle, end))
                continue

            # Schedule the chunk of data to be archived
            @self.log_errors
            def archive_chunk(query, expected):
                self.heartbeat()
                if collection.archive(query):
                    self.counter_archive.increment(f'{collection.name}', increment_by=expected)
                else:
                    self.log.error("Failed to archive range {query}")

            query = f'archive_ts:[{epoch_to_iso(start)} TO {epoch_to_iso(end)}}}'
            futures.append(pool.submit(archive_chunk, query, count))

        return len(futures) == MAX_TASKS

    def try_run(self):
        while self.running:
            expiry_maxed_out = False
            archive_maxed_out = False

            with ThreadPoolExecutor(self.config.core.expiry.workers) as pool:
                try:
                    expiry_maxed_out = self.run_expiry_once(pool)
                except Exception as e:
                    self.log.exception(str(e))

                try:
                    archive_maxed_out = self.run_archive_once(pool)
                except Exception as e:
                    self.log.exception(str(e))

            if not expiry_maxed_out and not archive_maxed_out:
                self.sleep_with_heartbeat(self.config.core.expiry.sleep_time)


if __name__ == "__main__":
    with ExpiryManager() as em:
        em.serve_forever()
