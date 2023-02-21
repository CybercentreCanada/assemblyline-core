#!/usr/bin/env python
from __future__ import annotations
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future, as_completed
from concurrent.futures.process import BrokenProcessPool
import functools
from typing import Callable, Optional, Union, TYPE_CHECKING
import elasticapm
import time

from datemath import dm

from assemblyline.common.isotime import epoch_to_iso, now_as_iso, iso_to_epoch
from assemblyline_core.server_base import ServerBase
from assemblyline_core.dispatching.dispatcher import BAD_SID_HASH
from assemblyline.common import forge
from assemblyline.common.metrics import MetricsFactory
from assemblyline.filestore import FileStore
from assemblyline.odm.messages.expiry_heartbeat import Metrics
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.set import Set

if TYPE_CHECKING:
    from assemblyline.datastore.collection import ESCollection


def file_delete_worker(logger, filestore_urls, file_batch) -> list[str]:
    try:
        filestore = FileStore(*filestore_urls)

        def filestore_delete(sha256: str) -> Optional[str]:
            filestore.delete(sha256)
            if not filestore.exists(sha256):
                return sha256
            return None

        return _file_delete_worker(logger, filestore_delete, file_batch)

    except Exception as error:
        logger.exception("Error in filestore worker: " + str(error))
    return []


def _file_delete_worker(logger, delete_action: Callable[[str], Optional[str]], file_batch) -> list[str]:
    finished_files: list[str] = []
    try:
        futures = []

        with ThreadPoolExecutor(8) as pool:
            for filename in file_batch:
                futures.append(pool.submit(delete_action, filename))

            for future in as_completed(futures):
                try:
                    erased_name = future.result()
                    if erased_name:
                        finished_files.append(erased_name)
                except Exception as error:
                    logger.exception("Error in filestore worker: " + str(error))

    except Exception as error:
        logger.exception("Error in filestore worker: " + str(error))
    return finished_files


class ExpiryManager(ServerBase):
    def __init__(self, redis_persist=None):
        self.config = forge.get_config()

        super().__init__('assemblyline.expiry', shutdown_timeout=self.config.core.expiry.sleep_time + 5)
        self.datastore = forge.get_datastore(config=self.config)
        self.filestore = forge.get_filestore(config=self.config)
        self.classification = forge.get_classification()
        self.expirable_collections: list[ESCollection] = []
        self.counter = MetricsFactory('expiry', Metrics)
        self.file_delete_worker = ProcessPoolExecutor(self.config.core.expiry.delete_workers)
        self.same_storage = self.config.filestore.storage == self.config.filestore.archive

        self.redis_persist = redis_persist or get_client(
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )
        self.redis_bad_sids = Set(BAD_SID_HASH, host=self.redis_persist)

        self.fs_hashmap = {
            'file': self.filestore_delete,
            'cached_file': self.cachestore_delete
        }

        for name, definition in self.datastore.ds.get_models().items():
            if hasattr(definition, 'expiry_ts'):
                self.expirable_collections.append(getattr(self.datastore, name))

        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = forge.get_apm_client("expiry")
        else:
            self.apm_client = None

    @property
    def expiry_size(self):
        return self.config.core.expiry.delete_batch_size

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

    def filestore_delete(self, file_batch, _):
        return self.file_delete_worker.submit(file_delete_worker, logger=self.log,
                                              filestore_urls=list(self.config.filestore.storage),
                                              file_batch=file_batch)

    def cachestore_delete(self, file_batch, _):
        return self.file_delete_worker.submit(file_delete_worker, logger=self.log,
                                              filestore_urls=list(self.config.filestore.cache),
                                              file_batch=file_batch)

    def _finish_delete(self, collection: ESCollection, task: Future, expire_only: list[str]):
        # Wait until the worker process finishes deleting files
        file_list: list[str] = []
        while self.running:
            self.heartbeat()
            try:
                file_list = task.result(5)
                break
            except concurrent.futures.TimeoutError:
                pass

        file_list.extend(expire_only)

        # build a batch delete job for all the removed files
        bulk = collection.get_bulk_plan()
        for sha256 in file_list:
            bulk.add_delete_operation(sha256)

        if len(file_list) > 0:
            self.log.info(f'    Deleted associated files from the '
                          f'{"cachestore" if "cache" in collection.name else "filestore"}...')
            collection.bulk(bulk)
            self.counter.increment(f'{collection.name}', increment_by=len(file_list))
            self.log.info(f"    Deleted {len(file_list)} items from the datastore...")
        else:
            self.log.warning('    Expiry unable to clean up any of the files in filestore.')

    def _simple_delete(self, collection, delete_query, number_to_delete):
        self.heartbeat()
        collection.delete_by_query(delete_query)
        self.counter.increment(f'{collection.name}', increment_by=number_to_delete)
        self.log.info(f"    Deleted {number_to_delete} items from the datastore...")

    def run_expiry_once(self, pool: ThreadPoolExecutor):
        now = now_as_iso()
        reached_max = False

        # Delete canceled submissions
        for submission in self.datastore.submission.stream_search("to_be_deleted:true", fl="sid"):
            if self.apm_client:
                self.apm_client.begin_transaction("Delete canceled submissions")

            self.log.info(f"Deleting incomplete submission {submission.sid}...")
            self.datastore.delete_submission_tree_bulk(submission.sid, self.classification, transport=self.filestore)
            self.redis_bad_sids.remove(submission.sid)

            if self.apm_client:
                self.apm_client.end_transaction("canceled_submissions", 'deleted')

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
            unchecked_chunks: list[tuple[float, float]] = [(self._find_expiry_start(collection), final_date)]
            ready_chunks: dict[tuple[float, float], int] = {}
            while unchecked_chunks and len(ready_chunks) < self.config.core.expiry.iteration_max_tasks:
                start, end = unchecked_chunks.pop()
                chunk_size = self._count_expired(collection, start, end)

                # Empty chunks are fine
                if chunk_size == 0:
                    continue

                # We found a small enough chunk to
                #  run on
                if chunk_size < self.expiry_size:
                    ready_chunks[(start, end)] = chunk_size
                    continue

                # Break this chunk into parts
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
                    delete_objects: list[str] = []
                    for item in collection.stream_search(delete_query, fl='id', as_obj=False):
                        delete_objects.append(item['id'])

                    # Filter archived documents if archive filestore is the same as the filestore
                    expire_only = []
                    if self.same_storage and self.config.datastore.archive.enabled and collection.name == 'file':
                        archived_files = self.datastore.file.multiexists_in_archive(delete_objects)
                        delete_objects = [k for k, v in archived_files.items() if not v]
                        expire_only = [k for k, v in archived_files.items() if v]

                    delete_tasks = self.fs_hashmap[collection.name](delete_objects, final_date_string)

                    # Proceed with deletion, but only after all the scheduled deletes for this
                    self.log.info(f"Scheduled {len(delete_objects)}/{number_to_delete} "
                                  f"files to be removed for: {collection.name}")
                    pool.submit(self.log_errors(self._finish_delete), collection, delete_tasks, expire_only)

                else:
                    # Proceed with deletion
                    pool.submit(self.log_errors(self._simple_delete),
                                collection, delete_query, number_to_delete)

            # End of expiry transaction
            if self.apm_client:
                self.apm_client.end_transaction(collection.name, 'deleted')

        return reached_max

    def _find_expiry_start(self, container: ESCollection):
        """Find earliest expiring item in this container."""
        rows = container.search(f"expiry_ts: [* TO {epoch_to_iso(time.time())}]",
                                rows=1, sort='expiry_ts asc', as_obj=False, fl='expiry_ts')
        if rows['items']:
            return iso_to_epoch(rows['items'][0]['expiry_ts'])
        return time.time()

    def _count_expired(self, container: ESCollection, start: Union[float, str], end: float) -> int:
        """Count how many items need to be erased in the given window."""
        if start == 0:
            start = '*'
        if isinstance(start, (float, int)):
            start = epoch_to_iso(start)
        query = f'expiry_ts:[{start} TO {epoch_to_iso(end)}}}'
        return container.search(query, rows=0, as_obj=False, track_total_hits=self.expiry_size)['total']

    def try_run(self):
        while self.running:
            try:
                expiry_maxed_out = False

                with ThreadPoolExecutor(self.config.core.expiry.workers) as pool:
                    try:
                        expiry_maxed_out = self.run_expiry_once(pool)
                    except Exception as e:
                        self.log.exception(str(e))

                if not expiry_maxed_out:
                    self.sleep_with_heartbeat(self.config.core.expiry.sleep_time)

            except BrokenProcessPool:
                self.log.error("File delete worker pool crashed.")
                self.file_delete_worker = ProcessPoolExecutor(self.config.core.expiry.delete_workers)


if __name__ == "__main__":
    with ExpiryManager() as em:
        em.serve_forever()
