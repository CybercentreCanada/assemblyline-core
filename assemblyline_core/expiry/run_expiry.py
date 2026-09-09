#!/usr/bin/env python
from __future__ import annotations

import concurrent.futures
import threading
import functools
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Future, as_completed
from concurrent.futures.process import BrokenProcessPool
from typing import Callable, Optional, TYPE_CHECKING

import elasticapm
from datemath import dm

from assemblyline_core.server_base import ServerBase
from assemblyline_core.dispatching.dispatcher import BAD_SID_HASH
from assemblyline.common import forge, chunk
from assemblyline.common.isotime import epoch_to_iso, now_as_iso, now
from assemblyline.common.metrics import MetricsFactory
from assemblyline.filestore import FileStore
from assemblyline.odm.messages.expiry_heartbeat import Metrics
from assemblyline.remote.datatypes import get_client
from assemblyline.datastore.collection import Index
from assemblyline.remote.datatypes.set import Set

if TYPE_CHECKING:
    from assemblyline.datastore.collection import ESCollection

QUERY_DELETE_SIZE = 100_000
QUERY_WORKER_CHECK_VOLUME = 5_000_000


def file_delete_worker(logger, filestore_urls, file_batch: list[tuple[str, bool]],
                       archive_filestore_urls=None) -> list[tuple[str, bool]]:
    try:
        filestore = FileStore(*filestore_urls)
        if archive_filestore_urls and filestore_urls != archive_filestore_urls:
            archivestore = FileStore(*archive_filestore_urls)
        else:
            archivestore = filestore

        return _file_delete_worker(logger, filestore, archivestore, file_batch)

    except Exception as error:
        logger.exception("Error in filestore worker: " + str(error))
    return []


def _confirm_delete(store, from_archive, sha256: str) -> tuple[Optional[str], Optional[bool]]:
    if not store.exists(sha256):
        return sha256, from_archive
    return None, None


def _file_delete_worker(logger, filestore, archivestore, file_batch: list[tuple[str, bool]]) -> list[tuple[str, bool]]:
    finished_files: list[tuple[str, bool]] = []
    try:

        with ThreadPoolExecutor(16) as pool:
            # Delete the two batches in parallel if needed
            futures: list[Future] = []
            archive_files = [sha256 for sha256, from_archive in file_batch if from_archive]
            hot_files = [sha256 for sha256, from_archive in file_batch if not from_archive]
            futures.extend(
                pool.submit(archivestore.delete_batch, batch)
                for batch in chunk.chunk(archive_files, archivestore.delete_batch_chunk_size())
            )
            futures.extend(
                pool.submit(filestore.delete_batch, batch)
                for batch in chunk.chunk(hot_files, filestore.delete_batch_chunk_size())
            )

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as error:
                    logger.exception("Error in filestore worker: " + str(error))

            # It is CRITICALLY IMPORTANT that we don't delete the file records
            # for files that do exists, confirm that we have actually deleted files
            # before we procede with removing the datastore records
            futures = [pool.submit(_confirm_delete, filestore, False, filename) for filename in hot_files]
            futures.extend(pool.submit(_confirm_delete, archivestore, True, filename) for filename in archive_files)
            for future in as_completed(futures):
                try:
                    erased_name, from_archive = future.result()
                    if erased_name and from_archive is not None:
                        finished_files.append((erased_name, from_archive))
                except Exception as error:
                    logger.exception("Error in filestore worker: " + str(error))

    except Exception as error:
        logger.exception("Error in filestore worker: " + str(error))
    return finished_files


class ExpiryManager(ServerBase):
    def __init__(self, redis_persist=None, datastore=None, filestore=None, config=None, classification=None):
        self.config = config or forge.get_config()

        super().__init__('assemblyline.expiry', shutdown_timeout=self.config.core.expiry.sleep_time + 5)

        # Set Archive related configs
        if self.config.datastore.archive.enabled:
            self.archive_access = True
            self.index_type = Index.HOT_AND_ARCHIVE
        else:
            self.archive_access = False
            self.index_type = Index.HOT

        self.datastore = datastore or forge.get_datastore(config=self.config, archive_access=self.archive_access)
        self.filestore = filestore or forge.get_filestore(config=self.config)
        self.classification = classification or forge.get_classification()
        self.expirable_collections: list[ESCollection] = []
        self.counter = MetricsFactory('expiry', Metrics)
        self.file_delete_worker = ProcessPoolExecutor(self.config.core.expiry.delete_workers)
        if self.config.filestore.archive:
            self.same_storage = self.config.filestore.storage == self.config.filestore.archive
        else:
            self.same_storage = True
        self.current_submission_cleanup = set()

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
                                              file_batch=file_batch,
                                              archive_filestore_urls=list(self.config.filestore.archive))

    def cachestore_delete(self, file_batch, _):
        return self.file_delete_worker.submit(file_delete_worker, logger=self.log,
                                              filestore_urls=list(self.config.filestore.cache),
                                              file_batch=file_batch)

    def _finish_delete(self, collection: ESCollection, task: Future, expire_only: list[tuple[str, bool]]):
        # Wait until the worker process finishes deleting files
        file_list: list[str] = []
        while self.running:
            self.heartbeat()
            try:
                file_list = task.result(5)
                break
            except concurrent.futures.TimeoutError:
                pass

        if file_list:
            self.log.info(f'[{collection.name}] Deleted associated files from the '
                          f'{"cachestore" if "cache" in collection.name else "filestore"}...')
        else:
            self.log.info(f'[{collection.name}] Nothing was deleted from the '
                          f'{"cachestore" if "cache" in collection.name else "filestore"}...')

        # From the files to be deleted, check which are from the hot index
        hot_file_list = [x[0] for x in file_list if not x[1]]
        hot_file_list.extend([x[0] for x in expire_only if not x[1]])

        # From the files to be deleted, check which are from the archive index
        archive_file_list = [x[0] for x in file_list if x[1]]
        archive_file_list.extend([x[0] for x in expire_only if x[1]])

        for cur_file_list, index_type in [(hot_file_list, Index.HOT), (archive_file_list, Index.ARCHIVE)]:
            if not cur_file_list:
                # Nothing to delete from this index type
                continue

            # build a batch delete job for all the removed files
            bulk = collection.get_bulk_plan(index_type=index_type)
            for sha256 in cur_file_list:
                bulk.add_delete_operation(sha256)

            collection.bulk(bulk)
            self.counter.increment(f'{collection.name}', increment_by=len(cur_file_list))
            self.log.info(f"[{collection.name}] Deleted {len(cur_file_list)} items from the datastore...")

        if not hot_file_list and not archive_file_list:
            self.log.warning(f'[{collection.name}] Expiry unable to clean up any of the files in filestore.')

    def _cleanup_canceled_submission(self, sid):
        # Allowing us at minimum 5 minutes to cleanup the submission
        self.heartbeat(int(time.time() + 5 * 60))
        if self.apm_client:
            self.apm_client.begin_transaction("Delete canceled submissions")

        # Cleaning up the submission
        self.log.info(f"[submission] Deleting incomplete submission {sid}...")
        self.datastore.delete_submission_tree_bulk(sid, self.classification, transport=self.filestore)
        self.redis_bad_sids.remove(sid)

        # We're done cleaning up the sid, mark it as done
        self.current_submission_cleanup.remove(sid)

        if self.apm_client:
            self.apm_client.end_transaction("canceled_submissions", 'deleted')

    def _process_chunk(self, collection: ESCollection, start, end, final_date, number_to_delete):
        # We assume that no records are ever inserted such that their expiry_ts is in the past.
        # We also assume that the `end` dates are also in the past.
        # As long as these two things are true, the set returned by this query should be consistent.
        # The one race condition is that a record might be refreshed while the file
        # blob would be deleted anyway, leaving a file record with no filestore object
        delete_query = f"expiry_ts:{{{start} TO {end}]"

        # Delete associated files
        delete_objects: list[tuple[str, bool]] = []
        for item in collection.stream_search(
                delete_query, fl='id,from_archive', as_obj=False, index_type=self.index_type):
            self.heartbeat()
            delete_objects.append((item['id'], item.get('from_archive', False)))

        # Filter archived documents if archive filestore is the same as the filestore
        expire_only: list[tuple[str, bool]] = []
        if self.same_storage and self.archive_access and collection.name == 'file':
            # Separate hot and archive files
            delete_from_archive = [i[0] for i in delete_objects if i[1]]
            delete_from_hot = [i[0] for i in delete_objects if not i[1]]

            # Check for overlap
            overlap = set(delete_from_archive).intersection(set(delete_from_hot))
            delete_from_archive = list(set(delete_from_archive)-overlap)
            delete_from_hot = list(set(delete_from_hot)-overlap)

            # Create the original delete_object form the overlap
            delete_objects = [(k, False) for k in overlap]
            delete_objects.extend([(k, True) for k in overlap])

            if delete_from_hot:
                # Check hot objects to delete if they are in archive
                archived_files = self.datastore.file.multiexists(delete_from_hot, index_type=Index.ARCHIVE)
                delete_objects.extend([(k, False) for k, v in archived_files.items() if not v])
                expire_only.extend([(k, False) for k, v in archived_files.items() if v])

            if delete_from_archive:
                # Check hot objects to delete if they are in archive
                hot_files = self.datastore.file.multiexists(delete_from_archive, index_type=Index.HOT)
                delete_objects.extend([(k, True) for k, v in hot_files.items() if not v])
                expire_only.extend([(k, True) for k, v in hot_files.items() if v])

        delete_tasks = self.fs_hashmap[collection.name](delete_objects, final_date)

        # Proceed with deletion, but only after all the scheduled deletes for this
        self.log.info(f"[{collection.name}] Scheduled {len(delete_objects)}/{number_to_delete} files to be "
                      f"removed from the {'cachestore' if 'cache' in collection.name else 'filestore'}")
        self._finish_delete(collection, delete_tasks, expire_only)

    def feed_expiry_jobs(self, collection, start, jobs: list[concurrent.futures.Future],
                         pool: ThreadPoolExecutor) -> tuple[str, bool]:
        _process_chunk = self.log_errors(self._process_chunk)
        number_to_delete = 0
        self.heartbeat()

        # Start of expiry transaction
        if self.apm_client:
            self.apm_client.begin_transaction("Delete expired documents")

        final_date = self._get_final_date()

        # Break down the expiry window into smaller chunks of data
        while len(jobs) < self.config.core.expiry.iteration_max_tasks:

            # Get the next chunk
            end, number_to_delete = self._get_next_chunk(collection, start, final_date)

            # Check if we got anything
            if number_to_delete == 0:
                break

            # Process the chunk in the threadpool
            jobs.append(pool.submit(_process_chunk, collection, start, end, final_date, number_to_delete))

            # Prepare for next chunk
            start = end

        # End of expiry transaction
        if self.apm_client:
            self.apm_client.end_transaction(collection.name, 'deleted')

        return start, number_to_delete < self.expiry_size

    def _get_final_date(self):
        _now = now_as_iso()
        if self.config.core.expiry.batch_delete:
            final_date = dm(f"{_now}||-{self.config.core.expiry.delay}h/d").float_timestamp
        else:
            final_date = dm(f"{_now}||-{self.config.core.expiry.delay}h").float_timestamp
        return epoch_to_iso(final_date)

    def _get_next_chunk(self, collection: ESCollection, start, final_date):
        """Find date of item at chunk size and the number of items that
           will be affected in between start date and the date found"""
        rows = collection.search(f"expiry_ts: {{{start} TO {final_date}]", rows=1,
                                 offset=self.expiry_size - 1, sort='expiry_ts asc',
                                 as_obj=False, fl='expiry_ts', index_type=self.index_type)
        if rows['items']:
            return rows['items'][0]['expiry_ts'], self.expiry_size
        return final_date, rows['total']

    def try_run(self):
        pool = ThreadPoolExecutor(self.config.core.expiry.workers)
        main_threads = []

        # Launch a thread that will expire submissions that have been deleted
        thread = threading.Thread(target=self.clean_deleted_submissions, args=[pool])
        thread.start()
        main_threads.append(thread)

        # Launch threads that expire data from each collection of data
        for collection in self.expirable_collections:
            # check if we are dealing with an index that needs file cleanup
            if self.config.core.expiry.delete_storage and collection.name in self.fs_hashmap:
                thread = threading.Thread(target=self.run_file_collection, args=[pool, collection])
            else:
                thread = threading.Thread(target=self.run_collection, args=[collection])
            thread.start()
            main_threads.append(thread)

        # Wait for all the threads to exit
        for thread in main_threads:
            thread.join()

    def clean_deleted_submissions(self, pool):
        """Delete canceled submissions"""
        while self.running:
            # Make sure we're not dedicating more then a quarter of the pool to this operation because it is costly
            for submission in self.datastore.submission.search(
                    "to_be_deleted:true", fl="sid", rows=max(1, int(self.config.core.expiry.workers / 4)))['items']:
                if submission.sid not in self.current_submission_cleanup:
                    self.current_submission_cleanup.add(submission.sid)
                    pool.submit(self.log_errors(self._cleanup_canceled_submission), submission.sid)
            self.sleep_with_heartbeat(self.config.core.expiry.sleep_time)

    def run_file_collection(self, pool: concurrent.futures.ThreadPoolExecutor, collection):
        """
        Feed batches of jobs to delete to the thread pool for the given collection.

        The files must be cleaned up for this collection so we operate by
        batching rather than delete by query.
        """
        start = "*"
        jobs: list[concurrent.futures.Future] = []

        while self.running:
            try:
                try:
                    # Fill up 'jobs' with tasks that have been sent to the thread pool
                    # 'jobs' may already have items in it, but 'start' makes sure the new
                    # task added starts where the last finshed
                    start, final_job_small = self.feed_expiry_jobs(collection, start, jobs, pool)

                    # Wait until some of our work finishes and there is room in the queue for more work
                    finished, _jobs = concurrent.futures.wait(jobs, return_when=concurrent.futures.FIRST_COMPLETED)
                    jobs = list(_jobs)
                    for job in finished:
                        job.result()

                    # If we have expired all the data reset the start pointer
                    if len(jobs) == 0:
                        start = '*'

                except Exception as e:
                    self.log.exception(str(e))
                    continue

                # IF the most recent job added to the jobs list is short then
                # all the data is currently queued up to delete and we can sleep
                if final_job_small:
                    self.sleep_with_heartbeat(self.config.core.expiry.sleep_time)

            except BrokenProcessPool:
                self.log.error("File delete worker pool crashed.")
                self.file_delete_worker = ProcessPoolExecutor(self.config.core.expiry.delete_workers)

    def run_collection(self, collection):
        """
        For collections where no file cleanup is needed we can simply run delete by query.

        In cases where there are large quantities of records expiring (or a large backlog) multiple
        delete by query calls running on non-overlapping sections of the data can be needed to catch
        up.

        The metric we use here is that every day gets its own delete query. When the expiry daemon
        starts we will probe to see how many days of historical data we need to clean up and
        start that many query workers.

        Perodically a workers will terminate and this calculation will be redone.
        """
        while self.running:
            # Calculate how many queries we want to run
            queries = self.day_chunks(collection)

            # prepare a thread pool suitable for that operation
            with ThreadPoolExecutor(len(queries)) as pool:
                # Prepare a signal so we can stop all the workers as desired
                stop = threading.Event()

                # dispatch each of these queries
                futures = [pool.submit(self.run_collection_query, collection, stop, query) for query in queries]

                # wait for one of them to finish
                for future in as_completed(futures):
                    stop.set()
                    future.result()

    def day_chunks(self, collection):
        # Figure out the range of time we want to build queries for
        earliest = self.get_earliest_expiring(collection)
        if not earliest:
            return []
        days = int((now() - earliest)/(60 * 60 * 24)) + 1

        # Base no settings we will truncade expiry ranges by the day
        if self.config.core.expiry.batch_delete:
            suffix = f"-{self.config.core.expiry.delay}h/d"
        else:
            suffix = f"-{self.config.core.expiry.delay}h"

        # construct the ranges that build queries covering all of those days, bounded by
        # the appropirately modifide NOW on the high end, and modfied to be open on the low end
        ranges = [(f"now-1d{suffix}", f"now{suffix}")]
        for days in range(1, days):
            ranges.append((f"now-{days + 1}d{suffix}", f"now-{days}d{suffix}"))
        ranges[-1] = ("*", ranges[-1][1])

        # Convert those ranges to queries, each inclusive on the low end and exclusive on the high end
        queries = [f"expiry_ts: [{s} TO {e}}}" for s, e in ranges]
        return queries

    def get_earliest_expiring(self, collection) -> None | float:
        final = self._get_final_date()
        rows = collection.search(f"expiry_ts: [* TO {final}]", rows=1, fl='expiry_ts', sort="expiry_ts asc")
        if rows['items']:
            return rows['items'][0]['expiry_ts'].timestamp()
        return None

    def run_collection_query(self, collection, stop, query):
        total_deleted = 0
        while self.running or stop.is_set():
            self.heartbeat()
            deleted = 0

            try:
                deleted = collection.simple_delete_by_query(query, sort='expiry_ts asc', max_docs=QUERY_DELETE_SIZE)
                total_deleted += deleted
                self.counter.increment(f'{collection.name}', increment_by=deleted)
                self.log.info(f"[{collection.name}] Deleted {deleted} items from the datastore...")

            except Exception as e:
                self.log.exception(str(e))

            if total_deleted >= QUERY_WORKER_CHECK_VOLUME:
                return

            # If the number deleted is small wait before running the delete command again
            if deleted < QUERY_DELETE_SIZE * 0.9:
                self.sleep_with_heartbeat(self.config.core.expiry.sleep_time)


if __name__ == "__main__":
    with ExpiryManager() as em:
        em.serve_forever()
