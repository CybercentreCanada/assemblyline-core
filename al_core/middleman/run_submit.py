"""
Submit workers output files from middleman to dispatcher.

"""
import json
import time

from assemblyline.datastore.exceptions import DataStoreException
from assemblyline.filestore import CorruptedFileStoreException, FileStoreException
from assemblyline.common import forge, log, exceptions

from al_core.middleman.middleman import Middleman, IngestTask, _dup_prefix
from al_core.server_base import ServerBase


class MiddlemanSubmitter(ServerBase):
    def __init__(self, logger=None, datastore=None, redis=None, persistent_redis=None):
        super().__init__('assemblyline.middleman.submitter', logger)

        # Connect to all sorts of things
        datastore = datastore or forge.get_datastore()
        classification_engine = forge.get_classification()

        # Initialize the middleman specific resources
        self.middleman = Middleman(datastore=datastore, classification=classification_engine, logger=self.log,
                                   redis=redis, persistent_redis=persistent_redis)

    def start(self):
        super().start()
        # Start the auxillary threads
        self.middleman.start_counters()

    def try_run(self, volatile=False):
        middleman = self.middleman
        logger = self.log
        while self.running:
            try:
                # Check if there is room for more submissions
                length = middleman.scanning.length()
                if length < 0 or middleman.config.core.dispatcher.max_inflight <= length:
                    time.sleep(1)
                    continue

                #
                raw = middleman.unique_queue.pop()
                if not raw:
                    continue
                task = IngestTask(json.loads(raw))

                # noinspection PyBroadException
                if len(task.sha256) != 64:
                    logger.error("Malformed entry on submission queue: %s", raw)
                    continue

                # If between the initial ingestion and now the drop/whitelist status
                # of this submission has chaged, then drop it now
                if middleman.drop(task):
                    continue

                if middleman.is_whitelisted(task):
                    continue

                # Check if this file has been previously processed.
                pprevious, previous, score, scan_key = None, False, None, None
                if not task.params.ignore_cache:
                    pprevious, previous, score, scan_key = middleman.check(task)
                else:
                    scan_key = middleman.stamp_filescore_key(task)

                # If it HAS been previously processed, we are dealing with a resubmission
                # finalize will decide what to do, and put the task back in the queue
                # rewritten properly if we are going to run it again
                if previous:
                    if not task.params.services.resubmit and not pprevious:
                        logger.warning(f"No psid for what looks like a resubmission of {task.sha256}: {scan_key}")
                    middleman.finalize(pprevious, previous, score, task)
                    continue

                # We have decided this file is worth processing

                # Add the task to the scanning table, this is atomic across all submit
                # workers, so if it fails, someone beat us to the punch, record the file
                # as a duplicate then.
                if not middleman.scanning.add(scan_key, task.as_primitives()):
                    logger.debug('Duplicate %s', task.sha256)
                    middleman.ingester_counts.increment('ingest.duplicates')
                    middleman.duplicate_queue.push(_dup_prefix + scan_key, task.json())
                    continue

                # We have managed to add the task to the scan table, so now we go
                # ahead with the submission process
                try:
                    middleman.submit(task)
                    continue
                except Exception as _ex:
                    # For some reason (contained in `ex`) we have failed the submission
                    # The rest of this function is error handling/recovery
                    ex = _ex
                    traceback = _ex.__traceback__

                middleman.ingester_counts.increment('ingest.error')

                should_retry = True
                if isinstance(ex, CorruptedFileStoreException):
                    logger.error("Submission for file '%s' failed due to corrupted filestore: %s"
                                 % (task.sha256, ex.message))
                    should_retry = False
                elif isinstance(ex, DataStoreException):
                    trace = exceptions.get_stacktrace_info(ex)
                    logger.error("Submission for file '%s' failed due to data store error:\n%s" % (task.sha256, trace))
                elif not isinstance(ex, FileStoreException):
                    trace = exceptions.get_stacktrace_info(ex)
                    logger.error("Submission for file '%s' failed: %s" % (task.sha256, trace))

                task = IngestTask(middleman.scanning.pop(scan_key))
                if not task:
                    logger.error('No scanning entry for for %s', task.sha256)
                    continue

                if not should_retry:
                    continue

                middleman.retry(task, scan_key, ex)
                if volatile:
                    raise ex.with_traceback(traceback)

            except Exception:  # pylint:disable=W0703
                logger.exception("Unexpected error")
                if volatile:
                    raise


if __name__ == '__main__':
    log.init_logging("middleman")
    MiddlemanSubmitter().serve_forever()
