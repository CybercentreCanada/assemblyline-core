"""
Submit workers output files from ingester to dispatcher.

"""
import json
import time

from assemblyline.datastore.exceptions import DataStoreException
from assemblyline.filestore import CorruptedFileStoreException, FileStoreException
from assemblyline.common import forge, log, exceptions

from al_core.ingester.ingester import Ingester, IngestTask, _dup_prefix
from al_core.server_base import ServerBase


class IngesterSubmitter(ServerBase):
    def __init__(self, logger=None, datastore=None, redis=None, persistent_redis=None):
        super().__init__('assemblyline.ingester.submitter')

        # Connect to all sorts of things
        datastore = datastore or forge.get_datastore()
        classification_engine = forge.get_classification()

        # Initialize the ingester specific resources
        self.ingester = Ingester(datastore=datastore, classification=classification_engine, logger=self.log,
                                 redis=redis, persistent_redis=persistent_redis)

    def try_run(self, volatile=False):
        ingester = self.ingester
        logger = self.log
        while self.running:
            # noinspection PyBroadException
            try:
                # Check if there is room for more submissions
                length = ingester.scanning.length()
                if length < 0 or ingester.config.core.dispatcher.max_inflight <= length:
                    time.sleep(1)
                    continue

                #
                raw = ingester.unique_queue.pop()
                if not raw:
                    continue
                task = IngestTask(raw)

                # noinspection PyBroadException
                if any(len(file.sha256) != 64 for file in task.submission.files):
                    logger.error("Malformed entry on submission queue: %s", task.ingest_id)
                    continue

                # If between the initial ingestion and now the drop/whitelist status
                # of this submission has changed, then drop it now
                if ingester.drop(task):
                    continue

                if ingester.is_whitelisted(task):
                    continue

                # Check if this file has been previously processed.
                pprevious, previous, score, scan_key = None, False, None, None
                if not task.submission.params.ignore_cache:
                    pprevious, previous, score, scan_key = ingester.check(task)
                else:
                    scan_key = ingester.stamp_filescore_key(task)

                # If it HAS been previously processed, we are dealing with a resubmission
                # finalize will decide what to do, and put the task back in the queue
                # rewritten properly if we are going to run it again
                if previous:
                    if not task.submission.params.services.resubmit and not pprevious:
                        logger.warning(f"No psid for what looks like a resubmission of {task.submission.files[0].sha256}: {scan_key}")
                    ingester.finalize(pprevious, previous, score, task)
                    continue

                # We have decided this file is worth processing

                # Add the task to the scanning table, this is atomic across all submit
                # workers, so if it fails, someone beat us to the punch, record the file
                # as a duplicate then.
                if not ingester.scanning.add(scan_key, task.as_primitives()):
                    logger.debug('Duplicate %s', task.submission.files[0].sha256)
                    ingester.duplicates_counter.increment()
                    ingester.duplicate_queue.push(_dup_prefix + scan_key, task.as_primitives())
                    continue

                # We have managed to add the task to the scan table, so now we go
                # ahead with the submission process
                try:
                    ingester.submit(task)
                    continue
                except Exception as _ex:
                    # For some reason (contained in `ex`) we have failed the submission
                    # The rest of this function is error handling/recovery
                    ex = _ex
                    traceback = _ex.__traceback__

                ingester.error_counter.increment()

                should_retry = True
                if isinstance(ex, CorruptedFileStoreException):
                    logger.error("Submission for file '%s' failed due to corrupted filestore: %s"
                                 % (task.sha256, str(ex)))
                    should_retry = False
                elif isinstance(ex, DataStoreException):
                    trace = exceptions.get_stacktrace_info(ex)
                    logger.error("Submission for file '%s' failed due to data store error:\n%s" % (task.sha256, trace))
                elif not isinstance(ex, FileStoreException):
                    trace = exceptions.get_stacktrace_info(ex)
                    logger.error("Submission for file '%s' failed: %s" % (task.sha256, trace))

                task = IngestTask(ingester.scanning.pop(scan_key))
                if not task:
                    logger.error('No scanning entry for for %s', task.sha256)
                    continue

                if not should_retry:
                    continue

                ingester.retry(task, scan_key, ex)
                if volatile:
                    raise ex.with_traceback(traceback)

            except Exception:
                logger.exception("Unexpected error")
                if volatile:
                    raise


if __name__ == '__main__':
    IngesterSubmitter().serve_forever()
