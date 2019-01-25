import logging
import time

from assemblyline.common import forge
from assemblyline.common import log
from middleman.middleman import Middleman, IngestTask


def submitter(logger, datastore=None):
    # Connect to all sorts of things
    datastore = datastore or forge.get_datastore()
    classification_engine = forge.get_classification()

    # Initialize the middleman specific resources
    middleman = Middleman(datastore=datastore, classification=classification_engine, logger=logger)

    # Start the auxillary threads
    middleman.start()

    while middleman.running:
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
            task = IngestTask(raw)

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
            if not task.ignore_cache:
                pprevious, previous, score, scan_key = middleman.check(task)
            else:
                scan_key = middleman.stamp_filescore_key(task)

            # If it HAS been previously processed, we are dealing with a resubmission
            # finalize will decide what to do, and put the task back in the queue
            # rewritten properly if we are going to run it again
            if previous:
                if not task.resubmit_to and not pprevious:
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
                dupq.push(dup_prefix + scan_key, notice.raw)
                continue

            # We have managed to add the task to the scan table, so now we go
            # ahead with the submission process
            ex = None
            try:
                submit(client, notice)
                continue
            except Exception as ex:
                # For some reason (contained in `ex`) we have failed the submission
                # The rest of this function is error handling/recovery
                pass

            middleman.ingester_counts.increment('ingest.error')

            should_retry = True
            if isinstance(ex, CorruptedFileStoreException):
                logger.error("Submission for file '%s' failed due to corrupted filestore: %s" % (sha256, ex.message))
                should_retry = False
            elif isinstance(ex, riak.RiakError):
                if 'too_large' in ex.message:
                    should_retry = False
                trace = get_stacktrace_info(ex)
                logger.error("Submission for file '%s' failed due to Riak error:\n%s" % (sha256, trace))
            elif not isinstance(ex, FileStoreException):
                trace = get_stacktrace_info(ex)
                logger.error("Submission for file '%s' failed: %s" % (sha256, trace))

            raw = scanning.pop(scan_key)
            if not raw:
                logger.error('No scanning entry for for %s', sha256)
                continue

            if not should_retry:
                continue

            retry(raw, scan_key, sha256, ex)

        except Exception:  # pylint:disable=W0703
            logger.exception("Unexpected error")


if __name__ == '__main__':
    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.submitter')

    try:
        submitter(logger)
    except:
        logger.exception("Exiting:")
