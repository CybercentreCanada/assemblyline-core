import logging

from assemblyline.common import forge
from assemblyline.common import log
from middleman.middleman import Middleman, IngestTask


def submitter():
    # Connect to all sorts of things
    datastore = forge.get_datastore()
    classification_engine = forge.get_classification()

    # Initialize the middleman specific resources
    middleman = Middleman(datastore=datastore, classification=classification_engine, logger=logger)

    # Start the auxillary threads
    middleman.start()

    while middleman.running:
        try:
            raw = submissionq.pop(timeout=1)  # df pull pop
            if not raw:
                continue

            # noinspection PyBroadException
            try:
                sha256 = raw['sha256']
            except Exception:  # pylint: disable=W0703
                logger.exception("Malformed entry on submission queue:")
                continue

            if not sha256:
                logger.error("Malformed entry on submission queue: %s", raw)
                continue

            notice = Notice(raw)
            if drop(notice):  # df push calls
                continue

            if is_whitelisted(notice):  # df push calls
                continue

            pprevious, previous, score, scan_key = None, False, None, None
            if not notice.get('ignore_cache', False):
                pprevious, previous, score, scan_key = check(datastore, notice)
            else:
                scan_key = stamp_filescore_key(notice)

            if previous:
                if not notice.get('resubmit_to', []) and not pprevious:
                    logger.warning("No psid for what looks like a resubmission of %s: %s", sha256, scan_key)
                finalize(pprevious, previous, score, notice)  # df push calls
                continue

            with ScanLock(scan_key):
                if scanning.exists(scan_key):
                    logger.debug('Duplicate %s', sha256)
                    ingester_counts.increment('ingest.duplicates')
                    dupq.push(dup_prefix + scan_key, notice.raw)  # df push push
                    continue

                scanning.add(scan_key, notice.raw)  # df push add

            ex = return_exception(submit, client, notice)
            if not ex:
                continue

            ingester_counts.increment('ingest.error')

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
