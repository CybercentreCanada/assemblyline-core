"""
An ingest worker is responsible for processing input messages to the middleman.

These are:
 - Messages from dispatcher saying that jobs are complete.
 - Messages from THE OUTSIDE telling us to scan files.

"""
import logging
import json

from assemblyline.common import forge
from assemblyline.common import log
from .middleman import Middleman, IngestTask


def ingester(logger, datastore=None):
    # Connect to all sorts of things
    datastore = datastore or forge.get_datastore()
    classification_engine = forge.get_classification()

    # Initialize the middleman specific resources
    middleman = Middleman(datastore=datastore, classification=classification_engine, logger=logger)

    # Start the auxillary threads
    middleman.start()

    # Move from ingest to unique and waiting queues.
    # While there are entries in the ingest queue we consume chunk_size
    # entries at a time and move unique entries to uniqueq / queued and
    # duplicates to their own queues / waiting.
    while middleman.running:
        while True:
            result = middleman.complete_queue.pop(blocking=False)
            if not result:
                break

            middleman.completed(result)

        message = middleman.ingest_queue.pop(timeout=1)
        if not message:
            continue

        middleman.traffic_queue.publish(message)
        try:
            task = IngestTask(json.loads(message))
        except ValueError:
            logger.warning(f"Dropped ingest submission {message}")
            continue

        sha256 = task.sha256
        if not sha256 or len(sha256) != 64:
            logger.error(f"Invalid sha256: {sha256}")
            continue

        # task.md5 = task.md5.lower()
        # task.sha1 = task.sha1.lower()
        task.sha256 = sha256.lower()

        middleman.ingest(task)

    middleman.stop()


if __name__ == '__main__':
    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    try:
        ingester(logger)
    except:
        logger.exception("Exiting:")
