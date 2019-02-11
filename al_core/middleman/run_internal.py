"""
The internal worker for middleman runs several routing processes

TODO
 - can any of the processes in this can be inlined to the ingest/submit processes

Things handled here:
 - Retrying after a delay
 - Processing timeouts
 - dropper

"""

import logging
import time
import concurrent.futures

from assemblyline.common import log, forge, isotime
from al_core.middleman.middleman import Middleman, _dup_prefix


# noinspection PyBroadException
def process_timeouts(middleman):
    logger = logging.getLogger("assemblyline.middleman.timeouts")

    while middleman.running:
        timeouts = middleman.timeout_queue.dequeue_range(upper_limit=isotime.now(), num=10)

        # Wait for more work
        if not timeouts:
            time.sleep(1)

        for scan_key in timeouts:
            try:
                actual_timeout = False

                # Remove the entry from the hash of submissions in progress.
                entry = middleman.scanning.pop(scan_key)
                if entry:
                    actual_timeout = True
                    logger.error("Submission timed out for %s: %s", scan_key, str(entry))

                dup = middleman.duplicate_queue.pop(_dup_prefix + scan_key, blocking=False)
                if dup:
                    actual_timeout = True

                while dup:
                    logger.error("Submission timed out for %s: %s", scan_key, str(dup))
                    dup = middleman.duplicate_queue.pop(_dup_prefix + scan_key, blocking=False)

                if actual_timeout:
                    middleman.ingester_counts.increment('ingest.timed_out')
            except:
                logger.exception("Problem timing out %s:", scan_key)


def process_retries(middleman):
    while middleman.running:
        tasks = middleman.retry_queue.dequeue_range(upper_limit=isotime.now(), num=10)

        if not tasks:
            time.sleep(1)

        for task in tasks:
            middleman.ingest_queue.push(task)



# def send_heartbeat():
#     t = now()
#
#     up_hours = (t - start_time) / (60.0 * 60.0)
#
#     queues = {}
#     drop_p = {}
#
#     for level in ('low', 'medium', 'critical', 'high'):
#         queues[level] = uniqueq.count(*priority_range[level])
#         threshold = sample_threshold[level]
#         # noinspection PyTypeChecker
#         drop_p[level] = 1 - max(0, drop_chance(queues[level], threshold))
#
#     heartbeat = {
#         'hostinfo': hostinfo,
#         'inflight': scanning.length(),
#         'ingest': ingestq.length(),
#         'ingesting': drop_p,
#         'queues': queues,
#         'shard': shard,
#         'up_hours': up_hours,
#         'waiting': submissionq.length(),
#
#         'ingest.bytes_completed': 0,
#         'ingest.bytes_ingested': 0,
#         'ingest.duplicates': 0,
#         'ingest.files_completed': 0,
#         'ingest.skipped': 0,
#         'ingest.submissions_completed': 0,
#         'ingest.submissions_ingested': 0,
#         'ingest.timed_out': 0,
#         'ingest.whitelisted': 0,
#     }
#
#     # Send ingester stats.
#     exported = ingester_counts.export()
#
#     # Add ingester stats to our heartbeat.
#     heartbeat.update(exported)
#
#     # Send our heartbeat.
#     raw = message.Message(to="*", sender='middleman',
#                           mtype=message.MT_INGESTHEARTBEAT,
#                           body=heartbeat).as_dict()
#     statusq.publish(raw)
#
#     # Send whitelister stats.
#     whitelister_counts.export()
#
#
#
#
# def dropper():  # df node def
#     datastore = forge.get_datastore()
#
#     while running:
#         raw = dropq.pop(timeout=1)  # df pull pop
#         if not raw:
#             continue
#
#         notice = Notice(raw)
#
#         send_notification(notice)
#
#         c12n = notice.get('classification', config.core.middleman.classification)
#         expiry = now_as_iso(86400)
#         sha256 = notice.get('sha256')
#
#         datastore.save_or_freshen_file(sha256, {'sha256': sha256}, expiry, c12n)
#
#     datastore.close()
#

# init()
#
# Thread(target=send_heartbeats, name="send_heartbeats").start()
#


def run_internals(logger, datastore=None, redis=None, persistent_redis=None):
    # Connect to all sorts of things
    datastore = datastore or forge.get_datastore()
    classification_engine = forge.get_classification()

    # Initialize the middleman specific resources
    middleman = Middleman(datastore=datastore, classification=classification_engine, logger=logger,
                          redis=redis, persistent_redis=persistent_redis)
    middleman.start()

    tasks = {
        'timeouts': process_timeouts,
        'retries': process_retries
    }

    params = {
        'middleman': middleman
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        handles = {}

        while middleman.running:
            for name, fn in tasks.items():
                # If we don't have a running instance of that task, start it
                if name not in handles:
                    handles[name] = pool.submit(fn, **params)
                    continue

                if handles[name].running():
                    continue

                # So the task WAS running, and isn't now, is there an error?
                exception = handles[name].exception(timeout=0)
                if exception:
                    logger.error(f"An error was encountered while running {name}:\n {str(exception)}")
                del handles[name]

            time.sleep(3)


if __name__ == '__main__':
    log.init_logging("middleman")
    _logger = logging.getLogger('assemblyline.middleman')

    try:
        run_internals(_logger)
    except BaseException as error:
        _logger.exception("Exiting:")

