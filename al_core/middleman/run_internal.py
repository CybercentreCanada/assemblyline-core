"""
The internal worker for middleman runs several routing processes

TODO
 - can any of the processes in this can be inlined to the ingest/submit processes

Things handled here:
 - Retrying after a delay
 - Processing timeouts (?)
 - dropper

"""

from assemblyline.common import log

import logging


# noinspection PyBroadException
def process_timeouts():  # df node def
    global timeouts  # pylint:disable=W0603

    with timeouts_lock:
        current_time = now()
        index = 0

        for t in timeouts:
            if t.time >= current_time:
                break

            index += 1

            try:
                timed_out(t.scan_key)  # df push calls
            except:  # pylint: disable=W0702
                logger.exception("Problem timing out %s:", t.scan_key)

        timeouts = timeouts[index:]


# Invoked when a timeout fires. (Timeouts always fire).
def timed_out(scan_key):  # df node def
    actual_timeout = False

    with ScanLock(scan_key):
        # Remove the entry from the hash of submissions in progress.
        entry = scanning.pop(scan_key)  # df pull pop
        if entry:
            actual_timeout = True
            logger.error("Submission timed out for %s: %s", scan_key, str(entry))

        dup = dupq.pop(dup_prefix + scan_key, blocking=False)  # df pull pop
        if dup:
            actual_timeout = True

        while dup:
            logger.error("Submission timed out for %s: %s", scan_key, str(dup))
            dup = dupq.pop(dup_prefix + scan_key, blocking=False)

    if actual_timeout:
        ingester_counts.increment('ingest.timed_out')


def process_retries():  # df node def
    while running:
        raw = retryq.pop(timeout=1)  # df pull pop
        if not raw:
            continue

        retry_at = raw['retry_at']
        delay = retry_at - now()

        if delay >= 0.125:
            retryq.unpop(raw)
            time.sleep(min(delay, 1))
            continue

        ingestq.push(raw)  # df push push



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
# def send_heartbeats():
#     while running:
#         send_heartbeat()
#         time.sleep(1)
#
#
#
# for i in range(dropper_threads):
#     Thread(target=dropper, name="dropper_%s" % i).start()  # df line thread
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
# while running:
# Thread(target=process_retries, name="process_retries").start()
# dropper
#     process_timeouts()
#     time.sleep(60)

# df text }


def run_internals():
    jobs


if __name__ == '__main__':
    log.init_logging("middleman")
    _logger = logging.getLogger('assemblyline.middleman')

    try:
        run_internals()
    except BaseException as error:
        _logger.exception("Exiting:")

