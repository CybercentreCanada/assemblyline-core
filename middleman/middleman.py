#!/usr/bin/env python
"""
Middleman

Middleman is responsible for monitoring for incoming submission requests,
sending submissions, waiting for submissions to complete, sending a message
to a notification queue as specified by the submission and, based on the
score received, possibly sending a message to indicate that an alert should
be created.
"""

import threading
import redis
import signal
import time

# from collections import namedtuple
from math import tanh, isnan
from random import random

from assemblyline.common.str_utils import dotdump, safe_str
from assemblyline.common.exceptions import get_stacktrace_info
from assemblyline.common.isotime import iso_to_epoch, now, now_as_iso
from assemblyline.common.importing import load_module_by_path
# from assemblyline.common.net import get_hostip, get_hostname, get_mac_for_ip
from assemblyline.common import net
from assemblyline.common import forge
from .document_keys import create_filescore_key

from assemblyline import odm

from assemblyline.remote.datatypes.exporting_counter import AutoExportingCounters
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.queues.priority import PriorityQueue
from assemblyline.remote.datatypes.queues.comms import CommsQueue
from assemblyline.remote.datatypes import get_client


def install_interrupt_handler(handler):
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

# Timeout = namedtuple('Timeout', ['time', 'scan_key'])


_completeq_name = 'm-complete'
_ingestq_name = 'm-ingest'
_min_priority = 1


# TODO move to config file
@odm.model()
class Ingest(odm.Model):
    # The ingest defaults are the values given to ingest tasks when no input
    # has been provided
    default_user = odm.Keyword()
    default_services = odm.List(odm.Keyword(), default=['Antivirus', 'Extraction', 'Filtering',
                                                        'Networking', 'Static Analysis'])
    default_resubmit_services = odm.List(odm.Keyword(), default=['Dynamic Analysis'])

    # Maximum permitted length of metadata values
    max_value_size = odm.Integer()

    # Maximum file size for ingestion
    max_size = odm.Integer()


@odm.model()
class IngestTask(odm.Model):
    # parameters that control middleman's handling of the task
    ignore_size = odm.Boolean(default=False)
    never_drop = odm.Boolean(default=False)
    ignore_cache = odm.Boolean(default=False)
    deep_scan = odm.Boolean(default=False)
    ignore_dynamic_recursion_prevention = odm.Boolean(default=False)
    ignore_cache = odm.Boolean(default=False)
    ignore_filtering = odm.Boolean(default=False)
    max_extracted = odm.Integer()
    max_supplementary = odm.Integer()
    completed_queue = odm.Keyword()

    # Information about the ingestion itself
    ingest_time = odm.Date()
    scan_key = odm.Keyword(default_set=True)  # overide the filescore key
    priority = odm.Float(default=float('nan'))

    # describe the file being ingested
    sha256 = odm.Keyword()
    file_size = odm.Integer()
    classification = odm.Keyword()
    metadata = odm.Mapping(odm.Keyword())
    description = odm.Keyword()

    # Information about who wants this file ingested
    submitter = odm.Keyword()
    groups = odm.List(odm.Keyword())

    # What services should this be submitted to
    selected_services = odm.List(odm.Keyword())
    resubmit_services = odm.List(odm.Keyword())
    params = odm.Mapping(odm.Keyword(), default={})


class Middleman:
    """Internal interface to the ingestion queues."""

    def __init__(self, datastore, logger, classification=None):
        self.datastore = datastore
        self.log = logger

        # Cache the user groups
        self._user_groups = {}
        self.cache = {}
        self.cache_lock = threading.RLock()  # TODO are middle man instances single threaded now?
        self.whitelisted = {}
        self.whitelisted_lock = threading.RLock()
        self.running = True

        # Create a config cache that will refresh config values periodically
        self.config = forge.CachedObject(forge.get_config)

        # TODO Should any of these values be read dynamically
        self.is_low_priority = load_module_by_path(self.config.core.middleman.is_low_priority)
        self.get_whitelist_verdict = load_module_by_path(self.config.core.middleman.get_whitelist_verdict)
        self.whitelist = load_module_by_path(self.config.core.middleman.whitelist)

        # Constants are loaded based on a non-constant path, so has to be done at init rather than load
        constants = forge.get_constants(self.config)
        self.priority_value = constants.PRIORITIES
        self.priority_range = constants.PRIORITY_RANGES
        self.threshold_value = constants.PRIORITY_THRESHOLDS

        # Connect to the redis servers
        self.redis = get_client(
            db=self.config.core.redis.nonpersistent.db,
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )
        self.persistent_redis = get_client(
            db=self.config.core.redis.persistent.db,
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )

        # Classification engine
        self.ce = classification or forge.get_classification()

        self.ingester_counts = AutoExportingCounters(
            name='ingester',
            host=net.get_hostip(),
            auto_flush=True,
            auto_log=False,
            export_interval_secs=self.config.logging.export_interval,
            channel=forge.get_metrics_sink())

        self.whitelister_counts = AutoExportingCounters(
            name='whitelister',
            host=net.get_hostip(),
            auto_flush=True,
            auto_log=False,
            export_interval_secs=self.config.logging.export_interval,
            channel=forge.get_metrics_sink())

        # Input. An external process creates a record when any submission completes.
        self.complete_queue = NamedQueue(_completeq_name, self.redis)

        # Output. Dropped entries are placed on this queue.
        # dropq = queue.NamedQueue('m-drop-' + shard, **persistent)

        # Input. An external process places submission requests on this queue.
        self.ingest_queue = NamedQueue(_ingestq_name, self.persistent_redis)

        # Traffic (TODO: What traffic?)
        self.traffic_queue = CommsQueue('traffic', self.redis)

        # Input/Output. Unique requests are placed in and processed from this queue.
        self.unique_queue = PriorityQueue('m-unique', self.persistent_redis)

    def start(self):
        """Start shared middleman auxillary components."""
        self.ingester_counts.start()
        self.whitelister_counts.start()
        install_interrupt_handler(self.interrupt_handler)

    def interrupt_handler(self, *_):
        self.log.info("Caught signal. Coming down...")
        self.running = False

    def stop(self):
        """Stop shared middleman auxillary components."""
        self.ingester_counts.stop()
        self.whitelister_counts.stop()

    def get_user_groups(self, user):
        groups = self._user_groups.get(user, None)
        if groups is None:
            ruser = self.datastore.users.get(user)
            if not ruser:
                return None
            groups = ruser.get('groups', [])
            self._user_groups[user] = groups
        return groups

    def ingest(self, task: IngestTask):
        # Load a snapshot of ingest parameters as of right now.
        # self.config is a timed cache
        conf = self.config.core.middleman
        max_file_size = self.config.submission.max_file_size

        # Make sure we have a submitter ...
        if not task.submitter:
            task.submitter = conf.default_user

        # ... and groups.
        if not task.groups:
            task.groups = self.get_user_groups(task.submitter)
            if task.groups is None:
                error_message = f"User not found [{task.submitter}] ingest failed"
                send_notification(task, failure=error_message, logfunc=self.log.warning)
                return

        #
        if not task.selected_services:
            task.selected_services = conf.default_services
            task.resubmit_services = conf.default_resubmit_services

        if not task.resubmit_services:
            task.resubmit_services = conf.default_resubmit_services

        self.ingester_counts.increment('ingest.bytes_ingested', task.file_size)
        self.ingester_counts.increment('ingest.submissions_ingested')

        if not task.sha256:
            send_notification(task, failure="Invalid sha256", logfunc=self.log.warning)
            return

        c12n = task.classification
        if not self.ce.is_valid(c12n):
            send_notification(task, failure=f"Invalid classification {c12n}", logfunc=self.log.warning)
            return

        # Clean up metadata strings, since we may delete some, iterate on a copy of the keys
        for key in list(task.metadata.keys()):
            value = task.metadata[key]
            meta_size = len(value)
            if meta_size > self.config.submission.max_metadata_length:
                self.log.info(f'Removing {key} from {task.sha256} from {task.submitter}')
                task.metadata.pop(key)

        if task.file_size > max_file_size and not task.ignore_size and not task.never_drop:
            task.failure = f"File too large ({task.file_size} > {conf.max_size})"
            dropq.push(notice.raw)  # df push push
            self.ingester_counts.increment('ingest.skipped')
            return

        pprevious, previous, score = None, False, None
        if not task.ignore_cache:
            pprevious, previous, score, _ = self.check(task)

        # Assign priority.
        low_priority = self.is_low_priority(task)

        priority = task.priority
        if isnan(priority):
            priority = self.priority_value['medium']

            if score is not None:
                priority = self.priority_value['low']
                for level, threshold in self.threshold_value.items():
                    if score >= threshold:
                        priority = self.priority_value[level]
                        break
            elif low_priority:
                priority = self.priority_value['low']

        # Reduce the priority by an order of magnitude for very old files.
        current_time = now()
        if priority and self.expired(current_time - seconds(task.ingest_time), 0):
            priority = (priority / 10) or 1

        task.priority = priority

        # Do this after priority has been assigned.
        # (So we don't end up dropping the resubmission).
        if previous:
            self.ingester_counts.increment('ingest.duplicates')
            finalize(pprevious, previous, score, task)
            return

        if self.drop(task):
            return

        if self.is_whitelisted(task):
            return

        self.unique_queue.push(priority, task.json())

    def check(self, task: IngestTask):
        key = self.stamp_filescore_key(task)

        with self.cache_lock:
            result = self.cache.get(key, None)

        counter_name = 'ingest.cache_hit_local'
        if result:
            self.log.info('Local cache hit')
        else:
            counter_name = 'ingest.cache_hit'

            result = self.datastore.filescore.get(key)
            if result:
                self.log.info('Remote cache hit')
            else:
                self.ingester_counts.increment('ingest.cache_miss')
                return None, False, None, key

            with self.cache_lock:
                self.cache[key] = {
                    'errors': result.get('errors', 0),
                    'psid': result.get('psid', None),
                    'score': result['score'],
                    'sid': result['sid'],
                    'time': result['time'],
                }

        current_time = now()
        delta = current_time - result.get('time', current_time)
        errors = result.get('errors', 0)

        if expired(delta, errors):
            self.ingester_counts.increment('ingest.cache_expired')
            with self.cache_lock:
                self.cache.pop(key, None)
                self.delete_filescore(key)
            return None, False, None, key
        elif stale(delta, errors):
            self.ingester_counts.increment('ingest.cache_stale')
            return None, False, result['score'], key

        self.ingester_counts.increment(counter_name)

        return result.get('psid', None), result['sid'], result['score'], key

    def stamp_filescore_key(self, task: IngestTask, sha256=None):
        if not sha256:
            sha256 = task.sha256

        raw_task = task.as_primitives()
        selected = task.selected_services
        key = task.scan_key

        if not key:
            key = create_filescore_key(sha256, raw_task, selected)
            task.scan_key = key

        return key

    def completed(self, task):
        """Invoked when notified that a submission has completed."""
        sha256 = task.root_sha256

        psid = task.psid
        score = task.score
        sid = task.sid

        scan_key = task.scan_key

        with ScanLock(scan_key):
            # Remove the entry from the hash of submissions in progress.
            raw = scanning.pop(scan_key)  # df pull pop
            if not raw:
                logger.warning("Untracked submission (score=%d) for: %s %s",
                               int(score), sha256, str(task.metadata))

                # Not a result we care about. We are notified for every
                # submission that completes. Some submissions will not be ours.
                if task.metadata:
                    stype = None
                    try:
                        stype = task.metadata.get('type', None)
                    except:  # pylint: disable=W0702
                        logger.exception("Malformed metadata: %s:", sid)

                    if not stype:
                        return scan_key

                    if (task.description or '').startswith(default_prefix):
                        raw = {
                            'metadata': task.metadata,
                            'overrides': get_submission_overrides(task, overrides),
                            'sha256': sha256,
                            'type': stype,
                        }

                        finalize(psid, sid, score, Notice(raw))
                return scan_key

            errors = task.raw.get('error_count', 0)
            file_count = task.raw.get('file_count', 0)
            ingester_counts.increment('ingest.submissions_completed')
            ingester_counts.increment('ingest.files_completed', file_count)
            ingester_counts.increment('ingest.bytes_completed', int(task.size or 0))

            notice = Notice(raw)

            with self.cache_lock:
                cache[key] = {
                    'errors': errors,
                    'psid': psid,
                    'score': score,
                    'sid': sid,
                    'time': now(),
                }

            finalize(psid, sid, score, notice)  # df push calls

            def exhaust():
                while True:
                    res = dupq.pop(  # df pull pop
                        dup_prefix + scan_key, blocking=False
                    )
                    if res is None:
                        break
                    yield res

            # You may be tempted to remove the assignment to dups and use the
            # value directly in the for loop below. That would be a mistake.
            # The function finalize may push on the duplicate queue which we
            # are pulling off and so condensing those two lines creates a
            # potential infinite loop.
            dups = [dup for dup in exhaust()]
            for dup in dups:
                finalize(psid, sid, score, Notice(dup))

        return scan_key

    def send_notification(self, notice, failure=None, logfunc=None):
        if logfunc is None:
            logfunc = self.log.info

        if failure:
            notice.set('failure', failure)

        failure = notice.get('failure', None)
        if failure:
            logfunc("%s: %s", failure, str(notice.raw))

        queue_name = notice.get('notification_queue', False)
        if not queue_name:
            return

        score = notice.get('al_score', 0)
        threshold = notice.get('notification_threshold', None)
        if threshold and score < int(threshold):
            return

        q = notificationq.get(queue_name, None)
        if not q:
            notificationq[queue_name] = q = \
                queue.NamedQueue(queue_name, **persistent)
        q.push(notice.raw)


    def expired(self, delta, errors):
        # incomplete_expire_after_seconds = 3600
        # incomplete_stale_after_seconds = 1800

        if errors:
            return delta >= self.config.core.middleman.incomplete_expire_after_seconds
        else:
            return delta >= self.config.core.middleman.expire_after

    def drop(self, task: IngestTask) -> bool:
        priority = task.priority
        sample_threshold = self.config.core.middleman.sampling_at

        dropped = False
        if priority <= _min_priority:
            dropped = True
        else:
            for level, rng in self.priority_range.items():
                if rng[0] <= priority <= rng[1] and level in sample_threshold:
                    dropped = must_drop(self.unique_queue.count(*rng), sample_threshold[level])
                    break

            if not dropped:
                if task.file_size > self.config.submission.max_file_size or task.file_size == 0:
                    dropped = True

        if task.never_drop or not dropped:
            return False

        task.failure = 'Skipped'
        dropq.push(notice.raw)  # df push push

        self.ingester_counts.increment('ingest.skipped')

        return True

    def is_whitelisted(self, task: IngestTask):
        reason, hit = self.get_whitelist_verdict(self.whitelist, task)
        hit = {x: dotdump(safe_str(y)) for x, y in hit.items()}

        if not reason:
            with self.whitelisted_lock:
                reason = self.whitelisted.get(task.sha256, None)
                if reason:
                    hit = 'cached'

        if reason:
            if hit != 'cached':
                with self.whitelisted_lock:
                    self.whitelisted[task.sha256] = reason

            task.failure = "Whitelisting due to reason %s (%s)" % (dotdump(safe_str(reason)), hit)
            dropq.push(notice.raw)  # df push push

            self.ingester_counts.increment('ingest.whitelisted')
            self.whitelister_counts.increment('whitelist.' + reason)

        return reason

# shards = 1
# try:
#     shards = int(config.core.middleman.shards)
# except AttributeError:
#     logger.warning("No shards setting. Defaulting to %d.", shards)
#
# arg_parser = argparse.ArgumentParser()
# arg_parser.add_argument('-s', '--shard', default='0')
# opts = arg_parser.parse_args()
# shard = opts.shard
#
# # Globals
# alertq = queue.NamedQueue('m-alert', **persistent)  # df line queue
# chunk_size = 1000
# date_fmt = '%Y-%m-%dT%H:%M:%SZ'
# dup_prefix = 'w-' + shard + '-'
# dupq = queue.MultiQueue(**persistent)  # df line queue
# ip = get_hostip()
# hostinfo = {
#     'ip:': ip,
#     'mac_address': get_mac_for_ip(ip),
#     'host': get_hostname(),
# }
# max_retries = 10
# max_time = 2 * 24 * 60 * 60  # Wait 2 days for responses.
# max_waiting = int(config.core.dispatcher.max.inflight) / (2 * shards)
# retry_delay = 180
# retryq = queue.NamedQueue('m-retry-' + shard, **persistent)  # df line queue
# running = True
# sampling = False

# stale_after_seconds = config.core.middleman.stale_after
# start_time = now()
# submissionq = queue.NamedQueue('m-submission-' + shard, **persistent)  # df line queue
# timeouts = []
# timeouts_lock = RLock()
#
# dropper_threads = 1
# try:
#     dropper_threads = int(config.core.middleman.dropper_threads)
# except AttributeError:
#     logger.warning(
#         "No dropper_threads setting. Defaulting to %d.",
#         dropper_threads
#     )
#
#
# ingester_threads = 1
# try:
#     ingester_threads = int(config.core.middleman.ingester_threads)
# except AttributeError:
#     logger.warning(
#         "No ingester_threads setting. Defaulting to %d.",
#         ingester_threads
#     )
#
# submitter_threads = 1
# try:
#     submitter_threads = int(config.core.middleman.submitter_threads)
# except AttributeError:
#     logger.warning(
#         "No submitter_threads setting. Defaulting to %d.",
#         submitter_threads
#     )
#
#

#
# # Output. Notifications are placed on a notification queue.
# notificationq = {}
#
#
# # State. The submissions in progress are stored in Redis in order to
# # persist this state and recover in case we crash.
# scanning = Hash('m-scanning-' + shard, **persistent)  # df line hash
#
# # Status.
# statusq = queue.CommsQueue('status')
#


def determine_resubmit_selected(selected, resubmit_to):
    resubmit_selected = None

    selected = set(selected)
    resubmit_to = set(resubmit_to)

    if not selected.issuperset(resubmit_to):
        resubmit_selected = sorted(selected.union(resubmit_to))

    return resubmit_selected



def drop_chance(length, maximum):
    return tanh(float(length - maximum) / maximum * 2.0)


def dropper():  # df node def
    datastore = forge.get_datastore()

    while running:
        raw = dropq.pop(timeout=1)  # df pull pop
        if not raw:
            continue

        notice = Notice(raw)

        send_notification(notice)

        c12n = notice.get('classification', config.core.middleman.classification)
        expiry = now_as_iso(86400)
        sha256 = notice.get('sha256')

        datastore.save_or_freshen_file(sha256, {'sha256': sha256}, expiry, c12n)

    datastore.close()


def finalize(psid, sid, score, notice):  # df node def
    logger.debug("Finalizing (score=%d) %s", score, notice.get('sha256'))
    if psid:
        notice.set('psid', psid)
    notice.set('sid', sid)
    notice.set('al_score', score)

    selected = notice.get('selected', [])
    resubmit_to = notice.get('resubmit_to', [])

    resubmit_selected = determine_resubmit_selected(selected, resubmit_to)
    will_resubmit = resubmit_selected and should_resubmit(score)
    if will_resubmit:
        notice.set('psid', None)

    if is_alert(notice, score):
        alertq.push(notice.raw)  # df push push

    send_notification(notice)

    if will_resubmit:
        notice.set('psid', sid)
        notice.set('resubmit_to', [])
        notice.set('scan_key', None)
        notice.set('sid', None)
        notice.set('selected', resubmit_selected)
        priority = notice.get('priority', 0)

        uniqueq.push(priority, notice.raw)  # df push push


# noinspection PyBroadException
def init():
    datastore = forge.get_datastore()
    datastore.commit_index('submission')

    sids = [
        x['submission.sid'] for x in datastore.stream_search(
            'submission',
            'state:submitted AND times.submitted:[NOW-1DAY TO *] '
            'AND submission.metadata.type:* '
            'AND NOT submission.description:Resubmit*'
        )
    ]

    submissions = {}
    submitted = {}
    for submission in datastore.get_submissions(sids):
        task = Task(submission)

        if not task.original_selected or not task.root_sha256 or not task.scan_key:
            continue

        if forge.determine_ingest_queue(task.root_sha256) != ingestq_name:
            continue

        scan_key = task.scan_key
        submissions[task.sid] = submission
        submitted[scan_key] = task.sid

    # Outstanding is the set of things Riak believes are being scanned.
    outstanding = set(submitted.keys())

    # Keys is the set of things middleman believes are being scanned.
    keys = set(scanning.keys())

    # Inflight is the set of submissions middleman and Riak agree are inflight.
    inflight = outstanding.intersection(keys)

    # Missing is the set of submissions middleman thinks are in flight but
    # according to Riak are not incomplete.
    missing = keys.difference(inflight)

    # Process the set of submissions Riak believes are incomplete but
    # middleman doesn't know about.
    for scan_key in outstanding.difference(inflight):
        sid = submitted.get(scan_key, None)

        if not sid:
            logger.info("Init: No sid found for incomplete")
            continue

        submission = submissions[sid]
        task = Task(submission)

        if not task.original_selected or not task.root_sha256 or not task.scan_key:
            logger.info("Init: Not root_sha256 or original_selected")
            continue

        if not task.metadata:
            logger.info(
                "Init: Incomplete submission is not one of ours: %s", sid
            )

        stype = None
        try:
            stype = task.metadata.get('type', None)
        except:  # pylint: disable=W0702
            logger.exception(
                "Init: Incomplete submission has malformed metadata: %s", sid
            )

        if not stype:
            logger.info("Init: Incomplete submission missing type: %s", sid)

        raw = {
            'metadata': task.metadata,
            'overrides': get_submission_overrides(task, overrides),
            'sha256': task.root_sha256,
            'type': stype,
        }
        raw['overrides']['selected'] = task.original_selected

        reinsert(datastore, " (incomplete)", Notice(raw), logger)

    r = redis.StrictRedis(persistent['host'],
                          persistent['port'],
                          persistent['db'])

    # Duplicates is the set of sha256s where a duplicate queue exists.
    duplicates = [
        x.replace(dup_prefix, '', 1) for x in r.keys(dup_prefix + '*')
    ]

    # Process the set of duplicates where no scanning or riak entry exists.
    for scan_key in set(duplicates).difference(outstanding.union(keys)):
        raw = dupq.pop(dup_prefix + scan_key, blocking=False)
        if not raw:
            logger.warning("Init: Couldn't pop off dup queue (%s)", scan_key)
            dupq.delete(dup_prefix + scan_key)
            continue

        reinsert(datastore, " (missed duplicate)", Notice(raw), logger)

    while True:
        res = completeq.pop(blocking=False)
        if not res:
            break

        scan_key = completed(Task(res))
        try:
            missing.remove(scan_key)
        except:  # pylint: disable=W0702
            pass

    # Process the set of submissions middleman thinks are in flight but
    # according to Riak are not incomplete.
    for scan_key in missing:
        raw = scanning.pop(scan_key)
        if raw:
            reinsert(datastore, '', Notice(raw), logger, retry_all=False)

    # Set up time outs for all inflight submissions.
    expiry_time = now(max_time)
    for scan_key in inflight:
        # No need to lock. We're the only thing running at this point.
        timeouts.append(Timeout(scan_key, expiry_time))


    datastore.close()


def is_alert(notice, score):
    generate_alert = notice.get('generate_alert', True)
    if not generate_alert:
        return False

    if score < threshold_value['critical']:
        return False

    return True


def maintain_inflight():  # df node def
    while running:
        # If we are scanning less than the max_waiting, submit more.
        length = scanning.length() + submissionq.length()
        if length < 0:
            time.sleep(1)
            continue

        num = max_waiting - length
        if num <= 0:
            time.sleep(1)
            continue

        entries = uniqueq.pop(num)  # df pull pop
        if not entries:
            time.sleep(1)
            continue

        for raw in entries:
            # Remove the key event_timestamp if it exists.
            raw.pop('event_timestamp', None)

            submissionq.push(raw)  # df push push


###############################################################################
#
# To calculate the probability of dropping an incoming submission we compare
# the number returned by random() which will be in the range [0,1) and the
# number returned by tanh() which will be in the range (-1,1).
#
# If length is less than maximum the number returned by tanh will be negative
# and so drop will always return False since the value returned by random()
# cannot be less than 0.
#
# If length is greater than maximum, drop will return False with a probability
# that increases as the distance between maximum and length increases:
#
#     Length           Chance of Dropping
#
#     <= maximum       0
#     1.5 * maximum    0.76
#     2 * maximum      0.96
#     3 * maximum      0.999
#
###############################################################################
def must_drop(length, maximum):
    return random() < drop_chance(length, maximum)


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


def reinsert(datastore, msg, notice, out, retry_all=True):
    sha256 = notice.get('sha256')
    if not sha256:
        logger.error("Invalid sha256: %s", notice.raw)

    if forge.determine_ingest_queue(sha256) != ingestq_name:
        return

    pprevious, previous, score = None, False, None
    if not notice.get('ignore_cache', False):
        pprevious, previous, score, _ = check(datastore, notice)

    if previous:
        out.info("Init: Found%s: %s", msg, notice.get('sha256'))
        finalize(pprevious, previous, score, notice)
    elif retry_all or not score:
        logger.info("Init: Retrying%s: %s", msg, notice.get('sha256'))
        ingestq.push(notice.raw)
    else:
        logger.info("Init: Stale%s: %s", msg, notice.get('sha256'))


def retry(raw, scan_key, sha256, ex):  # df node def
    current_time = now()

    notice = Notice(raw)
    retries = notice.get('retries', 0) + 1

    if retries > max_retries:
        trace = ''
        if ex:
            trace = ': ' + get_stacktrace_info(ex)
        logger.error('Max retries exceeded for %s%s', sha256, trace)
        dupq.delete(dup_prefix + scan_key)
    elif expired(current_time - seconds(notice.get('ts', current_time)), 0):
        logger.info('No point retrying expired submission for %s', sha256)
        dupq.delete(dup_prefix + scan_key)  # df pull delete
    else:
        logger.info('Requeuing %s (%s)', sha256, ex or 'unknown')
        notice.set('retries', retries)
        notice.set('retry_at', now(retry_delay))

        retryq.push(notice.raw)  # df push push


def return_exception(func, *args, **kwargs):
    try:
        func(*args, **kwargs)
        return None
    except Exception as ex:  # pylint: disable=W0703
        return ex


# noinspection PyBroadException
def seconds(t, default=0):
    try:
        try:
            return float(t)
        except ValueError:
            return iso_to_epoch(t)
    except:  # pylint:disable=W0702
        return default


def send_heartbeat():
    t = now()

    up_hours = (t - start_time) / (60.0 * 60.0)

    queues = {}
    drop_p = {}

    for level in ('low', 'medium', 'critical', 'high'):
        queues[level] = uniqueq.count(*priority_range[level])
        threshold = sample_threshold[level]
        # noinspection PyTypeChecker
        drop_p[level] = 1 - max(0, drop_chance(queues[level], threshold))

    heartbeat = {
        'hostinfo': hostinfo,
        'inflight': scanning.length(),
        'ingest': ingestq.length(),
        'ingesting': drop_p,
        'queues': queues,
        'shard': shard,
        'up_hours': up_hours,
        'waiting': submissionq.length(),

        'ingest.bytes_completed': 0,
        'ingest.bytes_ingested': 0,
        'ingest.duplicates': 0,
        'ingest.files_completed': 0,
        'ingest.skipped': 0,
        'ingest.submissions_completed': 0,
        'ingest.submissions_ingested': 0,
        'ingest.timed_out': 0,
        'ingest.whitelisted': 0,
    }

    # Send ingester stats.
    exported = ingester_counts.export()

    # Add ingester stats to our heartbeat.
    heartbeat.update(exported)

    # Send our heartbeat.
    raw = message.Message(to="*", sender='middleman',
                          mtype=message.MT_INGESTHEARTBEAT,
                          body=heartbeat).as_dict()
    statusq.publish(raw)

    # Send whitelister stats.
    whitelister_counts.export()


def send_heartbeats():
    while running:
        send_heartbeat()
        time.sleep(1)


def should_resubmit(score):

    # Resubmit:
    #
    # 100%     with a score above 400.
    # 10%      with a score of 301 to 400.
    # 1%       with a score of 201 to 300.
    # 0.1%     with a score of 101 to 200.
    # 0.01%    with a score of 1 to 100.
    # 0.001%   with a score of 0.
    # 0%       with a score below 0.

    if score < 0:
        return False

    if score > 400:
        return True

    resubmit_probability = 1.0 / 10 ** ((500 - score) / 100)

    return random() < resubmit_probability


def stale(delta, errors):
    if errors:
        return delta >= incomplete_stale_after_seconds
    else:
        return delta >= stale_after_seconds


def submit(client, notice):
    priority = notice.get('priority')
    sha256 = notice.get('sha256')

    hdr = notice.parse(
        description=': '.join((default_prefix, sha256 or '')), **defaults
    )

    user = hdr.pop('submitter')
    hdr.pop('priority', None)

    path = notice.get('filename', None) or sha256
    client.submit(sha256, path, priority, user, **hdr)
    with timeouts_lock:
        timeouts.append(Timeout(now(max_time), notice.get('scan_key')))




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
