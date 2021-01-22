#!/usr/bin/env python
"""
Ingester

Ingester is responsible for monitoring for incoming submission requests,
sending submissions, waiting for submissions to complete, sending a message
to a notification queue as specified by the submission and, based on the
score received, possibly sending a message to indicate that an alert should
be created.
"""

import threading
import time
from random import random
from typing import Iterable, List

from assemblyline.common.metrics import MetricsFactory
from assemblyline.common.str_utils import dotdump, safe_str
from assemblyline.common.exceptions import get_stacktrace_info
from assemblyline.common.isotime import now, now_as_iso
from assemblyline.common.importing import load_module_by_path
from assemblyline.common import forge
from assemblyline.odm.models.filescore import FileScore
from assemblyline.odm.messages.ingest_heartbeat import Metrics
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.queues.priority import PriorityQueue
from assemblyline.remote.datatypes.queues.comms import CommsQueue
from assemblyline.remote.datatypes.queues.multi import MultiQueue
from assemblyline.remote.datatypes.hash import Hash
from assemblyline.remote.datatypes import get_client
from assemblyline import odm
from assemblyline.odm.models.submission import SubmissionParams
from assemblyline.odm.models.alert import EXTENDED_SCAN_VALUES
from assemblyline.odm.messages.submission import Submission, SubmissionMessage

from assemblyline_core.alerter.run_alerter import ALERT_QUEUE_NAME
from assemblyline_core.submission_client import SubmissionClient
from .constants import INGEST_QUEUE_NAME, drop_chance

_completeq_name = 'm-complete'
_dup_prefix = 'w-m-'
_notification_queue_prefix = 'nq-'
_min_priority = 1
_max_retries = 10
_retry_delay = 60 * 4  # Wait 4 minutes to retry
_max_time = 2 * 24 * 60 * 60  # Wait 2 days for responses.
HOUR_IN_SECONDS = 60 * 60


def must_drop(length, maximum):
    """
    To calculate the probability of dropping an incoming submission we compare
    the number returned by random() which will be in the range [0,1) and the
    number returned by tanh() which will be in the range (-1,1).

    If length is less than maximum the number returned by tanh will be negative
    and so drop will always return False since the value returned by random()
    cannot be less than 0.

    If length is greater than maximum, drop will return False with a probability
    that increases as the distance between maximum and length increases:

        Length           Chance of Dropping

        <= maximum       0
        1.5 * maximum    0.76
        2 * maximum      0.96
        3 * maximum      0.999
    """
    return random() < drop_chance(length, maximum)


def determine_resubmit_selected(selected, resubmit_to):
    resubmit_selected = None

    selected = set(selected)
    resubmit_to = set(resubmit_to)

    if not selected.issuperset(resubmit_to):
        resubmit_selected = sorted(selected.union(resubmit_to))

    return resubmit_selected


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


@odm.model()
class IngestTask(odm.Model):
    # Submission Parameters
    submission: Submission = odm.Compound(Submission)

    # Shortcut for properties of the submission
    @property
    def file_size(self) -> int:
        return sum(file.size for file in self.submission.files)

    @property
    def params(self) -> SubmissionParams:
        return self.submission.params

    @property
    def sha256(self) -> str:
        return self.submission.files[0].sha256

    # Information about the ingestion itself, parameters irrelevant
    scan_key = odm.Optional(odm.Keyword())  # the filescore key
    retries = odm.Integer(default=0)

    # Fields added after a submission is complete for notification/bookkeeping processes
    failure = odm.Text(default='')  # If the ingestion has failed for some reason, what is it?
    score = odm.Optional(odm.Integer())  # Score from previous processing of this file
    extended_scan = odm.Enum(EXTENDED_SCAN_VALUES, default="skipped")
    ingest_id = odm.UUID()
    ingest_time = odm.Date(default="NOW")


class Ingester:
    """Internal interface to the ingestion queues."""

    def __init__(self, datastore, logger, classification=None, redis=None, persistent_redis=None,
                 metrics_name='ingester'):
        self.datastore = datastore
        self.log = logger

        # Cache the user groups
        self.cache_lock = threading.RLock()  # TODO are middle man instances single threaded now?
        self._user_groups = {}
        self._user_groups_reset = time.time()//HOUR_IN_SECONDS
        self.cache = {}
        self.notification_queues = {}
        self.whitelisted = {}
        self.whitelisted_lock = threading.RLock()

        # Create a config cache that will refresh config values periodically
        self.config = forge.CachedObject(forge.get_config)

        # Module path parameters are fixed at start time. Changing these involves a restart
        self.is_low_priority = load_module_by_path(self.config.core.ingester.is_low_priority)
        self.get_whitelist_verdict = load_module_by_path(self.config.core.ingester.get_whitelist_verdict)
        self.whitelist = load_module_by_path(self.config.core.ingester.whitelist)

        # Constants are loaded based on a non-constant path, so has to be done at init rather than load
        constants = forge.get_constants(self.config)
        self.priority_value = constants.PRIORITIES
        self.priority_range = constants.PRIORITY_RANGES
        self.threshold_value = constants.PRIORITY_THRESHOLDS

        # Connect to the redis servers
        self.redis = redis or get_client(
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )
        self.persistent_redis = persistent_redis or get_client(
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )

        # Classification engine
        self.ce = classification or forge.get_classification()

        # Metrics gathering factory
        self.counter = MetricsFactory(metrics_type='ingester', schema=Metrics, redis=self.redis,
                                      config=self.config, name=metrics_name)

        # State. The submissions in progress are stored in Redis in order to
        # persist this state and recover in case we crash.
        self.scanning = Hash('m-scanning-table', self.persistent_redis)

        # Input. The dispatcher creates a record when any submission completes.
        self.complete_queue = NamedQueue(_completeq_name, self.redis)

        # Internal. Dropped entries are placed on this queue.
        # self.drop_queue = NamedQueue('m-drop', self.persistent_redis)

        # Input. An external process places submission requests on this queue.
        self.ingest_queue = NamedQueue(INGEST_QUEUE_NAME, self.persistent_redis)

        # Output. Duplicate our input traffic into this queue so it may be cloned by other systems
        self.traffic_queue = CommsQueue('submissions', self.redis)

        # Internal. Unique requests are placed in and processed from this queue.
        self.unique_queue = PriorityQueue('m-unique', self.persistent_redis)

        # Internal, delay queue for retrying
        self.retry_queue = PriorityQueue('m-retry', self.persistent_redis)

        # Internal, timeout watch queue
        self.timeout_queue = PriorityQueue('m-timeout', self.redis)

        # Internal, queue for processing duplicates
        #   When a duplicate file is detected (same cache key => same file, and same
        #   submission parameters) the file won't be ingested normally, but instead a reference
        #   will be written to a duplicate queue. Whenever a file is finished, in the complete
        #   method, not only is the original ingestion finalized, but all entries in the duplicate queue
        #   are finalized as well. This has the effect that all concurrent ingestion of the same file
        #   are 'merged' into a single submission to the system.
        self.duplicate_queue = MultiQueue(self.persistent_redis)

        # Output. submissions that should have alerts generated
        self.alert_queue = NamedQueue(ALERT_QUEUE_NAME, self.persistent_redis)

        # Utility object to help submit tasks to dispatching
        self.submit_client = SubmissionClient(datastore=self.datastore, redis=self.redis)

    def get_groups_from_user(self, username: str) -> List[str]:
        # Reset the group cache at the top of each hour
        if time.time()//HOUR_IN_SECONDS > self._user_groups_reset:
            self._user_groups = {}
            self._user_groups_reset = time.time()//HOUR_IN_SECONDS

        # Get the groups for this user if not known
        if username not in self._user_groups:
            user_data = self.datastore.user.get(username)
            if user_data:
                self._user_groups[username] = user_data.groups
            else:
                self._user_groups[username] = []
        return self._user_groups[username]

    def ingest(self, task: IngestTask):
        self.log.info(f"[{task.ingest_id} :: {task.sha256}] Task received for processing")
        # Load a snapshot of ingest parameters as of right now.
        max_file_size = self.config.submission.max_file_size
        param = task.params

        self.counter.increment('bytes_ingested', increment_by=task.file_size)
        self.counter.increment('submissions_ingested')

        if any(len(file.sha256) != 64 for file in task.submission.files):
            self.log.error(f"[{task.ingest_id} :: {task.sha256}] Invalid sha256, skipped")
            self.send_notification(task, failure="Invalid sha256", logfunc=self.log.warning)
            return

        # Clean up metadata strings, since we may delete some, iterate on a copy of the keys
        for key in list(task.submission.metadata.keys()):
            value = task.submission.metadata[key]
            meta_size = len(value)
            if meta_size > self.config.submission.max_metadata_length:
                self.log.info(f'[{task.ingest_id} :: {task.sha256}] '
                              f'Removing {key} from metadata because value is too big')
                task.submission.metadata.pop(key)

        if task.file_size > max_file_size and not task.params.ignore_size and not task.params.never_drop:
            task.failure = f"File too large ({task.file_size} > {max_file_size})"
            self._notify_drop(task)
            self.counter.increment('skipped')
            self.log.error(f"[{task.ingest_id} :: {task.sha256}] {task.failure}")
            return

        # Set the groups from the user, if they aren't already set
        if not task.params.groups:
            task.params.groups = self.get_groups_from_user(task.params.submitter)

        # Check if this file is already being processed
        pprevious, previous, score = None, False, None
        if not param.ignore_cache:
            pprevious, previous, score, _ = self.check(task)

        # Assign priority.
        low_priority = self.is_low_priority(task)

        priority = param.priority
        if priority < 0:
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
        if priority and self.expired(current_time - task.submission.time.timestamp(), 0):
            priority = (priority / 10) or 1

        param.priority = priority

        # Do this after priority has been assigned.
        # (So we don't end up dropping the resubmission).
        if previous:
            self.counter.increment('duplicates')
            self.finalize(pprevious, previous, score, task)

            # On cache hits of any kind we want to send out a completed message
            self.traffic_queue.publish(SubmissionMessage({
                'msg': task.submission,
                'msg_type': 'SubmissionCompleted',
                'sender': 'ingester',
            }).as_primitives())
            return

        if self.drop(task):
            self.log.info(f"[{task.ingest_id} :: {task.sha256}] Dropped")
            return

        if self.is_whitelisted(task):
            self.log.info(f"[{task.ingest_id} :: {task.sha256}] Whitelisted")
            return

        self.unique_queue.push(priority, task.as_primitives())

    def check(self, task: IngestTask):
        key = self.stamp_filescore_key(task)

        with self.cache_lock:
            result = self.cache.get(key, None)

        if result:
            self.counter.increment('cache_hit_local')
            self.log.info(f'[{task.ingest_id} :: {task.sha256}] Local cache hit')
        else:
            result = self.datastore.filescore.get(key)
            if result:
                self.counter.increment('cache_hit')
                self.log.info(f'[{task.ingest_id} :: {task.sha256}] Remote cache hit')
            else:
                self.counter.increment('cache_miss')
                return None, False, None, key

            with self.cache_lock:
                self.cache[key] = result

        current_time = now()
        age = current_time - result.time
        errors = result.errors

        if self.expired(age, errors):
            self.log.info(f"[{task.ingest_id} :: {task.sha256}] Cache hit dropped, cache has expired")
            self.counter.increment('cache_expired')
            self.cache.pop(key, None)
            self.datastore.filescore.delete(key)
            return None, False, None, key
        elif self.stale(age, errors):
            self.log.info(f"[{task.ingest_id} :: {task.sha256}] Cache hit dropped, cache is stale")
            self.counter.increment('cache_stale')
            return None, False, result.score, key

        return result.psid, result.sid, result.score, key

    def stale(self, delta: float, errors: int):
        if errors:
            return delta >= self.config.core.ingester.incomplete_stale_after_seconds
        else:
            return delta >= self.config.core.ingester.stale_after_seconds

    @staticmethod
    def stamp_filescore_key(task: IngestTask, sha256=None):
        if not sha256:
            sha256 = task.submission.files[0].sha256

        key = task.scan_key

        if not key:
            key = task.params.create_filescore_key(sha256)
            task.scan_key = key

        return key

    def completed(self, sub):
        """Invoked when notified that a submission has completed."""
        # There is only one file in the submissions we have made
        sha256 = sub.files[0].sha256
        scan_key = sub.params.create_filescore_key(sha256)
        raw = self.scanning.pop(scan_key)

        psid = sub.params.psid
        score = sub.max_score
        sid = sub.sid

        if not raw:
            # Some other worker has already popped the scanning queue?
            self.log.warning(f"[{sub.metadata.get('ingest_id', 'unknown')} :: {sha256}] "
                             f"Submission completed twice")
            return scan_key

        task = IngestTask(raw)
        task.submission.sid = sid

        errors = sub.error_count
        file_count = sub.file_count
        self.counter.increment('submissions_completed')
        self.counter.increment('files_completed', increment_by=file_count)
        self.counter.increment('bytes_completed', increment_by=task.file_size)

        with self.cache_lock:
            fs = self.cache[scan_key] = FileScore({
                'expiry_ts': now(self.config.core.ingester.cache_dtl * 24 * 60 * 60),
                'errors': errors,
                'psid': psid,
                'score': score,
                'sid': sid,
                'time': now(),
            })
            self.datastore.filescore.save(scan_key, fs)

        self.finalize(psid, sid, score, task)

        def exhaust() -> Iterable[IngestTask]:
            while True:
                res = self.duplicate_queue.pop(_dup_prefix + scan_key, blocking=False)
                if res is None:
                    break
                res = IngestTask(res)
                res.submission.sid = sid
                yield res

        # You may be tempted to remove the assignment to dups and use the
        # value directly in the for loop below. That would be a mistake.
        # The function finalize may push on the duplicate queue which we
        # are pulling off and so condensing those two lines creates a
        # potential infinite loop.
        dups = [dup for dup in exhaust()]
        for dup in dups:
            self.finalize(psid, sid, score, dup)

        return scan_key

    def send_notification(self, task: IngestTask, failure=None, logfunc=None):
        if logfunc is None:
            logfunc = self.log.info

        if failure:
            task.failure = failure

        failure = task.failure
        if failure:
            logfunc("%s: %s", failure, str(task.json()))

        if not task.submission.notification.queue:
            return

        note_queue = _notification_queue_prefix + task.submission.notification.queue
        threshold = task.submission.notification.threshold

        if threshold is not None and task.score is not None and task.score < threshold:
            return

        q = self.notification_queues.get(note_queue, None)
        if not q:
            self.notification_queues[note_queue] = q = NamedQueue(note_queue, self.persistent_redis)
        q.push(task.as_primitives())

    def expired(self, delta: float, errors) -> bool:
        if errors:
            return delta >= self.config.core.ingester.incomplete_expire_after_seconds
        else:
            return delta >= self.config.core.ingester.expire_after

    def drop(self, task: IngestTask) -> bool:
        priority = task.params.priority
        sample_threshold = self.config.core.ingester.sampling_at

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

        if task.params.never_drop or not dropped:
            return False

        task.failure = 'Skipped'
        self._notify_drop(task)
        self.counter.increment('skipped')
        return True

    def _notify_drop(self, task: IngestTask):
        self.send_notification(task)

        c12n = task.params.classification
        expiry = now_as_iso(86400)
        sha256 = task.submission.files[0].sha256

        self.datastore.save_or_freshen_file(sha256, {'sha256': sha256}, expiry, c12n, redis=self.redis)

    def is_whitelisted(self, task: IngestTask):
        reason, hit = self.get_whitelist_verdict(self.whitelist, task)
        hit = {x: dotdump(safe_str(y)) for x, y in hit.items()}
        sha256 = task.submission.files[0].sha256

        if not reason:
            with self.whitelisted_lock:
                reason = self.whitelisted.get(sha256, None)
                if reason:
                    hit = 'cached'

        if reason:
            if hit != 'cached':
                with self.whitelisted_lock:
                    self.whitelisted[sha256] = reason

            task.failure = "Whitelisting due to reason %s (%s)" % (dotdump(safe_str(reason)), hit)
            self._notify_drop(task)

            self.counter.increment('whitelisted')

        return reason

    def submit(self, task: IngestTask):
        self.submit_client.submit(
            submission_obj=task.submission,
            completed_queue=_completeq_name,
        )

        self.timeout_queue.push(int(now(_max_time)), task.scan_key)
        self.log.info(f"[{task.ingest_id} :: {task.sha256}] Submitted to dispatcher for analysis")

    def retry(self, task, scan_key, ex):
        current_time = now()

        retries = task.retries + 1

        if retries > _max_retries:
            trace = ''
            if ex:
                trace = ': ' + get_stacktrace_info(ex)
            self.log.error(f'[{task.ingest_id} :: {task.sha256}] Max retries exceeded {trace}')
            self.duplicate_queue.delete(_dup_prefix + scan_key)
        elif self.expired(current_time - task.ingest_time.timestamp(), 0):
            self.log.info(f'[{task.ingest_id} :: {task.sha256}] No point retrying expired submission')
            self.duplicate_queue.delete(_dup_prefix + scan_key)
        else:
            self.log.info(f'[{task.ingest_id} :: {task.sha256}] Requeuing ({ex or "unknown"})')
            task.retries = retries
            self.retry_queue.push(int(now(_retry_delay)), task.as_primitives())

    def finalize(self, psid, sid, score, task: IngestTask):
        self.log.info(f"[{task.ingest_id} :: {task.sha256}] Completed")
        if psid:
            task.params.psid = psid
        task.score = score
        task.submission.sid = sid

        selected = task.params.services.selected
        resubmit_to = task.params.services.resubmit

        resubmit_selected = determine_resubmit_selected(selected, resubmit_to)
        will_resubmit = resubmit_selected and should_resubmit(score)
        if will_resubmit:
            task.extended_scan = 'submitted'
            task.params.psid = None

        if self.is_alert(task, score):
            self.log.info(f"[{task.ingest_id} :: {task.sha256}] Notifying alerter "
                          f"to {'update' if will_resubmit else 'create'} an alert")
            self.alert_queue.push(task.as_primitives())

        self.send_notification(task)

        if will_resubmit:
            self.log.info(f"[{task.ingest_id} :: {task.sha256}] Resubmitted for extended analysis")
            task.params.psid = sid
            task.submission.sid = None
            task.params.services.resubmit = []
            task.scan_key = None
            task.params.services.selected = resubmit_selected

            self.unique_queue.push(task.params.priority, task.as_primitives())

    def is_alert(self, task: IngestTask, score):
        if not task.params.generate_alert:
            return False

        if score < self.threshold_value['critical']:
            return False

        return True
