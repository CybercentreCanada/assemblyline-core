#!/usr/bin/env python
"""
Ingester

Ingester is responsible for monitoring for incoming submission requests,
sending submissions, waiting for submissions to complete, sending a message
to a notification queue as specified by the submission and, based on the
score received, possibly sending a message to indicate that an alert should
be created.
"""

import logging
import threading
import time
from os import environ
from random import random
from typing import Any, Iterable, List, Optional, Tuple

import elasticapm

from assemblyline.common.postprocess import ActionWorker
from assemblyline_core.server_base import ThreadedCoreBase
from assemblyline.common.metrics import MetricsFactory
from assemblyline.common.str_utils import dotdump, safe_str
from assemblyline.common.exceptions import get_stacktrace_info
from assemblyline.common.isotime import now, now_as_iso
from assemblyline.common.importing import load_module_by_path
from assemblyline.common import forge, exceptions, isotime
from assemblyline.datastore.exceptions import DataStoreException
from assemblyline.filestore import CorruptedFileStoreException, FileStoreException
from assemblyline.odm.models.filescore import FileScore
from assemblyline.odm.models.user import User
from assemblyline.odm.messages.ingest_heartbeat import Metrics
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.queues.priority import PriorityQueue
from assemblyline.remote.datatypes.queues.comms import CommsQueue
from assemblyline.remote.datatypes.queues.multi import MultiQueue
from assemblyline.remote.datatypes.hash import Hash
from assemblyline.remote.datatypes.user_quota_tracker import UserQuotaTracker
from assemblyline import odm
from assemblyline.odm.models.submission import SubmissionParams, Submission as DatabaseSubmission
from assemblyline.odm.models.alert import EXTENDED_SCAN_VALUES
from assemblyline.odm.messages.submission import Submission as MessageSubmission, SubmissionMessage

from assemblyline_core.dispatching.dispatcher import Dispatcher
from assemblyline_core.submission_client import SubmissionClient
from .constants import INGEST_QUEUE_NAME, drop_chance, COMPLETE_QUEUE_NAME

_dup_prefix = 'w-m-'
_notification_queue_prefix = 'nq-'
_min_priority = 1
_max_retries = 10
_retry_delay = 60 * 4  # Wait 4 minutes to retry
_max_time = 2 * 24 * 60 * 60  # Wait 2 days for responses.
HOUR_IN_SECONDS = 60 * 60
COMPLETE_THREADS = int(environ.get('INGESTER_COMPLETE_THREADS', 4))
INGEST_THREADS = int(environ.get('INGESTER_INGEST_THREADS', 1))
SUBMIT_THREADS = int(environ.get('INGESTER_SUBMIT_THREADS', 4))


def must_drop(length: int, maximum: int) -> bool:
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


@odm.model()
class IngestTask(odm.Model):
    # Submission Parameters
    submission: MessageSubmission = odm.compound(MessageSubmission)

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
    retries = odm.Integer(default=0)

    # Fields added after a submission is complete for notification/bookkeeping processes
    failure = odm.Text(default='')  # If the ingestion has failed for some reason, what is it?
    score = odm.Optional(odm.Integer())  # Score from previous processing of this file
    extended_scan = odm.Enum(EXTENDED_SCAN_VALUES, default="skipped")  # Status of the extended scan
    ingest_id = odm.UUID()  # Ingestion Identifier
    ingest_time = odm.Date(default="NOW")  # Time at which the file was ingested
    notify_time = odm.Optional(odm.Date())  # Time at which the user is notify the submission is finished


class Ingester(ThreadedCoreBase):
    def __init__(self, datastore=None, logger: Optional[logging.Logger] = None,
                 classification=None, redis=None, persistent_redis=None,
                 metrics_name='ingester', config=None):
        super().__init__('assemblyline.ingester', logger, redis=redis, redis_persist=persistent_redis,
                         datastore=datastore, config=config)

        # Cache the user groups
        self.cache_lock = threading.RLock()
        self._user_groups: dict[str, list[str]] = {}
        self._user_groups_reset = time.time()//HOUR_IN_SECONDS
        self.cache: dict[str, FileScore] = {}
        self.notification_queues: dict[str, NamedQueue] = {}
        self.whitelisted: dict[str, Any] = {}
        self.whitelisted_lock = threading.RLock()

        # Module path parameters are fixed at start time. Changing these involves a restart
        self.is_low_priority = load_module_by_path(self.config.core.ingester.is_low_priority)
        self.get_whitelist_verdict = load_module_by_path(self.config.core.ingester.get_whitelist_verdict)
        self.whitelist = load_module_by_path(self.config.core.ingester.whitelist)

        # Constants are loaded based on a non-constant path, so has to be done at init rather than load
        constants = forge.get_constants(self.config)
        self.priority_value: dict[str, int] = constants.PRIORITIES
        self.priority_range: dict[str, Tuple[int, int]] = constants.PRIORITY_RANGES
        self.threshold_value: dict[str, int] = constants.PRIORITY_THRESHOLDS

        # Classification engine
        self.ce = classification or forge.get_classification()

        # Metrics gathering factory
        self.counter = MetricsFactory(metrics_type='ingester', schema=Metrics, redis=self.redis,
                                      config=self.config, name=metrics_name)

        # State. The submissions in progress are stored in Redis in order to
        # persist this state and recover in case we crash.
        self.scanning = Hash('m-scanning-table', self.redis_persist)

        # Input. The dispatcher creates a record when any submission completes.
        self.complete_queue = NamedQueue(COMPLETE_QUEUE_NAME, self.redis)

        # Input. An external process places submission requests on this queue.
        self.ingest_queue = NamedQueue(INGEST_QUEUE_NAME, self.redis_persist)

        # Output. Duplicate our input traffic into this queue so it may be cloned by other systems
        self.traffic_queue = CommsQueue('submissions', self.redis)

        # Internal. Unique requests are placed in and processed from this queue.
        self.unique_queue = PriorityQueue('m-unique', self.redis_persist)

        # Internal, delay queue for retrying
        self.retry_queue = PriorityQueue('m-retry', self.redis_persist)

        # Internal, timeout watch queue
        self.timeout_queue: PriorityQueue[str] = PriorityQueue('m-timeout', self.redis)

        # Internal, queue for processing duplicates
        #   When a duplicate file is detected (same cache key => same file, and same
        #   submission parameters) the file won't be ingested normally, but instead a reference
        #   will be written to a duplicate queue. Whenever a file is finished, in the complete
        #   method, not only is the original ingestion finalized, but all entries in the duplicate queue
        #   are finalized as well. This has the effect that all concurrent ingestion of the same file
        #   are 'merged' into a single submission to the system.
        self.duplicate_queue = MultiQueue(self.redis_persist)

        # Utility object to help submit tasks to dispatching
        self.submit_client = SubmissionClient(datastore=self.datastore, redis=self.redis)
        # Utility object to handle post-processing actions
        self.postprocess_worker = ActionWorker(cache=True, config=self.config, datastore=self.datastore,
                                               redis_persist=self.redis_persist)
        # Async Submission quota tracker
        self.async_submission_tracker = UserQuotaTracker('async_submissions', timeout=24 * 60 * 60,  # 1 day timeout
                                                         redis=self.redis_persist)

        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = forge.get_apm_client("ingester")
        else:
            self.apm_client = None

    def try_run(self):
        threads_to_maintain = {
            'Retries': self.handle_retries,
            'Timeouts': self.handle_timeouts,
            'Missing': self.handle_missing,
        }
        threads_to_maintain.update({f'Complete_{n}': self.handle_complete for n in range(COMPLETE_THREADS)})
        threads_to_maintain.update({f'Ingest_{n}': self.handle_ingest for n in range(INGEST_THREADS)})
        threads_to_maintain.update({f'Submit_{n}': self.handle_submit for n in range(SUBMIT_THREADS)})
        self.maintain_threads(threads_to_maintain)

    def handle_ingest(self):
        cpu_mark = time.process_time()
        time_mark = time.time()

        # Move from ingest to unique and waiting queues.
        # While there are entries in the ingest queue we consume chunk_size
        # entries at a time and move unique entries to uniqueq / queued and
        # duplicates to their own queues / waiting.
        while self.running:
            while not self.active:
                # Ingester is disabled... waiting for it to be reactivated
                self.sleep(0.1)

            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

            message = self.ingest_queue.pop(timeout=1)

            cpu_mark = time.process_time()
            time_mark = time.time()

            if not message:
                continue

            # Start of ingest message
            if self.apm_client:
                self.apm_client.begin_transaction('ingest_msg')

            try:
                if 'submission' in message:
                    # A retried task
                    task = IngestTask(message)
                else:
                    # A new submission
                    sub = MessageSubmission(message)
                    task = IngestTask(dict(
                        submission=sub,
                        ingest_id=sub.sid,
                    ))
                    task.submission.sid = None  # Reset to new random uuid
                    # Write all input to the traffic queue
                    self.traffic_queue.publish(SubmissionMessage({
                        'msg': sub,
                        'msg_type': 'SubmissionIngested',
                        'sender': 'ingester',
                    }).as_primitives())

            except (ValueError, TypeError) as error:
                self.counter.increment('error')
                self.log.exception(f"Dropped ingest submission {message} because {str(error)}")

                # End of ingest message (value_error)
                if self.apm_client:
                    self.apm_client.end_transaction('ingest_input', 'value_error')
                continue

            self.ingest(task)

            # End of ingest message (success)
            if self.apm_client:
                self.apm_client.end_transaction('ingest_input', 'success')

    def handle_submit(self):
        time_mark, cpu_mark = time.time(), time.process_time()

        while self.running:
            # noinspection PyBroadException
            try:
                self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
                self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

                # Check if there is room for more submissions
                length = self.scanning.length()
                if length >= self.config.core.ingester.max_inflight:
                    self.sleep(0.1)
                    time_mark, cpu_mark = time.time(), time.process_time()
                    continue

                raw = self.unique_queue.blocking_pop(timeout=3)
                time_mark, cpu_mark = time.time(), time.process_time()
                if not raw:
                    continue

                # Start of ingest message
                if self.apm_client:
                    self.apm_client.begin_transaction('ingest_msg')

                task = IngestTask(raw)

                # Check if we need to drop a file for capacity reasons, but only if the
                # number of files in flight is alreay over 80%
                if length >= self.config.core.ingester.max_inflight * 0.8 and self.drop(task):
                    # End of ingest message (dropped)
                    if self.apm_client:
                        self.apm_client.end_transaction('ingest_submit', 'dropped')
                    continue

                if self.is_whitelisted(task):
                    # End of ingest message (whitelisted)
                    if self.apm_client:
                        self.apm_client.end_transaction('ingest_submit', 'whitelisted')
                    continue

                # Check if this file has been previously processed.
                pprevious, previous, score, scan_key = None, None, None, None
                if not task.submission.params.ignore_cache:
                    pprevious, previous, score, scan_key = self.check(task)
                else:
                    scan_key = self.stamp_filescore_key(task)

                # If it HAS been previously processed, we are dealing with a resubmission
                # finalize will decide what to do, and put the task back in the queue
                # rewritten properly if we are going to run it again
                if previous:
                    if not task.submission.params.services.resubmit and not pprevious:
                        self.log.warning(f"No psid for what looks like a resubmission of "
                                         f"{task.submission.files[0].sha256}: {scan_key}")
                    self.finalize(pprevious, previous, score, task, cache=True)
                    # End of ingest message (finalized)
                    if self.apm_client:
                        self.apm_client.end_transaction('ingest_submit', 'finalized')

                    continue

                # We have decided this file is worth processing

                # Add the task to the scanning table, this is atomic across all submit
                # workers, so if it fails, someone beat us to the punch, record the file
                # as a duplicate then.
                if not self.scanning.add(scan_key, task.as_primitives()):
                    self.log.debug('Duplicate %s', task.submission.files[0].sha256)
                    self.counter.increment('duplicates')
                    self.duplicate_queue.push(_dup_prefix + scan_key, task.as_primitives())
                    # End of ingest message (duplicate)
                    if self.apm_client:
                        self.apm_client.end_transaction('ingest_submit', 'duplicate')

                    continue

                # We have managed to add the task to the scan table, so now we go
                # ahead with the submission process
                try:
                    self.submit(task)
                    # End of ingest message (submitted)
                    if self.apm_client:
                        self.apm_client.end_transaction('ingest_submit', 'submitted')

                    continue
                except Exception as _ex:
                    # For some reason (contained in `ex`) we have failed the submission
                    # The rest of this function is error handling/recovery
                    ex = _ex
                    # traceback = _ex.__traceback__

                self.counter.increment('error')

                should_retry = True
                if isinstance(ex, CorruptedFileStoreException):
                    self.log.error("Submission for file '%s' failed due to corrupted "
                                   "filestore: %s" % (task.sha256, str(ex)))
                    should_retry = False
                elif isinstance(ex, DataStoreException):
                    trace = exceptions.get_stacktrace_info(ex)
                    self.log.error("Submission for file '%s' failed due to "
                                   "data store error:\n%s" % (task.sha256, trace))
                elif not isinstance(ex, FileStoreException):
                    trace = exceptions.get_stacktrace_info(ex)
                    self.log.error("Submission for file '%s' failed: %s" % (task.sha256, trace))

                task = IngestTask(self.scanning.pop(scan_key))
                if not task:
                    self.log.error('No scanning entry for for %s', task.sha256)
                    # End of ingest message (no_scan_entry)
                    if self.apm_client:
                        self.apm_client.end_transaction('ingest_submit', 'no_scan_entry')

                    continue

                if not should_retry:
                    # End of ingest message (cannot_retry)
                    if self.apm_client:
                        self.apm_client.end_transaction('ingest_submit', 'cannot_retry')

                    continue

                self.retry(task, scan_key, ex)
                # End of ingest message (retry)
                if self.apm_client:
                    self.apm_client.end_transaction('ingest_submit', 'retried')

            except Exception:
                self.log.exception("Unexpected error")
                # End of ingest message (exception)
                if self.apm_client:
                    self.apm_client.end_transaction('ingest_submit', 'exception')

    def handle_complete(self):
        while self.running:
            result = self.complete_queue.pop(timeout=3)
            if not result:
                continue

            cpu_mark = time.process_time()
            time_mark = time.time()

            # Start of ingest message
            if self.apm_client:
                self.apm_client.begin_transaction('ingest_msg')

            sub = DatabaseSubmission(result)
            self.completed(sub)

            # End of ingest message (success)
            if self.apm_client:
                elasticapm.label(sid=sub.sid)
                self.apm_client.end_transaction('ingest_complete', 'success')

            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

    def handle_retries(self):
        tasks = []
        while self.sleep(0 if tasks else 3):
            cpu_mark = time.process_time()
            time_mark = time.time()

            # Start of ingest message
            if self.apm_client:
                self.apm_client.begin_transaction('ingest_retries')

            tasks = self.retry_queue.dequeue_range(upper_limit=isotime.now(), num=100)

            for task in tasks:
                self.ingest_queue.push(task)

            # End of ingest message (success)
            if self.apm_client:
                elasticapm.label(retries=len(tasks))
                self.apm_client.end_transaction('ingest_retries', 'success')

            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

    def handle_timeouts(self):
        timeouts = []
        while self.sleep(0 if timeouts else 3):
            cpu_mark = time.process_time()
            time_mark = time.time()

            # Start of ingest message
            if self.apm_client:
                self.apm_client.begin_transaction('ingest_timeouts')

            timeouts = self.timeout_queue.dequeue_range(upper_limit=isotime.now(), num=100)

            for scan_key in timeouts:
                # noinspection PyBroadException
                try:
                    actual_timeout = False

                    # Remove the entry from the hash of submissions in progress.
                    entry = self.scanning.pop(scan_key)
                    if entry:
                        actual_timeout = True
                        self.log.error("Submission timed out for %s: %s", scan_key, str(entry))

                    dup = self.duplicate_queue.pop(_dup_prefix + scan_key, blocking=False)
                    if dup:
                        actual_timeout = True

                    while dup:
                        self.log.error("Submission timed out for %s: %s", scan_key, str(dup))
                        dup = self.duplicate_queue.pop(_dup_prefix + scan_key, blocking=False)

                    if actual_timeout:
                        self.counter.increment('timed_out')
                except Exception:
                    self.log.exception("Problem timing out %s:", scan_key)

            # End of ingest message (success)
            if self.apm_client:
                elasticapm.label(timeouts=len(timeouts))
                self.apm_client.end_transaction('ingest_timeouts', 'success')

            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

    def handle_missing(self) -> None:
        """
        Messages get dropped or only partially processed when ingester and dispatcher containers scale up and down.

        This loop checks for submissions that are in two invalid states:
         - finished but still listed as being scanned by ingester (message probably dropped by ingester)
         - listed by ingester but unknown by dispatcher (message could have been dropped on either end)

        Loading all the info needed to do these checks is a bit slow, but doing them every 5 or 15 minutes
        per ingester shouldn't be noteworthy. While these missing messages are bound to happen from time to time
        they should be rare. With that in mind, a warning is raised whenever this worker processes something
        so that if a constant stream of items are falling through and getting processed here it might stand out.
        """
        last_round: set[str] = set()

        while self.sleep(300 if last_round else 900):
            # Get the current set of outstanding tasks
            outstanding: dict[str, dict] = self.scanning.items()

            # Get jobs being processed by dispatcher or in dispatcher queue
            assignment: dict[str, str] = {}
            for data in self.submit_client.dispatcher.queued_submissions():
                assignment[data['submission']['sid']] = ''
            for dis in Dispatcher.all_instances(self.redis_persist):
                for key in Dispatcher.instance_assignment(self.redis_persist, dis):
                    assignment[key] = dis

            # Filter out outstanding tasks currently assigned or in queue
            outstanding = {
                key: doc
                for key, doc in outstanding.items()
                if doc["submission"]["sid"] not in assignment
            }

            unprocessed = []
            for key, data in outstanding.items():
                task = IngestTask(data)
                sid = task.submission.sid

                # Check if its already complete in the database
                from_db = self.datastore.submission.get_if_exists(sid)
                if from_db and from_db.state == "completed":
                    self.log.warning(f"Completing a hanging finished submission [{sid}]")
                    self.completed(from_db)

                # Check for items that have been in an unknown state since the last round
                # and put it back in processing
                elif sid in last_round:
                    self.log.warning(f"Recovering a submission dispatcher hasn't processed [{sid}]")
                    self.submit(task)

                # Otherwise defer looking at this until next iteration
                else:
                    unprocessed.append(sid)

            # store items for next round
            last_round = set(unprocessed)

    def get_groups_from_user(self, username: str) -> List[str]:
        # Reset the group cache at the top of each hour
        if time.time()//HOUR_IN_SECONDS > self._user_groups_reset:
            self._user_groups = {}
            self._user_groups_reset = time.time()//HOUR_IN_SECONDS

        # Get the groups for this user if not known
        if username not in self._user_groups:
            user_data: User = self.datastore.user.get(username)
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
            task.params.groups = [g for g in self.get_groups_from_user(
                task.params.submitter) if g in str(task.params.classification)]

        # Check if this file is already being processed
        pprevious, previous, score = None, None, None
        if not param.ignore_cache:
            pprevious, previous, score, _ = self.check(task, count_miss=False)

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

            if self.config.core.ingester.always_create_submission:
                # Create a submission record based on the cache hit
                submission = self.datastore.submission.get(previous, as_obj=False)

                # Assign the current submission as the PSID for the new submission
                pprevious = previous
                previous = task.ingest_id
                task.params.psid = pprevious

                submission.update({
                    'archived': False,
                    'archive_ts': None,
                    'classification': task.params.classification,
                    'expiry_ts': now_as_iso(task.params.ttl * 24 * 60 * 60),
                    'from_archive': False,
                    'metadata': task.submission.metadata,
                    'params': task.params.as_primitives(),
                    'sid': previous,
                    'to_be_deleted': False,
                    'times': {
                        'submitted': task.ingest_time,
                        'completed': "NOW"
                    },
                })
                self.datastore.submission.save(previous, submission)

            self.finalize(pprevious, previous, score, task, cache=True)

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

    def check(self, task: IngestTask, count_miss=True) -> Tuple[Optional[str], Optional[str], Optional[float], str]:
        key = self.stamp_filescore_key(task)

        with self.cache_lock:
            result: Optional[FileScore] = self.cache.get(key, None)

        if result:
            self.counter.increment('cache_hit_local')
            self.log.info(f'[{task.ingest_id} :: {task.sha256}] Local cache hit')
        else:
            result = self.datastore.filescore.get_if_exists(key)
            if result:
                self.counter.increment('cache_hit')
                self.log.info(f'[{task.ingest_id} :: {task.sha256}] Remote cache hit')
            else:
                if count_miss:
                    self.counter.increment('cache_miss')
                return None, None, None, key

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
            return None, None, None, key
        elif self.stale(age, errors):
            self.log.info(f"[{task.ingest_id} :: {task.sha256}] Cache hit dropped, cache is stale")
            self.counter.increment('cache_stale')
            return None, None, result.score, key

        return result.psid, result.sid, result.score, key

    def stop(self):
        super().stop()
        if self.apm_client:
            elasticapm.uninstrument()
        self.submit_client.stop()
        self.postprocess_worker.stop()

    def stale(self, delta: float, errors: int):
        if errors:
            return delta >= self.config.core.ingester.incomplete_stale_after_seconds
        else:
            return delta >= self.config.core.ingester.stale_after_seconds

    @staticmethod
    def stamp_filescore_key(task: IngestTask, sha256: Optional[str] = None) -> str:
        if not sha256:
            sha256 = task.submission.files[0].sha256

        key = task.submission.scan_key

        if not key:
            key = task.params.create_filescore_key(sha256)
            task.submission.scan_key = key

        return key

    def completed(self, sub: DatabaseSubmission):
        """Invoked when notified that a submission has completed."""
        # There is only one file in the submissions we have made
        sha256 = sub.files[0].sha256
        scan_key = sub.scan_key
        if not scan_key:
            self.log.warning(f"[{sub.metadata.get('ingest_id', 'unknown')} :: {sha256}] "
                             f"Submission missing scan key")
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
            self.finalize(psid, sid, score, dup, cache=True)

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
            self.notification_queues[note_queue] = q = NamedQueue(note_queue, self.redis_persist)

        # Mark at which time an item was queued
        task.notify_time = now_as_iso()

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
                if (task.file_size > self.config.submission.max_file_size and not task.params.ignore_size) \
                        or task.file_size == 0:
                    dropped = True

        if task.params.never_drop or not dropped:
            return False

        task.failure = 'Skipped'
        self._notify_drop(task)
        self.counter.increment('skipped')
        return True

    def _notify_drop(self, task: IngestTask):
        if self.config.ui.enforce_quota:
            self.async_submission_tracker.end(task.params.submitter)

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
            completed_queue=COMPLETE_QUEUE_NAME,
        )

        self.timeout_queue.push(int(now(_max_time)), task.submission.scan_key)
        self.log.info(f"[{task.ingest_id} :: {task.sha256}] Submitted to dispatcher for analysis")

    def retry(self, task: IngestTask, scan_key: str, ex):
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

    def finalize(self, psid: str, sid: str, score: float, task: IngestTask, cache=False):
        self.log.info(f"[{task.ingest_id} :: {task.sha256}] Completed")
        if psid:
            task.params.psid = psid
        task.score = score
        task.submission.sid = sid

        if cache:
            did_resubmit = self.postprocess_worker.process_cachehit(task.submission, score)

            if did_resubmit:
                task.extended_scan = 'submitted'
                task.params.psid = None

        if self.config.ui.enforce_quota:
            self.async_submission_tracker.end(task.params.submitter)

        self.send_notification(task)
