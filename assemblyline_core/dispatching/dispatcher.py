import bisect
import uuid
import os
import threading
import queue
import time
from collections import defaultdict
from contextlib import contextmanager
from typing import Dict, List, Tuple, Set, Optional
import json

import elasticapm

from assemblyline.common import isotime
from assemblyline.common.constants import make_watcher_list_name, SUBMISSION_QUEUE, \
    DISPATCH_RUNNING_TASK_HASH, SCALER_TIMEOUT_QUEUE, DISPATCH_TASK_HASH, SERVICE_VERSION_HASH
from assemblyline.common.forge import get_service_queue
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.metrics import MetricsFactory
from assemblyline.common.tagging import tag_dict_to_list
from assemblyline.odm.messages.dispatcher_heartbeat import Metrics
from assemblyline.odm.messages.service_heartbeat import Metrics as ServiceMetrics
from assemblyline.odm.messages.dispatching import WatchQueueMessage, CreateWatch, DispatcherCommandMessage, \
    CREATE_WATCH, LIST_OUTSTANDING, ListOutstanding
from assemblyline.odm.messages.submission import SubmissionMessage, from_datastore_submission
from assemblyline.odm.messages.task import FileInfo, Task as ServiceTask
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.service import Service
from assemblyline.odm.models.submission import Submission
from assemblyline.remote.datatypes.exporting_counter import export_metrics_once
from assemblyline.remote.datatypes.hash import ExpiringHash, Hash
from assemblyline.remote.datatypes.queues.comms import CommsQueue
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.set import ExpiringSet
from assemblyline.remote.datatypes.user_quota_tracker import UserQuotaTracker
from assemblyline_core.server_base import ThreadedCoreBase

from .schedules import Scheduler

APM_SPAN_TYPE = 'handle_message'


@contextmanager
def apm_span(client, span_name: str):
    try:
        if client:
            client.begin_transaction(APM_SPAN_TYPE)
        yield None
        if client:
            client.end_transaction(span_name, 'success')
    except Exception:
        if client:
            client.end_transaction(span_name, 'exception')
        raise


class ResultSummary:
    def __init__(self, key, drop, score, children):
        self.key: str = key
        self.drop: bool = drop
        self.score: int = score
        self.children: List[str] = children


class SubmissionTask:
    """Dispatcher internal model for submissions"""

    def __init__(self, submission, completed_queue):
        self.submission: Submission = Submission(submission)
        self.completed_queue = str(completed_queue)
        self.lock = threading.Lock()
        self.timeout_at: Optional[int] = None

        self.file_info: Dict[str, FileInfo] = {}
        self.file_names: Dict[str, str] = {}
        self.file_schedules: Dict[str, List[Dict[str, Service]]] = {}
        self.file_tags = defaultdict(list)
        self.file_depth: Dict[str, int] = {}
        self.file_temporary_data = defaultdict(dict)
        self.extra_errors = []
        self.dropped_files = set()

        self.service_results: Dict[Tuple[str, str], ResultSummary] = {}
        self.service_errors: Dict[Tuple[str, str], dict] = {}
        self.service_attempts: Dict[Tuple[str, str], int] = defaultdict(int)
        self.queue_keys: Dict[Tuple[str, str], bytes] = {}
        self.running_services: Dict[Tuple[str, str], Tuple[int, str]] = {}

    @property
    def sid(self):
        return self.submission.sid


DISPATCH_TASK_ASSIGNMENT = 'dispatcher-tasks-assigned-to-'
DISPATCH_START_EVENTS = 'dispatcher-start-events-'
DISPATCH_RESULT_QUEUE = 'dispatcher-results-'
DISPATCH_COMMAND_QUEUE = 'dispatcher-commands-'
DISPATCH_DIRECTORY = 'dispatchers-directory'
QUEUE_EXPIRY = 60*60
SERVICE_VERSION_CACHE_TIME = 30  # Time between checking redis for new service version info
SERVICE_VERSION_EXPIRY_TIME = 30 * 60  # How old service version info can be before we ignore it
GUARD_TIMEOUT = 60*2
GLOBAL_TASK_CHECK_INTERVAL = 60*3
TIMEOUT_EXTRA_TIME = 30  # 30 seconds grace for message handling.
TIMEOUT_TEST_INTERVAL = 5
MAX_RESULT_BUFFER = 64
RESULT_THREADS = max(1, int(os.getenv('DISPATCHER_RESULT_THREADS', '2')))

# After 20 minutes, check if a submission is still making progress.
# In the case of a crash somewhere else in the system, we may not have
# gotten a message we are expecting. This should prompt a retry in most
# cases.
SUBMISSION_TOTAL_TIMEOUT = 60 * 20


class Dispatcher(ThreadedCoreBase):
    @staticmethod
    def all_instances(persistent_redis):
        return Hash(DISPATCH_DIRECTORY, host=persistent_redis).keys()

    @staticmethod
    def instance_assignment_size(persistent_redis, instance_id):
        return Hash(DISPATCH_TASK_ASSIGNMENT + instance_id, host=persistent_redis).length()

    @staticmethod
    def all_queue_lengths(redis, instance_id):
        return {
            'start': NamedQueue(DISPATCH_START_EVENTS + instance_id, host=redis).length(),
            'result': NamedQueue(DISPATCH_RESULT_QUEUE + instance_id, host=redis).length(),
            'command': NamedQueue(DISPATCH_COMMAND_QUEUE + instance_id, host=redis).length()
        }

    def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None,
                 config=None, counter_name='dispatcher'):
        super().__init__('assemblyline.dispatcher', config=config, datastore=datastore,
                         redis=redis, redis_persist=redis_persist, logger=logger)

        # Load the datastore collections that we are going to be using
        self.instance_id = uuid.uuid4().hex
        # self.submissions: Collection = datastore.submission
        # self.results: Collection = datastore.result
        # self.errors: Collection = datastore.error
        # self.files: Collection = datastore.file
        self._tasks: Dict[str, SubmissionTask] = {}
        self._tasks_lock = threading.Lock()

        #
        # # Build some utility classes
        self.scheduler = Scheduler(self.datastore, self.config, self.redis)
        self.running_tasks = Hash(DISPATCH_RUNNING_TASK_HASH, host=self.redis)
        self.scaler_timeout_queue = NamedQueue(SCALER_TIMEOUT_QUEUE, host=self.redis_persist)

        # Track what the most recent service versions seen by the service server are
        self.service_versions = Hash(SERVICE_VERSION_HASH, host=self.redis)
        self._service_versions: Dict[str, Tuple[float, Tuple[str, str]]] = {}

        # self.classification_engine = forge.get_classification()
        #
        # Output. Duplicate our input traffic into this queue so it may be cloned by other systems
        self.traffic_queue = CommsQueue('submissions', self.redis)
        self.quota_tracker = UserQuotaTracker('submissions', timeout=60 * 60, host=self.redis_persist)
        self.submission_queue = NamedQueue(SUBMISSION_QUEUE, self.redis)

        # Table to track the running dispatchers
        self.dispatchers_directory = Hash(DISPATCH_DIRECTORY, host=self.redis_persist)
        self.running_dispatchers_estimate = 1

        # Tables to track what submissions are running where
        self.active_submissions = ExpiringHash(DISPATCH_TASK_ASSIGNMENT+self.instance_id, host=self.redis_persist)
        self.submissions_assignments = ExpiringHash(DISPATCH_TASK_HASH, host=self.redis_persist)

        # Communications queues
        self.start_queue = NamedQueue(DISPATCH_START_EVENTS+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)
        self.result_queue = NamedQueue(DISPATCH_RESULT_QUEUE+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)
        self.command_queue = NamedQueue(DISPATCH_COMMAND_QUEUE+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)

        # Publish counters to the metrics sink.
        self.counter = MetricsFactory(metrics_type='dispatcher', schema=Metrics, name=counter_name,
                                      redis=self.redis, config=self.config)

        self.apm_client = None
        if self.config.core.metrics.apm_server.server_url:
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="dispatcher")

        self.timeout_list_lock = threading.Lock()
        self.timeout_list: List[Tuple[int, str, str, str]] = []

        # Setup queues for work to be divided into
        self.process_queues: List[queue.Queue] = [queue.Queue(MAX_RESULT_BUFFER) for _ in range(RESULT_THREADS)]
        self.internal_process_queues: List[queue.Queue] = [queue.Queue() for _ in range(RESULT_THREADS)]

    def process_queue_index(self, key: str) -> int:
        return sum(ord(_x) for _x in key) % RESULT_THREADS

    def find_process_queue(self, key: str) -> queue.Queue:
        return self.process_queues[self.process_queue_index(key)]

    def find_internal_process_queue(self, key: str) -> queue.Queue:
        return self.internal_process_queues[self.process_queue_index(key)]

    @elasticapm.capture_span(span_type='dispatcher')
    def get_service_version(self, service_name: str) -> Tuple[Optional[str], Optional[str]]:
        # Try to load a cached value if we can
        if service_name in self._service_versions:
            read_time, row = self._service_versions[service_name]
            if time.time() - read_time < SERVICE_VERSION_CACHE_TIME:
                return row

        # Read a new value from redis
        row = self.service_versions.get(service_name)
        if not row:
            self._service_versions.pop(service_name, None)
            return None, None
        write_time, service_version, tool_version = row
        if time.time() - write_time > SERVICE_VERSION_EXPIRY_TIME:
            return None, None
        self._service_versions[service_name] = time.time(), (service_version, tool_version)
        return service_version, tool_version

    @elasticapm.capture_span(span_type='dispatcher')
    def add_task(self, task: SubmissionTask):
        with self._tasks_lock:
            self._tasks[task.submission.sid] = task

    @elasticapm.capture_span(span_type='dispatcher')
    def get_task(self, sid: str) -> Optional[SubmissionTask]:
        with self._tasks_lock:
            return self._tasks.get(sid)

    def service_worker_factory(self, index: int):
        def service_worker():
            return self.service_worker(index)
        return service_worker

    def try_run(self):
        self.log.info(f'Using dispatcher id {self.instance_id}')
        threads = {
            # Pull in new submissions
            'Pull Submissions': self.pull_submissions,
            # pull start messages
            'Pull Service Start': self.pull_service_starts,
            # pull result messages
            'Pull Service Result': self.pull_service_results,
            # Handle timeouts
            'Process Timeouts': self.handle_timeouts,
            # Work guard/thief
            'Guard Work': self.work_guard,
            'Work Thief': self.work_thief,
            # Handle RPC commands
            'Commands': self.handle_commands,
            # Process to protect against old dead tasks timing out
            'Global Timeout Backstop': self.timeout_backstop,
        }
        for ii in range(RESULT_THREADS):
            # Process results
            threads[f'Service Update Worker #{ii}'] = self.service_worker_factory(ii)

        self.maintain_threads(threads)

        # If the dispatcher is exiting cleanly remove as many tasks from the service queues as we can
        service_queues = {}
        for task in self._tasks.values():
            for (sha256, service_name), dispatch_key in task.queue_keys.items():
                try:
                    s_queue = service_queues[service_name]
                except KeyError:
                    s_queue = get_service_queue(service_name, self.redis)
                    service_queues[service_name] = s_queue
                s_queue.remove(dispatch_key)

    def pull_submissions(self):
        sub_queue = self.submission_queue
        cpu_mark = time.process_time()
        time_mark = time.time()

        while self.running:
            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

            # Check if we are at the submission limit globally
            if self.submissions_assignments.length() >= self.config.core.dispatcher.max_inflight:
                self.sleep(1)
                cpu_mark = time.process_time()
                time_mark = time.time()
                continue

            # Check if we are maxing out our share of the submission limit
            max_tasks = self.config.core.dispatcher.max_inflight / self.running_dispatchers_estimate
            if self.active_submissions.length() >= max_tasks:
                self.sleep(1)
                cpu_mark = time.process_time()
                time_mark = time.time()
                continue

            # Grab a submission message
            message = sub_queue.pop(timeout=1)
            cpu_mark = time.process_time()
            time_mark = time.time()

            if not message:
                continue

            # Start of process dispatcher transaction
            with apm_span(self.apm_client, 'submission_message'):
                # This is probably a complete task
                task = SubmissionTask(**message)
                if self.apm_client:
                    elasticapm.label(sid=task.submission.sid)

                with task.lock:
                    self.dispatch_submission(task)

    @elasticapm.capture_span(span_type='dispatcher')
    def dispatch_submission(self, task: SubmissionTask):
        """
        Find any files associated with a submission and dispatch them if they are
        not marked as in progress. If all files are finished, finalize the submission.

        This version of dispatch submission doesn't verify each result, but assumes that
        the dispatch table has been kept up to date by other components.

        Preconditions:
            - File exists in the filestore and file collection in the datastore
            - Submission is stored in the datastore
        """
        submission = task.submission
        sid = submission.sid

        if not self.active_submissions.exists(sid):
            self.log.info(f"[{sid}] New submission received")
            self.active_submissions.add(sid, {
                'completed_queue': task.completed_queue,
                'submission': submission.as_primitives()
            })
            self.submissions_assignments.set(sid, self.instance_id)

            # Write all new submissions to the traffic queue
            self.traffic_queue.publish(SubmissionMessage({
                'msg': from_datastore_submission(task.submission),
                'msg_type': 'SubmissionStarted',
                'sender': 'dispatcher',
            }).as_primitives())

        else:
            self.log.warning(f"[{sid}] Received a pre-existing submission, check if it is complete")

        # Refresh the quota hold
        if submission.params.quota_item and submission.params.submitter:
            self.log.info(f"[{sid}] Submission counts towards {submission.params.submitter.upper()} quota")

        # Apply initial data parameter
        if submission.params.initial_data:
            try:
                task.file_temporary_data[submission.files[0].sha256] = json.loads(submission.params.initial_data)
            except ValueError as err:
                self.log.warning(f"[{sid}] could not process initialization data: {err}")

        self.add_task(task)
        self.set_submission_timeout(task)

        task.file_depth[submission.files[0].sha256] = 0
        task.file_names[submission.files[0].sha256] = submission.files[0].name or submission.files[0].sha256
        self.dispatch_file(task, submission.files[0].sha256)

    @elasticapm.capture_span(span_type='dispatcher')
    def dispatch_file(self, task: SubmissionTask, sha256: str) -> bool:
        """
        Dispatch to any outstanding services for the given file.
        If nothing can be dispatched, check if the submission is finished.

        :param task: Submission task object.
        :param sha256: hash of the file to check.
        :param timed_out_host: Name of the host that timed out after maximum service attempts.
        :return: true if submission is finished.
        """
        submission = task.submission
        sid = submission.sid
        elasticapm.label(sid=sid, sha256=sha256)

        # If its the first time we've seen this file, we won't have a schedule for it
        if sha256 not in task.file_schedules:
            with elasticapm.capture_span('build_schedule'):
                # We are processing this file, load the file info, and build the schedule
                filestore_info = self.datastore.file.get(sha256)
                if filestore_info is None:
                    task.dropped_files.add(sha256)
                    self._dispatching_error(task, Error({
                        'archive_ts': task.submission.archive_ts,
                        'expiry_ts': task.submission.expiry_ts,
                        'response': {
                            'message': f"Couldn't find file info for {sha256} in submission {sid}",
                            'service_name': 'Dispatcher',
                            'service_tool_version': '4.0',
                            'service_version': '4.0',
                            'status': 'FAIL_NONRECOVERABLE'
                        },
                        'sha256': sha256,
                        'type': 'UNKNOWN'
                    }))
                    task.file_info[sha256] = None
                    task.file_schedules[sha256] = []
                else:
                    file_info = task.file_info[sha256] = FileInfo(dict(
                        magic=filestore_info.magic,
                        md5=filestore_info.md5,
                        mime=filestore_info.mime,
                        sha1=filestore_info.sha1,
                        sha256=filestore_info.sha256,
                        size=filestore_info.size,
                        type=filestore_info.type,
                    ))
                    task.file_schedules[sha256] = self.scheduler.build_schedule(submission, file_info.type)
        file_info = task.file_info[sha256]
        schedule = list(task.file_schedules[sha256])

        # Go through each round of the schedule removing complete/failed services
        # Break when we find a stage that still needs processing
        outstanding = {}
        started_stages = []
        with elasticapm.capture_span('check_result_table'):
            while schedule and not outstanding:
                stage = schedule.pop(0)
                started_stages.append(stage)

                for service_name in stage:
                    service = self.scheduler.services.get(service_name)
                    if not service:
                        continue

                    key = (sha256, service_name)

                    # If the service terminated in an error, count the error and continue
                    if key in task.service_errors:
                        continue

                    # If we have no error, and no result, its not finished
                    result = task.service_results.get(key)
                    if not result:
                        outstanding[service_name] = service
                        continue

                    # if the service finished, count the score, and check if the file has been dropped
                    if not submission.params.ignore_filtering and result.drop:
                        # Clear out anything in the schedule after this stage
                        task.file_schedules[sha256] = started_stages
                        schedule.clear()

        # Try to retry/dispatch any outstanding services
        if outstanding:
            sent, enqueued, running, cache_hits = [], [], [], []

            for service_name, service in outstanding.items():
                with elasticapm.capture_span('dispatch_task', labels={'service': service_name}):
                    service_queue = get_service_queue(service_name, self.redis)

                    key = (sha256, service_name)
                    # Check if the task is already running
                    if key in task.running_services:
                        running.append(service_name)
                        continue

                    # Check if this task is already sitting in queue
                    with elasticapm.capture_span('check_queue'):
                        dispatch_key = task.queue_keys.get(key, None)
                        if dispatch_key is not None and service_queue.rank(dispatch_key) is not None:
                            enqueued.append(service_name)
                            continue

                    # Build the actual service dispatch message
                    config = self.build_service_config(service, submission)
                    service_task = ServiceTask(dict(
                        sid=sid,
                        metadata=submission.metadata,
                        min_classification=task.submission.classification,
                        service_name=service_name,
                        service_config=config,
                        fileinfo=file_info,
                        filename=task.file_names.get(sha256, sha256),
                        depth=task.file_depth[sha256],
                        max_files=task.submission.params.max_extracted,
                        ttl=submission.params.ttl,
                        ignore_cache=submission.params.ignore_cache,
                        ignore_dynamic_recursion_prevention=submission.params.ignore_dynamic_recursion_prevention,
                        tags=task.file_tags.get(sha256, []),
                        temporary_submission_data=[
                            {'name': name, 'value': value}
                            for name, value in task.file_temporary_data[sha256].items()
                        ],
                        deep_scan=submission.params.deep_scan,
                        priority=submission.params.priority,
                    ))
                    service_task.metadata['dispatcher__'] = self.instance_id

                    # Check if this task is a cache hit
                    if self.check_result_cache(task, service_task):
                        cache_hits.append(service_name)
                        continue

                    # Check if we have attempted this too many times already.
                    task.service_attempts[key] += 1
                    if task.service_attempts[key] > 3:
                        self.retry_error(task, sha256, service_name)
                        continue

                    # Its a new task, send it to the service
                    queue_key = service_queue.push(service_task.priority, service_task.as_primitives())
                    task.queue_keys[(sha256, service_name)] = queue_key
                    sent.append(service_name)

            if sent or enqueued or running or cache_hits:
                # If we have confirmed that we are waiting, or have taken an action, log that.
                self.log.info(f"[{sid}] File {sha256} sent to: {sent} "
                              f"already in queue for: {enqueued} "
                              f"running on: {running} "
                              f"cache hit on: {cache_hits}")
            else:
                # If we are not waiting, and have not taken an action, we must have hit the
                # retry limit on the only service running. In that case, we can move directly
                # onto the next stage of services, so recurse to trigger them.
                return self.dispatch_file(task, sha256)

        else:
            self.counter.increment('files_completed')
            if len(task.queue_keys) > 0 or len(task.running_services) > 0:
                self.log.info(f"[{sid}] Finished processing file '{sha256}', submission incomplete "
                              f"(queued: {len(task.queue_keys)} running: {len(task.running_services)})")
            else:
                self.log.info(f"[{sid}] Finished processing file '{sha256}', checking if submission complete")
                return self.check_submission(task)
        return False

    @elasticapm.capture_span(span_type='dispatcher')
    def check_result_cache(self, task: SubmissionTask, service_task: ServiceTask) -> bool:
        # Check if caching is disabled for this task/service
        service_data = self.service_info.get(service_task.service_name, None)
        if service_task.ignore_cache or service_data is None or service_data.disable_cache:
            return False

        # Make sure we know what service version the task would be sent to
        service_version, service_tool_version = self.get_service_version(service_task.service_name)
        if service_version is None:
            return False

        # Build the result key and see if the result exists
        result_key = Result.help_build_key(sha256=service_task.fileinfo.sha256,
                                           service_name=service_task.service_name,
                                           service_version=service_version,
                                           service_tool_version=service_tool_version,
                                           is_empty=False,
                                           task=service_task)

        stats = defaultdict(int)
        try:
            result = self.datastore.result.get_if_exists(result_key)
            if result:
                stats['cache_hit'] += 1
                if result.result.score:
                    stats['scored'] += 1
                else:
                    stats['not_scored'] += 1
                self.set_timeout(task, service_task.fileinfo.sha256, service_task.service_name, None)
                self.find_internal_process_queue(task.sid).put(('result', {
                    'service_task': service_task.as_primitives(),
                    'result': result.as_primitives(),
                    'result_key': result_key,
                    'temporary_data': None
                }))
                return True

            result = self.datastore.emptyresult.get_if_exists(f"{result_key}.e")
            if result:
                stats['cache_hit'] += 1
                stats['not_scored'] += 1
                result = self.datastore.create_empty_result_from_key(result_key)
                self.set_timeout(task, service_task.fileinfo.sha256, service_task.service_name, None)
                self.find_internal_process_queue(task.sid).put(('result', {
                    'service_task': service_task.as_primitives(),
                    'result': result.as_primitives(),
                    'result_key': f"{result_key}.e",
                    'temporary_data': None
                }))
                return True
            return False
        finally:
            if stats:
                export_metrics_once(service_task.service_name, ServiceMetrics, stats,
                                    counter_type='service', host='dispatcher')

    @elasticapm.capture_span(span_type='dispatcher')
    def check_submission(self, task: SubmissionTask) -> bool:
        """
        Check if a submission is finished.

        :param task: Task object for the submission in question.
        :return: true if submission has been finished.
        """
        # Track which files we have looked at already
        checked: Set[str] = set()
        unchecked: List[str] = list(task.file_depth.keys())

        # Categorize files as pending/processing (can be both) all others are finished
        pending_files = []  # Files where we are missing a service and it is not being processed
        processing_files = []  # Files where at least one service is in progress/queued

        # Track information about the results as we hit them
        file_scores: Dict[str, int] = {}

        # Make sure we have either a result or
        while unchecked:
            sha256 = unchecked.pop()
            checked.add(sha256)

            if sha256 in task.dropped_files:
                continue

            if sha256 not in task.file_schedules:
                pending_files.append(sha256)
                continue
            schedule = list(task.file_schedules[sha256])

            while schedule and sha256 not in pending_files and sha256 not in processing_files:
                stage = schedule.pop(0)
                for service_name in stage:

                    # Only active services should be in this dict, so if a service that was placed in the
                    # schedule is now missing it has been disabled or taken offline.
                    service = self.scheduler.services.get(service_name)
                    if not service:
                        continue

                    # If there is an error we are finished with this service
                    key = sha256, service_name
                    if key in task.service_errors:
                        continue

                    # if there is a result, then the service finished already
                    result = task.service_results.get(key)
                    if result:
                        if not task.submission.params.ignore_filtering and result.drop:
                            schedule.clear()

                        # Collect information about the result
                        file_scores[sha256] = file_scores.get(sha256, 0) + result.score
                        unchecked += set(result.children) - checked
                        continue

                    # If the file is in process, we may not need to dispatch it, but we aren't finished
                    # with the submission.
                    if key in task.running_services:
                        processing_files.append(sha256)
                        # another service may require us to dispatch it though so continue rather than break
                        continue

                    # Check if the service is in queue, and handle it the same as being in progress.
                    # Check this one last, since it can require a remote call to redis rather than checking a dict.
                    service_queue = get_service_queue(service_name, self.redis)
                    if key in task.queue_keys and service_queue.rank(task.queue_keys[key]) is not None:
                        processing_files.append(sha256)
                        continue

                    # Since the service is not finished or in progress, it must still need to start
                    pending_files.append(sha256)
                    break

        # Filter out things over the depth limit
        depth_limit = self.config.submission.max_extraction_depth
        pending_files = [sha for sha in pending_files if task.file_depth[sha] < depth_limit]

        # If there are pending files, then at least one service, on at least one
        # file isn't done yet, and hasn't been filtered by any of the previous few steps
        # poke those files.
        if pending_files:
            self.log.debug(f"[{task.submission.sid}] Dispatching {len(pending_files)} files: {list(pending_files)}")
            for file_hash in pending_files:
                if self.dispatch_file(task, file_hash):
                    return True
        elif processing_files:
            self.log.debug(f"[{task.submission.sid}] Not finished waiting on {len(processing_files)} "
                           f"files: {list(processing_files)}")
        else:
            self.log.debug(f"[{task.submission.sid}] Finalizing submission.")
            max_score = max(file_scores.values()) if file_scores else 0  # Submissions with no results have no score
            self.finalize_submission(task, max_score, checked)
            return True
        return False

    @classmethod
    def build_service_config(cls, service: Service, submission: Submission) -> Dict[str, str]:
        """Prepare the service config that will be used downstream.

        v3 names: get_service_params get_config_data
        """
        # Load the default service config
        params = {x.name: x.default for x in service.submission_params}

        # Over write it with values from the submission
        if service.name in submission.params.service_spec:
            params.update(submission.params.service_spec[service.name])
        return params

    @elasticapm.capture_span(span_type='dispatcher')
    def finalize_submission(self, task: SubmissionTask, max_score, file_list):
        """All of the services for all of the files in this submission have finished or failed.

        Update the records in the datastore, and flush the working data from redis.
        """
        submission = task.submission
        sid = submission.sid

        #
        results = list(task.service_results.values())
        errors = list(task.service_errors.values())
        errors.extend(task.extra_errors)

        submission.classification = submission.params.classification
        submission.error_count = len(errors)
        submission.errors = errors
        submission.file_count = len(file_list)
        submission.results = [r.key for r in results]
        submission.max_score = max_score
        submission.state = 'completed'
        submission.times.completed = isotime.now_as_iso()
        self.datastore.submission.save(sid, submission)

        self._cleanup_submission(task)
        self.log.info(f"[{sid}] Completed; files: {len(file_list)} results: {len(results)} "
                      f"errors: {len(errors)} score: {max_score}")

    def _watcher_list(self, sid):
        return ExpiringSet(make_watcher_list_name(sid), host=self.redis)

    def _cleanup_submission(self, task: SubmissionTask):
        """Clean up code that is the same for canceled and finished submissions"""
        submission = task.submission
        sid = submission.sid

        # Now that a submission is finished, we can remove it from the timeout list
        self.clear_submission_timeout(task)

        if submission.params.quota_item and submission.params.submitter:
            self.log.info(f"[{sid}] Submission no longer counts toward {submission.params.submitter.upper()} quota")
            self.quota_tracker.end(submission.params.submitter)

        if task.completed_queue:
            NamedQueue(task.completed_queue, self.redis).push(submission.as_primitives())

        # Send complete message to any watchers.
        watcher_list = self._watcher_list(sid)
        for w in watcher_list.members():
            NamedQueue(w).push(WatchQueueMessage({'status': 'STOP'}).as_primitives())

        # Clear the timeout watcher
        watcher_list.delete()
        self.active_submissions.pop(sid)
        self.submissions_assignments.pop(sid)
        with self._tasks_lock:
            self._tasks.pop(sid)

        # Count the submission as 'complete' either way
        self.counter.increment('submissions_completed')

        # Write all finished submissions to the traffic queue
        self.traffic_queue.publish(SubmissionMessage({
            'msg': from_datastore_submission(submission),
            'msg_type': 'SubmissionCompleted',
            'sender': 'dispatcher',
        }).as_primitives())

    def retry_error(self, task: SubmissionTask, sha256, service_name):
        self.log.warning(f"[{task.submission.sid}/{sha256}] "
                         f"{service_name} marking task failed: TASK PREEMPTED ")

        ttl = task.submission.params.ttl
        error = Error(dict(
            archive_ts=now_as_iso(self.config.datastore.ilm.days_until_archive * 24 * 60 * 60),
            created='NOW',
            expiry_ts=now_as_iso(ttl * 24 * 60 * 60) if ttl else None,
            response=dict(
                message='The number of retries has passed the limit.',
                service_name=service_name,
                service_version='0',
                status='FAIL_NONRECOVERABLE',
            ),
            sha256=sha256,
            type="TASK PRE-EMPTED",
        ))

        error_key = error.build_key()
        self.datastore.error.save(error_key, error.as_primitives())

        task.queue_keys.pop((sha256, service_name), None)
        task.running_services.pop((sha256, service_name), None)
        task.service_errors[(sha256, service_name)] = error_key

        export_metrics_once(service_name, ServiceMetrics, dict(fail_nonrecoverable=1),
                            counter_type='service', host='dispatcher')

        # Send the result key to any watching systems
        msg = {'status': 'FAIL', 'cache_key': error_key}
        for w in self._watcher_list(task.submission.sid).members():
            NamedQueue(w).push(msg)

    def pull_service_results(self):
        result_queue = self.result_queue
        cpu_mark = time.process_time()
        time_mark = time.time()

        while self.running:
            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

            message = result_queue.pop(timeout=1)

            cpu_mark = time.process_time()
            time_mark = time.time()

            if not message:
                continue

            sid = message['service_task']['sid']
            self.find_process_queue(sid).put(('result', message))

    def service_worker(self, index: int):
        self.log.info(f"Start service worker {index}")
        work_queue = self.process_queues[index]
        internal_queue = self.internal_process_queues[index]
        cpu_mark = time.process_time()
        time_mark = time.time()

        while self.running:
            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

            try:
                message = internal_queue.get_nowait()
            except queue.Empty:
                try:
                    message = work_queue.get(timeout=1)
                except queue.Empty:
                    cpu_mark = time.process_time()
                    time_mark = time.time()
                    continue

            cpu_mark = time.process_time()
            time_mark = time.time()

            # Unpack what kind of message to process
            kind, message = message

            if kind == 'start':
                with apm_span(self.apm_client, 'service_start_message'):
                    sid, sha256, service_name, worker_id = message
                    task = self.get_task(sid)
                    if not task:
                        self.log.warning(f'[{sid}] Service started for finished task.')
                        continue

                    with task.lock:
                        key = sha256, service_name
                        if task.queue_keys.pop(key, None) is not None:
                            # If this task is already finished (result message processed before start
                            # message) we can skip setting a timeout
                            if key in task.service_errors or key in task.service_results:
                                continue
                            self.set_timeout(task, sha256, service_name, worker_id)
            elif kind == 'result':
                with apm_span(self.apm_client, "dispatcher_results"):
                    sid = message['service_task']['sid']
                    task = self.get_task(sid)
                    if not task:
                        self.log.warning(f'[{sid}] Result returned for finished task.')
                        continue

                    with task.lock:
                        if 'result' in message:
                            self.process_service_result(task, message['result_key'],
                                                        Result(message['result']),
                                                        message['temporary_data'])
                        elif 'error' in message:
                            self.process_service_error(task, message['error_key'], Error(message['error']))
            else:
                self.log.warning(f'Invalid work order kind {kind}')

    @elasticapm.capture_span(span_type='dispatcher')
    def process_service_result(self, task: SubmissionTask, result_key, result, temporary_data):
        submission: Submission = task.submission
        sid = submission.sid
        service_name = result.response.service_name
        self.clear_timeout(task, result.sha256, service_name)

        # Don't process duplicates
        if (result.sha256, service_name) in task.service_results:
            return

        # Let the logs know we have received a result for this task
        if result.drop_file:
            self.log.debug(f"[{sid}/{result.sha256}] {service_name} succeeded. "
                           f"Result will be stored in {result_key} but processing will stop after this service.")
        else:
            self.log.debug(f"[{sid}/{result.sha256}] {service_name} succeeded. "
                           f"Result will be stored in {result_key}")

        # Check if the service is a candidate for dynamic recursion prevention
        if not submission.params.ignore_dynamic_recursion_prevention:
            service_info = self.scheduler.services.get(result.response.service_name, None)
            if service_info and service_info.category == "Dynamic Analysis":
                submission.params.services.runtime_excluded.append(result.response.service_name)

        # Save the tags
        for section in result.result.sections:
            task.file_tags[result.sha256].extend(tag_dict_to_list(section.tags.as_primitives()))

        # Update the temporary data table for this file
        for key, value in (temporary_data or {}).items():
            task.file_temporary_data[result.sha256][key] = value

        # Record the result as a summary
        task.service_results[(result.sha256, service_name)] = ResultSummary(
            key=result_key,
            drop=result.drop_file,
            score=result.result.score,
            children=[r.sha256 for r in result.response.extracted]
        )

        # Set the depth of all extracted files, even if we won't be processing them
        depth_limit = self.config.submission.max_extraction_depth
        new_depth = task.file_depth[result.sha256] + 1
        for extracted_data in result.response.extracted:
            task.file_depth.setdefault(extracted_data.sha256, new_depth)
            if extracted_data.name and extracted_data.sha256 not in task.file_names:
                task.file_names[extracted_data.sha256] = extracted_data.name

        # Send the extracted files to the dispatcher
        with elasticapm.capture_span('process_extracted_files'):
            dispatched = 0
            if new_depth < depth_limit:
                # Prepare the temporary data from the parent to build the temporary data table for
                # these newly extract files
                parent_data = task.file_temporary_data[result.sha256]

                for extracted_data in result.response.extracted:
                    if extracted_data.sha256 in task.dropped_files or extracted_data.sha256 in task.file_schedules:
                        continue

                    if len(task.file_schedules) > submission.params.max_extracted:
                        self.log.info(f'[{sid}] hit extraction limit, dropping {extracted_data.sha256}')
                        task.dropped_files.add(extracted_data.sha256)
                        self._dispatching_error(task, Error({
                            'archive_ts': result.archive_ts,
                            'expiry_ts': result.expiry_ts,
                            'response': {
                                'message': f"Too many files extracted for submission {sid} "
                                           f"{extracted_data.sha256} extracted by "
                                           f"{service_name} will be dropped",
                                'service_name': service_name,
                                'service_tool_version': result.response.service_tool_version,
                                'service_version': result.response.service_version,
                                'status': 'FAIL_NONRECOVERABLE'
                            },
                            'sha256': extracted_data.sha256,
                            'type': 'MAX FILES REACHED'
                        }))
                        continue

                    dispatched += 1
                    task.file_temporary_data[extracted_data.sha256] = dict(parent_data)
                    if self.dispatch_file(task, extracted_data.sha256):
                        return

            else:
                for extracted_data in result.response.extracted:
                    self._dispatching_error(task, Error({
                        'archive_ts': result.archive_ts,
                        'expiry_ts': result.expiry_ts,
                        'response': {
                            'message': f"{service_name} has extracted a file "
                                       f"{extracted_data.sha256} beyond the depth limits",
                            'service_name': result.response.service_name,
                            'service_tool_version': result.response.service_tool_version,
                            'service_version': result.response.service_version,
                            'status': 'FAIL_NONRECOVERABLE'
                        },
                        'sha256': extracted_data.sha256,
                        'type': 'MAX DEPTH REACHED'
                    }))

        # Check if its worth trying to run the next stage
        # Not worth running if we know we are waiting for another service
        if any(_s == result.sha256 for _s, _ in task.running_services.keys()):
            return
        # Not worth running if we know we have services in queue
        if any(_s == result.sha256 for _s, _ in task.queue_keys.keys()):
            return
        # Try to run the next stage
        self.dispatch_file(task, result.sha256)

    @elasticapm.capture_span(span_type='dispatcher')
    def _dispatching_error(self, task: SubmissionTask, error):
        error_key = error.build_key()
        task.extra_errors.append(error_key)
        self.datastore.error.save(error_key, error)
        msg = {'status': 'FAIL', 'cache_key': error_key}
        for w in self._watcher_list(task.submission.sid).members():
            NamedQueue(w).push(msg)

    @elasticapm.capture_span(span_type='dispatcher')
    def process_service_error(self, task: SubmissionTask, error_key, error):
        self.log.info(f'[{task.submission.sid}] Error from service {error.response.service_name} on {error.sha256}')
        self.clear_timeout(task, error.sha256, error.response.service_name)
        if error.response.status == "FAIL_NONRECOVERABLE":
            task.service_errors[(error.sha256, error.response.service_name)] = error_key
        self.dispatch_file(task, error.sha256)

    def pull_service_starts(self):
        start_queue = self.start_queue
        cpu_mark = time.process_time()
        time_mark = time.time()

        while self.running:
            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

            message = start_queue.pop(timeout=1)

            cpu_mark = time.process_time()
            time_mark = time.time()

            if not message:
                continue

            sid = message[0]
            self.find_process_queue(sid).put(('start', message))

    def set_submission_timeout(self, task: SubmissionTask):
        sid = task.submission.sid
        timeout_at = int(time.time() + SUBMISSION_TOTAL_TIMEOUT)
        task.timeout_at = timeout_at
        with self.timeout_list_lock:
            bisect.insort(self.timeout_list, (timeout_at, sid, '', ''))

    def clear_submission_timeout(self, task: SubmissionTask):
        with self.timeout_list_lock:
            key = (task.timeout_at, task.submission.sid, '', '')
            task.timeout_at = None
            index = bisect.bisect_left(self.timeout_list, key)
            if index < len(self.timeout_list) and self.timeout_list[index] == key:
                self.timeout_list.pop(index)

    @elasticapm.capture_span(span_type='dispatcher')
    def set_timeout(self, task, sha256, service_name, worker_id):
        sid = task.submission.sid
        service = self.scheduler.services.get(service_name)
        if not service:
            return

        timeout_at = int(time.time() + service.timeout + TIMEOUT_EXTRA_TIME)

        task.running_services[(sha256, service_name)] = timeout_at, worker_id
        with self.timeout_list_lock:
            bisect.insort(self.timeout_list, (timeout_at, sid, sha256, service_name))

    @elasticapm.capture_span(span_type='dispatcher')
    def clear_timeout(self, task, sha256, service_name):
        sid = task.submission.sid
        task.queue_keys.pop((sha256, service_name), None)
        row = task.running_services.pop((sha256, service_name), None)
        if row is None:
            return
        timeout_at, worker_id = row
        with self.timeout_list_lock:
            key = (timeout_at, sid, sha256, service_name)
            index = bisect.bisect_left(self.timeout_list, key)
            if index < len(self.timeout_list) and self.timeout_list[index] == key:
                self.timeout_list.pop(index)

    def handle_timeouts(self):
        while self.sleep(TIMEOUT_TEST_INTERVAL):
            with apm_span(self.apm_client, 'process_timeouts'):
                cpu_mark = time.process_time()
                time_mark = time.time()

                timeouts = []
                with self.timeout_list_lock:
                    while self.timeout_list and self.timeout_list[0][0] < time_mark:
                        timeouts.append(self.timeout_list.pop(0))

                service_timeouts = 0
                for _, sid, sha, service_name in timeouts:
                    task = self.get_task(sid)
                    if not task:
                        self.log.warning(f'[{sid}] timeout on finished task.')
                        continue
                    with task.lock:
                        if sha and service_name:
                            service_timeouts += 1
                            self.timeout_service(task, sha, service_name)
                        else:
                            self.log.info(f'[{sid}] submission timeout, checking dispatch status...')
                            self.check_submission(task)

                            # If we didn't finish the submission here, wait another 20 minutes
                            task = self.get_task(sid)
                            if task is not None:
                                self.set_submission_timeout(task)

                self.counter.increment('service_timeouts', service_timeouts)
                self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
                self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

    @elasticapm.capture_span(span_type='dispatcher')
    def timeout_service(self, task: SubmissionTask, sha256, service_name):
        # We believe a service task has timed out, try and read it from running tasks
        # If we can't find the task in running tasks, it finished JUST before timing out, let it go
        sid = task.submission.sid
        task.queue_keys.pop((sha256, service_name), None)
        task_key = ServiceTask.make_key(sid=sid, service_name=service_name, sha=sha256)
        service_task = self.running_tasks.pop(task_key)
        if not service_task:
            self.log.warning(f"[{sid}] Service {service_name} "
                             f"timed out on {sha256} but task isn't running.")

        # We can confirm that the task is ours now, even if the worker finished, the result will be ignored
        service_task = ServiceTask(service_task)
        self.log.info(f"[{service_task.sid}] Service {service_task.service_name} "
                      f"timed out on {service_task.fileinfo.sha256}.")

        _, worker_id = task.running_services.pop((sha256, service_name))
        self.dispatch_file(task, sha256)

        # We push the task of killing the container off on the scaler, which already has root access
        # the scaler can also double check that the service name and container id match, to be sure
        # we aren't accidentally killing the wrong container
        if worker_id is not None:
            self.scaler_timeout_queue.push({
                'service': service_name,
                'container': worker_id
            })

            # Report to the metrics system that a recoverable error has occurred for that service
            export_metrics_once(service_name, ServiceMetrics, dict(fail_recoverable=1),
                                host=worker_id, counter_type='service')

    def work_guard(self):
        check_interval = GUARD_TIMEOUT/8
        old_value = int(time.time())
        self.dispatchers_directory.set(self.instance_id, old_value)

        try:
            while self.sleep(check_interval):
                cpu_mark = time.process_time()
                time_mark = time.time()

                # Increase the guard number
                gap = int(time.time() - old_value)
                updated_value = self.dispatchers_directory.increment(self.instance_id, gap)

                # If for whatever reason, there was a moment between the previous increment
                # and the one before that, that the gap reached the timeout, someone may have
                # started stealing our work. We should just exit.
                if time.time() - old_value > GUARD_TIMEOUT:
                    self.log.warning(f'Dispatcher closing due to guard interval failure: '
                                     f'{time.time() - old_value} > {GUARD_TIMEOUT}')
                    self.stop()
                    return

                # Everything is fine, prepare for next round
                old_value = updated_value

                self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
                self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)
        finally:
            if not self.running:
                self.dispatchers_directory.pop(self.instance_id)

    def work_thief(self):

        assignment_pattern = DISPATCH_TASK_ASSIGNMENT + '*'
        last_seen = {}

        try:
            while self.running:
                self.sleep(GUARD_TIMEOUT / 4)
                cpu_mark = time.process_time()
                time_mark = time.time()

                # Load guards
                last_seen.update(self.dispatchers_directory.items())

                # List all dispatchers with jobs assigned
                for raw_key in self.redis_persist.keys(assignment_pattern):
                    key: str = raw_key.decode()
                    key = key[len(DISPATCH_TASK_ASSIGNMENT):]
                    if key not in last_seen:
                        last_seen[key] = time.time()
                self.running_dispatchers_estimate = len(last_seen)

                self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
                self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

                # Check if any of the dispatchers
                if last_seen:
                    oldest = min(last_seen.keys(), key=lambda _x: last_seen[_x])
                    if time.time() - last_seen[oldest] > GUARD_TIMEOUT:
                        self.steal_work(oldest)
                        last_seen.pop(oldest)
        finally:
            if not self.running:
                self.dispatchers_directory.pop(self.instance_id)

    def steal_work(self, target):
        target_jobs = ExpiringHash(DISPATCH_TASK_ASSIGNMENT+target, host=self.redis_persist)
        self.log.info(f'Starting to steal work from {target}')

        keys = target_jobs.keys()
        while self.running and keys:
            # Don't load more than the proper portion of work. Let the thief
            # go a fixed margin over the limit, so that recovering past work
            # will continue even when max submissions are in progress.
            # Consider one less running dispatcher, because we're in the middle of processing a dead one
            max_tasks = self.config.core.dispatcher.max_inflight / max(self.running_dispatchers_estimate - 1, 1)
            if self.active_submissions.length() >= max_tasks + 500:
                self.sleep(1)
                continue

            message = target_jobs.pop(keys.pop())
            if not keys:
                keys = target_jobs.keys()

            if not message:
                continue

            cpu_mark = time.process_time()
            time_mark = time.time()

            # Start of process dispatcher transaction
            if self.apm_client:
                self.apm_client.begin_transaction(APM_SPAN_TYPE)

            try:
                # This is probably a complete task
                task = SubmissionTask(**message)
                if self.apm_client:
                    elasticapm.label(sid=task.submission.sid)

                with task.lock:
                    self.dispatch_submission(task)

                # End of process dispatcher transaction (success)
                if self.apm_client:
                    self.apm_client.end_transaction('submission_message', 'success')
            except Exception:
                if self.apm_client:
                    self.apm_client.end_transaction('submission_message', 'exception')
                raise

            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

        self.log.info(f'Finished stealing work from {target}')
        self.dispatchers_directory.pop(target)

    def handle_commands(self):
        while self.running:

            message = self.command_queue.pop(timeout=3)
            if not message:
                continue

            cpu_mark = time.process_time()
            time_mark = time.time()

            # Start of process dispatcher transaction
            with apm_span(self.apm_client, 'command_message'):

                command = DispatcherCommandMessage(message)
                if command.kind == CREATE_WATCH:
                    payload: CreateWatch = command.payload()
                    self.setup_watch_queue(payload.submission, payload.queue_name)
                elif command.kind == LIST_OUTSTANDING:
                    payload: ListOutstanding = command.payload()
                    self.list_outstanding(payload.submission, payload.response_queue)
                else:
                    self.log.warning(f"Unknown command code: {command.kind}")

                self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
                self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

    @elasticapm.capture_span(span_type='dispatcher')
    def setup_watch_queue(self, sid, queue_name):
        # Create a unique queue
        watch_queue = NamedQueue(queue_name, ttl=30)
        watch_queue.push(WatchQueueMessage({'status': 'START'}).as_primitives())

        #
        task = self.get_task(sid)
        if not task:
            watch_queue.push(WatchQueueMessage({"status": "STOP"}).as_primitives())
            return

        with task.lock:
            # Add the newly created queue to the list of queues for the given submission
            self._watcher_list(sid).add(queue_name)

            # Push all current keys to the newly created queue (Queue should have a TTL of about 30 sec to 1 minute)
            for result_data in task.service_results.values():
                watch_queue.push(WatchQueueMessage({"status": "OK", "cache_key": result_data.key}).as_primitives())

            for error_key in task.service_errors.values():
                watch_queue.push(WatchQueueMessage({"status": "FAIL", "cache_key": error_key}).as_primitives())

    @elasticapm.capture_span(span_type='dispatcher')
    def list_outstanding(self, sid: str, queue_name: str):
        response_queue = NamedQueue(queue_name, host=self.redis)
        outstanding = defaultdict(int)
        task = self.get_task(sid)
        if task:
            with task.lock:
                for sha, service_name in task.queue_keys.keys():
                    outstanding[service_name] += 1
                for sha, service_name in task.running_services.keys():
                    outstanding[service_name] += 1
        response_queue.push(outstanding)

    def timeout_backstop(self):
        while self.sleep(GLOBAL_TASK_CHECK_INTERVAL):
            cpu_mark = time.process_time()
            time_mark = time.time()

            # Start of process dispatcher transaction
            with apm_span(self.apm_client, 'timeout_backstop'):
                # Download all running tasks
                all_tasks = self.running_tasks.items()

                # Filter out all that belong to a running dispatcher
                all_tasks = [ServiceTask(_s) for _s in all_tasks.values()]
                dispatcher_instances = set(Dispatcher.all_instances(persistent_redis=self.redis_persist))
                all_tasks = [_s for _s in all_tasks if _s.metadata['dispatcher__'] not in dispatcher_instances]

                # The remaining running tasks belong to dead dispatchers and can be killed
                for task in all_tasks:
                    if not self.running_tasks.pop(task.key()):
                        continue
                    self.log.warning(f"[{task.sid}]Task killed by backstop {task.service_name} {task.fileinfo.sha256}")

                    self.scaler_timeout_queue.push({
                        'service': task.service_name,
                        'container': task.metadata['worker__']
                    })

                    # Report to the metrics system that a recoverable error has occurred for that service
                    export_metrics_once(task.service_name, ServiceMetrics, dict(fail_recoverable=1),
                                        host=task.metadata['worker__'], counter_type='service')

            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)
