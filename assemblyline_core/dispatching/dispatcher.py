from __future__ import annotations
import uuid
import os
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from typing import Optional, Any, TYPE_CHECKING, Iterable
import json
import enum
from queue import PriorityQueue, Empty, Queue
import dataclasses

import elasticapm

from assemblyline.common import isotime
from assemblyline.common.constants import make_watcher_list_name, SUBMISSION_QUEUE, \
    DISPATCH_RUNNING_TASK_HASH, SCALER_TIMEOUT_QUEUE, DISPATCH_TASK_HASH
from assemblyline.common.forge import get_service_queue
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.metrics import MetricsFactory
from assemblyline.common.postprocess import ActionWorker
from assemblyline.odm.messages.dispatcher_heartbeat import Metrics
from assemblyline.odm.messages.service_heartbeat import Metrics as ServiceMetrics
from assemblyline.odm.messages.dispatching import WatchQueueMessage, CreateWatch, DispatcherCommandMessage, \
    CREATE_WATCH, LIST_OUTSTANDING, ListOutstanding
from assemblyline.odm.messages.submission import SubmissionMessage, from_datastore_submission
from assemblyline.odm.messages.task import FileInfo, Task as ServiceTask
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.service import Service
from assemblyline.odm.models.submission import Submission
from assemblyline.odm.models.result import Result
from assemblyline.remote.datatypes.exporting_counter import export_metrics_once
from assemblyline.remote.datatypes.hash import Hash
from assemblyline.remote.datatypes.queues.comms import CommsQueue
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.set import ExpiringSet
from assemblyline.remote.datatypes.user_quota_tracker import UserQuotaTracker
from assemblyline_core.server_base import ThreadedCoreBase
from assemblyline_core.alerter.run_alerter import ALERT_QUEUE_NAME


if TYPE_CHECKING:
    from assemblyline.odm.models.file import File


from .schedules import Scheduler
from .timeout import TimeoutTable
from ..ingester.constants import COMPLETE_QUEUE_NAME

APM_SPAN_TYPE = 'handle_message'

AL_SHUTDOWN_GRACE = int(os.environ.get('AL_SHUTDOWN_GRACE', '60'))
AL_SHUTDOWN_QUIT = 60
FINALIZING_WINDOW = max(AL_SHUTDOWN_GRACE - AL_SHUTDOWN_QUIT, 0)
RESULT_BATCH_SIZE = int(os.environ.get('DISPATCHER_RESULT_BATCH_SIZE', '50'))
ERROR_BATCH_SIZE = int(os.environ.get('DISPATCHER_ERROR_BATCH_SIZE', '50'))
DYNAMIC_ANALYSIS_CATEGORY = 'Dynamic Analysis'


class Action(enum.IntEnum):
    start = 0
    result = 1
    dispatch_file = 2
    service_timeout = 3
    check_submission = 4


@dataclasses.dataclass(order=True)
class DispatchAction:
    kind: Action
    sid: str = dataclasses.field(compare=False)
    sha: Optional[str] = dataclasses.field(compare=False, default=None)
    service_name: Optional[str] = dataclasses.field(compare=False, default=None)
    worker_id: Optional[str] = dataclasses.field(compare=False, default=None)
    data: Any = dataclasses.field(compare=False, default=None)


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
        self.children: list[str] = children


class SubmissionTask:
    """Dispatcher internal model for submissions"""

    def __init__(self, submission, completed_queue, scheduler, results=None,
                 file_infos=None, file_tree=None, errors: Optional[Iterable[str]] = None):
        self.submission: Submission = Submission(submission)

        self.completed_queue = None
        if completed_queue:
            self.completed_queue = str(completed_queue)

        self.file_info: dict[str, Optional[FileInfo]] = {}
        self.file_names: dict[str, str] = {}
        self.file_schedules: dict[str, list[dict[str, Service]]] = {}
        self.file_tags: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        self.file_depth: dict[str, int] = {}
        self.file_temporary_data: dict[str, dict] = defaultdict(dict)
        self.extra_errors: list[str] = []
        self.active_files: set[str] = set()
        self.dropped_files: set[str] = set()
        self.dynamic_recursion_bypass: set[str] = set()

        # mapping from file hash to a set of services that shouldn't be run on
        # any children (recursively) of that file
        self._forbidden_services: dict[str, set[str]] = {}
        self._parent_map: dict[str, set[str]] = {}

        self.service_results: dict[tuple[str, str], ResultSummary] = {}
        self.service_errors: dict[tuple[str, str], str] = {}
        self.service_attempts: dict[tuple[str, str], int] = defaultdict(int)
        self.queue_keys: dict[tuple[str, str], str] = {}
        self.running_services: set[tuple[str, str]] = set()

        if file_infos is not None:
            self.file_info.update({k: FileInfo(v) for k, v in file_infos.items()})

        if file_tree is not None:
            def recurse_tree(tree, depth):
                for sha256, file_data in tree.items():
                    self.file_depth[sha256] = depth
                    self.file_names[sha256] = file_data['name'][0]
                    recurse_tree(file_data['children'], depth + 1)

            recurse_tree(file_tree, 0)

        if results is not None:
            rescan = scheduler.expand_categories(self.submission.params.services.rescan)

            # Replay the process of routing files for dispatcher internal state.
            for k, result in results.values():
                sha256, service, _ = k.split('.', 2)
                service = scheduler.services.get(service)
                if not service:
                    continue
                if service.category == DYNAMIC_ANALYSIS_CATEGORY:
                    self.forbid_for_children(sha256, service.name)

            # Replay the process of receiving results for dispatcher internal state
            for k, result in results.items():
                sha256, service, _ = k.split('.', 2)
                if service not in rescan:
                    children = [r['sha256'] for r in result['response']['extracted']]
                    self.register_children(sha256, children)
                    self.service_results[(sha256, service)] = ResultSummary(
                        key=k, drop=result['drop_file'], score=result['result']['score'],
                        children=children)

                tags = Result(result).scored_tag_dict()
                for key in tags.keys():
                    if key in self.file_tags[sha256].keys():
                        # Sum score of already known tags
                        self.file_tags[sha256][key]['score'] += tags[key]['score']
                    else:
                        self.file_tags[sha256][key] = tags[key]

        if errors is not None:
            for e in errors:
                sha256, service, _ = e.split('.', 2)
                self.service_errors[(sha256, service)] = e

    @property
    def sid(self):
        return self.submission.sid

    def forbid_for_children(self, sha256: str, service_name: str):
        """Mark that children of a given file should not be routed to a service."""
        try:
            self._forbidden_services[sha256].add(service_name)
        except KeyError:
            self._forbidden_services[sha256] = {service_name}

    def register_children(self, parent: str, children: list[str]):
        """
        Note for the purposes of dynamic recursion prevention which
        files extracted other files.
        """
        for child in children:
            try:
                self._parent_map[child].add(parent)
            except KeyError:
                self._parent_map[child] = {parent}

    def all_ancestors(self, sha256: str) -> list[str]:
        visited = set()
        to_visit = [sha256]
        while len(to_visit) > 0:
            current = to_visit.pop()
            for parent in self._parent_map.get(current, []):
                if parent not in visited:
                    visited.add(parent)
                    to_visit.append(parent)
        return list(visited)

    def find_recursion_excluded_services(self, sha256: str) -> list[str]:
        """
        Return a list of services that should be excluded for the given file.

        Note that this is computed dynamically from the parent map every time it is
        called. This is to account for out of order result collection in unusual
        circumstances like replay.
        """
        return list(set().union(*[
            self._forbidden_services.get(parent, set())
            for parent in self.all_ancestors(sha256)
        ]))


DISPATCH_TASK_ASSIGNMENT = 'dispatcher-tasks-assigned-to-'
TASK_ASSIGNMENT_PATTERN = DISPATCH_TASK_ASSIGNMENT + '*'
DISPATCH_START_EVENTS = 'dispatcher-start-events-'
DISPATCH_RESULT_QUEUE = 'dispatcher-results-'
DISPATCH_COMMAND_QUEUE = 'dispatcher-commands-'
DISPATCH_DIRECTORY = 'dispatchers-directory'
QUEUE_EXPIRY = 60*60
SERVICE_VERSION_EXPIRY_TIME = 30 * 60  # How old service version info can be before we ignore it
GUARD_TIMEOUT = 60*2
GLOBAL_TASK_CHECK_INTERVAL = 60*10
TIMEOUT_EXTRA_TIME = 5
TIMEOUT_TEST_INTERVAL = 5
MAX_RESULT_BUFFER = 64
RESULT_THREADS = max(1, int(os.getenv('DISPATCHER_RESULT_THREADS', '2')))
FINALIZE_THREADS = max(1, int(os.getenv('DISPATCHER_FINALIZE_THREADS', '2')))

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
        self.tasks: dict[str, SubmissionTask] = {}
        self.finalizing = threading.Event()
        self.finalizing_start = 0.0

        #
        # # Build some utility classes
        self.scheduler = Scheduler(self.datastore, self.config, self.redis)
        self.running_tasks = Hash(DISPATCH_RUNNING_TASK_HASH, host=self.redis)
        self.scaler_timeout_queue = NamedQueue(SCALER_TIMEOUT_QUEUE, host=self.redis_persist)

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
        self.active_submissions = Hash(DISPATCH_TASK_ASSIGNMENT+self.instance_id, host=self.redis_persist)
        self.submissions_assignments = Hash(DISPATCH_TASK_HASH, host=self.redis_persist)
        self.ingester_scanning = Hash('m-scanning-table', self.redis_persist)

        # Communications queues
        self.start_queue = NamedQueue(DISPATCH_START_EVENTS+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)
        self.result_queue = NamedQueue(DISPATCH_RESULT_QUEUE+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)
        self.command_queue = NamedQueue(DISPATCH_COMMAND_QUEUE+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)

        # Submissions that should have alerts generated
        self.alert_queue = NamedQueue(ALERT_QUEUE_NAME, self.redis_persist)

        # Publish counters to the metrics sink.
        self.counter = MetricsFactory(metrics_type='dispatcher', schema=Metrics, name=counter_name,
                                      redis=self.redis, config=self.config)

        self.apm_client = None
        if self.config.core.metrics.apm_server.server_url:
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="dispatcher")

        self._service_timeouts: TimeoutTable[tuple[str, str, str], str] = TimeoutTable()
        self._submission_timeouts: TimeoutTable[str, None] = TimeoutTable()

        # Setup queues for work to be divided into
        self.process_queues: list[PriorityQueue[DispatchAction]] = [PriorityQueue() for _ in range(RESULT_THREADS)]
        self.queue_ready_signals: list[threading.Semaphore] = [threading.Semaphore(MAX_RESULT_BUFFER)
                                                               for _ in range(RESULT_THREADS)]

        # Queue of finished submissions/errors waiting to be saved into elastic
        self.finalize_queue = Queue()
        self.error_queue: Queue[tuple[str, Error]] = Queue()

        # Queue to hold of service timeouts that need to be processed
        # They will be held in this queue until results in redis are
        # already processed
        self.timeout_queue: Queue[DispatchAction] = Queue()

        # Utility object to handle post-processing actions
        self.postprocess_worker = ActionWorker(cache=False, config=self.config, datastore=self.datastore,
                                               redis_persist=self.redis_persist)

    def stop(self):
        super().stop()
        self.postprocess_worker.stop()

    def interrupt_handler(self, signum, stack_frame):
        self.log.info("Instance caught signal. Beginning to drain work.")
        self.finalizing_start = time.time()
        self._shutdown_timeout = AL_SHUTDOWN_QUIT
        self.finalizing.set()

    def process_queue_index(self, key: str) -> int:
        return sum(ord(_x) for _x in key) % RESULT_THREADS

    def find_process_queue(self, key: str):
        return self.process_queues[self.process_queue_index(key)]

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
            # Save errors to DB
            'Save Errors': self.save_errors,
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

        for ii in range(FINALIZE_THREADS):
            # Finilize submissions that are done
            threads[f'Save Submissions #{ii}'] = self.save_submission

        for ii in range(RESULT_THREADS):
            # Process results
            threads[f'Service Update Worker #{ii}'] = self.service_worker_factory(ii)

        self.maintain_threads(threads)

        # If the dispatcher is exiting cleanly remove as many tasks from the service queues as we can
        service_queues = {}
        for task in self.tasks.values():
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
            while not self.active:
                # Dispatcher is disabled... waiting for it to be reactivated
                self.sleep(0.1)

            if self.finalizing.is_set():
                finalizing_time = time.time() - self.finalizing_start
                if self.active_submissions.length() > 0 and finalizing_time < FINALIZING_WINDOW:
                    self.sleep(1)
                else:
                    self.stop()
            else:
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
                    task = SubmissionTask(scheduler=self.scheduler, **message)
                    if self.apm_client:
                        elasticapm.label(sid=task.submission.sid)
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

        if not self.submissions_assignments.add(sid, self.instance_id):
            self.log.warning(f"[{sid}] Received an assigned submission dropping")
            return

        if not self.active_submissions.exists(sid):
            self.log.info(f"[{sid}] New submission received")
            self.active_submissions.add(sid, {
                'completed_queue': task.completed_queue,
                'submission': submission.as_primitives()
            })

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
                task.file_temporary_data[submission.files[0].sha256] = {
                    key: value
                    for key, value in dict(json.loads(submission.params.initial_data)).items()
                    if len(str(value)) <= self.config.submission.max_temp_data_length
                }

            except (ValueError, TypeError) as err:
                self.log.warning(f"[{sid}] could not process initialization data: {err}")

        self.tasks[sid] = task
        self._submission_timeouts.set(task.sid, SUBMISSION_TOTAL_TIMEOUT, None)

        task.file_depth[submission.files[0].sha256] = 0
        task.file_names[submission.files[0].sha256] = submission.files[0].name or submission.files[0].sha256
        task.active_files.add(submission.files[0].sha256)
        action = DispatchAction(kind=Action.dispatch_file, sid=sid, sha=submission.files[0].sha256)
        self.find_process_queue(sid).put(action)

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
        if self.apm_client:
            elasticapm.label(sid=sid, sha256=sha256)

        file_depth: int = task.file_depth[sha256]
        # If its the first time we've seen this file, we won't have a schedule for it
        if sha256 not in task.file_schedules:
            with elasticapm.capture_span('build_schedule'):
                # We are processing this file, load the file info, and build the schedule
                filestore_info: Optional[File] = self.datastore.file.get(sha256)

                if filestore_info is None:
                    task.dropped_files.add(sha256)
                    self._dispatching_error(task, Error({
                        'archive_ts': None,
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
                    return False
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

                    forbidden_services = None

                    # If Dynamic Recursion Prevention is in effect and the file is not part of the bypass list,
                    # Find the list of services this file is forbidden from being sent to.
                    if not submission.params.ignore_dynamic_recursion_prevention and sha256 not in task.dynamic_recursion_bypass:
                        forbidden_services = task.find_recursion_excluded_services(sha256)

                    task.file_schedules[sha256] = self.scheduler.build_schedule(submission, file_info.type,
                                                                                file_depth, forbidden_services)

        file_info = task.file_info[sha256]
        schedule: list = list(task.file_schedules[sha256])
        deep_scan, ignore_filtering = submission.params.deep_scan, submission.params.ignore_filtering

        # Go through each round of the schedule removing complete/failed services
        # Break when we find a stage that still needs processing
        outstanding: dict[str, Service] = {}
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
                    if not ignore_filtering and result.drop:
                        # Clear out anything in the schedule after this stage
                        task.file_schedules[sha256] = started_stages
                        schedule.clear()

        # Try to retry/dispatch any outstanding services
        if outstanding:
            sent, enqueued, running = [], [], []

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

                    # Check if we have attempted this too many times already.
                    task.service_attempts[key] += 1
                    if task.service_attempts[key] > 3:
                        self.retry_error(task, sha256, service_name)
                        continue

                    # Load the list of tags we will pass
                    tags = []
                    if service.uses_tags or service.uses_tag_scores:
                        tags = list(task.file_tags.get(sha256, {}).values())

                    # Load the temp submission data we will pass
                    temp_data = {}
                    if service.uses_temp_submission_data:
                        temp_data = task.file_temporary_data[sha256]

                    # Load the metadata we will pass
                    metadata = {}
                    if service.uses_metadata:
                        metadata = submission.metadata

                    tag_fields = ['type', 'value', 'short_type']
                    if service.uses_tag_scores:
                        tag_fields.append('score')

                    # Mark this routing for the purposes of dynamic recursion prevention
                    if service.category == DYNAMIC_ANALYSIS_CATEGORY:
                        task.forbid_for_children(sha256, service_name)

                    # Build the actual service dispatch message
                    config = self.build_service_config(service, submission)
                    service_task = ServiceTask(dict(
                        sid=sid,
                        metadata=metadata,
                        min_classification=task.submission.classification,
                        service_name=service_name,
                        service_config=config,
                        fileinfo=file_info,
                        filename=task.file_names.get(sha256, sha256),
                        depth=file_depth,
                        max_files=task.submission.params.max_extracted,
                        ttl=submission.params.ttl,
                        ignore_cache=submission.params.ignore_cache,
                        ignore_dynamic_recursion_prevention=submission.params.ignore_dynamic_recursion_prevention,
                        ignore_filtering=ignore_filtering,
                        tags=[{field: x[field] for field in tag_fields} for x in tags],
                        temporary_submission_data=[
                            {'name': name, 'value': value} for name, value in temp_data.items()
                        ],
                        deep_scan=deep_scan,
                        priority=submission.params.priority,
                        safelist_config=self.config.services.safelist
                    ))
                    service_task.metadata['dispatcher__'] = self.instance_id

                    # Its a new task, send it to the service
                    queue_key = service_queue.push(service_task.priority, service_task.as_primitives())
                    task.queue_keys[(sha256, service_name)] = queue_key
                    sent.append(service_name)

            if sent or enqueued or running:
                # If we have confirmed that we are waiting, or have taken an action, log that.
                self.log.info(f"[{sid}] File {sha256} sent to: {sent} "
                              f"already in queue for: {enqueued} "
                              f"running on: {running}")
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
    def check_submission(self, task: SubmissionTask) -> bool:
        """
        Check if a submission is finished.

        :param task: Task object for the submission in question.
        :return: true if submission has been finished.
        """
        # Track which files we have looked at already
        checked: set[str] = set()
        unchecked: set[str] = set(list(task.file_depth.keys()))

        # Categorize files as pending/processing (can be both) all others are finished
        pending_files = []  # Files where we are missing a service and it is not being processed
        processing_files = []  # Files where at least one service is in progress/queued

        # Track information about the results as we hit them
        file_scores: dict[str, int] = {}

        # Make sure we have either a result or
        while unchecked:
            sha256 = next(iter(unchecked))
            unchecked.remove(sha256)
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
                        unchecked.update(set(result.children) - checked)
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
            if self.tasks.pop(task.sid, None):
                self.finalize_queue.put((task, max_score, checked))
            return True
        return False

    @classmethod
    def build_service_config(cls, service: Service, submission: Submission) -> dict[str, str]:
        """Prepare the service config that will be used downstream.

        v3 names: get_service_params get_config_data
        """
        # Load the default service config
        params = {x.name: x.default for x in service.submission_params}

        # Over write it with values from the submission
        if service.name in submission.params.service_spec:
            params.update(submission.params.service_spec[service.name])
        return params

    def save_submission(self):
        while self.running:
            self.counter.set('save_queue', self.finalize_queue.qsize())
            try:
                task, max_score, checked = self.finalize_queue.get(block=True, timeout=3)
                self.finalize_submission(task, max_score, checked)
            except Empty:
                pass

    def save_errors(self):
        while self.running:
            self.counter.set('error_queue', self.error_queue.qsize())

            try:
                errors = [self.error_queue.get(block=True, timeout=3)]
            except Empty:
                continue

            with apm_span(self.apm_client, 'save_error'):
                try:
                    while len(errors) < ERROR_BATCH_SIZE:
                        errors.append(self.error_queue.get_nowait())
                except Empty:
                    pass

                plan = self.datastore.error.get_bulk_plan()
                for error_key, error in errors:
                    plan.add_upsert_operation(error_key, error)
                self.datastore.error.bulk(plan)

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
        self._submission_timeouts.clear(task.sid)

        if submission.params.quota_item and submission.params.submitter:
            self.log.info(f"[{sid}] Submission no longer counts toward {submission.params.submitter.upper()} quota")
            self.quota_tracker.end(submission.params.submitter)

        if task.completed_queue:
            NamedQueue(task.completed_queue, self.redis).push(submission.as_primitives())

        # Send complete message to any watchers.
        watcher_list = self._watcher_list(sid)
        for w in watcher_list.members():
            NamedQueue(w).push(WatchQueueMessage({'status': 'STOP'}).as_primitives())

        # Pull the tags keys and values into a searchable form
        tags = [
            {'value': _t['value'], 'type': _t['type']}
            for file_tags in task.file_tags.values()
            for _t in file_tags.values()
        ]

        # Send the submission for alerting or resubmission
        self.postprocess_worker.process_submission(submission, tags)

        # Clear the timeout watcher
        watcher_list.delete()
        self.active_submissions.pop(sid)
        self.submissions_assignments.pop(sid)
        self.tasks.pop(sid, None)

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
            archive_ts=None,
            created='NOW',
            expiry_ts=now_as_iso(ttl * 24 * 60 * 60) if ttl else None,
            response=dict(
                message='The number of retries has passed the limit.',
                service_name=service_name,
                service_version='0',
                status='FAIL_NONRECOVERABLE',
            ),
            sha256=sha256, type="TASK PRE-EMPTED",
        ))

        error_key = error.build_key()
        self.error_queue.put((error_key, error))

        task.queue_keys.pop((sha256, service_name), None)
        task.running_services.discard((sha256, service_name))
        task.service_errors[(sha256, service_name)] = error_key

        export_metrics_once(service_name, ServiceMetrics, dict(fail_nonrecoverable=1),
                            counter_type='service', host='dispatcher', redis=self.redis)

        # Send the result key to any watching systems
        msg = {'status': 'FAIL', 'cache_key': error_key}
        for w in self._watcher_list(task.submission.sid).members():
            NamedQueue(w).push(msg)

    def pull_service_results(self):
        result_queue = self.result_queue

        while self.running:
            # Try to get a batch of results to process
            messages = result_queue.pop_batch(RESULT_BATCH_SIZE)

            # If there are no messages and no timeouts to process block for a second
            if not messages and self.timeout_queue.empty():
                message = result_queue.pop(timeout=1)
                if message:
                    messages = [message]

            # If we have any messages, schedule them to be processed by the right worker thread
            for message in messages:
                sid = message['sid']
                self.queue_ready_signals[self.process_queue_index(sid)].acquire()
                self.find_process_queue(sid).put(DispatchAction(kind=Action.result, sid=sid, data=message))

            # If we got an incomplete batch, we have taken everything in redis
            # and its safe to process timeouts, put some into the processing queues
            if len(messages) < RESULT_BATCH_SIZE:
                for _ in range(RESULT_BATCH_SIZE):
                    try:
                        message = self.timeout_queue.get_nowait()
                        self.find_process_queue(message.sid).put(message)
                    except Empty:
                        break

    def service_worker(self, index: int):
        self.log.info(f"Start service worker {index}")
        work_queue = self.process_queues[index]
        cpu_mark = time.process_time()
        time_mark = time.time()

        while self.running:
            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

            try:
                message = work_queue.get(timeout=1)
            except Empty:
                cpu_mark = time.process_time()
                time_mark = time.time()
                continue

            cpu_mark = time.process_time()
            time_mark = time.time()

            kind = message.kind

            if kind == Action.start:
                with apm_span(self.apm_client, 'service_start_message'):
                    task = self.tasks.get(message.sid)
                    if not task:
                        self.log.warning(f'[{message.sid}] Service started for finished task.')
                        continue

                    if not message.sha or not message.service_name:
                        self.log.warning(f'[{message.sid}] Service started missing data.')
                        continue

                    key = (message.sha, message.service_name)
                    if task.queue_keys.pop(key, None) is not None:
                        # If this task is already finished (result message processed before start
                        # message) we can skip setting a timeout
                        if key in task.service_errors or key in task.service_results:
                            continue
                        self.set_timeout(task, message.sha, message.service_name, message.worker_id)

            elif kind == Action.result:
                self.queue_ready_signals[self.process_queue_index(message.sid)].release()
                with apm_span(self.apm_client, "dispatcher_results"):
                    task = self.tasks.get(message.sid)
                    if not task:
                        self.log.warning(f'[{message.sid}] Result returned for finished task.')
                        continue
                    self._submission_timeouts.set(message.sid, SUBMISSION_TOTAL_TIMEOUT, None)

                    if 'result_summary' in message.data:
                        self.process_service_result(task, message.data)
                    elif 'error' in message.data:
                        self.process_service_error(task, message.data['error_key'], Error(message.data['error']))

            elif kind == Action.check_submission:
                with apm_span(self.apm_client, "check_submission_message"):
                    task = self.tasks.get(message.sid)
                    if task:
                        self.log.info(f'[{message.sid}] submission timeout, checking dispatch status...')
                        self.check_submission(task)

                        # If we didn't finish the submission here, wait another 20 minutes
                        if message.sid in self.tasks:
                            self._submission_timeouts.set(message.sid, SUBMISSION_TOTAL_TIMEOUT, None)

            elif kind == Action.service_timeout:
                task = self.tasks.get(message.sid)
                if task:
                    self.timeout_service(task, message.sha, message.service_name, message.worker_id)

            elif kind == Action.dispatch_file:
                task = self.tasks.get(message.sid)
                if task:
                    self.dispatch_file(task, message.sha)

            else:
                self.log.warning(f'Invalid work order kind {kind}')

    @elasticapm.capture_span(span_type='dispatcher')
    def process_service_result(self, task: SubmissionTask, data: dict):
        try:
            submission: Submission = task.submission
            sid = submission.sid
            service_name = data['service_name']
            service_version = data['service_version']
            service_tool_version = data['service_tool_version']
            expiry_ts = data['expiry_ts']

            sha256 = data['sha256']
            summary = ResultSummary(**data['result_summary'])
            tags = data['tags']
            temporary_data = data['temporary_data'] or {}
            extracted_names = data['extracted_names']
            dynamic_recursion_bypass = data.get('dynamic_recursion_bypass', [])

        except KeyError as missing:
            self.log.exception(f"Malformed result message, missing key: {missing}")
            return

        # Add SHA256s of files that allowed to run regardless of Dynamic Recursion Prevention
        task.dynamic_recursion_bypass = task.dynamic_recursion_bypass.union(set(dynamic_recursion_bypass))

        # Immediately remove timeout so we don't cancel now
        self.clear_timeout(task, sha256, service_name)

        # Don't process duplicates
        if (sha256, service_name) in task.service_results:
            return

        # Let the logs know we have received a result for this task
        if summary.drop:
            self.log.debug(f"[{sid}/{sha256}] {service_name} succeeded. "
                           f"Result will be stored in {summary.key} but processing will stop after this service.")
        else:
            self.log.debug(f"[{sid}/{sha256}] {service_name} succeeded. "
                           f"Result will be stored in {summary.key}")

        # The depth is set for the root file, and for all extracted files whether we process them or not
        if sha256 not in task.file_depth:
            self.log.warning(f"[{sid}/{sha256}] {service_name} returned result for file that wasn't requested.")
            return

        # Account for the possibility of cache hits or services that aren't updated (tagged as compatible but not)
        if isinstance(tags, list):
            self.log.warning("Deprecation: Old format of tags found. "
                             "This format changed with the release of 4.3 on 09-2022. "
                             f"Rebuilding {service_name} may be required or the result of a cache hit. "
                             "Proceeding with conversion to compatible format..")
            alt_tags = {}
            for t in tags:
                key = f"{t['type']}:{t['value']}"
                t.update({'score': 0})
                alt_tags[key] = t
            tags = alt_tags

        # Update score of tag as it moves through different services
        for key, value in tags.items():
            if key in task.file_tags[sha256].keys():
                task.file_tags[sha256][key]['score'] += value['score']
            else:
                task.file_tags[sha256][key] = value

        # Update the temporary data table for this file
        for key, value in (temporary_data or {}).items():
            if len(str(value)) <= self.config.submission.max_temp_data_length:
                task.file_temporary_data[sha256][key] = value

        # Record the result as a summary
        task.service_results[(sha256, service_name)] = summary
        task.register_children(sha256, summary.children)

        # Set the depth of all extracted files, even if we won't be processing them
        depth_limit = self.config.submission.max_extraction_depth
        new_depth = task.file_depth[sha256] + 1
        for extracted_sha256 in summary.children:
            task.file_depth.setdefault(extracted_sha256, new_depth)
            extracted_name = extracted_names.get(extracted_sha256)
            if extracted_name and extracted_sha256 not in task.file_names:
                task.file_names[extracted_sha256] = extracted_name

        # Send the extracted files to the dispatcher
        with elasticapm.capture_span('process_extracted_files'):
            dispatched = 0
            if new_depth < depth_limit:
                # Prepare the temporary data from the parent to build the temporary data table for
                # these newly extract files
                parent_data = task.file_temporary_data[sha256]

                for extracted_sha256 in summary.children:
                    if extracted_sha256 in task.dropped_files or extracted_sha256 in task.active_files:
                        continue

                    if len(task.active_files) > submission.params.max_extracted:
                        self.log.info(f'[{sid}] hit extraction limit, dropping {extracted_sha256}')
                        task.dropped_files.add(extracted_sha256)
                        self._dispatching_error(task, Error({
                            'archive_ts': None,
                            'expiry_ts': expiry_ts,
                            'response': {
                                'message': f"Too many files extracted for submission {sid} "
                                           f"{extracted_sha256} extracted by "
                                           f"{service_name} will be dropped",
                                'service_name': service_name,
                                'service_tool_version': service_tool_version,
                                'service_version': service_version,
                                'status': 'FAIL_NONRECOVERABLE'
                            },
                            'sha256': extracted_sha256,
                            'type': 'MAX FILES REACHED'
                        }))
                        continue

                    dispatched += 1
                    task.active_files.add(extracted_sha256)
                    task.file_temporary_data[extracted_sha256] = dict(parent_data)
                    self.find_process_queue(sid).put(DispatchAction(kind=Action.dispatch_file, sid=sid,
                                                                    sha=extracted_sha256))
            else:
                for extracted_sha256 in summary.children:
                    task.dropped_files.add(sha256)
                    self._dispatching_error(task, Error({
                        'archive_ts': None,
                        'expiry_ts': expiry_ts,
                        'response': {
                            'message': f"{service_name} has extracted a file "
                                       f"{extracted_sha256} beyond the depth limits",
                            'service_name': service_name,
                            'service_tool_version': service_tool_version,
                            'service_version': service_version,
                            'status': 'FAIL_NONRECOVERABLE'
                        },
                        'sha256': extracted_sha256,
                        'type': 'MAX DEPTH REACHED'
                    }))

        # Check if its worth trying to run the next stage
        # Not worth running if we know we are waiting for another service
        if any(_s == sha256 for _s, _ in task.running_services):
            return
        # Not worth running if we know we have services in queue
        if any(_s == sha256 for _s, _ in task.queue_keys.keys()):
            return
        # Try to run the next stage
        self.dispatch_file(task, sha256)

    @elasticapm.capture_span(span_type='dispatcher')
    def _dispatching_error(self, task: SubmissionTask, error):
        error_key = error.build_key()
        task.extra_errors.append(error_key)
        self.error_queue.put((error_key, error))
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

            messages = start_queue.pop_batch(100)
            if not messages:
                message = start_queue.pop(timeout=1)
                if message:
                    messages = [message]

            cpu_mark = time.process_time()
            time_mark = time.time()

            for message in messages:
                sid, sha, service_name, worker_id = message
                self.find_process_queue(sid).put(DispatchAction(kind=Action.start, sid=sid, sha=sha,
                                                                service_name=service_name, worker_id=worker_id))

    @elasticapm.capture_span(span_type='dispatcher')
    def set_timeout(self, task: SubmissionTask, sha256, service_name, worker_id):
        sid = task.submission.sid
        service = self.scheduler.services.get(service_name)
        if not service:
            return
        self._service_timeouts.set((sid, sha256, service_name), service.timeout + TIMEOUT_EXTRA_TIME, worker_id)
        task.running_services.add((sha256, service_name))

    @elasticapm.capture_span(span_type='dispatcher')
    def clear_timeout(self, task, sha256, service_name):
        sid = task.submission.sid
        task.queue_keys.pop((sha256, service_name), None)
        task.running_services.discard((sha256, service_name))
        self._service_timeouts.clear((sid, sha256, service_name))

    def handle_timeouts(self):
        while self.sleep(TIMEOUT_TEST_INTERVAL):
            with apm_span(self.apm_client, 'process_timeouts'):
                cpu_mark = time.process_time()
                time_mark = time.time()

                # Check for submission timeouts
                submission_timeouts = self._submission_timeouts.timeouts()
                for sid in submission_timeouts.keys():
                    _q = self.find_process_queue(sid)
                    _q.put(DispatchAction(kind=Action.check_submission, sid=sid))

                # Check for service timeouts
                service_timeouts = self._service_timeouts.timeouts()
                for (sid, sha, service_name), worker_id in service_timeouts.items():
                    # Put our timeouts into special timeout queue so they are delayed
                    # until redis results are processed
                    self.timeout_queue.put(
                        DispatchAction(kind=Action.service_timeout, sid=sid, sha=sha,
                                       service_name=service_name, worker_id=worker_id)
                    )

                self.counter.increment('service_timeouts', len(service_timeouts))
                self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
                self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

    @elasticapm.capture_span(span_type='dispatcher')
    def timeout_service(self, task: SubmissionTask, sha256, service_name, worker_id):
        # We believe a service task has timed out, try and read it from running tasks
        # If we can't find the task in running tasks, it finished JUST before timing out, let it go
        sid = task.submission.sid
        task.queue_keys.pop((sha256, service_name), None)
        task_key = ServiceTask.make_key(sid=sid, service_name=service_name, sha=sha256)
        service_task = self.running_tasks.pop(task_key)
        if not service_task and (sha256, service_name) not in task.running_services:
            self.log.debug(f"[{sid}] Service {service_name} "
                           f"timed out on {sha256} but task isn't running.")
            return False

        # We can confirm that the task is ours now, even if the worker finished, the result will be ignored
        task.running_services.discard((sha256, service_name))
        self.log.info(f"[{sid}] Service {service_name} "
                      f"running on {worker_id} timed out on {sha256}.")
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
                                host=worker_id, counter_type='service', redis=self.redis)
        return True

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

        last_seen = {}

        try:
            while self.sleep(GUARD_TIMEOUT / 4):
                cpu_mark = time.process_time()
                time_mark = time.time()

                # Load guards
                last_seen.update(self.dispatchers_directory.items())

                # List all dispatchers with jobs assigned
                for raw_key in self.redis_persist.keys(TASK_ASSIGNMENT_PATTERN):
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
        target_jobs = Hash(DISPATCH_TASK_ASSIGNMENT+target, host=self.redis_persist)
        self.log.info(f'Starting to steal work from {target}')

        # Start of process dispatcher transaction
        if self.apm_client:
            self.apm_client.begin_transaction(APM_SPAN_TYPE)

        cpu_mark = time.process_time()
        time_mark = time.time()

        keys = target_jobs.keys()
        while keys:
            key = keys.pop()
            message = target_jobs.pop(key)
            if not keys:
                keys = target_jobs.keys()

            if not message:
                continue

            if self.submissions_assignments.pop(key):
                self.submission_queue.unpop(message)

        self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
        self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

        if self.apm_client:
            self.apm_client.end_transaction('submission_message', 'success')

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
                    watch_payload: CreateWatch = command.payload()
                    self.setup_watch_queue(watch_payload.submission, watch_payload.queue_name)
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
        task = self.tasks.get(sid)
        if not task:
            watch_queue.push(WatchQueueMessage({"status": "STOP"}).as_primitives())
            return

        # Add the newly created queue to the list of queues for the given submission
        self._watcher_list(sid).add(queue_name)

        # Push all current keys to the newly created queue (Queue should have a TTL of about 30 sec to 1 minute)
        for result_data in list(task.service_results.values()):
            watch_queue.push(WatchQueueMessage({"status": "OK", "cache_key": result_data.key}).as_primitives())

        for error_key in list(task.service_errors.values()):
            watch_queue.push(WatchQueueMessage({"status": "FAIL", "cache_key": error_key}).as_primitives())

    @elasticapm.capture_span(span_type='dispatcher')
    def list_outstanding(self, sid: str, queue_name: str):
        response_queue = NamedQueue(queue_name, host=self.redis)
        outstanding: defaultdict[str, int] = defaultdict(int)
        task = self.tasks.get(sid)
        if task:
            for sha, service_name in list(task.queue_keys.keys()):
                outstanding[service_name] += 1
            for sha, service_name in list(task.running_services):
                outstanding[service_name] += 1
        response_queue.push(outstanding)

    def timeout_backstop(self):
        while self.running:
            cpu_mark = time.process_time()
            time_mark = time.time()

            # Start of process dispatcher transaction
            with apm_span(self.apm_client, 'timeout_backstop'):
                dispatcher_instances = set(Dispatcher.all_instances(persistent_redis=self.redis_persist))
                error_tasks = []

                # iterate running tasks
                for task_key, task_body in self.running_tasks:
                    task = ServiceTask(task_body)
                    # Its a bad task if it's dispatcher isn't running
                    if task.metadata['dispatcher__'] not in dispatcher_instances:
                        error_tasks.append(task)
                    # Its a bad task if its OUR task, but we aren't tracking that submission anymore
                    if task.metadata['dispatcher__'] == self.instance_id and task.sid not in self.tasks:
                        error_tasks.append(task)

                # Refresh our dispatcher list.
                dispatcher_instances = set(Dispatcher.all_instances(persistent_redis=self.redis_persist))
                other_dispatcher_instances = dispatcher_instances - {self.instance_id}

                # The remaining running tasks (probably) belong to dead dispatchers and can be killed
                for task in error_tasks:
                    # Check against our refreshed dispatcher list in case it changed during the previous scan
                    if task.metadata['dispatcher__'] in other_dispatcher_instances:
                        continue

                    # If its already been handled, we don't need to
                    if not self.running_tasks.pop(task.key()):
                        continue

                    # Kill the task that would report to a dead dispatcher
                    self.log.warning(f"[{task.sid}]Task killed by backstop {task.service_name} {task.fileinfo.sha256}")
                    self.scaler_timeout_queue.push({
                        'service': task.service_name,
                        'container': task.metadata['worker__']
                    })

                    # Report to the metrics system that a recoverable error has occurred for that service
                    export_metrics_once(task.service_name, ServiceMetrics, dict(fail_recoverable=1),
                                        host=task.metadata['worker__'], counter_type='service', redis=self.redis)

            # Look for unassigned submissions in the datastore if we don't have a
            # large number of outstanding things in the queue already.
            with apm_span(self.apm_client, 'orphan_submission_check'):
                assignments = self.submissions_assignments.items()
                recovered_from_database = []
                if self.submission_queue.length() < 500:
                    with apm_span(self.apm_client, 'abandoned_submission_check'):
                        # Get the submissions belonging to an dispatcher we don't know about
                        for item in self.datastore.submission.stream_search('state: submitted', fl='sid'):
                            if item['sid'] in assignments:
                                continue
                            recovered_from_database.append(item['sid'])

            # Look for instances that are in the assignment table, but the instance its assigned to doesn't exist.
            # We try to remove the instance from the table to prevent multiple dispatcher instances from
            # recovering it at the same time
            with apm_span(self.apm_client, 'orphan_submission_check'):
                # Get the submissions belonging to an dispatcher we don't know about
                assignments = self.submissions_assignments.items()
                dispatcher_instances = set(Dispatcher.all_instances(persistent_redis=self.redis_persist))
                # List all dispatchers with jobs assigned
                for raw_key in self.redis_persist.keys(TASK_ASSIGNMENT_PATTERN):
                    key: str = raw_key.decode()
                    dispatcher_instances.add(key[len(DISPATCH_TASK_ASSIGNMENT):])

                # Submissions that didn't belong to anyone should be recovered
                for sid, instance in assignments.items():
                    if instance in dispatcher_instances:
                        continue
                    if self.submissions_assignments.conditional_remove(sid, instance):
                        self.recover_submission(sid, 'from assignment table')

            # Go back over the list of sids from the database now that we have a copy of the
            # assignments table taken after our database scan
            for sid in recovered_from_database:
                if sid not in assignments:
                    self.recover_submission(sid, 'from database scan')

            self.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            self.counter.increment_execution_time('busy_seconds', time.time() - time_mark)
            self.sleep(GLOBAL_TASK_CHECK_INTERVAL)

    def recover_submission(self, sid: str, message: str) -> bool:
        # Make sure we can load the submission body
        submission: Optional[Submission] = self.datastore.submission.get_if_exists(sid)
        if not submission:
            return False
        if submission.state != 'submitted':
            return False

        self.log.warning(f'Recovered dead submission: {sid} {message}')

        # Try to recover the completion queue value by checking with the ingest table
        completed_queue = ''
        if submission.scan_key:
            completed_queue = COMPLETE_QUEUE_NAME

        # Put the file back into processing
        self.submission_queue.unpop(dict(
            submission=submission.as_primitives(),
            completed_queue=completed_queue,
        ))
        return True
