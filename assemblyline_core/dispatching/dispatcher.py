from __future__ import annotations

import dataclasses
import enum
import os
import threading
import time
import uuid
from contextlib import contextmanager
from copy import deepcopy
from queue import Empty, PriorityQueue, Queue
from typing import TYPE_CHECKING, Any, Iterable, Optional

import elasticapm

from assemblyline.common.constants import (
    DISPATCH_RUNNING_TASK_HASH,
    DISPATCH_TASK_HASH,
    SCALER_TIMEOUT_QUEUE,
    SUBMISSION_QUEUE,
    make_watcher_list_name,
)
from assemblyline.common.forge import (
    get_apm_client,
    get_classification,
    get_service_queue,
)
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.metrics import MetricsFactory
from assemblyline.common.postprocess import ActionWorker
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.messages.changes import Operation, ServiceChange
from assemblyline.odm.messages.dispatcher_heartbeat import Metrics
from assemblyline.odm.messages.dispatching import (
    CREATE_WATCH,
    LIST_OUTSTANDING,
    UPDATE_BAD_SID,
    CreateWatch,
    DispatcherCommandMessage,
    ListOutstanding,
    WatchQueueMessage,
)
from assemblyline.odm.messages.service_heartbeat import Metrics as ServiceMetrics
from assemblyline.odm.messages.submission import (
    SubmissionMessage,
    from_datastore_submission,
)
from assemblyline.odm.messages.task import FileInfo
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.service import Service
from assemblyline.odm.models.submission import Submission, TraceEvent
from assemblyline.odm.models.user import User
from assemblyline.remote.datatypes.events import EventWatcher
from assemblyline.remote.datatypes.exporting_counter import export_metrics_once
from assemblyline.remote.datatypes.hash import Hash
from assemblyline.remote.datatypes.queues.comms import CommsQueue
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.set import ExpiringSet, Set
from assemblyline.remote.datatypes.user_quota_tracker import UserQuotaTracker
from assemblyline_core.server_base import ThreadedCoreBase

from ..ingester.constants import COMPLETE_QUEUE_NAME
from .schedules import Scheduler

if TYPE_CHECKING:
    from redis import Redis

    from assemblyline.odm.models.file import File
    from assemblyline.odm.models.config import Config


APM_SPAN_TYPE = 'handle_message'

AL_SHUTDOWN_GRACE = int(os.environ.get('AL_SHUTDOWN_GRACE', '60'))
AL_SHUTDOWN_QUIT = 60
FINALIZING_WINDOW = max(AL_SHUTDOWN_GRACE - AL_SHUTDOWN_QUIT, 0)
RESULT_BATCH_SIZE = int(os.environ.get('DISPATCHER_RESULT_BATCH_SIZE', '50'))
ERROR_BATCH_SIZE = int(os.environ.get('DISPATCHER_ERROR_BATCH_SIZE', '50'))
DAY_IN_SECONDS = 24 * 60 * 60


class KeyType(enum.Enum):
    OVERWRITE = 'overwrite'
    UNION = 'union'


class Action(enum.IntEnum):
    start = 0
    result = 1
    dispatch_file = 2
    service_timeout = 3
    check_submission = 4
    bad_sid = 5


@dataclasses.dataclass(order=True)
class DispatchAction:
    kind: Action
    sid: str = dataclasses.field(compare=False)
    sha: Optional[str] = dataclasses.field(compare=False, default=None)
    service_name: Optional[str] = dataclasses.field(compare=False, default=None)
    worker_id: Optional[str] = dataclasses.field(compare=False, default=None)
    data: Any = dataclasses.field(compare=False, default=None)
    event: Optional[threading.Event] = dataclasses.field(compare=False, default=None)


@dataclasses.dataclass()
class MonitorTask:
    """Tracks whether a task needs to be rerun based on """
    # Service name
    service: str
    # sha256 of file in question
    sha: str
    # The temporary values this task was last dispatached with
    values: dict[str, Optional[str]]
    # Should aservice be dispatched again when possible
    dispatch_needed: bool = dataclasses.field(default=False)


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
    def __init__(self, key, drop, score, children, partial=False) -> None:
        self.key: str = key
        self.drop: bool = drop
        self.partial: bool = partial
        self.score: int = score
        self.children: list[tuple[str, str]] = children


class TemporaryFileData:
    def __init__(self,
                 sha256: str,
                 config: dict[str, str],
                 shared: Optional[dict[str, Any]] = None,
                 local: Optional[dict[str, Any]] = None
                 ) -> None:
        self.sha256 = sha256
        self.config = config
        self.shared_values: dict[str, Any] = {} if shared is None else shared
        self.local_values: dict[str, Any] = {} if local is None else local

    def new_file(self, sha256: str) -> TemporaryFileData:
        """Create an entry for another file with reference to the shared values."""
        return TemporaryFileData(sha256, self.config, self.shared_values, deepcopy(self.local_values))

    def read(self) -> dict[str, Any]:
        """Get a copy of the current data"""
        # Start with a shallow copy of the local data
        data = dict(self.local_values)

        # mix in whatever the latest submission wide values are values are
        data.update(self.shared_values)
        return data

    def read_key(self, key: str) -> Any:
        """Get a copy of the current data"""
        try:
            return self.shared_values[key]
        except KeyError:
            return self.local_values.get(key)

    def set_value(self, key: str, value: Any) -> bool:
        """Set the value of a temporary data key using the appropriate method for the key.

        Return true if this change could mean partial results should be reevaluated.
        """
        if self.config.get(key) == KeyType.UNION.value:
            return self._union_shared_value(key, value)

        if self.config.get(key) == KeyType.OVERWRITE.value:
            change = self.shared_values.get(key) != value
            self.shared_values[key] = value
            return change

        self.local_values[key] = value
        return False

    def _union_shared_value(self, key: str, values: Any) -> bool:
        # Make sure the existing value is the right type
        self.shared_values.setdefault(key, [])
        if not isinstance(self.shared_values[key], list):
            self.shared_values[key] = []

        # make sure the input is the right type
        if not isinstance(values, (list, tuple)):
            return False

        # Add each value one at a time testing for new values
        # This is slower than using set intersection, but isn't type sensitive
        changed = False
        for new_item in values:
            if new_item in self.shared_values[key]:
                continue
            self.shared_values[key].append(new_item)
            changed = True
        return changed


DISPATCH_TASK_ASSIGNMENT = 'dispatcher-tasks-assigned-to-'
TASK_ASSIGNMENT_PATTERN = DISPATCH_TASK_ASSIGNMENT + '*'
DISPATCH_START_EVENTS = 'dispatcher-start-events-'
DISPATCH_RESULT_QUEUE = 'dispatcher-results-'
DISPATCH_COMMAND_QUEUE = 'dispatcher-commands-'
DISPATCH_DIRECTORY = 'dispatchers-directory'
DISPATCH_DIRECTORY_FINALIZE = 'dispatchers-directory-finalizing'
BAD_SID_HASH = 'bad-sid-hash'
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
    def all_instances(persistent_redis: Redis):
        return Hash(DISPATCH_DIRECTORY, host=persistent_redis).keys()

    @staticmethod
    def instance_assignment_size(persistent_redis, instance_id):
        return Hash(DISPATCH_TASK_ASSIGNMENT + instance_id, host=persistent_redis).length()

    @staticmethod
    def instance_assignment(persistent_redis, instance_id) -> list[str]:
        return Hash(DISPATCH_TASK_ASSIGNMENT + instance_id, host=persistent_redis).keys()

    @staticmethod
    def all_queue_lengths(redis, instance_id):
        return {
            'start': NamedQueue(DISPATCH_START_EVENTS + instance_id, host=redis).length(),
            'result': NamedQueue(DISPATCH_RESULT_QUEUE + instance_id, host=redis).length(),
            'command': NamedQueue(DISPATCH_COMMAND_QUEUE + instance_id, host=redis).length()
        }

    def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None,
                 config=None, counter_name: str = 'dispatcher'):
        super().__init__('assemblyline.dispatcher', config=config, datastore=datastore,
                         redis=redis, redis_persist=redis_persist, logger=logger)

        # Load the datastore collections that we are going to be using
        self.instance_id = uuid.uuid4().hex
        self.tasks: dict[str, SubmissionTask] = {}
        self.finalizing = threading.Event()
        self.finalizing_start = 0.0

        # Build some utility classes
        self.scheduler = Scheduler(self.datastore, self.config, self.redis)
        self.running_tasks: Hash[dict] = Hash(DISPATCH_RUNNING_TASK_HASH, host=self.redis)
        self.scaler_timeout_queue = NamedQueue(SCALER_TIMEOUT_QUEUE, host=self.redis_persist)

        self.classification_engine = get_classification()

        # Output. Duplicate our input traffic into this queue so it may be cloned by other systems
        self.traffic_queue = CommsQueue('submissions', self.redis)
        self.quota_tracker = UserQuotaTracker('submissions', timeout=60 * 60, host=self.redis_persist)
        self.submission_queue = NamedQueue(SUBMISSION_QUEUE, self.redis)

        # Table to track the running dispatchers
        self.dispatchers_directory: Hash[int] = Hash(DISPATCH_DIRECTORY, host=self.redis_persist)
        self.dispatchers_directory_finalize: Hash[int] = Hash(DISPATCH_DIRECTORY_FINALIZE, host=self.redis_persist)
        self.running_dispatchers_estimate = 1

        # Tables to track what submissions are running where
        self.active_submissions = Hash(DISPATCH_TASK_ASSIGNMENT+self.instance_id, host=self.redis_persist)
        self.submissions_assignments = Hash(DISPATCH_TASK_HASH, host=self.redis_persist)
        self.ingester_scanning = Hash('m-scanning-table', self.redis_persist)

        # Communications queues
        self.start_queue: NamedQueue[tuple[str, str, str, str]] =\
            NamedQueue(DISPATCH_START_EVENTS+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)
        self.result_queue: NamedQueue[dict] =\
            NamedQueue(DISPATCH_RESULT_QUEUE+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)
        self.command_queue: NamedQueue[dict] =\
            NamedQueue(DISPATCH_COMMAND_QUEUE+self.instance_id, host=self.redis, ttl=QUEUE_EXPIRY)

        # Publish counters to the metrics sink.
        self.counter = MetricsFactory(metrics_type='dispatcher', schema=Metrics, name=counter_name,
                                      redis=self.redis, config=self.config)

        self.apm_client = None
        if self.config.core.metrics.apm_server.server_url:
            elasticapm.instrument()
            self.apm_client = get_apm_client("dispatcher")

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

        # Update bad sid list
        self.redis_bad_sids = Set(BAD_SID_HASH, host=self.redis_persist)
        self.bad_sids: set[str] = set(self.redis_bad_sids.members())

        # Event Watchers
        self.service_change_watcher = EventWatcher(self.redis, deserializer=ServiceChange.deserialize)
        self.service_change_watcher.register('changes.services.*', self._handle_service_change_event)

    def stop(self):
        super().stop()
        self.service_change_watcher.stop()
        self.postprocess_worker.stop()

    def interrupt_handler(self, signum, stack_frame):
        self.log.info("Instance caught signal. Beginning to drain work.")
        self.finalizing_start = time.time()
        self._shutdown_timeout = AL_SHUTDOWN_QUIT
        self.finalizing.set()
        self.dispatchers_directory_finalize.set(self.instance_id, int(time.time()))

    def _handle_status_change(self, status: Optional[bool]):
        super()._handle_status_change(status)

        # If we may have lost redis connection check all of our submissions
        if status is None:
            for sid in self.tasks.keys():
                _q = self.find_process_queue(sid)
                _q.put(DispatchAction(kind=Action.check_submission, sid=sid))

    def _handle_service_change_event(self, data: Optional[ServiceChange]):
        if not data:
            # We may have missed change messages, flush cache
            self.scheduler.c12n_services.clear()
            return
        if data.operation == Operation.Removed:
            # Remove all current instances of service from scheduler cache
            for service_set in self.scheduler.c12n_services.values():
                if data.name in service_set:
                    service_set.remove(data.name)
        else:
            # If Added/Modifed, pull the service information and modify cache
            service: Service = self.datastore.get_service_with_delta(data.name)
            for c12n, service_set in self.scheduler.c12n_services.items():
                if self.classification_engine.is_accessible(c12n, service.classification):
                    # Classification group is allowed to use this service
                    service_set.add(service.name)
                else:
                    # Classification group isn't allowed to use this service
                    if service.name in service_set:
                        service_set.remove(service.name)

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
        self.service_change_watcher.start()
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
            for (_sha256, service_name), dispatch_key in task.queue_keys.items():
                try:
                    s_queue = service_queues[service_name]
                except KeyError:
                    s_queue = get_service_queue(service_name, self.redis)
                    service_queues[service_name] = s_queue
                s_queue.remove(dispatch_key)

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

    def timeout_backstop(self):
        while self.running:
            cpu_mark = time.process_time()
            time_mark = time.time()

            # Start of process dispatcher transaction
            with apm_span(self.apm_client, 'timeout_backstop'):
                dispatcher_instances = set(Dispatcher.all_instances(persistent_redis=self.redis_persist))
                error_tasks = []

                # iterate running tasks
                for _task_key, task_body in self.running_tasks:
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

