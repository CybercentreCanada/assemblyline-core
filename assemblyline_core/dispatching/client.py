"""
An interface to the core system for the edge services.


"""
import elasticapm
import functools
import logging
import time
import weakref

from typing import Optional, Any, cast

from assemblyline.common import forge
from assemblyline.common.constants import DISPATCH_RUNNING_TASK_HASH, SUBMISSION_QUEUE, \
    make_watcher_list_name, DISPATCH_TASK_HASH
from assemblyline.common.dict_utils import flatten
from assemblyline.common.forge import CachedObject, get_service_queue
from assemblyline.common.tagging import tag_dict_to_list
from assemblyline.datastore.exceptions import VersionConflictException
from assemblyline.odm.base import DATEFORMAT
from assemblyline.odm.messages.dispatching import DispatcherCommandMessage, CREATE_WATCH, \
    CreateWatch, LIST_OUTSTANDING, ListOutstanding
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.file import File
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.service import Service
from assemblyline.odm.models.submission import Submission
from assemblyline.remote.datatypes import get_client, reply_queue_name
from assemblyline.remote.datatypes.hash import ExpiringHash, Hash
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.set import ExpiringSet
from assemblyline_core.dispatching.dispatcher import DISPATCH_START_EVENTS, DISPATCH_RESULT_QUEUE, \
    DISPATCH_COMMAND_QUEUE, QUEUE_EXPIRY, ServiceTask, Dispatcher


def weak_lru(maxsize=128, typed=False):
    'LRU Cache decorator that keeps a weak reference to "self"'
    def wrapper(func):

        @functools.lru_cache(maxsize, typed)
        def _func(_self, *args, **kwargs):
            return func(_self(), *args, **kwargs)

        @functools.wraps(func)
        def inner(self, *args, **kwargs):
            return _func(weakref.ref(self), *args, **kwargs)

        return inner

    return wrapper


class RetryRequestWork(Exception):
    pass


class DispatchClient:
    def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None):
        self.config = forge.get_config()

        self.redis = redis or get_client(
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )

        self.redis_persist = redis_persist or get_client(
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )

        self.submission_queue = NamedQueue(SUBMISSION_QUEUE, self.redis)
        self.ds = datastore or forge.get_datastore(self.config)
        self.log = logger or logging.getLogger("assemblyline.dispatching.client")
        self.results = self.ds.result
        self.errors = self.ds.error
        self.files = self.ds.file
        self.submission_assignments = ExpiringHash(DISPATCH_TASK_HASH, host=self.redis_persist)
        self.running_tasks = Hash(DISPATCH_RUNNING_TASK_HASH, host=self.redis)
        self.service_data = cast(dict[str, Service], CachedObject(self._get_services))
        self.dispatcher_data = []
        self.dispatcher_data_age = 0.0
        self.dead_dispatchers = []

    @weak_lru(maxsize=128)
    def _get_queue_from_cache(self, name):
        return NamedQueue(name, host=self.redis, ttl=QUEUE_EXPIRY)

    def _get_services(self):
        # noinspection PyUnresolvedReferences
        return {x.name: x for x in self.ds.list_all_services(full=True)}

    def is_dispatcher(self, dispatcher_id) -> bool:
        if dispatcher_id in self.dead_dispatchers:
            return False
        if time.time() - self.dispatcher_data_age > 120 or dispatcher_id not in self.dispatcher_data:
            self.dispatcher_data = Dispatcher.all_instances(self.redis_persist)
            self.dispatcher_data_age = time.time()
        if dispatcher_id in self.dispatcher_data:
            return True
        else:
            self.dead_dispatchers.append(dispatcher_id)
            return False

    def dispatch_bundle(self, submission: Submission, results: dict[str, Result],
                        file_infos: dict[str, File], file_tree, errors: dict[str, Error], completed_queue: str = None):
        """Insert a bundle into the dispatching system and continue scanning of its files

        Prerequisites:
            - Submission, results, file_infos and errors should already be saved in the datastore
            - Files should already be in the filestore
        """
        self.submission_queue.push(dict(
            submission=submission.as_primitives(),
            results=results,
            file_infos=file_infos,
            file_tree=file_tree,
            errors=errors,
            completed_queue=completed_queue,
        ))

    def dispatch_submission(self, submission: Submission, completed_queue: str = None):
        """Insert a submission into the dispatching system.

        Note:
            You probably actually want to use the SubmissionTool

        Prerequsits:
            - submission should already be saved in the datastore
            - files should already be in the datastore and filestore
        """
        self.submission_queue.push(dict(
            submission=submission.as_primitives(),
            completed_queue=completed_queue,
        ))

    def outstanding_services(self, sid) -> Optional[dict[str, int]]:
        """
        List outstanding services for a given submission and the number of file each
        of them still have to process.

        :param sid: Submission ID
        :return: Dictionary of services and number of files
                 remaining per services e.g. {"SERVICE_NAME": 1, ... }
        """
        dispatcher_id = self.submission_assignments.get(sid)
        if dispatcher_id:
            queue_name = reply_queue_name(prefix="D", suffix="ResponseQueue")
            queue: NamedQueue[dict[str, int]] = NamedQueue(queue_name, host=self.redis, ttl=30)
            command_queue = NamedQueue(DISPATCH_COMMAND_QUEUE+dispatcher_id, ttl=QUEUE_EXPIRY, host=self.redis)
            command_queue.push(DispatcherCommandMessage({
                'kind': LIST_OUTSTANDING,
                'payload_data': ListOutstanding({
                    'response_queue': queue_name,
                    'submission': sid
                })
            }).as_primitives())
            return queue.pop(timeout=30)
        return {}

    @elasticapm.capture_span(span_type='dispatch_client')
    def request_work(self, worker_id, service_name, service_version,
                     timeout: float = 60, blocking=True, low_priority=False) -> Optional[ServiceTask]:
        """Pull work from the service queue for the service in question.

        :param service_version:
        :param worker_id:
        :param service_name: Which service needs work.
        :param timeout: How many seconds to block before returning if blocking is true.
        :param blocking: Whether to wait for jobs to enter the queue, or if false, return immediately
        :return: The job found, and a boolean value indicating if this is the first time this task has
                 been returned by request_work.
        """
        start = time.time()
        remaining = timeout
        while int(remaining) > 0:
            work = self._request_work(worker_id, service_name, service_version,
                                      blocking=blocking, timeout=remaining, low_priority=low_priority)
            if work or not blocking:
                return work
            remaining = timeout - (time.time() - start)
        return None

    def _request_work(self, worker_id, service_name, service_version,
                      timeout, blocking, low_priority=False) -> Optional[ServiceTask]:
        # For when we recursively retry on bad task dequeue-ing
        if int(timeout) <= 0:
            self.log.info(f"{service_name}:{worker_id} no task returned [timeout]")
            return None

        # Get work from the queue
        work_queue = get_service_queue(service_name, self.redis)
        if blocking:
            result = work_queue.blocking_pop(timeout=int(timeout), low_priority=low_priority)
        else:
            if low_priority:
                result = work_queue.unpush(1)
            else:
                result = work_queue.pop(1)
            if result:
                result = result[0]

        if not result:
            self.log.info(f"{service_name}:{worker_id} no task returned: [empty message]")
            return None
        task = ServiceTask(result)
        task.metadata['worker__'] = worker_id
        dispatcher = task.metadata['dispatcher__']

        if not self.is_dispatcher(dispatcher):
            self.log.info(f"{service_name}:{worker_id} no task returned: [task from dead dispatcher]")
            return None

        if self.running_tasks.add(task.key(), task.as_primitives()):
            self.log.info(f"[{task.sid}/{task.fileinfo.sha256}] {service_name}:{worker_id} task found")
            start_queue = self._get_queue_from_cache(DISPATCH_START_EVENTS + dispatcher)
            start_queue.push((task.sid, task.fileinfo.sha256, service_name, worker_id))
            return task
        return None

    @elasticapm.capture_span(span_type='dispatch_client')
    def service_finished(self, sid: str, result_key: str, result: Result,
                         temporary_data: Optional[dict[str, Any]] = None):
        """Notifies the dispatcher of service completion, and possible new files to dispatch."""
        # Make sure the dispatcher knows we were working on this task
        task_key = ServiceTask.make_key(sid=sid, service_name=result.response.service_name, sha=result.sha256)
        task = self.running_tasks.pop(task_key)
        if not task:
            self.log.warning(f"[{sid}/{result.sha256}] {result.response.service_name} could not find the specified "
                             f"task in its set of running tasks while processing successful results.")
            return
        task = ServiceTask(task)

        # Save or freshen the result, the CONTENT of the result shouldn't change, but we need to keep the
        # most distant expiry time to prevent pulling it out from under another submission too early
        if result.is_empty():
            # Empty Result will not be archived therefore result.archive_ts drives their deletion
            self.ds.emptyresult.save(result_key, {"expiry_ts": result.archive_ts})
        else:
            while True:
                old, version = self.ds.result.get_if_exists(
                    result_key, archive_access=self.config.datastore.ilm.update_archive, version=True)
                if old:
                    if old.expiry_ts and result.expiry_ts:
                        result.expiry_ts = max(result.expiry_ts, old.expiry_ts)
                    else:
                        result.expiry_ts = None
                try:
                    self.ds.result.save(result_key, result, version=version)
                    break
                except VersionConflictException as vce:
                    self.log.info(f"Retrying to save results due to version conflict: {str(vce)}")

        # Send the result key to any watching systems
        msg = {'status': 'OK', 'cache_key': result_key}
        for w in self._get_watcher_list(task.sid).members():
            NamedQueue(w, host=self.redis).push(msg)

        # Save the tags
        tags = []
        for section in result.result.sections:
            tags.extend(tag_dict_to_list(flatten(section.tags.as_primitives())))

        # Pull out file names if we have them
        file_names = {}
        for extracted_data in result.response.extracted:
            if extracted_data.name:
                file_names[extracted_data.sha256] = extracted_data.name

        #
        dispatcher = task.metadata['dispatcher__']
        result_queue = self._get_queue_from_cache(DISPATCH_RESULT_QUEUE + dispatcher)
        ex_ts = result.expiry_ts.strftime(DATEFORMAT) if result.expiry_ts else result.archive_ts.strftime(DATEFORMAT)
        result_queue.push({
            # 'service_task': task.as_primitives(),
            # 'result': result.as_primitives(),
            'sid': task.sid,
            'sha256': result.sha256,
            'service_name': task.service_name,
            'service_version': result.response.service_version,
            'service_tool_version': result.response.service_tool_version,
            'archive_ts': result.archive_ts.strftime(DATEFORMAT),
            'expiry_ts': ex_ts,
            'result_summary': {
                'key': result_key,
                'drop': result.drop_file,
                'score': result.result.score,
                'children': [r.sha256 for r in result.response.extracted],
            },
            'tags': tags,
            'extracted_names': file_names,
            'temporary_data': temporary_data
        })

    @elasticapm.capture_span(span_type='dispatch_client')
    def service_failed(self, sid: str, error_key: str, error: Error):
        task_key = ServiceTask.make_key(sid=sid, service_name=error.response.service_name, sha=error.sha256)
        task = self.running_tasks.pop(task_key)
        if not task:
            self.log.warning(f"[{sid}/{error.sha256}] {error.response.service_name} could not find the specified "
                             f"task in its set of running tasks while processing an error.")
            return
        task = ServiceTask(task)

        self.log.debug(f"[{sid}/{error.sha256}] {task.service_name} Failed with {error.response.status} error.")
        if error.response.status == "FAIL_NONRECOVERABLE":
            # This is a NON_RECOVERABLE error, error will be saved and transmitted to the user
            self.errors.save(error_key, error)

            # Send the result key to any watching systems
            msg = {'status': 'FAIL', 'cache_key': error_key}
            for w in self._get_watcher_list(task.sid).members():
                NamedQueue(w, host=self.redis).push(msg)

        dispatcher = task.metadata['dispatcher__']
        result_queue = self._get_queue_from_cache(DISPATCH_RESULT_QUEUE + dispatcher)
        result_queue.push({
            'sid': task.sid,
            'service_task': task.as_primitives(),
            'error': error.as_primitives(),
            'error_key': error_key
        })

    def setup_watch_queue(self, sid: str) -> Optional[str]:
        """
        This function takes a submission ID as a parameter and creates a unique queue where all service
        result keys for that given submission will be returned to as soon as they come in.

        If the submission is in the middle of processing, this will also send all currently received keys through
        the specified queue so the client that requests the watch queue is up to date.

        :param sid: Submission ID
        :return: The name of the watch queue that was created
        """
        dispatcher_id = self.submission_assignments.get(sid)
        if dispatcher_id:
            queue_name = reply_queue_name(prefix="D", suffix="WQ")
            command_queue = NamedQueue(DISPATCH_COMMAND_QUEUE+dispatcher_id, host=self.redis)
            command_queue.push(DispatcherCommandMessage({
                'kind': CREATE_WATCH,
                'payload_data': CreateWatch({
                    'queue_name': queue_name,
                    'submission': sid
                })
            }).as_primitives())
            return queue_name
        return None

    def _get_watcher_list(self, sid):
        return ExpiringSet(make_watcher_list_name(sid), host=self.redis)
