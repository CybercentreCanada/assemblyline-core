"""
An interface to the core system for the edge services.


"""
import logging
import time
from typing import Dict, Optional, Any, cast

from assemblyline.common.forge import CachedObject, get_service_queue

from assemblyline.odm.models.service import Service

from assemblyline.remote.datatypes.exporting_counter import export_metrics_once

from assemblyline.odm.messages.service_heartbeat import Metrics

from assemblyline.common import forge
from assemblyline.common.constants import DISPATCH_RUNNING_TASK_HASH, FILE_QUEUE, SUBMISSION_QUEUE, \
    make_watcher_list_name, get_temporary_submission_data_name, get_tag_set_name
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.tagging import tag_dict_to_list
from assemblyline.odm.messages.dispatching import WatchQueueMessage
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.submission import Submission
from assemblyline.odm.models.error import Error
from assemblyline.remote.datatypes import get_client, reply_queue_name
from assemblyline.remote.datatypes.hash import ExpiringHash
from assemblyline.remote.datatypes.lock import Lock
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.set import ExpiringSet

from assemblyline_core.dispatching.dispatcher import SubmissionTask, FileTask, ServiceTask
from assemblyline_core.dispatching.dispatch_hash import DispatchHash
from assemblyline_core.watcher.client import WatcherClient


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

        redis_persist = redis_persist or get_client(
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )

        self.timeout_watcher = WatcherClient(redis_persist)

        self.submission_queue = NamedQueue(SUBMISSION_QUEUE, self.redis)
        self.file_queue = NamedQueue(FILE_QUEUE, self.redis)
        self.ds = datastore or forge.get_datastore(self.config)
        self.log = logger or logging.getLogger("assemblyline.dispatching.client")
        self.results = datastore.result
        self.errors = datastore.error
        self.files = datastore.file
        self.running_tasks = ExpiringHash(DISPATCH_RUNNING_TASK_HASH, host=self.redis)
        self.service_data = cast(Dict[str, Service], CachedObject(self._get_services))

    def _get_services(self):
        # noinspection PyUnresolvedReferences
        return {x.name: x for x in self.ds.list_all_services(full=True)}

    def dispatch_submission(self, submission: Submission, completed_queue: str = None):
        """Insert a submission into the dispatching system.

        Note:
            You probably actually want to use the SubmissionTool

        Prerequsits:
            - submission should already be saved in the datastore
            - files should already be in the datastore and filestore
        """
        self.submission_queue.push(SubmissionTask(dict(
            submission=submission,
            completed_queue=completed_queue,
        )).as_primitives())

    def outstanding_services(self, sid) -> Dict[str, int]:
        """
        List outstanding services for a given submission and the number of file each
        of them still have to process.

        :param sid: Submission ID
        :return: Dictionary of services and number of files
                 remaining per services e.g. {"SERVICE_NAME": 1, ... }
        """
        # Download the entire status table from redis
        dispatch_hash = DispatchHash(sid, self.redis)
        all_service_status = dispatch_hash.all_results()
        all_files = dispatch_hash.all_files()
        self.log.info(f"[{sid}] Listing outstanding services {len(all_files)} files "
                      f"and {len(all_service_status)} entries found")

        output: Dict[str, int] = {}

        for file_hash in all_files:
            # The schedule might not be in the cache if the submission or file was just issued,
            # but it will be as soon as the file passes through the dispatcher
            schedule = dispatch_hash.schedules.get(file_hash)
            status_values = all_service_status.get(file_hash, {})

            # Go through the schedule stage by stage so we can react to drops
            # either we have a result and we don't need to count the file (but might drop it)
            # or we don't have a result, and we need to count that file
            while schedule:
                stage = schedule.pop(0)
                for service_name in stage:
                    status = status_values.get(service_name)
                    if status:
                        if status.drop:
                            schedule.clear()
                    else:
                        output[service_name] = output.get(service_name, 0) + 1

        return output

    def request_work(self, worker_id, service_name, service_version,
                     timeout: float = 60, blocking=True) -> Optional[ServiceTask]:
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
            try:
                return self._request_work(worker_id, service_name, service_version,
                                          blocking=blocking, timeout=remaining)
            except RetryRequestWork:
                remaining = timeout - (time.time() - start)
        return None

    def _request_work(self, worker_id, service_name, service_version, timeout, blocking) -> Optional[ServiceTask]:
        # For when we recursively retry on bad task dequeue-ing
        if int(timeout) <= 0:
            self.log.info(f"{service_name}:{worker_id} no task returned [timeout]")
            return None

        # Get work from the queue
        work_queue = get_service_queue(service_name, self.redis)
        if blocking:
            result = work_queue.blocking_pop(timeout=int(timeout))
        else:
            result = work_queue.pop(1)
            if result:
                result = result[0]

        if not result:
            self.log.info(f"{service_name}:{worker_id} no task returned: [empty message]")
            return None
        task = ServiceTask(result)

        # If someone is supposed to be working on this task right now, we won't be able to add it
        if self.running_tasks.add(task.key(), task.as_primitives()):
            self.log.info(f"[{task.sid}/{task.fileinfo.sha256}] {service_name}:{worker_id} task found")

            process_table = DispatchHash(task.sid, self.redis)

            abandoned = process_table.dispatch_time(file_hash=task.fileinfo.sha256, service=task.service_name) == 0
            finished = process_table.finished(file_hash=task.fileinfo.sha256, service=task.service_name) is not None

            if abandoned or finished:
                self.log.info(f"[{task.sid}/{task.fileinfo.sha256}] {service_name}:{worker_id} task already complete")
                self.running_tasks.pop(task.key())
                raise RetryRequestWork()

            # Check if this task has reached the retry limit
            attempt_record = ExpiringHash(f'dispatch-hash-attempts-{task.sid}', host=self.redis)
            total_attempts = attempt_record.increment(task.key())
            self.log.info(f"[{task.sid}/{task.fileinfo.sha256}] {service_name}:{worker_id} "
                          f"task attempt {total_attempts}/3")
            if total_attempts > 3:
                self.log.warning(f"[{task.sid}/{task.fileinfo.sha256}] "
                                 f"{service_name}:{worker_id} marking task failed: TASK PREEMPTED ")
                error = Error(dict(
                    archive_ts=now_as_iso(self.config.datastore.ilm.days_until_archive * 24 * 60 * 60),
                    created='NOW',
                    expiry_ts=now_as_iso(task.ttl * 24 * 60 * 60) if task.ttl else None,
                    response=dict(
                        message=f'The number of retries has passed the limit.',
                        service_name=task.service_name,
                        service_version=service_version,
                        status='FAIL_NONRECOVERABLE',
                    ),
                    sha256=task.fileinfo.sha256,
                    type="TASK PRE-EMPTED",
                ))
                error_key = error.build_key(task=task)
                self.service_failed(task.sid, error_key, error)
                export_metrics_once(service_name, Metrics, dict(fail_nonrecoverable=1),
                                    host=worker_id, counter_type='service')
                raise RetryRequestWork()

            # Get the service information
            service_data = self.service_data[task.service_name]
            self.timeout_watcher.touch_task(timeout=int(service_data.timeout), key=f'{task.sid}-{task.key()}',
                                            worker=worker_id, task_key=task.key())
            return task
        raise RetryRequestWork()

    def _dispatching_error(self, task, process_table, error):
        error_key = error.build_key(task=task)
        if process_table.add_error(error_key):
            self.errors.save(error_key, error)
            msg = {'status': 'FAIL', 'cache_key': error_key}
            for w in self._get_watcher_list(task.sid).members():
                NamedQueue(w).push(msg)

    def service_finished(self, sid: str, result_key: str, result: Result,
                         temporary_data: Optional[Dict[str, Any]] = None):
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
            with Lock(f"lock-{result_key}", 5, self.redis):
                old = self.ds.result.get(result_key)
                if old and old.expiry_ts and result.expiry_ts:
                    result.expiry_ts = max(result.expiry_ts, old.expiry_ts)
                self.ds.result.save(result_key, result)

        # Let the logs know we have received a result for this task
        if result.drop_file:
            self.log.debug(f"[{sid}/{result.sha256}] {task.service_name} succeeded. "
                           f"Result will be stored in {result_key} but processing will stop after this service.")
        else:
            self.log.debug(f"[{sid}/{result.sha256}] {task.service_name} succeeded. "
                           f"Result will be stored in {result_key}")

        # Store the result object and mark the service finished in the global table
        process_table = DispatchHash(task.sid, self.redis)
        remaining, duplicate = process_table.finish(task.fileinfo.sha256, task.service_name, result_key,
                                                    result.result.score, result.classification, result.drop_file)
        self.timeout_watcher.clear(f'{task.sid}-{task.key()}')
        if duplicate:
            self.log.warning(f"[{sid}/{result.sha256}] {result.response.service_name}'s current task was already "
                             f"completed in the global processing table.")
            return

        # Push the result tags into redis
        new_tags = []
        for section in result.result.sections:
            new_tags.extend(tag_dict_to_list(section.tags.as_primitives()))
        if new_tags:
            tag_set = ExpiringSet(get_tag_set_name(sid=task.sid, file_hash=task.fileinfo.sha256), host=self.redis)
            tag_set.add(*new_tags)

        # Update the temporary data table for this file
        temp_data_hash = ExpiringHash(get_temporary_submission_data_name(sid=task.sid, file_hash=task.fileinfo.sha256),
                                      host=self.redis)
        for key, value in (temporary_data or {}).items():
            temp_data_hash.set(key, value)

        # Send the extracted files to the dispatcher
        depth_limit = self.config.submission.max_extraction_depth
        new_depth = task.depth + 1
        if new_depth < depth_limit:
            # Prepare the temporary data from the parent to build the temporary data table for
            # these newly extract files
            parent_data = dict(temp_data_hash.items())

            for extracted_data in result.response.extracted:
                if not process_table.add_file(extracted_data.sha256, task.max_files, parent_hash=task.fileinfo.sha256):
                    if parent_data:
                        child_hash_name = get_temporary_submission_data_name(task.sid, extracted_data.sha256)
                        ExpiringHash(child_hash_name, host=self.redis).multi_set(parent_data)

                    self._dispatching_error(task, process_table, Error({
                        'archive_ts': result.archive_ts,
                        'expiry_ts': result.expiry_ts,
                        'response': {
                            'message': f"Too many files extracted for submission {task.sid} "
                                       f"{extracted_data.sha256} extracted by "
                                       f"{task.service_name} will be dropped",
                            'service_name': task.service_name,
                            'service_tool_version': result.response.service_tool_version,
                            'service_version': result.response.service_version,
                            'status': 'FAIL_NONRECOVERABLE'
                        },
                        'sha256': extracted_data.sha256,
                        'type': 'MAX FILES REACHED'
                    }))
                    continue
                file_data = self.files.get(extracted_data.sha256)
                self.file_queue.push(FileTask(dict(
                    sid=task.sid,
                    file_info=dict(
                        magic=file_data.magic,
                        md5=file_data.md5,
                        mime=file_data.mime,
                        sha1=file_data.sha1,
                        sha256=file_data.sha256,
                        size=file_data.size,
                        type=file_data.type,
                    ),
                    depth=new_depth,
                    parent_hash=task.fileinfo.sha256,
                    max_files=task.max_files
                )).as_primitives())
        else:
            for extracted_data in result.response.extracted:
                self._dispatching_error(task, process_table, Error({
                    'archive_ts': result.archive_ts,
                    'expiry_ts': result.expiry_ts,
                    'response': {
                        'message': f"{task.service_name} has extracted a file "
                                   f"{extracted_data.sha256} beyond the depth limits",
                        'service_name': result.response.service_name,
                        'service_tool_version': result.response.service_tool_version,
                        'service_version': result.response.service_version,
                        'status': 'FAIL_NONRECOVERABLE'
                    },
                    'sha256': extracted_data.sha256,
                    'type': 'MAX DEPTH REACHED'
                }))

        # If the global table said that this was the last outstanding service,
        # send a message to the dispatchers.
        if remaining <= 0:
            self.file_queue.push(FileTask(dict(
                sid=task.sid,
                file_info=task.fileinfo,
                depth=task.depth,
                max_files=task.max_files
            )).as_primitives())

        # Send the result key to any watching systems
        msg = {'status': 'OK', 'cache_key': result_key}
        for w in self._get_watcher_list(task.sid).members():
            NamedQueue(w).push(msg)

    def service_failed(self, sid: str, error_key: str, error: Error):
        task_key = ServiceTask.make_key(sid=sid, service_name=error.response.service_name, sha=error.sha256)
        task = self.running_tasks.pop(task_key)
        if not task:
            self.log.warning(f"[{sid}/{error.sha256}] {error.response.service_name} could not find the specified "
                             f"task in its set of running tasks while processing an error.")
            return
        task = ServiceTask(task)

        self.log.debug(f"[{sid}/{error.sha256}] {task.service_name} Failed with {error.response.status} error.")
        remaining = -1
        # Mark the attempt to process the file over in the dispatch table
        process_table = DispatchHash(task.sid, self.redis)
        if error.response.status == "FAIL_RECOVERABLE":
            # Because the error is recoverable, we will not save it nor we will notify the user
            process_table.fail_recoverable(task.fileinfo.sha256, task.service_name)
        else:
            # This is a NON_RECOVERABLE error, error will be saved and transmitted to the user
            self.errors.save(error_key, error)

            remaining, _duplicate = process_table.fail_nonrecoverable(task.fileinfo.sha256,
                                                                      task.service_name, error_key)

            # Send the result key to any watching systems
            msg = {'status': 'FAIL', 'cache_key': error_key}
            for w in self._get_watcher_list(task.sid).members():
                NamedQueue(w).push(msg)
        self.timeout_watcher.clear(f'{task.sid}-{task.key()}')

        # Send a message to prompt the re-issue of the task if needed
        if remaining <= 0:
            self.file_queue.push(FileTask(dict(
                sid=task.sid,
                file_info=task.fileinfo,
                depth=task.depth,
                max_files=task.max_files
            )).as_primitives())

    def setup_watch_queue(self, sid):
        """
        This function takes a submission ID as a parameter and creates a unique queue where all service
        result keys for that given submission will be returned to as soon as they come in.

        If the submission is in the middle of processing, this will also send all currently received keys through
        the specified queue so the client that requests the watch queue is up to date.

        :param sid: Submission ID
        :return: The name of the watch queue that was created
        """
        # Create a unique queue
        queue_name = reply_queue_name(prefix="D", suffix="WQ")
        watch_queue = NamedQueue(queue_name, ttl=30)
        watch_queue.push(WatchQueueMessage({'status': 'START'}).as_primitives())

        # Add the newly created queue to the list of queues for the given submission
        self._get_watcher_list(sid).add(queue_name)

        # Push all current keys to the newly created queue (Queue should have a TTL of about 30 sec to 1 minute)
        # Download the entire status table from redis
        dispatch_hash = DispatchHash(sid, self.redis)
        if dispatch_hash.dispatch_count() == 0 and dispatch_hash.finished_count() == 0:
            # This table is empty? do we have this submission at all?
            submission = self.ds.submission.get(sid)
            if not submission or submission.state == 'completed':
                watch_queue.push(WatchQueueMessage({"status": "STOP"}).as_primitives())
            else:
                # We do have a submission, remind the dispatcher to work on it
                self.submission_queue.push({'sid': sid})

        else:
            all_service_status = dispatch_hash.all_results()
            for status_values in all_service_status.values():
                for status in status_values.values():
                    if status.is_error:
                        watch_queue.push(WatchQueueMessage({"status": "FAIL", "cache_key": status.key}).as_primitives())
                    else:
                        watch_queue.push(WatchQueueMessage({"status": "OK", "cache_key": status.key}).as_primitives())

        return queue_name

    def _get_watcher_list(self, sid):
        return ExpiringSet(make_watcher_list_name(sid), host=self.redis)
