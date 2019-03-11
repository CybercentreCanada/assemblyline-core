"""
An interface to the core system for the edge services.


"""
import logging
from typing import Dict

from assemblyline.common import forge
from assemblyline.odm.messages.dispatching import WatchQueueMessage
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.submission import Submission
from assemblyline.odm.models.error import Error
from assemblyline.remote.datatypes import get_client, reply_queue_name
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.set import ExpiringSet

from al_core.dispatching.dispatcher import SubmissionTask, ServiceTask, FileTask, SUBMISSION_QUEUE, \
    make_watcher_list_name, FILE_QUEUE
from al_core.dispatching.dispatch_hash import DispatchHash
from al_core.dispatching.scheduler import Scheduler


class DispatchClient:
    def __init__(self, datastore=None, redis=None, logger=None):
        self.config = forge.get_config()

        self.redis = redis or get_client(
            db=self.config.core.redis.nonpersistent.db,
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )

        self.submission_queue = NamedQueue(SUBMISSION_QUEUE, self.redis)
        self.file_queue = NamedQueue(FILE_QUEUE, self.redis)
        self.ds = datastore or forge.get_datastore(self.config)
        self.log = logger or logging.getLogger("assemblyline.dispatching.client")
        self.results = datastore.result
        self.errors = datastore.error
        self.files = datastore.file
        self.schedule_builder = Scheduler(self.ds, self.config)

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

        output: Dict[str, int] = {}

        for file_hash, status_values in all_service_status.items():
            # The schedule might not be in the cache if the submission or file was just issued,
            # but it will be as soon as the file passes through the dispatcher
            schedule = dispatch_hash.schedules.get(file_hash)

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

    def request_work(self, service_name, timeout=60):
        raise NotImplementedError()

    def _dispatching_error(self, task, process_table, error):
        error_key = error.build_key()
        if process_table.add_error(error_key):
            self.errors.save(error_key, error)
            msg = {'status': 'FAIL', 'cache_key': error_key}
            for w in self._get_watcher_list(task.sid).members():
                NamedQueue(w).push(msg)

    def service_finished(self, task: ServiceTask, result: Result, result_key):
        """Notifies the dispatcher of service completion, and possible new files to dispatch."""
        if result.drop_file:
            self.log.debug(f"Service {task.service_name} succeeded. "
                           f"Result will be stored in {result_key} but processing will stop after this service.")
        else:
            self.log.debug(f"Service {task.service_name} succeeded. Result will be stored in {result_key}")

        # Store the result object and mark the service finished in the global table
        process_table = DispatchHash(task.sid, self.redis)
        remaining = process_table.finish(task.fileinfo.sha256, task.service_name, result_key,
                                         result.result.score, result.drop_file)

        # Send the extracted files to the dispatcher
        depth_limit = self.config.submission.max_extraction_depth
        new_depth = task.depth + 1
        if new_depth < depth_limit:
            for extracted_data in result.response.extracted:
                if not process_table.add_file(extracted_data.sha256, task.max_files):
                    self._dispatching_error(task, process_table, Error({
                        'expiry_ts': result.expiry_ts,
                        'response': {
                            'message': f"Too many files extracted for submission {task.sid} "
                                       f"{extracted_data.sha256} extracted by "
                                       f"{task.service_name} will be dropped",
                            'service_name': 'dispatcher',
                            'service_version': '0',  # TODO what values actually make sense for service_*
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
                    'expiry_ts': result.expiry_ts,
                    'response': {
                        'message': f"{task.service_name} has extracted a file "
                                   f"{extracted_data.sha256} beyond the depth limits",
                        'service_name': 'dispatcher',
                        'service_version': '0',  # TODO what values actually make sense for service_*
                        'status': 'FAIL_NONRECOVERABLE'
                    },
                    'sha256': extracted_data.sha256,
                    'type': 'MAX DEPTH REACHED'
                }))

        # If the global table said that this was the last outstanding service,
        # send a message to the dispatchers.
        if remaining == 0:
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

    def service_failed(self, task: ServiceTask, error: Error, error_key):
        self.log.debug(f"Service {task.service_name} failed with {error.response.status} error.")

        # Add an error to the datastore
        self.errors.save(error_key, error)

        # Mark the attempt to process the file over in the dispatch table
        process_table = DispatchHash(task.sid, self.redis)
        if error.response.status == "FAIL_RECOVERABLE":
            process_table.fail_recoverable(task.fileinfo.sha256, task.service_name, error_key)
        else:
            process_table.fail_nonrecoverable(task.fileinfo.sha256, task.service_name, error_key)

        # Send a message to prompt the re-issue of the task if needed
        self.file_queue.push(FileTask(dict(
            sid=task.sid,
            file_info=task.fileinfo,
            depth=task.depth,
            max_files=task.max_files
        )).as_primitives())

        # Send the result key to any watching systems
        msg = {'status': 'FAIL', 'cache_key': error_key}
        for w in self._get_watcher_list(task.sid).members():
            NamedQueue(w).push(msg)

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
