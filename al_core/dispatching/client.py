"""
An interface to the core system for the edge services.


"""
import uuid
import logging
from typing import Dict

from assemblyline.common import forge
from assemblyline.odm.messages.dispatching import WatchQueueMessage
from assemblyline.odm.models.submission import Submission
from assemblyline.remote.datatypes import get_client, reply_queue_name
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.hash import ExpiringHash

from al_core.dispatching.dispatcher import SubmissionTask, ServiceTask, FileTask, SUBMISSION_QUEUE, DISPATCH_TASK_HASH
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
        )).json())

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

    def service_finished(self, task: ServiceTask, result):
        # Store the result object and mark the service finished in the global table
        self.results.save(task.result_key, result)
        process_table = DispatchHash(task.sid, *self.redis)
        remaining = process_table.finish(task.sha256, task.service, task.result_key, drop=result.drop_file)

        if files.pop(srl, None):
            dispatcher.completed[sid][srl] = entry.task.classification

        # Add the extracted files
        for extracted_data in result.response.extracted:
            file_data = self.files.get(extracted_data.sha256)
            self.file_queue.push(FileTask(dict(
                sid=task.sid,
                sha256=extracted_data.sha256,
                file_type=file_data.type,
                depth=task.depth+1,
            )))

        # If the global table said that this was the last outstanding service,
        # send a message to the dispatchers.
        if remaining == 0:
            self.file_queue.push(FileTask(dict(
                sid=task.sid,
                sha256=task.sha256,
                file_type=task.file_type,
                depth=task.depth,
            )))


        cache_key = task.cache_key
        if cache_key:
            msg = {'status': status[:4], 'cache_key': cache_key}
            for w in self.watchers.get(sid, {}).itervalues():
                w.push(msg)

    def service_failed(self, task: ServiceTask, error=None):
        # Add an error to the datastore
        if error:
            self.errors.save(uuid.uuid4().hex, error)
        else:
            self.errors.save(uuid.uuid4().hex, create_generic_error(task))

        # Mark the attempt to process the file over in the dispatch table
        process_table = DispatchHash(task.sid, *self.redis)
        process_table.fail_dispatch(task.sha256, task.service_name)

        # Send a message to prompt the re-issue of the task if needed
        self.file_queue.push(FileTask(dict(
            sid=task.sid,
            sha256=task.sha256,
            file_type=task.file_type,
            depth=task.depth,
        )))

        cache_key = task.cache_key
        if cache_key:
            msg = {'status': status[:4], 'cache_key': cache_key}
            for w in self.watchers.get(sid, {}).itervalues():
                w.push(msg)

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

        # TODO: Add the newly created queue to the list of queues for the given submission

        # TODO: Push all current keys to the newly created queue (Queue should have a TTL of about 30 sec to 1 minute)

        # FAKE DATA
        # TODO: remove fake data when done
        watch_queue.push(WatchQueueMessage({"status": "START"}))
        sub = self.ds.submission.get(sid, as_obj=False) or {}
        for key in sub.get('results', []):
            watch_queue.push(WatchQueueMessage({"status": "OK", "cache_key": key}))
        for key in sub.get('errors', []):
            watch_queue.push(WatchQueueMessage({"status": "FAIL", "cache_key": key}))
        # END OF FAKE DATA

        return queue_name

    def _get_watcher_list(self, sid):
        return ExpiringSet(make_watcher_list_name(sid), host=self.redis).members()