"""
An interface to the core system for the edge services.


"""
import uuid
import logging

from assemblyline.common import forge
from assemblyline.odm.models.submission import Submission
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.named import NamedQueue

from al_core.dispatching.dispatcher import ServiceTask, FileTask, SUBMISSION_QUEUE
from al_core.dispatching.dispatch_hash import DispatchHash


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

    def dispatch_submission(self, submission: Submission):
        """Insert a submission into the dispatching system.

        Note:
            You probably actually want to use the SubmissionTool

        Prerequsits:
            - submission should already be saved in the datastore
            - files should already be in the datastore and filestore
        """
        self.submission_queue.push(submission.json())

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
