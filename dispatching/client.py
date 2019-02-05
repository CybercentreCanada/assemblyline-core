"""
An interface to the core system for the edge services.


"""
import uuid
import logging

from dispatching.dispatcher import ServiceTask, FileTask
from dispatching.dispatcher import Submission, DispatchHash
from .configuration import Scheduler
from assemblyline.common import forge


class DispatchClient:
    def __init__(self, datastore, redis, logger=None):
        self.redis = redis
        self.ds = datastore
        self.log = logger or logging.getLogger("assemblyline.dispatching.client")
        self.results = datastore.result
        self.errors = datastore.error
        self.files = datastore.file

        # Create a config cache that will refresh config values periodically
        self.config = forge.CachedObject(forge.get_config)
        # self.scheduler = Scheduler(datastore, self.config)

    def dispatch_submission(self, submission: Submission):
        """Insert a submission into the dispatching system.

        Note:
            You probably actually want to use the SubmissionTool

        Prerequsits:
            - submission should already be saved in the datastore
            - files should already be in the datastore and filestore
        """
        raise NotImplementedError()

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
            self.errors.save(uuid.guid4().hex, error)
        else:
            self.errors.save(uuid.guid4().hex, create_generic_error(task))

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
