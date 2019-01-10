"""
An interface to the core system for the edge services.


"""
import uuid

from dispatching.dispatcher import ServiceTask, FileTask, Result, DispatchHash

class ServiceDispatchClient:

    def request_work(self, service_name):
        raise NotImplementedError()

    def service_finished(self, task: ServiceTask, result: Result):
        raise NotImplementedError()

    def service_failed(self, task: ServiceTask, error=None):
        # Add an error to the datastore
        if error:
            self.errors.save(uuid.guid4().hex, error)
        else:
            self.errors.save(uuid.guid4().hex, create_generic_error(task))

        # Mark the attempt to process the file over in the dispatch table
        process_table = DispatchHash(task.sid, *self.redis)
        process_table.fail_dispatch(task.file_hash, task.service_name)

        # Send a message to prompt the re-issue of the task if needed
        self.file_queue.push(FileTask(dict(
            sid=task.sid,
            file_hash=task.file_hash,
            file_type=task.file_type,
            depth=task.depth,
        )))
