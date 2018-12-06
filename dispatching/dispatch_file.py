import json

from assemblyline.remote.datatypes.queues.named import NamedQueue
from dispatch_hash import DispatchHash
from assemblyline.datastore import odm
from configuration import ConfigManager
import watcher


@odm.model()
class Task(odm.Model):
    sid = odm.Keyword()
    file_hash = odm.Keyword()
    file_type = odm.Keyword()
    depth = odm.Integer()


DISPATCH_QUEUE = 'dispatch-file'
SUBMISSION_QUEUE = 'submission'


class FileDispatcher:

    def __init__(self, datastore, redis):
        self.ds = datastore
        self.submissions = datastore.submissions
        self.config = ConfigManager(datastore)
        self.redis = redis
        self.running = True
        self.submission_queue = NamedQueue(SUBMISSION_QUEUE, *redis)
        self.file_dispatch = NamedQueue(SUBMISSION_QUEUE, *redis)
        self.timeout_seconds = 30 * 60

    def handle(self, message):
        """ Handle a message describing a file to be processed.

        This file may be:
            - A new submission or extracted file.
            - A file that has just completed a stage of processing.
            - A file that has not completed a a stage of processing, but this
              call has been triggered by a timeout or similar.

        If the file is totally new, we will setup a dispatch table, and fill it in.

        Once we make/load a dispatch table, we will dispatch whichever group the table
        shows us hasn't been completed yet.

        When we dispatch to a service, we check if the task is already in the dispatch
        queue. If it isn't proceed normally. If it is, check that the service is still online.
        """
        # Read the message content
        task = Task(json.loads(message))
        file_hash = task.file_hash
        submission = self.submissions.get(task.sid)

        # Refresh the watch on the submission, we are still working on it
        watcher.touch(self.redis, key=task.sid, timeout=self.timeout_seconds, queue=SUBMISSION_QUEUE, message={'sid': task.sid})

        # Open up the file/service table for this submission
        process_table = DispatchHash(self.redis, submission.id)

        # Calculate the schedule for the file
        schedule = self.config.build_schedule(submission, task.file_type)

        # Go through each round of the schedule removing complete/failed services
        # Break when we find a stage that still needs processing
        outstanding = []
        while schedule and not outstanding:
            stage = schedule.pop(0)

            for service in stage:
                # If the result is in the process table we are fine
                if process_table.finished(service):
                    continue

                # Check if something, an error/a result already exists, to resolve this service
                access_key = self.process_key(submission, file_hash, service)
                if access_key:
                    process_table.finish(service, file_hash, access_key)
                    continue

                outstanding.append(service)

        # Try to retry/dispatch any outstanding services
        if outstanding:
            self.dispatch(submission, file_hash, service)

        else:
            # There are no outstanding services, this file is done
            self.finish_file(submission, file_hash)
