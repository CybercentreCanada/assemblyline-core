import json

from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.datastore import odm
from configuration import ConfigManager
import watcher


# @odm.model()
# class Task(odm.Model):
#


DISPATCH_QUEUE = 'dispatch-file'
SUBMISSION_QUEUE = 'submission'


class SubmissionDispatcher:

    def __init__(self, datastore, redis):
        self.ds = datastore
        self.submissions = datastore.submissions
        self.config = ConfigManager(datastore)
        self.redis = redis
        self.running = True
        self.submission_queue = NamedQueue(SUBMISSION_QUEUE, *redis)
        self.file_dispatch = NamedQueue(SUBMISSION_QUEUE, *redis)
        self.timeout_seconds = 30 * 60

    def handle_submission(self, message):
        # Resolve it to submission object
        message = json.loads(message)
        sid = message['sid']
        if 'selected_services' not in message:
            submission = self.submissions.get(sid)
        else:
            submission = Submission(message)
            self.submissions.save(sid, submission)

        # Refresh the watch
        watcher.touch(self.redis, timeout=self.timeout_seconds, queue=SUBMISSION_QUEUE, message={'sid': sid})
        self.process_submission(submission)

    def process_submission(self, submission):
        # Try to find all files, and extracted files
        unchecked_files = list(submission.files)
        pending_files = []

        # For each file, we will look through all its results, any exctracted files
        # found 
        while unchecked_files:
            sha = unchecked_files.pop()
            file_type = self.files.get(sha).type
            schedule = self.config.build_schedule(submission, file_type)

            for service_name in reduce(lambda a, b: a + b, schedule):
