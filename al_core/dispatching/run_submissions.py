"""
A dispatcher server that ensures all of the files in a submission are complete.
"""
import json

from assemblyline.odm.models.submission import Submission
from assemblyline.common import forge

from al_core.dispatching.dispatcher import Dispatcher, SubmissionTask
from al_core.server_base import ServerBase


class SubmissionDispatchServer(ServerBase):
    def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None):
        super().__init__('assemblyline.dispatcher.submissions', logger)
        datastore = datastore or forge.get_datastore()
        self.dispatcher = Dispatcher(logger=self.log, redis=redis, redis_persist=redis_persist, datastore=datastore)

    def try_run(self):
        self.log.info("starting")
        queue = self.dispatcher.submission_queue

        while self.running:
            try:
                message = queue.pop(timeout=1)
                if not message:
                    continue

                # This is probably a complete task
                if 'submission' in message:
                    task = SubmissionTask(message)

                # This is just as sid nudge, this submission should already be running
                elif 'sid' in message:
                    active_task = self.dispatcher.active_tasks.get(message['sid'])
                    if active_task is None:
                        continue

                    task = SubmissionTask(active_task)

                else:
                    self.log.error(f'Corrupted submission message in dispatcher {message}')
                    continue

                self.dispatcher.dispatch_submission(task)
            except Exception as error:
                self.log.exception(error)

        self.log.info("stopped")

    def stop(self):
        self.dispatcher.submission_queue.push(None)
        super().stop()


if __name__ == '__main__':
    SubmissionDispatchServer().serve_forever()