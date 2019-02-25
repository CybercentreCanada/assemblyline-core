import json

from assemblyline.odm.models.submission import Submission

from al_core.dispatching.dispatcher import Dispatcher
from al_core.server_base import ServerBase


class SubmissionDispatchServer(ServerBase):
    def __init__(self, datastore=None, redis=None, redis_presist=None, logger=None):
        super().__init__('assemblyline.dispatcher.submissions', logger)
        self.dispatcher = Dispatcher(logger=self.log, redis=redis, redis_persist=redis_presist, datastore=datastore)

    def try_run(self):

        queue = self.dispatcher.submission_queue
        submissions = self.dispatcher.submissions

        while self.running:
            try:
                message = queue.pop(timeout=1)
                if not message:
                    continue

                message = json.loads(message)
                try:
                    sub = Submission(message)
                except ValueError:
                    sub = submissions.get(message.get('sid'))

                if not sub:
                    self.log.error(f"Tried to dispatch submission missing from datastore: {message['sid']}")
                    continue

                self.dispatcher.dispatch_submission(sub)
            except Exception as error:
                self.log.exception(error)

    def stop(self):
        self.dispatcher.submission_queue.push(None)
        super().stop()


if __name__ == '__main__':
    from assemblyline.common import log
    log.init_logging()
    SubmissionDispatchServer().serve_forever()