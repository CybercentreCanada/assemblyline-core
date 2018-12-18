import threading
import json
import logging
from dispatcher import Dispatcher


class SubmissionDispatchServer(threading.Thread):
    def __init__(self, redis, datastore, logger=None):
        super().__init__()
        self.running = False
        self.logger = logger if logger else logging.getLogger('assemblyline.dispatcher.submissions')
        self.dispatcher = Dispatcher(logger=self.logger, redis=redis, datastore=datastore)

    def start(self):
        self.running = True
        super().start()

    def run(self):

        queue = self.dispatcher.submission_queue
        submissions = self.dispatcher.submissions

        while self.running:
            try:
                message = queue.pop(timeout=1)
                if not message:
                    continue

                message = json.loads(message)
                sub = submissions.get(message['sid'])
                if not sub:
                    self.logger.error(f"Tried to dispatch submission missing from datastore: {message['sid']}")
                    continue

                self.dispatcher.dispatch_submission(sub)
            except Exception as error:
                self.logger.exception(error)
                break

    def stop(self):
        self.running = False
        self.dispatcher.submission_queue.push(None)

    def serve_forever(self):
        self.start()
        self.join()


if __name__ == '__main__':

    from assemlyline.common import log
    log.init_logging()

    server = SubmissionDispatchServer()
    server.serve_forever()
