import threading
import json
from dispatcher import Dispatcher


class SubmissionDispatchServer(threading.Thread):
    def __init__(self, logger):
        self.running = False
        self.dispatcher = Dispatcher()
        self.logger = logger

    def start(self):
        self.running = True
        super().start()

    def run(self):

        queue = self.dispatcher.submission_queue
        submissions = self.dispatcher.submissions

        while self.running:
            try:
                message = json.loads(queue.pop())
                sub = submissions.get(message['sid'])
                self.dispatcher.dispatch_submission(sub)
            except Exception as error:
                self.logger.error(error)

    def serve_forever(self):
        self.start()
        self.join()


if __name__ == '__main__':

    from assemlyline.common import log
    log.init_logging()
    logger = log.logging.getLogger('assemblyline.dispatcher.submissions')

    server = SubmissionDispatchServer(logger)
    server.serve_forever()
