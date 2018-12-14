import threading
import json
from dispatcher import Dispatcher, FileTask


class FileDispatchServer(threading.Thread):
    def __init__(self, logger, redis, datastore):
        self.running = False
        self.dispatcher = Dispatcher(logger=logger, redis=redis, datastore=datastore)
        self.logger = logger

    def start(self):
        self.running = True
        super().start()

    def run(self):

        queue = self.dispatcher.file_queue

        while self.running:
            try:
                message = FileTask(json.loads(queue.pop()))
                self.dispatcher.dispatch_file(message)
            except Exception as error:
                self.logger.error(error)

    def serve_forever(self):
        self.start()
        self.join()


if __name__ == '__main__':

    from assemlyline.common import log
    log.init_logging()
    logger = log.logging.getLogger('assemblyline.dispatcher.file')

    server = FileDispatchServer(logger)
    server.serve_forever()
