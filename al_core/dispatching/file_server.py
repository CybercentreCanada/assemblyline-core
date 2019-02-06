import threading
import logging
import json
from dispatcher import Dispatcher, FileTask


class FileDispatchServer(threading.Thread):
    def __init__(self, redis, datastore, logger=None):
        super().__init__()
        self.running = False
        self.logger = logger if logger else logging.getLogger('assemblyline.dispatcher.file')
        self.dispatcher = Dispatcher(redis=redis, datastore=datastore, logger=self.logger)

    def start(self):
        self.running = True
        super().start()

    def run(self):

        queue = self.dispatcher.file_queue

        while self.running:
            try:
                message = queue.pop(timeout=1)
                if not message:
                    continue

                message = FileTask(json.loads(message))
                self.dispatcher.dispatch_file(message)
            except Exception as error:
                self.logger.exception(error)
                break

    def stop(self):
        self.running = False
        self.dispatcher.file_queue.push(None)

    def serve_forever(self):
        self.start()
        self.join()


if __name__ == '__main__':

    from assemlyline.common import log
    log.init_logging()

    server = FileDispatchServer()
    server.serve_forever()
