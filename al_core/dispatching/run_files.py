import json

from al_core.dispatching.dispatcher import Dispatcher, FileTask
from al_core.server_base import ServerBase

class FileDispatchServer(ServerBase):
    def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None):
        super().__init__('assemblyline.dispatcher.file', logger)
        self.dispatcher = Dispatcher(redis=redis, redis_persist=redis_persist, datastore=datastore, logger=self.log)

    def try_run(self):
        queue = self.dispatcher.file_queue

        while self.running:
            try:
                message = queue.pop(timeout=1)
                if not message:
                    continue

                message = FileTask(json.loads(message))
                self.dispatcher.dispatch_file(message)
            except Exception as error:
                self.log.exception(error)
                break

    def stop(self):
        self.dispatcher.file_queue.push(None)
        super().stop()


if __name__ == '__main__':
    from assemblyline.common import log
    log.init_logging()
    FileDispatchServer().serve_forever()
