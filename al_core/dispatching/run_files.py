import logging

from assemblyline.common import forge
from al_core.dispatching.dispatcher import Dispatcher, FileTask
from al_core.server_base import ServerBase


class FileDispatchServer(ServerBase):
    def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None):
        log_level = logging.DEBUG if forge.get_config().core.dispatcher.debug_logging else logging.INFO
        super().__init__('assemblyline.dispatcher.file', logger, log_level=log_level)

        datastore = datastore or forge.get_datastore()
        self.dispatcher = Dispatcher(redis=redis, redis_persist=redis_persist, datastore=datastore, logger=self.log)

    def try_run(self):
        queue = self.dispatcher.file_queue

        while self.running:
            try:
                message = queue.pop(timeout=1)
                if not message:
                    continue

                message = FileTask(message)
                self.dispatcher.dispatch_file(message)
            except Exception as error:
                self.log.exception(error)

    def stop(self):
        self.dispatcher.file_queue.push(None)
        super().stop()


if __name__ == '__main__':
    FileDispatchServer().serve_forever()
