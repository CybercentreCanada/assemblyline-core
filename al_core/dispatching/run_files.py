
import elasticapm
import logging

from assemblyline.common import forge
from al_core.dispatching.dispatcher import Dispatcher, FileTask
from al_core.server_base import ServerBase


class FileDispatchServer(ServerBase):
    def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None):
        log_level = logging.DEBUG if forge.get_config().core.dispatcher.debug_logging else logging.INFO
        super().__init__('assemblyline.dispatcher.file', logger, log_level=log_level)
        
        config = forge.get_config()
        datastore = datastore or forge.get_datastore(config)
        self.dispatcher = Dispatcher(redis=redis, redis_persist=redis_persist, datastore=datastore, logger=self.log)
        
        if config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=config.core.metrics.apm_server.server_url,
                                                service_name="dispatcher")
        else:
            self.apm_client = None
            
    def close(self):
        if self.apm_client:
            elasticapm.uninstrument()

    def try_run(self):
        queue = self.dispatcher.file_queue

        while self.running:
            try:
                message = queue.pop(timeout=1)
                if not message:
                    continue
                
                # Start of process dispatcher transaction
                if self.apm_client:
                    self.apm_client.begin_transaction('Process dispatcher message')

                message = FileTask(message)
                if self.apm_client:
                    elasticapm.tag(sid=message.sid)
                    elasticapm.tag(sha256=message.file_info.sha256)
                    
                self.dispatcher.dispatch_file(message)
                
                # End of process dispatcher transaction (success)
                if self.apm_client:
                    self.apm_client.end_transaction('file_message', 'success')
                    
            except Exception as error:
                self.log.exception(error)
                # End of process dispatcher transaction (success)
                if self.apm_client:
                    self.apm_client.end_transaction('file_message', 'exception')

    def stop(self):
        self.dispatcher.file_queue.push(None)
        super().stop()


if __name__ == '__main__':
    FileDispatchServer().serve_forever()
