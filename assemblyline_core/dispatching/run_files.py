
import elasticapm
import time

from assemblyline.common import forge
from assemblyline_core.dispatching.dispatcher import Dispatcher, FileTask
from assemblyline_core.server_base import ServerBase

from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.models.config import Config


class FileDispatchServer(ServerBase):
    def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None):
        super().__init__('assemblyline.dispatcher.file', logger)
        
        config: Config = forge.get_config()
        datastore: AssemblylineDatastore = datastore or forge.get_datastore(config)
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
        cpu_mark = time.process_time()
        time_mark = time.time()

        while self.running:
            try:
                self.heartbeat()
                self.dispatcher.counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
                self.dispatcher.counter.increment_execution_time('busy_seconds', time.time() - time_mark)

                message = queue.pop(timeout=1)

                cpu_mark = time.process_time()
                time_mark = time.time()

                if not message:
                    continue
                
                # Start of process dispatcher transaction
                if self.apm_client:
                    self.apm_client.begin_transaction('Process dispatcher message')

                if 'service_timeout' in message:
                    continue

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
