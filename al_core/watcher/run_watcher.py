import elasticapm
import time

from assemblyline.common import forge
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.queues.priority import UniquePriorityQueue
from assemblyline.remote.datatypes.hash import ExpiringHash

from al_core.server_base import ServerBase
from al_core.watcher.client import WATCHER_HASH, WATCHER_QUEUE, MAX_TIMEOUT


class WatcherServer(ServerBase):
    def __init__(self, redis=None):
        super().__init__('assemblyline.watcher')
        config = forge.get_config()

        self.redis = redis or get_client(
            db=config.core.redis.nonpersistent.db,
            host=config.core.redis.nonpersistent.host,
            port=config.core.redis.nonpersistent.port,
            private=False,
        )
        self.hash = ExpiringHash(name=WATCHER_HASH, ttl=MAX_TIMEOUT, host=redis)
        self.queue = UniquePriorityQueue(WATCHER_QUEUE, redis)

        if config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=config.core.metrics.apm_server.server_url,
                                                service_name="watcher")
        else:
            self.apm_client = None

    def try_run(self):
        while self.running:
            seconds, _ = self.redis.time()
            messages = self.queue.dequeue_range(0, seconds)
            for key in messages:
                # Start of transaction
                if self.apm_client:
                    self.apm_client.begin_transaction('process_messages')

                message = self.hash.pop(key)
                if message:
                    try:
                        queue = NamedQueue(message['queue'], self.redis)
                        queue.push(message['message'])

                        # End of transaction (success)
                        if self.apm_client:
                            self.apm_client.end_transaction('watch_message', 'success')
                    except Exception as error:
                        # End of transaction (exception)
                        if self.apm_client:
                            self.apm_client.end_transaction('watch_message', 'error')

                        self.log.exception(error)
                else:
                    # End of transaction (duplicate)
                    if self.apm_client:
                        self.apm_client.end_transaction('watch_message', 'duplicate')

                    self.log.warning(f'Handled watch twice: {key} {len(key)} {type(key)}')

            if not messages:
                time.sleep(0.1)


if __name__ == "__main__":
    with WatcherServer() as watch:
        watch.serve_forever()
