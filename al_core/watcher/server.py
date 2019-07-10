import elasticapm
import time

from assemblyline.common import forge
from assemblyline.common.metrics import MetricsFactory
from assemblyline.odm.messages.watcher_heartbeat import Metrics
from assemblyline.remote.datatypes import get_client, retry_call
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
        self.counter = MetricsFactory(metrics_type='watcher', schema=Metrics, name='watcher',
                                      redis=self.redis, config=config)

        if config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=config.core.metrics.apm_server.server_url,
                                                service_name="watcher")
        else:
            self.apm_client = None

    def try_run(self):
        counter = self.counter
        apm_client = self.apm_client

        while self.running:
            # Download all messages from the queue that have expired
            seconds, _ = retry_call(self.redis.time)
            messages = self.queue.dequeue_range(0, seconds)

            cpu_mark = time.process_time()
            time_mark = time.time()

            # Try to pass on all the messages to their intended recipient, try not to let
            # the failure of one message from preventing the others from going through
            for key in messages:
                # Start of transaction
                if apm_client:
                    apm_client.begin_transaction('process_messages')

                message = self.hash.pop(key)
                if message:
                    try:
                        queue = NamedQueue(message['queue'], self.redis)
                        queue.push(message['message'])
                        self.counter.increment('expired')

                        # End of transaction (success)
                        if apm_client:
                            apm_client.end_transaction('watch_message', 'success')
                    except Exception as error:
                        # End of transaction (exception)
                        if apm_client:
                            apm_client.end_transaction('watch_message', 'error')

                        self.log.exception(error)
                else:
                    # End of transaction (duplicate)
                    if apm_client:
                        apm_client.end_transaction('watch_message', 'duplicate')

                    self.log.warning(f'Handled watch twice: {key} {len(key)} {type(key)}')

            counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            counter.increment_execution_time('busy_seconds', time.time() - time_mark)

            if not messages:
                time.sleep(0.1)

