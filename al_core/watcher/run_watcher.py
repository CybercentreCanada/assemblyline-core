import json
import time

from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.queues.priority import UniquePriorityQueue
from assemblyline.remote.datatypes.hash import ExpiringHash

from al_core.server_base import ServerBase
from al_core.watcher import WATCHER_HASH, WATCHER_QUEUE

class WatcherServer(ServerBase):
    def __init__(self, redis_connection):
        super().__init__('assemblyline.watcher')
        self.redis_connection = redis_connection
        self.hash = ExpiringHash(WATCHER_HASH, *redis_connection)
        self.queue = UniquePriorityQueue(WATCHER_QUEUE, *redis_connection)

    def handle(self, message):
        try:
            message = json.loads(message)
            queue = NamedQueue(message['queue'], self.redis_connection)
            queue.push(message['message'])

        except Exception as error:
            self.log.error(error)

    def try_run(self):
        while self.running:
            for key in self.queue.dequeue_range(0, time.time()):
                message = self.hash.get(key)
                self.hash.pop(key)
                self.handle(message)
