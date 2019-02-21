"""
A server used to request a message be sent to a given queue at a later date.

The message request can later be rescinded.

This can be used for things like:
 - asynchronous timeouts when there is no single component instance that is guaranteed
    to handle both halves of the operation.
 -
"""

from assemblyline.remote.datatypes.queues.priority import UniquePriorityQueue
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.hash import ExpiringHash

import time
import logging
import json

WATCHER_QUEUE = 'global-watcher-queue'
WATCHER_HASH = 'global-watcher-hash'
MAX_TIMEOUT = 60*60*48


class WatcherClient:
    def __init__(self, redis):
        self.redis = redis
        self.hash = ExpiringHash(WATCHER_HASH, MAX_TIMEOUT, redis)
        self.queue = UniquePriorityQueue(WATCHER_QUEUE, redis)

    def touch(self, timeout: int, key: str, queue: str, message: str):
        if timeout >= MAX_TIMEOUT:
            raise ValueError(f"Can't set watcher timeouts over {MAX_TIMEOUT}")

        self.hash.add(key, json.dumps({'queue': queue, 'message': message}))
        self.queue.push(int(time.time() + timeout), key)

    def clear(self, key: str):
        self.queue.remove(key)
        self.hash.pop(key)


class WatcherServer:
    def __init__(self, redis_connection):
        self.redis_connection = redis_connection
        self.hash = ExpiringHash(WATCHER_HASH, *redis_connection)
        self.queue = UniquePriorityQueue(WATCHER_QUEUE, *redis_connection)
        self.running = True

    def stop(self):
        self.running = False
        return False

    def handle(self, message):
        try:
            message = json.loads(message)
            queue = NamedQueue(message['queue'], self.redis_connection)
            queue.push(message['message'])

        except Exception as error:
            logging.error(error)

    def serve(self):
        while self.running:
            try:
                for key in self.queue.dequeue_range(0, time.time()):
                    message = self.hash.get(key)
                    self.hash.pop(key)
                    self.handle(message)
            except Exception as error:
                logging.error(error)
