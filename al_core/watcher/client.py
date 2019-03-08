"""
A server used to request a message be sent to a given queue at a later date.

The message request can later be rescinded.

This can be used for things like:
 - asynchronous timeouts when there is no single component instance that is guaranteed
    to handle both halves of the operation.
 -
"""

from assemblyline.remote.datatypes.queues.priority import UniquePriorityQueue
from assemblyline.remote.datatypes.hash import ExpiringHash

WATCHER_QUEUE = 'global-watcher-queue'
WATCHER_HASH = 'global-watcher-hash'
MAX_TIMEOUT = 60*60*48


class WatcherClient:
    def __init__(self, redis):
        self.redis = redis
        self.hash = ExpiringHash(name=WATCHER_HASH, ttl=MAX_TIMEOUT, host=redis)
        self.queue = UniquePriorityQueue(WATCHER_QUEUE, redis)

    def touch(self, timeout: int, key: str, queue: str, message: object):
        if timeout >= MAX_TIMEOUT:
            raise ValueError(f"Can't set watcher timeouts over {MAX_TIMEOUT}")
        self.hash.set(key, {'queue': queue, 'message': message})
        seconds, _ = self.redis.time()
        self.queue.push(int(seconds + timeout), key)
        assert self.hash.exists(key)

    def clear(self, key: str):
        self.queue.remove(key)
        self.hash.pop(key)

