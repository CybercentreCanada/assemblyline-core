"""
A server used to request a message sent or service task canceled at a later date.

The request can later be rescinded.

This can be used for things like:
 - asynchronous timeouts when there is no single component instance that is guaranteed
    to handle both halves of the operation.
 -
"""

import enum
from assemblyline.common import forge

from assemblyline.remote.datatypes.queues.priority import UniquePriorityQueue
from assemblyline.remote.datatypes.hash import ExpiringHash
from assemblyline.remote.datatypes import retry_call, get_client

WATCHER_QUEUE = 'global-watcher-queue'
WATCHER_HASH = 'global-watcher-hash'
MAX_TIMEOUT = 60*60*48


class WatcherAction(enum.IntEnum):
    Message = 0
    TimeoutTask = 1


class WatcherClient:
    def __init__(self, redis_persist=None):
        config = forge.get_config()

        self.redis = redis_persist or get_client(
            host=config.core.redis.persistent.host,
            port=config.core.redis.persistent.port,
            private=False,
        )
        self.hash = ExpiringHash(name=WATCHER_HASH, ttl=MAX_TIMEOUT, host=redis_persist)
        self.queue = UniquePriorityQueue(WATCHER_QUEUE, redis_persist)

    def touch(self, timeout: int, key: str, queue: str, message: dict):
        if timeout >= MAX_TIMEOUT:
            raise ValueError(f"Can't set watcher timeouts over {MAX_TIMEOUT}")
        self.hash.set(key, {'action': WatcherAction.Message, 'queue': queue, 'message': message})
        seconds, _ = retry_call(self.redis.time)
        self.queue.push(int(seconds + timeout), key)

    def touch_task(self, timeout: int, key: str, worker: str, task_key: str):
        if timeout >= MAX_TIMEOUT:
            raise ValueError(f"Can't set watcher timeouts over {MAX_TIMEOUT}")
        self.hash.set(key, {'action': WatcherAction.TimeoutTask, 'worker': worker, 'task_key': task_key})
        seconds, _ = retry_call(self.redis.time)
        self.queue.push(int(seconds + timeout), key)

    def clear(self, key: str):
        self.queue.remove(key)
        self.hash.pop(key)

