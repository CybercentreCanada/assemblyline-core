from assemblyline.remote.datatypes.queues.priority import PriorityQueue
from assemblyline.remote.datatypes import retry_call
from assemblyline.remote.datatypes.hash import ExpiringHash

import redis
import time
import logging
import json

WATCHER_QUEUE = 'global-watcher-queue'
WATCHER_HASH = 'global-watcher-hash'


class WatchQueue(object):
    def __init__(self, name, client):
        self.client = client
        self.name = name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.delete()

    def count(self, lowest, highest):
        return retry_call(self.client.zcount, self.name, lowest, highest)

    def delete(self):
        retry_call(self.client.delete, self.name)

    def length(self):
        return retry_call(self.client.zcard, self.name)

    def pop_range(self, lowest, highest):
        return retry_call(self.client.zremrangebyscore, self.name, lowest, highest)

    def push_unique(self, priority, key):
        return bool(retry_call(self.client.zadd, 'NX', priority, key))

    def push(self, priority, key):
        return bool(retry_call(self.client.zadd, priority, key))


MAX_TIMEOUT = 60*60*48


def touch(redis_connection: redis.Redis, timeout: int, key: str, queue: str, message: str):
    if timeout >= MAX_TIMEOUT:
        raise ValueError(f"Can't set watcher timeouts over {MAX_TIMEOUT}")

    hash = ExpiringHash(WATCHER_HASH, MAX_TIMEOUT, redis_connection)
    hash.add(key, json.dumps({'queue': queue, 'message': message}))

    queue = PriorityQueue(WATCHER_QUEUE, redis_connection)
    queue.push(time.time() + timeout, key)


class WatcherServer:
    def __init__(self, redis_connection):
        self.redis_connection = redis_connection
        self.hash = ExpiringHash(WATCHER_HASH, *redis_connection)
        self.queue = WatchQueue(WATCHER_QUEUE, *redis_connection)
        self.running = True

    def stop(self):
        self.running = False
        return False

    def handle(self, message):
        try:
            message = json.loads(message)
            queue = PriorityQueue(message['queue'], *self.redis_connection)
            queue.push(message['message'])

        except Exception as error:
            logging.error(error)

    def serve(self):
        while self.running:
            try:
                for key in self.queue.pop_range(0, time.time()):
                    message = self.hash.get(key)
                    self.hash.delete(key)
                    self.handle(message)
            except Exception as error:
                logging.error(error)
