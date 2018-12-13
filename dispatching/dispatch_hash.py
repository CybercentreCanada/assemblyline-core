"""
The dispatch hash table holds two lists of (file, service) pairs.

One table holds tasks not yet finished, the second holds all the finished tasks.
The values in the hash tables are the details of the task to be run.

The dispatch hash holds all the tasks that need to be run, regardless of schedule ordering.
"""
import time
from assemblyline.remote.datatypes import retry_call, get_client

dispatch_tail = '-dispatch'
finished_tail = '-finished'

finish_script = f"""
local sid = ARGS[0]
local key = ARGS[1]
local result_key = ARGS[2]

redis.call('hdel', sid .. '{dispatch_tail}', key)
redis.call('hset', sid .. '{finished_tail}', key, result_key)
return redis.call('hlen', sid .. '{dispatch_tail}')
"""


class DispatchHash:
    def __init__(self, redis, sid):
        self.client = get_client(*redis)
        self.sid = sid
        self._dispatch_key = f'{sid}{dispatch_tail}'
        self._finish_key = f'{sid}{finished_tail}'
        self._finish = self.client.register_script(finish_script)

    def dispatch(self, file_hash, service):
        # TODO use a script to get the time function in redis?
        retry_call(self.client.hset, self._dispatch_key, f"{file_hash}-{service}", time.time())

    def dispatch_time(self, file_hash, service):
        result = retry_call(self.client.hget, self._dispatch_key, f"{file_hash}-{service}")
        if result is None:
            return 0
        return result

    def fail_dispatch(self, file_hash, service):
        retry_call(self.client.hset, self._dispatch_key, f"{file_hash}-{service}", 0)

    def finish(self, service, file_hash, result_key):
        """
        As a single transaction:
         - Remove the service from the dispatched list
         - Add the file to the finished list, with the given result key
         - return the number of items in the dispatched list
        """
        return retry_call(self._finish, self.sid, f"{file_hash}-{service}", result_key)

    def finished(self, file_hash, service):
        return retry_call(self.client.hget, self._finish_key, f"{file_hash}-{service}")

    def all_finished(self):
        if retry_call(self.client.hlen, self._finish_key) == 0:
            return False
        return retry_call(self.client.hlen, self._dispatch_key) == 0

    def delete(self):
        retry_call(self.client.del, self._dispatch_key)
        retry_call(self.client.del, self._finish_key)
