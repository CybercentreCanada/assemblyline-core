"""
The dispatch hash is a table of services that should be running (or should soon)
and those that are already finished.
"""
import time
from assemblyline.remote.datatypes import retry_call
from assemblyline.remote.datatypes.set import ExpiringSet

dispatch_tail = '-dispatch'
finished_tail = '-finished'

finish_script = f"""
local sid = ARGV[1]
local key = ARGV[2]
local result_key = ARGV[3]

redis.call('hdel', sid .. '{dispatch_tail}', key)
redis.call('hset', sid .. '{finished_tail}', key, result_key)
return redis.call('hlen', sid .. '{dispatch_tail}')
"""


class DispatchHash:
    def __init__(self, sid, client):
        self.client = client
        self.sid = sid
        self.dropped_files = ExpiringSet(sid, host=self.client)
        self._dispatch_key = f'{sid}{dispatch_tail}'
        self._finish_key = f'{sid}{finished_tail}'
        self._finish = self.client.register_script(finish_script)
        retry_call(self.client.expire, self._dispatch_key, 60*60)
        retry_call(self.client.expire, self._finish_key, 60*60)

    def dispatch(self, file_hash, service):
        """Mark that a service has been dispatched for the given sha."""
        # TODO use a script to get the time function in redis?
        retry_call(self.client.hset, self._dispatch_key, f"{file_hash}-{service}", time.time())

    def dispatch_count(self):
        """How many tasks have been dispatched for this submission."""
        return retry_call(self.client.hlen, self._dispatch_key)

    def dispatch_time(self, file_hash, service) -> float:
        """When was dispatch called for this sha/service pair."""
        result = retry_call(self.client.hget, self._dispatch_key, f"{file_hash}-{service}")
        if result is None:
            return 0
        return float(result)

    def fail_dispatch(self, file_hash, service):
        """Remove a dispatched task, without marking it as finished."""
        retry_call(self.client.hdel, self._dispatch_key, f"{file_hash}-{service}")

    def finish(self, file_hash, service, result_key, drop=False):
        """
        As a single transaction:
         - Remove the service from the dispatched list
         - Add the file to the finished list, with the given result key
         - return the number of items in the dispatched list
        """
        if drop:
            self.dropped_files.add(file_hash + service, True)
        return retry_call(self._finish, args=[self.sid, f"{file_hash}-{service}", result_key])

    def finished_count(self):
        """How many tasks have been finished for this submission."""
        return retry_call(self.client.hlen, self._finish_key)

    def finished(self, file_hash, service):
        """If a service has been finished, return the key of the result document."""
        result = retry_call(self.client.hget, self._finish_key, f"{file_hash}-{service}")
        if result:
            return result.decode()
        return False

    def dropped(self, file_hash, service):
        return self.dropped_files.has(file_hash + service)

    def all_finished(self):
        """Are there no outstanding tasks, and at least one finished task."""
        if retry_call(self.client.hlen, self._finish_key) == 0:
            return False
        return self.dispatch_count() == 0

    def delete(self):
        """Clear the tables from the redis server."""
        retry_call(self.client.delete, self._dispatch_key)
        retry_call(self.client.delete, self._finish_key)
        self.dropped_files.delete()
