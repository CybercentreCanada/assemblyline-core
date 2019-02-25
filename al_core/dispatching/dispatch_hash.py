"""
The dispatch hash is a table of services that should be running (or should soon)
and those that are already finished.


# TODO dropped hash could be done in dispatch table
"""
import time
import json
import collections
from typing import Union, Tuple, List

from redis import StrictRedis, Redis


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

class DispatchRow(collections.namedtuple('DispatchRow', ['bucket', 'key', 'score', 'drop'])):
    @property
    def is_error(self):
        return self.bucket == 'error'


class DispatchHash:
    def __init__(self, sid: str, client: Union[Redis, StrictRedis]):
        self.client = client
        self.sid = sid
        self._dispatch_key = f'{sid}{dispatch_tail}'
        self._finish_key = f'{sid}{finished_tail}'
        self._finish = self.client.register_script(finish_script)
        # TODO set these expire times from the global time limit for submissions
        retry_call(self.client.expire, self._dispatch_key, 60*60)
        retry_call(self.client.expire, self._finish_key, 60*60)

    def dispatch(self, file_hash: str, service: str):
        """Mark that a service has been dispatched for the given sha."""
        # TODO use a script to get the time function in redis?
        retry_call(self.client.hset, self._dispatch_key, f"{file_hash}-{service}", time.time())

    def dispatch_count(self):
        """How many tasks have been dispatched for this submission."""
        return retry_call(self.client.hlen, self._dispatch_key)

    def dispatch_time(self, file_hash: str, service: str) -> float:
        """When was dispatch called for this sha/service pair."""
        result = retry_call(self.client.hget, self._dispatch_key, f"{file_hash}-{service}")
        if result is None:
            return 0
        return float(result)

    def fail_recoverable(self, file_hash: str, service: str):
        """A service task has failed, but should be retried, remove the dispatched task."""
        retry_call(self.client.hdel, self._dispatch_key, f"{file_hash}-{service}")

    def fail_nonrecoverable(self, file_hash: str, service, error_key) -> int:
        """A service task has failed and should not be retried, entry the error as the result.

        Has exactly the same semantics as `finish` but for errors.
        """
        return retry_call(self._finish, args=[self.sid, f"{file_hash}-{service}", json.dumps(['error', error_key, 0, False])])

    def finish(self, file_hash, service, result_key, score, drop=False) -> int:
        """
        As a single transaction:
         - Remove the service from the dispatched list
         - Add the file to the finished list, with the given result key
         - return the number of items in the dispatched list
        """
        return retry_call(self._finish, args=[self.sid, f"{file_hash}-{service}", json.dumps(['result', result_key, score, drop])])

    def finished_count(self) -> int:
        """How many tasks have been finished for this submission."""
        return retry_call(self.client.hlen, self._finish_key)

    def finished(self, file_hash, service) -> Union[DispatchRow, None]:
        """If a service has been finished, return the key of the result document."""
        result = retry_call(self.client.hget, self._finish_key, f"{file_hash}-{service}")
        if result:
            return DispatchRow(*json.loads(result))
        return None

    def all_finished(self) -> bool:
        """Are there no outstanding tasks, and at least one finished task."""
        if retry_call(self.client.hlen, self._finish_key) == 0:
            return False
        return self.dispatch_count() == 0

    def all_results(self) -> List[DispatchRow]:
        rows = retry_call(self.client.hgetall, self._finish_key)
        return [DispatchRow(*json.loads(item)) for item in rows.values()]

    def delete(self):
        """Clear the tables from the redis server."""
        retry_call(self.client.delete, self._dispatch_key)
        retry_call(self.client.delete, self._finish_key)
