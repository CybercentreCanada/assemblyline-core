"""
The dispatch hash is a table of services that should be running (or should soon)
and those that are already finished.


# TODO dropped hash could be done in dispatch table
"""
import time
import json
import collections
from typing import Union, List, Tuple, Dict

from redis import StrictRedis, Redis

from assemblyline.remote.datatypes import retry_call
from assemblyline.remote.datatypes.hash import ExpiringHash
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
        self.schedules = ExpiringHash(f'dispatch-hash-schedules-{sid}', host=self.client)
        self._files = ExpiringSet(f'dispatch-hash-files-{sid}', host=self.client)
        self._cached_files = set(self._files.members())
        # TODO set these expire times from the global time limit for submissions
        retry_call(self.client.expire, self._dispatch_key, 60*60)
        retry_call(self.client.expire, self._finish_key, 60*60)

    def add_file(self, file_hash: str, file_limit):
        # If it was already in the set, we don't need to check remotely
        if file_hash in self._cached_files:
            return True

        # If the set is already full, and its not in the set, then we don't need to check remotely
        if len(self._cached_files) >= file_limit:
            return False

        # Our local checks are unclear, check remotely
        result = self._files.limited_add(file_hash, file_limit)

        # If it was added, add it to the local cache so we don't need to check again
        if result:
            self._cached_files.add(file_hash)
        return result

    def dispatch(self, file_hash: str, service: str):
        """Mark that a service has been dispatched for the given sha."""
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
        """A service task has failed, but should be retried, clear that it has been dispatched."""
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
        return self.finished_count() > 0 and self.dispatch_count() == 0

    def all_results(self) -> Dict[str, Dict[str, DispatchRow]]:
        """Get all the records stored in the dispatch table.

        :return: outpu[file_hash][service_name] -> DispatchRow
        """
        rows = retry_call(self.client.hgetall, self._finish_key)
        output = {}
        for key, status in rows.items():
            file_hash, service = key.split(b'-', maxsplit=1)
            if file_hash not in output:
                output[file_hash] = {}
            output[file_hash][service] = DispatchRow(*json.loads(status))
        return output

    def delete(self):
        """Clear the tables from the redis server."""
        retry_call(self.client.delete, self._dispatch_key)
        retry_call(self.client.delete, self._finish_key)
        self.schedules.delete()
