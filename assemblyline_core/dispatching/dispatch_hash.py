"""
The dispatch hash is a table of services that should be running (or should soon)
and those that are already finished.
"""
import time
import json
import collections
from typing import Union, Dict, Tuple

from redis import StrictRedis, Redis

from assemblyline.remote.datatypes import retry_call
from assemblyline.remote.datatypes.hash import ExpiringHash
from assemblyline.remote.datatypes.set import ExpiringSet

dispatch_tail = '-dispatch'
finished_tail = '-finished'

finish_script = f"""
local sid = ARGV[1]
local file_hash = ARGV[2]
local service = ARGV[3]
local key = file_hash .. '-' .. service
local result_key = ARGV[4]

-- If the dispatch table has already been erased/task finished then don't create a new key.
-- We don't want the dispatch table to be re-created this way, after the submission has actually been
-- finalized. 
-- Return the number of outstanding files, and if the result we just 'finished' was a duplication.
if redis.call('hdel', sid .. '{dispatch_tail}', key) then
    redis.call('hsetnx', sid .. '{finished_tail}', key, result_key)
    return {{redis.call('hincrby', 'dispatch-hash-files-' .. sid, file_hash, -1), 0}}
end
return {{redis.call('hlen', sid .. '{dispatch_tail}'), 1}}
"""


class DispatchRow(collections.namedtuple('DispatchRow', ['bucket', 'key', 'score', 'drop', 'classification'])):
    @property
    def is_error(self):
        return self.bucket == 'error'


class DispatchHash:
    def __init__(self, sid: str, client: Union[Redis, StrictRedis], fetch_results=False):
        """

        :param sid:
        :param client:
        :param fetch_results: Preload all the results on the redis server.
        """
        self.client = client
        self.sid = sid
        self._dispatch_key = f'{sid}{dispatch_tail}'
        self._finish_key = f'{sid}{finished_tail}'
        self._finish = self.client.register_script(finish_script)

        # cache the schedules calculated for the dispatcher, used to prevent rebuilding the
        # schedule repeatedly, and for telling the UI what services are pending
        self.schedules = ExpiringHash(f'dispatch-hash-schedules-{sid}', host=self.client)

        # How many services are outstanding for each file in the submission
        self._outstanding_service_count = ExpiringHash(f'dispatch-hash-files-{sid}', host=self.client)
        # Track which files have been extracted by what, in order to rebuild the file tree later
        self._file_tree = ExpiringSet(f'dispatch-hash-parents-{sid}', host=self.client)
        self._attempts = ExpiringHash(f'dispatch-hash-attempts-{sid}', host=self.client)

        # Local caches for _files and finished table
        self._cached_files = set(self._outstanding_service_count.keys())
        self._cached_results = dict()
        if fetch_results:
            self._cached_results = self.all_results()

        # Errors that are related to a submission, but not the terminal errors of a service
        self._other_errors = ExpiringSet(f'dispatch-hash-errors-{sid}', host=self.client)

        # TODO set these expire times from the global time limit for submissions
        retry_call(self.client.expire, self._dispatch_key, 60*60)
        retry_call(self.client.expire, self._finish_key, 60*60)

    def add_file(self, file_hash: str, file_limit, parent_hash) -> bool:
        """Add a file to a submission.

        Returns: Whether the file could be added to the submission or has been rejected.
        """
        if parent_hash:
            self._file_tree.add(f'{file_hash}-{parent_hash}')
        else:
            self._file_tree.add(file_hash)

        # If it was already in the set, we don't need to check remotely
        if file_hash in self._cached_files:
            return True

        # If the set is already full, and its not in the set, then we don't need to check remotely
        if len(self._cached_files) >= file_limit:
            return False

        # Our local checks are unclear, check remotely,
        # 0 => already exists, still want to return true
        # 1 => didn't exist before
        # None => over size limit, return false
        if self._outstanding_service_count.limited_add(file_hash, 0, file_limit) is not None:
            # If it was added, add it to the local cache so we don't need to check again
            self._cached_files.add(file_hash)
            return True
        return False

    def add_error(self, error_key: str) -> bool:
        """Add an error to a submission.

        NOTE: This method is for errors occuring outside of any errors handled via 'fail_*recoverable'

        Returns true if the error is new, false if the error is a duplicate.
        """
        return self._other_errors.add(error_key) > 0

    def dispatch(self, file_hash: str, service: str):
        """Mark that a service has been dispatched for the given sha."""
        if retry_call(self.client.hset, self._dispatch_key, f"{file_hash}-{service}", time.time()):
            self._outstanding_service_count.increment(file_hash, 1)

    def drop_dispatch(self, file_hash: str, service: str):
        """If a dispatch has been found to be un-needed remove the counters."""
        if retry_call(self.client.hdel, self._dispatch_key, f"{file_hash}-{service}"):
            self._outstanding_service_count.increment(file_hash, -1)

    def dispatch_count(self):
        """How many tasks have been dispatched for this submission."""
        return retry_call(self.client.hlen, self._dispatch_key)

    def dispatch_time(self, file_hash: str, service: str) -> float:
        """When was dispatch called for this sha/service pair."""
        result = retry_call(self.client.hget, self._dispatch_key, f"{file_hash}-{service}")
        if result is None:
            return 0
        return float(result)

    def all_dispatches(self) -> Dict[str, Dict[str, float]]:
        """Load the entire table of things that should currently be running."""
        rows = retry_call(self.client.hgetall, self._dispatch_key)
        output = {}
        for key, timestamp in rows.items():
            file_hash, service = key.split(b'-', maxsplit=1)
            file_hash = file_hash.decode()
            service = service.decode()
            if file_hash not in output:
                output[file_hash] = {}
            output[file_hash][service] = float(timestamp)
        return output

    def fail_recoverable(self, file_hash: str, service: str, error_key: str = None):
        """A service task has failed, but should be retried, clear that it has been dispatched.

        After this call, the service is in a non-dispatched state, and the status can't be update
        until it is dispatched again.
        """
        if error_key:
            self._other_errors.add(error_key)
        retry_call(self.client.hdel, self._dispatch_key, f"{file_hash}-{service}")
        self._outstanding_service_count.increment(file_hash, -1)

    def fail_nonrecoverable(self, file_hash: str, service, error_key) -> Tuple[int, bool]:
        """A service task has failed and should not be retried, entry the error as the result.

        Has exactly the same semantics as `finish` but for errors.
        """
        return retry_call(self._finish, args=[self.sid, file_hash, service,
                                              json.dumps(['error', error_key, 0, False, ''])])

    def finish(self, file_hash, service, result_key, score, classification, drop=False) -> Tuple[int, bool]:
        """
        As a single transaction:
         - Remove the service from the dispatched list
         - Add the file to the finished list, with the given result key
         - return the number of items in the dispatched list and if this was a duplicate call to finish
        """
        return retry_call(self._finish, args=[self.sid, file_hash, service,
                                              json.dumps(['result', result_key, score, drop, str(classification)])])

    def finished_count(self) -> int:
        """How many tasks have been finished for this submission."""
        return retry_call(self.client.hlen, self._finish_key)

    def finished(self, file_hash, service) -> Union[DispatchRow, None]:
        """If a service has been finished, return the key of the result document."""
        # Try the local cache
        result = self._cached_results.get(file_hash, {}).get(service, None)
        if result:
            return result
        # Try the server
        result = retry_call(self.client.hget, self._finish_key, f"{file_hash}-{service}")
        if result:
            return DispatchRow(*json.loads(result))
        return None

    def all_finished(self) -> bool:
        """Are there no outstanding tasks, and at least one finished task."""
        return self.finished_count() > 0 and self.dispatch_count() == 0

    def all_results(self) -> Dict[str, Dict[str, DispatchRow]]:
        """Get all the records stored in the dispatch table.

        :return: output[file_hash][service_name] -> DispatchRow
        """
        rows = retry_call(self.client.hgetall, self._finish_key)
        output = {}
        for key, status in rows.items():
            file_hash, service = key.split(b'-', maxsplit=1)
            file_hash = file_hash.decode()
            service = service.decode()
            if file_hash not in output:
                output[file_hash] = {}
            output[file_hash][service] = DispatchRow(*json.loads(status))
        return output

    def all_extra_errors(self):
        """Return the set of errors not part of the dispatch table itself."""
        return self._other_errors.members()

    def all_files(self):
        return self._outstanding_service_count.keys()

    def file_tree(self):
        """Returns a mapping from file, to a list of files that are that file's parents.

        A none value being in the list indicates that the file is one of the root files of the submission.
        """
        edges = self._file_tree.members()
        output = {}
        for string in edges:
            if '-' in string:
                child, parent = string.split('-')
            else:
                child, parent = string, None

            if child not in output:
                output[child] = []
            output[child].append(parent)
        return output

    def delete(self):
        """Clear the tables from the redis server."""
        retry_call(self.client.delete, self._dispatch_key)
        retry_call(self.client.delete, self._finish_key)
        self.schedules.delete()
        self._outstanding_service_count.delete()
        self._file_tree.delete()
        self._other_errors.delete()
        self._attempts.delete()
