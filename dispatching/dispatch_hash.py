"""
The dispatch hash table holds two lists of (file, service) pairs.

One table holds tasks not yet finished, the second holds all the finished tasks.
The values in the hash tables are the details of the task to be run.

The dispatch hash holds all the tasks that need to be run, regardless of schedule ordering.
"""


class DispatchHash:
    def __init__(self, redis, sid):
        self.client = get_client(*redis)
        self.sid = sid

    def planned(self, service_name):
        raise NotImplementedError()

    def finished(self, service_name):
        raise NotImplementedError()

    def insert(self, service_name, file_hash, task):
        raise NotImplementedError()

    def delete(self):
        raise NotImplementedError()
        # retry_call(self.client.delete, self.name)

    def finish(self, service_name, file_hash, result_key):
        """
        As a single transaction:
         - Remove the service from the dispatched list
         - Add the file to the finished list, with the given result key
         - return the number of items in the dispatched list
        """
        raise NotImplementedError()
        # retry_call(self.client.delete, self.name)

    # def length(self):
    #     return retry_call(self.client.zcard, self.name)
    #
    # def pop_range(self, lowest, highest):
    #     return retry_call(self.client.zremrangebyscore, self.name, lowest, highest)
    #
    # def push_unique(self, priority, key):
    #     return bool(retry_call(self.client.zadd, 'NX', priority, key))
    #
    # def push(self, priority, key):
    #     return bool(retry_call(self.client.zadd, priority, key))
