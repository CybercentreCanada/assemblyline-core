"""
A data structure encapsulating the timeout logic for the dispatcher.
"""
import queue
import time
from queue import PriorityQueue
from dataclasses import dataclass, field
from typing import TypeVar, Dict

KeyType = TypeVar('KeyType')
DataType = TypeVar('DataType')


@dataclass(order=True)
class TimeoutItem:
    expiry: float
    key: KeyType = field(compare=False)
    data: DataType = field(compare=False)


class TimeoutTable:
    def __init__(self):
        self.timeout_queue: PriorityQueue[TimeoutItem] = PriorityQueue()
        self.event_data: Dict[KeyType, TimeoutItem] = {}

    def set(self, key: KeyType, timeout: float, data: DataType):
        # If a timeout is set repeatedly with the same key, only the last one will count
        # even though we aren't removing the old ones from the queue. When the items are
        # popped from the queue they
        entry = TimeoutItem(time.time() + timeout, key, data)
        self.event_data[key] = entry
        self.timeout_queue.put(entry)

    def clear(self, key: KeyType):
        self.event_data.pop(key, None)

    def __contains__(self, item):
        return item in self.event_data

    def timeouts(self) -> Dict[KeyType, DataType]:
        found = {}
        try:
            now = time.time()

            # Loop until we hit an entry that is active, and non expired
            current: TimeoutItem = self.timeout_queue.get_nowait()
            while current.expiry <= now or self.event_data.get(current.key, None) != current:
                if self.event_data.get(current.key, None) == current:
                    self.event_data.pop(current.key)
                    found[current.key] = current.data
                current = self.timeout_queue.get_nowait()

            # If we exit the loop, the last item was valid still, put it back
            self.timeout_queue.put(current)

        except queue.Empty:
            pass
        return found
