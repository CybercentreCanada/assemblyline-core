import time
import threading
import functools
from collections import namedtuple

import requests


Stream = namedtuple('Stream', [
    'id',
    'name',
    'description',
    'zone_id',
    'classification'
])


class StreamMap:
    UPDATE_INTERVAL = 60 * 15

    @staticmethod
    @functools.cache
    def load(url):
        return StreamMap(url)

    def __init__(self, url):
        self.url = url
        self.lock = threading.Lock()
        self.table: dict[int, Stream] = {}
        self.update_time = 0
        self._load_stream_map()

    def _load_stream_map(self):
        # Don't load more than once every 5 seconds
        if time.time() - self.update_time < 5:
            return

        with self.lock:
            # Recheck in case it was updated while waiting for lock
            if time.time() - self.update_time < 5:
                return

            res = requests.get(self.url)
            res.raise_for_status()

            table = {}
            for stream in res.json()['data']:
                stream = Stream(
                    id=int(stream['STREAM_ID']),
                    name=stream['STREAM_NAME'],
                    description=stream['STREAM_DESCRIPTION'],
                    zone_id=stream['ZONE'],
                    classification=f"{stream.get('LEVEL', 'PB')}//{stream.get('CAVEAT', 'CND')}"
                )
                table[stream.id] = stream

            self.table = table
            self.update_time = time.time()

    def _refresh_stream_map(self):
        if time.time() - self.update_time > self.UPDATE_INTERVAL:
            self._load_stream_map()

    def __getitem__(self, stream_id):
        self._refresh_stream_map()
        try:
            return self.table[stream_id]
        except KeyError:
            self._load_stream_map()
            return self.table.get(stream_id)


if __name__ == '__main__':
    streams = StreamMap('')
    print(streams[10])
    print(streams[10000000])
