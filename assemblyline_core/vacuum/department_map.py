import functools
import json
import time
import logging
import threading
from typing import Optional

from assemblyline.common.iprange import RangeTable

import requests


logger = logging.getLogger("assemblyline.vacuum.department_map")


class DepartmentMap:
    UPDATE_INTERVAL = 60 * 60

    @staticmethod
    @functools.cache
    def load(url: Optional[str], init: Optional[str]):
        return DepartmentMap(url, init)

    def __init__(self, url: Optional[str], init: Optional[str]):
        self.url = url
        self.init_data = init
        self.lock = threading.Lock()
        self.table = RangeTable()
        self.update_time = 0
        self._load_department_map()

    def _load_department_map(self):
        # Don't load more than once every 5 seconds
        if time.time() - self.update_time < 5:
            return

        with self.lock:
            # Recheck in case it was updated while waiting for lock
            if time.time() - self.update_time < 5:
                return

            table = RangeTable()

            try:
                if self.init_data:
                    for row in json.loads(self.init_data):
                        if ':' not in row['LOWER'] and ':' not in row['UPPER']:
                            # print(row["LOWER"], row['UPPER'], row['LABEL'])
                            table.add_range(row['LOWER'], row['UPPER'], row['LABEL'])
            except Exception:
                logger.exception("Error parsing department_map_init")

            try:
                if self.url:
                    res = requests.get(self.url, verify=False)
                    res.raise_for_status()

                    for row in res.json():
                        if ':' not in row['LOWER'] and ':' not in row['UPPER']:
                            # print(row["LOWER"], row['UPPER'], row['LABEL'])
                            table.add_range(row['LOWER'], row['UPPER'], row['LABEL'])
            except Exception:
                logger.exception("Error parsing department_map_url")

            self.table = table
            self.update_time = time.time()

    def _refresh_department_map(self):
        if time.time() - self.update_time > self.UPDATE_INTERVAL:
            self._load_department_map()

    def __getitem__(self, ip) -> Optional[str]:
        self._refresh_department_map()
        try:
            return self.table[ip]
        except KeyError:
            self._load_department_map()
            try:
                return self.table[ip]
            except KeyError:
                return None


if __name__ == '__main__':
    departments = DepartmentMap('', None)
    print(departments['48.49.39.100'])
