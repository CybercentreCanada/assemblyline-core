"""
Code for collecting metric data and feeding it into the orchestration framework.

The task of this module is to take data available from metrics across multiple hosts
providing the same service and combine them into general statistics about the service.
"""
import time
from typing import Dict
from collections import namedtuple

Row = namedtuple('Row', ['timestamp', 'busy', 'throughput'])


class Collection:
    def __init__(self, period, ttl=None):
        """
        A buffer for metrics data from multiple instances of multiple services.

        :param period: Expected seconds between updates
        :param ttl: Seconds before a message is dropped from the buffer
        """
        self.period: float = period
        self.ttl: float = ttl or (period * 1.5)
        self.services: Dict[str, Dict[str, Row]] = {}

    def update(self, service, host, busy_seconds, throughput):
        # Load the sequence of data points that
        try:
            hosts = self.services[service]
        except KeyError:
            hosts = self.services[service] = {}

        # Add the new data
        hosts[host] = Row(time.time(), busy_seconds, throughput)

    def read(self, service):
        now = time.time()

        # Load the last messages from this service
        try:
            hosts = self.services[service]
        except KeyError:
            return None

        # Flush out stale messages
        expired = [_h for _h, _v in hosts.items() if now - _v.timestamp > self.ttl]
        for host_name in expired:
            hosts.pop(host_name, None)

        # If flushing got rid of all of our messages our state is 'offline'
        if not hosts:
            return None

        #
        return {
            'instances': len(hosts),
            'duty_cycle': sum(_v.busy for _v in hosts.values())/(len(hosts) * self.period),
        }
