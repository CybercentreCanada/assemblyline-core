#!/usr/bin/env python
import time

import elasticsearch
import sys
import copy

from apscheduler.schedulers.background import BackgroundScheduler
from collections import Counter
from threading import Lock

from al_core.server_base import ServerBase
from assemblyline.common.isotime import now_as_iso
from assemblyline.common import forge, metrics
from assemblyline.remote.datatypes.queues.comms import CommsQueue

METRICS_QUEUE = "assemblyline_metrics"
STATUS_QUEUE = "status"

def cleanup_metrics(input_dict):
    output_dict = {}
    for k, v in input_dict.items():
        items = k.split(".")
        parent = output_dict
        for i in items:
            if i not in parent:
                if items.index(i) == (len(items) - 1):
                    # noinspection PyBroadException
                    try:
                        parent[i] = int(v)
                    except Exception:  # pylint:disable=W0702
                        if v == "true":
                            parent[i] = True
                        elif v == "false":
                            parent[i] = False
                        else:
                            parent[i] = v

                    break
                else:
                    parent[i] = {}
            parent = parent[i]

    return output_dict


class LegacyMetricsServer(ServerBase):
    """
    There can only be one of these type of metrics server running because it runs of a pubsub queue.
    """
    def __init__(self, config=None):
        super().__init__('assemblyline.legacy_metrics', shutdown_timeout=65)
        self.config = config or forge.get_config()

        self.elastic_ip = self.config.core.metrics.elasticsearch.host
        self.elastic_port = self.config.core.metrics.elasticsearch.port

        if not self.elastic_ip or not self.elastic_port:
            self.log.error("No elasticsearch cluster defined to store metrics. All gathered stats will be ignored...")
            sys.exit(1)

        self.scheduler = BackgroundScheduler(daemon=True)
        self.metrics_queue = None
        self.status_queue = None
        self.es = None

        self.counters_lock = Lock()
        self.counters = {}
        self.rolling_window = {}
        self.window_size = int(60 / self.config.core.metrics.export_interval)
        if self.window_size != 60 / self.config.core.metrics.export_interval:
            self.log.warning("Cannot calculate a proper window size for reporting heartbeats. "
                             "Metrics reported during hearbeat will be wrong.")

    def try_run(self):

        self.metrics_queue = CommsQueue(METRICS_QUEUE)
        self.status_queue = CommsQueue(STATUS_QUEUE)
        self.es = elasticsearch.Elasticsearch([{'host': self.elastic_ip, 'port': self.elastic_port}])

        self.scheduler.add_job(self._create_aggregated_metrics, 'interval', seconds=60)
        self.scheduler.add_job(self._export_hearbeats, 'interval', seconds=self.config.core.metrics.export_interval)
        self.scheduler.start()

        while self.running:
            for msg in self.metrics_queue.listen():
                m_name = msg.pop('name', None)
                m_type = msg.pop('type', None)
                m_host = msg.pop('host', None)
                msg.pop('instance', None)

                self.log.debug(f"Compiling metrics for component: {m_type}")
                if not m_name or not m_type:
                    continue

                w_key = (m_name, m_type, m_host)
                if w_key not in self.rolling_window:
                    self.rolling_window[w_key] = [Counter(msg)]
                else:
                    self.rolling_window[w_key].append(Counter(msg))

                with self.counters_lock:
                    c_key = (m_name, m_type)
                    if c_key not in self.counters:
                        self.counters[c_key] = Counter(msg)
                    else:
                        self.counters[c_key] += Counter(msg)

    def _create_aggregated_metrics(self):
        self.log.info("Copying counters.")
        with self.counters_lock:
            counter_copy = copy.deepcopy(self.counters)
            self.counters = {}

        self.log.info("Aggregating metrics.")
        timestamp = now_as_iso()
        for component, counts in counter_copy.items():
            component_name, component_type = component
            output_metrics = {'name': component_name,
                              'type': component_type}
            if component_type in metrics.METRIC_TYPES and component_type not in metrics.TIMED_METRICS:
                output_metrics.update({k: counts.get(k, 0) for k in metrics.METRIC_TYPES[component_type]})
            elif component_type in metrics.METRIC_TYPES and component_type in metrics.TIMED_METRICS:
                output_metrics.update({k: counts.get(k + ".t", 0) / counts.get(k + ".c", 1)
                                       for k in metrics.METRIC_TYPES[component_type]})
                output_metrics.update({k + "_count": counts.get(k + ".c", 0)
                                       for k in metrics.METRIC_TYPES[component_type]})
            else:
                self.log.info("Skipping unknown component type: {cpt}".format(cpt=component_type))
                continue
            output_metrics['timestamp'] = timestamp
            output_metrics = cleanup_metrics(output_metrics)
            index_time = timestamp[:10].replace("-", ".")

            self.log.info(output_metrics)
            try:
                self.es.index(f"al_metrics_{component_type}-{index_time}", component_type, output_metrics)
            except Exception as e:
                self.log.exception(e)

        self.log.info("Metrics aggregated... Waiting for next run.")

    def _export_hearbeats(self):
        self.log.info("Reporting heartbeats")
        # TODO: loop through self.rolling_window, cumulate matching metrics and .pop(0) an item of each list if len
        #       is bigger the the window size so the metrics are always 60 seconds based.
        #       Depending of which metrics type we're getting, generate the associate hearbeat which will be sent
        #       throught the STATUS_QUEUE

class HashMapMetricsServer(ServerBase):
    def __init__(self, config=None):
        super().__init__('assemblyline.metrics')
        self.config = config or forge.get_config()

        self.elastic_ip = self.config.core.metrics.elasticsearch.host
        self.elastic_port = self.config.core.metrics.elasticsearch.port

        if not self.elastic_ip or not self.elastic_port:
            self.log.error("No elasticsearch cluster defined to store metrics. All gathered stats will be ignored...")
            sys.exit(1)

        self.scheduler = BackgroundScheduler(daemon=True)
        self.es = None

    def try_run(self):
        self.es = elasticsearch.Elasticsearch([{'host': self.elastic_ip, 'port': self.elastic_port}])

        self.scheduler.add_job(self._create_aggregated_metrics, 'interval', seconds=60)
        self.scheduler.start()

        while self.running:
            # TODO: Create heartbeats and dispatch them
            time.sleep(self.config.core.metrics.export_interval)

    def _create_aggregated_metrics(self):
        pass

if __name__ == '__main__':
    config = forge.get_config()
    if config.core.metrics.type == metrics.LEGACY:
        with LegacyMetricsServer(config=config) as metricsd:
            metricsd.serve_forever()
    else:
        with HashMapMetricsServer(config=config) as metricsd:
            metricsd.serve_forever()