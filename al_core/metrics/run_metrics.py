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
        self.es = None

        self.counters_lock = Lock()
        self.counters = {}

    def try_run(self):

        self.metrics_queue = CommsQueue(METRICS_QUEUE)
        self.es = elasticsearch.Elasticsearch([{'host': self.elastic_ip, 'port': self.elastic_port}])

        self.scheduler.add_job(self._create_aggregated_metrics, 'interval', seconds=60)
        # TODO: Add another job to dispatch heartbeats...
        self.scheduler.start()

        while self.running:
            for msg in self.metrics_queue.listen():
                metrics_name = msg.pop('name', None)
                metrics_type = msg.pop('type', None)

                msg.pop('host', None)
                msg.pop('instance', None)

                self.log.debug(f"Compiling metrics for component: {metrics_type}")
                if not metrics_name or not metrics_type:
                    continue

                with self.counters_lock:
                    if (metrics_name, metrics_type) not in self.counters:
                        self.counters[(metrics_name, metrics_type)] = Counter(msg)
                    else:
                        self.counters[(metrics_name, metrics_type)] += Counter(msg)

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