#!/usr/bin/env python
import time

import elasticapm
import elasticsearch
import sys
import copy

from apscheduler.schedulers.background import BackgroundScheduler
from collections import Counter
from threading import Lock

from al_core.metrics.heartbeat_manager import HeartbeatManager
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
        super().__init__('assemblyline.legacy_metrics_aggregator', shutdown_timeout=65)
        self.config = config or forge.get_config()
        self.elastic_hosts = self.config.core.metrics.elasticsearch.hosts

        if not self.elastic_hosts:
            self.log.error("No elasticsearch cluster defined to store metrics. All gathered stats will be ignored...")
            sys.exit(1)

        self.scheduler = BackgroundScheduler(daemon=True)
        self.metrics_queue = None
        self.es = None
        self.counters_lock = Lock()
        self.counters = {}

        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="legacy_metrics_aggregator")
        else:
            self.apm_client = None

    def try_run(self):
        self.metrics_queue = CommsQueue(METRICS_QUEUE)
        self.es = elasticsearch.Elasticsearch(hosts=self.elastic_hosts)

        self.scheduler.add_job(self._create_aggregated_metrics, 'interval', seconds=60)
        self.scheduler.start()

        while self.running:
            for msg in self.metrics_queue.listen():
                # APM Transaction start
                if self.apm_client:
                    self.apm_client.begin_transaction('metrics')

                m_name = msg.pop('name', None)
                m_type = msg.pop('type', None)
                msg.pop('host', None)
                msg.pop('instance', None)

                self.log.debug(f"Received {m_type.upper()} metrics message")
                if not m_name or not m_type:
                    # APM Transaction end
                    if self.apm_client:
                        self.apm_client.end_transaction('process_message', 'invalid_message')

                    continue

                with self.counters_lock:
                    c_key = (m_name, m_type)
                    if c_key not in self.counters:
                        self.counters[c_key] = Counter(msg)
                    else:
                        self.counters[c_key] += Counter(msg)

                # APM Transaction end
                if self.apm_client:
                    self.apm_client.end_transaction('process_message', 'success')

    def _create_aggregated_metrics(self):
        self.log.info("Copying counters ...")
        # APM Transaction start
        if self.apm_client:
            self.apm_client.begin_transaction('metrics')

        with self.counters_lock:
            counter_copy = copy.deepcopy(self.counters)
            self.counters = {}

        self.log.info("Aggregating metrics ...")
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
                self.log.warning(f"Skipping unknown component type: {component_type}")
                continue
            output_metrics['timestamp'] = timestamp
            output_metrics = cleanup_metrics(output_metrics)
            index_time = timestamp[:10].replace("-", ".")

            self.log.info(output_metrics)
            try:
                self.es.index(f"al_metrics_{component_type}-{index_time}", component_type, output_metrics)
            except Exception as e:
                self.log.exception(e)

        self.log.info("Metrics aggregated. Waiting for next run...")

        # APM Transaction end
        if self.apm_client:
            self.apm_client.end_transaction('aggregate_metrics', 'success')


# noinspection PyBroadException
class LegacyHeartbeatManager(ServerBase):
    def __init__(self, config=None):
        super().__init__('assemblyline.legacy_heartbeat_manager')
        self.config = config or forge.get_config()
        self.metrics_queue = CommsQueue(METRICS_QUEUE)
        self.scheduler = BackgroundScheduler(daemon=True)
        self.hm = HeartbeatManager("legacy_heartbeat_manager", self.log, config=self.config)

        self.rolling_window = {}
        self.window_ttl = {}
        self.ttl = self.config.core.metrics.export_interval * 2
        self.window_size = int(60 / self.config.core.metrics.export_interval)
        if self.window_size != 60 / self.config.core.metrics.export_interval:
            self.log.warning("Cannot calculate a proper window size for reporting heartbeats. "
                             "Metrics reported during hearbeat will be wrong.")

        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="legacy_heartbeat_manager")
        else:
            self.apm_client = None

    def try_run(self):
        self.scheduler.add_job(self._export_hearbeats, 'interval', seconds=self.config.core.metrics.export_interval)
        self.scheduler.start()

        while self.running:
            for msg in self.metrics_queue.listen():
                # APM Transaction start
                if self.apm_client:
                    self.apm_client.begin_transaction('heartbeat')

                m_name = msg.pop('name', None)
                m_type = msg.pop('type', None)
                m_host = msg.pop('host', None)
                msg.pop('instance', None)

                self.log.debug(f"Received {m_type.upper()} metrics message")
                if not m_name or not m_type:
                    # APM Transaction end
                    if self.apm_client:
                        self.apm_client.end_transaction('process_message', 'invalid_message')

                    continue

                w_key = (m_name, m_type, m_host)
                if w_key not in self.rolling_window:
                    self.rolling_window[w_key] = [Counter(msg)]
                else:
                    self.rolling_window[w_key].append(Counter(msg))

                self.window_ttl[w_key] = time.time() + self.ttl

                # APM Transaction end
                if self.apm_client:
                    self.apm_client.end_transaction('process_message', 'success')

    def _export_hearbeats(self):
        try:
            self.log.info("Expiring unused counters...")
            # APM Transaction start
            if self.apm_client:
                self.apm_client.begin_transaction('heartbeat')

            c_time = time.time()
            for k in list(self.window_ttl.keys()):
                if self.window_ttl.get(k, c_time) < c_time:
                    c_name, c_type, c_host = k
                    self.log.info(f"Counter {c_name} [{c_type}] for host {c_host} is expired")
                    del self.window_ttl[k]
                    del self.rolling_window[k]

            self.log.info("Copying counters ...")
            window_copy = copy.deepcopy(self.rolling_window)

            self.log.info("Aggregating heartbeat data...")
            aggregated_counters = {}
            for component_parts, counters_list in window_copy.items():
                c_name, c_type, c_host = component_parts

                # Expiring data outside of the window
                if len(counters_list) > self.window_size:
                    self.rolling_window[component_parts].pop(0)
                    counters_list = counters_list[1:]

                if c_type in metrics.METRIC_TYPES:
                    key = (c_name, c_type)
                    if key not in aggregated_counters:
                        aggregated_counters[key] = Counter()

                    if c_type in metrics.TIMED_METRICS:
                        aggregated_counters[key]['instances.t'] += 1
                        aggregated_counters[key]['instances.c'] += 1
                    else:
                        aggregated_counters[key]['instances'] += 1

                    for c in counters_list:
                        aggregated_counters[key] += c

                else:
                    self.log.warning(f"Skipping unknown component type: {c_type}")

            self.log.info("Generating heartbeats...")
            for aggregated_parts, counter in aggregated_counters.items():
                agg_c_name, agg_c_type = aggregated_parts
                with elasticapm.capture_span(name=f"{agg_c_type}.{agg_c_name}", span_type="send_heartbeat"):
                    allowed_fields = metrics.METRIC_TYPES[agg_c_type] + ['instances']

                    if agg_c_type not in metrics.TIMED_METRICS:
                        metrics_data = {k: counter.get(k, 0) for k in allowed_fields}
                    else:
                        metrics_data = {k: counter.get(k + ".t", 0) / counter.get(k + ".c", 1) for k in allowed_fields}
                        metrics_data.update({k + "_count": counter.get(k + ".c", 0) for k in allowed_fields})

                    agg_c_instances = metrics_data.pop('instances', 1)
                    metrics_data.pop('instances_count', None)
                    self.hm.send_heartbeat(agg_c_type, agg_c_name, metrics_data, agg_c_instances)

            # APM Transaction end
            if self.apm_client:
                self.apm_client.end_transaction('send_heartbeats', 'success')

        except Exception:
            self.log.exception("Unknown exception occurred during heartbeat creation:")
