#!/usr/bin/env python

import tempfile
import sys
import time
from collections import Counter
from threading import Lock, Thread
from os import environ, path
from urllib.parse import urlparse

import elasticapm
import elasticsearch
import requests
from packaging import version

from apscheduler.schedulers.background import BackgroundScheduler
from assemblyline_core.metrics.heartbeat_formatter import HeartbeatFormatter
from assemblyline_core.metrics.helper import ensure_indexes, with_retries
from assemblyline_core.server_base import ServerBase
from assemblyline.common.isotime import now_as_iso
from assemblyline.common import forge
from assemblyline.remote.datatypes.queues.comms import CommsQueue

METRICS_QUEUE = "assemblyline_metrics"
NON_AGGREGATED = ['scaler', 'scaler_status']
NON_AGGREGATED_COUNTERS = {'dispatcher': {'save_queue', 'error_queue'}}

METRICSTORE_ROOT_CA_PATH = environ.get('METRICSTORE_ROOT_CA_PATH', '/etc/assemblyline/ssl/al_root-ca.crt')
METRICSTORE_VERIFY_CERTS = environ.get('METRICSTORE_VERIFY_CERTS', 'true').lower() == "true"


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


class StatisticsAggregator(ServerBase):
    """
    There's no need to be more then one of these since it's job is
    only to cache statistics about signatures and heuristics once per day
    """

    def __init__(self, config=None):
        super().__init__('assemblyline.statistics_aggregator')
        self.config = config or forge.get_config()
        self.datastore = forge.get_datastore(archive_access=True)
        self.sleep_time = 60 * 5  # Default sleep time (5 minutes)
        self.signature_lookback = "now-1d"  # Default lookback time for signatures

        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = forge.get_apm_client("metrics_aggregator")
        else:
            self.apm_client = None

    def try_run(self):
        while self.running:
            self._aggregated_statistics()

            # Non-block wait for sleep time duration
            self.sleep(self.sleep_time)

    def _heuristics_stats(self):
        self.log.info("Computing heuristics statistics")

        # APM Transaction start
        if self.apm_client:
            self.apm_client.begin_transaction('statistics')

        # Do heuristics update
        self.datastore.calculate_heuristic_stats()

        # APM Transaction end
        if self.apm_client:
            self.apm_client.end_transaction('heuristics_statistics', 'success')

    def _signature_stats(self):
        self.log.info("Computing signature statistics")

        # APM Transaction start
        if self.apm_client:
            self.apm_client.begin_transaction('statistics')

        # Do signature update
        self.signature_lookback = self.datastore.calculate_signature_stats(self.signature_lookback)

        # APM Transaction end
        if self.apm_client:
            self.apm_client.end_transaction('signature_statistics', 'success')

    def _aggregated_statistics(self):
        self.log.info("Start statistic collection run")
        self._heuristics_stats()
        self._signature_stats()
        self.log.info("Statistics generated successfully, waiting for next run")


class MetricsServer(ServerBase):
    """
    There can only be one of these type of metrics server running because it runs of a pubsub queue.
    """

    def __init__(self, config=None):
        super().__init__('assemblyline.metrics_aggregator', shutdown_timeout=65)
        self.config = config or forge.get_config()
        self.elastic_hosts = self.config.core.metrics.elasticsearch.hosts
        self.is_datastream = False

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
            self.apm_client = forge.get_apm_client("metrics_aggregator")
        else:
            self.apm_client = None

    def try_run(self):
        # If our connection to the metrics database requires a custom ca cert, prepare it
        ca_certs = None if not path.exists(METRICSTORE_ROOT_CA_PATH) else METRICSTORE_ROOT_CA_PATH
        if self.config.core.metrics.elasticsearch.host_certificates:
            with tempfile.NamedTemporaryFile(delete=False) as ca_certs_file:
                ca_certs = ca_certs_file.name
                ca_certs_file.write(self.config.core.metrics.elasticsearch.host_certificates.encode())

        self.metrics_queue = CommsQueue(METRICS_QUEUE)
        self.es = elasticsearch.Elasticsearch(hosts=self.elastic_hosts, ca_certs=ca_certs,
                                              verify_certs=METRICSTORE_VERIFY_CERTS)
        # Determine if ES will support data streams (>= 7.9)
        self.is_datastream = version.parse(self.es.info()['version']['number']) >= version.parse("7.9")

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
                    if c_key not in self.counters or m_type in NON_AGGREGATED:
                        self.counters[c_key] = Counter(msg)
                    else:
                        non_agg_values = {}
                        if m_type in NON_AGGREGATED_COUNTERS:
                            non_agg_values = {k: v for k, v in msg.items() if k in NON_AGGREGATED_COUNTERS[m_type]}
                        self.counters[c_key].update(Counter(msg))
                        for k, v in non_agg_values.items():
                            self.counters[c_key][k] = v

                # APM Transaction end
                if self.apm_client:
                    self.apm_client.end_transaction('process_message', 'success')

    def _create_aggregated_metrics(self):
        self.log.info("Copying counters ...")
        # APM Transaction start
        if self.apm_client:
            self.apm_client.begin_transaction('metrics')

        with self.counters_lock:
            counter_copy, self.counters = self.counters, {}

        self.log.info("Aggregating metrics ...")
        timestamp = now_as_iso()
        for component, counts in counter_copy.items():
            component_name, component_type = component
            output_metrics = {'name': component_name, 'type': component_type}

            for key, value in counts.items():
                # Skip counts, they will be paired with a time entry and we only want to count it once
                if key.endswith('.c'):
                    continue
                # We have an entry that is a timer, should also have a .c count
                elif key.endswith('.t'):
                    name = key.rstrip('.t')
                    output_metrics[name] = counts[key] / counts.get(name + ".c", 1)
                    output_metrics[name + "_count"] = counts.get(name + ".c", 0)
                # Plain old metric, no modifications needed
                else:
                    output_metrics[key] = value

            ensure_indexes(self.log, self.es, self.config.core.metrics.elasticsearch, [component_type],
                           datastream_enabled=self.is_datastream)

            index = f"al_metrics_{component_type}"
            # Were data streams created for the index specified?
            try:
                if self.es.indices.get_index_template(name=f"{index}_ds"):
                    output_metrics['@timestamp'] = timestamp
                    index = f"{index}_ds"
            except elasticsearch.exceptions.TransportError:
                pass
            output_metrics['timestamp'] = timestamp
            output_metrics = cleanup_metrics(output_metrics)

            self.log.info(output_metrics)
            with_retries(self.log, self.es.index, index=index, body=output_metrics)

        self.log.info("Metrics aggregated. Waiting for next run...")

        # APM Transaction end
        if self.apm_client:
            self.apm_client.end_transaction('aggregate_metrics', 'success')


# noinspection PyBroadException
class HeartbeatManager(ServerBase):
    def __init__(self, config=None):
        super().__init__('assemblyline.heartbeat_manager')
        self.config = config or forge.get_config()
        self.datastore = forge.get_datastore()
        self.metrics_queue = CommsQueue(METRICS_QUEUE)
        self.scheduler = BackgroundScheduler(daemon=True)
        self.hm = HeartbeatFormatter("heartbeat_manager", self.log, config=self.config)

        self.counters_lock = Lock()
        self.counters = {}
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
            self.apm_client = forge.get_apm_client("heartbeat_manager")
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
                if not m_name or not m_type or not m_host:
                    # APM Transaction end
                    if self.apm_client:
                        self.apm_client.end_transaction('process_message', 'invalid_message')

                    continue

                with self.counters_lock:
                    c_key = (m_name, m_type, m_host)
                    if c_key not in self.counters or m_type in NON_AGGREGATED:
                        self.counters[c_key] = Counter(msg)
                    else:
                        non_agg_values = {}
                        if m_type in NON_AGGREGATED_COUNTERS:
                            non_agg_values = {k: v for k, v in msg.items() if k in NON_AGGREGATED_COUNTERS[m_type]}
                        self.counters[c_key].update(Counter(msg))
                        for k, v in non_agg_values.items():
                            self.counters[c_key][k] = v

                # APM Transaction end
                if self.apm_client:
                    self.apm_client.end_transaction('process_message', 'success')

    def _export_hearbeats(self):
        try:
            self.heartbeat()
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

            self.log.info("Saving current counters to rolling window ...")
            with self.counters_lock:
                counter_copy, self.counters = self.counters, {}

            for w_key, counter in counter_copy.items():
                _, m_type, _ = w_key
                if w_key not in self.rolling_window or m_type in NON_AGGREGATED:
                    self.rolling_window[w_key] = [counter]
                else:
                    self.rolling_window[w_key].append(counter)

                self.rolling_window[w_key] = self.rolling_window[w_key][-self.window_size:]
                self.window_ttl[w_key] = time.time() + self.ttl

            self.log.info("Compiling service list...")
            aggregated_counters = {}
            for service in [s['name'] for s in self.datastore.list_all_services(as_obj=False) if s['enabled']]:
                data = {
                    'cache_hit': 0,
                    'cache_miss': 0,
                    'cache_skipped': 0,
                    'execute': 0,
                    'fail_recoverable': 0,
                    'fail_nonrecoverable': 0,
                    'scored': 0,
                    'not_scored': 0,
                    'instances': 0
                }
                aggregated_counters[(service, 'service')] = Counter(data)

            self.log.info("Aggregating heartbeat data...")
            for component_parts, counters_list in self.rolling_window.items():
                c_name, c_type, c_host = component_parts

                # Expiring data outside of the window
                counters_list = counters_list[-self.window_size:]

                key = (c_name, c_type)
                if key not in aggregated_counters:
                    aggregated_counters[key] = Counter()

                aggregated_counters[key]['instances'] += 1

                for c in counters_list:
                    aggregated_counters[key].update(c)

            self.log.info("Generating heartbeats...")
            for aggregated_parts, counter in aggregated_counters.items():
                agg_c_name, agg_c_type = aggregated_parts
                with elasticapm.capture_span(name=f"{agg_c_type}.{agg_c_name}", span_type="send_heartbeat"):

                    metrics_data = {}
                    for key, value in counter.items():
                        # Skip counts, they will be paired with a time entry and we only want to count it once
                        if key.endswith('.c'):
                            continue
                        # We have an entry that is a timer, should also have a .c count
                        elif key.endswith('.t'):
                            name = key.rstrip('.t')
                            metrics_data[name] = value / max(counter.get(name + ".c", 1), 1)
                            metrics_data[name + "_count"] = counter.get(name + ".c", 0)
                        # Plain old metric, no modifications needed
                        else:
                            metrics_data[key] = value

                    agg_c_instances = metrics_data.pop('instances', 1)
                    metrics_data.pop('instances_count', None)
                    self.hm.send_heartbeat(agg_c_type, agg_c_name, metrics_data, agg_c_instances)

            # APM Transaction end
            if self.apm_client:
                self.apm_client.end_transaction('send_heartbeats', 'success')

        except Exception:
            self.log.exception("Unknown exception occurred during heartbeat creation:")
