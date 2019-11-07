#!/usr/bin/env python

import copy
import elasticapm
import elasticsearch
import json
import sys
import time

from apscheduler.schedulers.background import BackgroundScheduler
from collections import Counter
from threading import Lock

from assemblyline_core.metrics.heartbeat_formatter import HeartbeatFormatter
from assemblyline_core.server_base import ServerBase
from assemblyline.common.isotime import now_as_iso
from assemblyline.common import forge
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

def ilm_policy_exists(es, name):
    conn = es.transport.get_connection()
    pol_req = conn.session.get(f"{conn.base_url}/_ilm/policy/{name}")
    return pol_req.ok

def create_ilm_policy(es, name, ilm_config):
    data_base = {
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "set_priority": {
                            "priority": 100
                        },
                        "rollover": {
                            "max_age": f"{ilm_config['warm']}{ilm_config['unit']}"
                        }
                    }
                },
                "warm": {
                    "actions": {
                        "set_priority": {
                            "priority": 50
                        }
                    }
                },
                "cold": {
                    "min_age": f"{ilm_config['cold']}{ilm_config['unit']}",
                    "actions": {
                        "set_priority": {
                            "priority": 20
                        }
                    }
                }
            }
        }
    }

    if ilm_config['delete']:
        data_base['policy']['phases']['delete'] = {
            "min_age": f"{ilm_config['delete']}{ilm_config['unit']}",
            "actions": {
                "delete": {}
            }
        }

    conn = es.transport.get_connection()
    pol_req = conn.session.put(f"{conn.base_url}/_ilm/policy/{name}",
                       headers={"Content-Type": "application/json"},
                       data=json.dumps(data_base))
    if not pol_req.ok:
        raise Exception(f"ERROR: Failed to create ILM policy: {name}")


class MetricsServer(ServerBase):
    """
    There can only be one of these type of metrics server running because it runs of a pubsub queue.
    """
    def __init__(self, config=None):
        super().__init__('assemblyline.metrics_aggregator', shutdown_timeout=65)
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
                                                service_name="metrics_aggregator")
        else:
            self.apm_client = None

    def try_run(self):
        self.metrics_queue = CommsQueue(METRICS_QUEUE)
        self.es = elasticsearch.Elasticsearch(hosts=self.elastic_hosts,
                                              connection_class=elasticsearch.RequestsHttpConnection)

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

            output_metrics['timestamp'] = timestamp
            output_metrics = cleanup_metrics(output_metrics)

            self.log.info(output_metrics)
            try:
                index = f"al_metrics_{component_type}"
                policy = f"{index}_policy"
                if not ilm_policy_exists(self.es, policy):
                    self.log.debug(f"ILM Policy {policy.upper()} does not exists. Creating it now...")
                    create_ilm_policy(self.es, policy, self.config.core.metrics.elasticsearch.as_primitives())

                if not self.es.indices.exists_template(index):
                    self.log.debug(f"Index template {index.upper()} does not exists. Creating it now...")

                    template_body = {
                        "index_patterns": [f"{index}-*"],
                        "order": 1,
                        "settings": {
                            "index.lifecycle.name": policy,
                            "index.lifecycle.rollover_alias": index
                        }
                    }

                    try:
                        self.es.indices.put_template(index, template_body)
                    except elasticsearch.exceptions.RequestError as e:
                        if "resource_already_exists_exception" not in str(e):
                            raise
                        self.log.warning(f"Tried to create an index template that already exists: {index.upper()}")

                if not self.es.indices.exists_alias(index):
                    self.log.debug(f"Index alias {index.upper()} does not exists. Creating it now...")

                    index_body = {"aliases": {index: {"is_write_index": True}}}

                    try:
                        self.es.indices.create(f"{index}-000001", index_body)
                    except elasticsearch.exceptions.RequestError as e:
                        if "resource_already_exists_exception" not in str(e):
                            raise
                        self.log.warning(f"Tried to create an index template that "
                                         f"already exists: {index.upper()}-000001")

                self.es.index(index=index, body=output_metrics)
            except Exception as e:
                self.log.exception(e)

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
                                                service_name="heartbeat_manager")
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
            for component_parts, counters_list in window_copy.items():
                c_name, c_type, c_host = component_parts

                # Expiring data outside of the window
                if len(counters_list) > self.window_size:
                    self.rolling_window[component_parts].pop(0)
                    counters_list = counters_list[1:]

                key = (c_name, c_type)
                if key not in aggregated_counters:
                    aggregated_counters[key] = Counter()

                # if c_type in metrics.TIMED_METRICS:
                #     aggregated_counters[key]['instances.t'] += 1
                #     aggregated_counters[key]['instances.c'] += 1
                # else:
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
