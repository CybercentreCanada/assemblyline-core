#!/usr/bin/env python
import time

import elasticsearch
import sys
import copy

from apscheduler.schedulers.background import BackgroundScheduler
from collections import Counter
from threading import Lock

from al_core.alerter.run_alerter import ALERT_QUEUE_NAME
from al_core.dispatching.dispatcher import DISPATCH_TASK_HASH, SUBMISSION_QUEUE
from al_core.ingester.ingester import drop_chance, INGEST_QUEUE_NAME
from al_core.server_base import ServerBase
from assemblyline.common.isotime import now_as_iso
from assemblyline.common import forge, metrics
from assemblyline.odm.messages.alerter_heartbeat import AlerterMessage
from assemblyline.odm.messages.dispatcher_heartbeat import DispatcherMessage
from assemblyline.odm.messages.expiry_heartbeat import ExpiryMessage
from assemblyline.odm.messages.ingest_heartbeat import IngestMessage
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.hash import Hash
from assemblyline.remote.datatypes.queues.comms import CommsQueue
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.queues.priority import PriorityQueue

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
        super().__init__('assemblyline.legacy_metrics_aggregator', shutdown_timeout=65)
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
        self.scheduler.start()

        while self.running:
            for msg in self.metrics_queue.listen():
                m_name = msg.pop('name', None)
                m_type = msg.pop('type', None)
                msg.pop('host', None)
                msg.pop('instance', None)

                self.log.debug(f"Received {m_type.upper()} metrics message")
                if not m_name or not m_type:
                    continue

                with self.counters_lock:
                    c_key = (m_name, m_type)
                    if c_key not in self.counters:
                        self.counters[c_key] = Counter(msg)
                    else:
                        self.counters[c_key] += Counter(msg)

    def _create_aggregated_metrics(self):
        self.log.info("Copying counters ...")
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


# noinspection PyBroadException
class LegacyHeartbeatManager(ServerBase):
    def __init__(self, config=None):
        super().__init__('assemblyline.legacy_heartbeat_manager')
        self.config = config or forge.get_config()
        self.datastore = forge.get_datastore(self.config)

        self.scheduler = BackgroundScheduler(daemon=True)

        self.redis = get_client(
            db=self.config.core.redis.nonpersistent.db,
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )
        self.redis_persist = get_client(
            db=self.config.core.redis.persistent.db,
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )

        self.metrics_queue = CommsQueue(METRICS_QUEUE, self.redis)
        self.status_queue = CommsQueue(STATUS_QUEUE, self.redis)
        self.dispatch_active_hash = Hash(DISPATCH_TASK_HASH, self.redis_persist)
        self.dispatcher_submission_queue = NamedQueue(SUBMISSION_QUEUE, self.redis)
        self.ingest_scanning = Hash('m-scanning-table', self.redis_persist)
        self.ingest_unique_queue = PriorityQueue('m-unique', self.redis_persist)
        self.ingest_queue = NamedQueue(INGEST_QUEUE_NAME, self.redis_persist)
        self.alert_queue = NamedQueue(ALERT_QUEUE_NAME, self.redis_persist)

        constants = forge.get_constants(self.config)
        self.c_rng = constants.PRIORITY_RANGES['critical']
        self.h_rng = constants.PRIORITY_RANGES['high']
        self.m_rng = constants.PRIORITY_RANGES['medium']
        self.l_rng = constants.PRIORITY_RANGES['low']
        self.c_s_at = self.config.core.ingester.sampling_at['critical']
        self.h_s_at = self.config.core.ingester.sampling_at['high']
        self.m_s_at = self.config.core.ingester.sampling_at['medium']
        self.l_s_at = self.config.core.ingester.sampling_at['low']

        self.to_expire = {k: 0 for k in metrics.EXPIRY_METRICS}
        if self.config.core.expiry.batch_delete:
            self.delete_query = f"expiry_ts:[* TO {self.datastore.ds.now}-{self.config.core.expiry.delay}" \
                f"{self.datastore.ds.hour}/DAY]"
        else:
            self.delete_query = f"expiry_ts:[* TO {self.datastore.ds.now}-{self.config.core.expiry.delay}" \
                f"{self.datastore.ds.hour}]"

        self.rolling_window = {}
        self.window_ttl = {}
        self.ttl = self.config.core.metrics.export_interval * 2
        self.window_size = int(60 / self.config.core.metrics.export_interval)
        if self.window_size != 60 / self.config.core.metrics.export_interval:
            self.log.warning("Cannot calculate a proper window size for reporting heartbeats. "
                             "Metrics reported during hearbeat will be wrong.")

    def try_run(self):
        self.scheduler.add_job(self._export_hearbeats, 'interval', seconds=self.config.core.metrics.export_interval)
        self.scheduler.add_job(self._reload_expiry_queues, 'interval',
                               seconds=self.config.core.metrics.export_interval * 4)
        self.scheduler.start()

        while self.running:
            for msg in self.metrics_queue.listen():
                m_name = msg.pop('name', None)
                m_type = msg.pop('type', None)
                m_host = msg.pop('host', None)
                msg.pop('instance', None)

                self.log.debug(f"Received {m_type.upper()} metrics message")
                if not m_name or not m_type:
                    continue

                w_key = (m_name, m_type, m_host)
                if w_key not in self.rolling_window:
                    self.rolling_window[w_key] = [Counter(msg)]
                else:
                    self.rolling_window[w_key].append(Counter(msg))

                self.window_ttl[w_key] = time.time() + self.ttl

    def _reload_expiry_queues(self):
        self.log.info("Refreshing expiry queues...")
        for collection_name in metrics.EXPIRY_METRICS:
            collection = getattr(self.datastore, collection_name)
            self.to_expire[collection_name] = collection.search(self.delete_query, rows=0, fl='id')['total']

    def _export_hearbeats(self):
        self.log.info("Expiring unused counters...")
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

                aggregated_counters[key]['instances'] += 1
                for c in counters_list:
                    aggregated_counters[key] += c

            else:
                self.log.warning(f"Skipping unknown component type: {c_type}")

        self.log.info("Generating heartbeats...")
        for aggregated_parts, counter in aggregated_counters.items():
            agg_c_name, agg_c_type = aggregated_parts
            allowed_fields = metrics.METRIC_TYPES[agg_c_type] + ['instances']

            if agg_c_type not in metrics.TIMED_METRICS:
                metrics_data = {k: counter.get(k, 0) for k in allowed_fields}
            else:
                metrics_data = {k: counter.get(k + ".t", 0) / counter.get(k + ".c", 1) for k in allowed_fields}
                metrics_data.update({k + "_count": counter.get(k + ".c", 0) for k in allowed_fields})

            if agg_c_type == "dispatcher":
                try:
                    msg = {
                        "sender": "legacy_heartbeat_manager",
                        "msg": {
                            "inflight": {
                                "max": self.config.core.dispatcher.max_inflight,
                                "outstanding": self.dispatch_active_hash.length()
                            },
                            "instances": metrics_data.pop('instances'),
                            "metrics": metrics_data,
                            "queues": {
                                "ingest": self.dispatcher_submission_queue.length()
                            }
                        }
                    }
                    self.status_queue.publish(DispatcherMessage(msg).as_primitives())
                    self.log.info(f"Sent dispatcher heartbeat: {msg['msg']}")
                except Exception:
                    self.log.exception("An exception occurred while generating DispatcherMessage")

            elif agg_c_type == "ingester":
                try:
                    c_q_len = self.ingest_unique_queue.count(*self.c_rng)
                    h_q_len = self.ingest_unique_queue.count(*self.h_rng)
                    m_q_len = self.ingest_unique_queue.count(*self.m_rng)
                    l_q_len = self.ingest_unique_queue.count(*self.l_rng)

                    msg = {
                        "sender": "legacy_heartbeat_manager",
                        "msg": {
                            "instances": metrics_data.pop('instances'),
                            "metrics": metrics_data,
                            "processing": {
                                "inflight": self.ingest_scanning.length()
                            },
                            "processing_chance": {
                                "critical": 1 - drop_chance(c_q_len, self.c_s_at),
                                "high": 1 - drop_chance(h_q_len, self.h_s_at),
                                "low": 1 - drop_chance(l_q_len, self.l_s_at),
                                "medium": 1 - drop_chance(m_q_len, self.m_s_at)
                            },
                            "queues": {
                                "critical": c_q_len,
                                "high": h_q_len,
                                "ingest": self.ingest_queue.length(),
                                "low": l_q_len,
                                "medium": m_q_len
                            }
                        }
                    }
                    self.status_queue.publish(IngestMessage(msg).as_primitives())
                    self.log.info(f"Sent ingester heartbeat: {msg['msg']}")
                except Exception:
                    self.log.exception("An exception occurred while generating IngestMessage")

            elif agg_c_type == "alerter":
                try:
                    msg = {
                        "sender": "legacy_heartbeat_manager",
                        "msg": {
                            "instances": metrics_data.pop('instances'),
                            "metrics": metrics_data,
                            "queues": {
                                "alert": self.alert_queue.length()
                            }
                        }
                    }
                    self.status_queue.publish(AlerterMessage(msg).as_primitives())
                    self.log.info(f"Sent alerter heartbeat: {msg['msg']}")
                except Exception:
                    self.log.exception("An exception occurred while generating AlerterMessage")

            elif agg_c_type == "expiry":
                try:
                    msg = {
                        "sender": "legacy_heartbeat_manager",
                        "msg": {
                            "instances": metrics_data.pop('instances'),
                            "metrics": metrics_data,
                            "queues": self.to_expire
                        }
                    }
                    self.status_queue.publish(ExpiryMessage(msg).as_primitives())
                    self.log.info(f"Sent expiry heartbeat: {msg['msg']}")
                except Exception:
                    self.log.exception("An exception occurred while generating ExpiryMessage")

            else:
                self.log.info(f"HB: {agg_c_name} [{agg_c_type}] ==> {metrics_data}")
                # TODO: generate HB message and post it through status_queue
