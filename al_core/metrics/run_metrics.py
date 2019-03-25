#!/usr/bin/env python

import elasticsearch
import json
import sys
import copy

from apscheduler.schedulers.background import BackgroundScheduler
from collections import Counter
from threading import Lock

from al_core.server_base import ServerBase
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


class MetricsServer(ServerBase):
    # Metrics keys
    ALERT_METRICS = ['alert.received', 'alert.err_no_submission', 'alert.heavy_ignored', 'alert.proto_http',
                     'alert.proto_smtp', 'alert.proto_other', 'alert.saved']
    DISPATCH_METRICS = ['dispatch.files_completed']
    EXPIRY_METRICS = ['expiry.alert', 'expiry.emptyresult', 'expiry.error', 'expiry.file', 'expiry.filescore',
                      'expiry.result', 'expiry.submission']
    INGEST_METRICS = ['ingest.duplicates', 'ingest.bytes_ingested', 'ingest.submissions_ingested', 'ingest.error',
                      'ingest.timed_out', 'ingest.submissions_completed', 'ingest.files_completed',
                      'ingest.bytes_completed', 'ingest.skipped', 'ingest.whitelisted']
    SRV_METRICS = ['svc.cache_hit', 'svc.cache_miss', 'svc.cache_skipped', 'svc.execute_start', 'svc.execute_done',
                   'svc.execute_fail_recov', 'svc.execute_fail_nonrecov', 'svc.job_scored', 'svc.job_not_scored']

    # Types of metrics
    METRIC_TYPES = {'alerter': ALERT_METRICS,
                    'dispatcher': DISPATCH_METRICS,
                    'expiry': EXPIRY_METRICS,
                    'ingester': INGEST_METRICS,
                    'services': SRV_METRICS}

    TIMED_METRICS = []

    def __init__(self):
        super().__init__('assemblyline.metrics', shutdown_timeout=65)
        self.config = forge.get_config()

        self.elastic_ip = self.config.core.metrics.elasticsearch.host
        self.elastic_port = self.config.core.metrics.elasticsearch.port

        if not self.elastic_ip or not self.elastic_port:
            self.log.error("No elasticsearch cluster defined to store metrics. All gathered stats will be ignored...")
            sys.exit(1)

        self.scheduler = BackgroundScheduler(daemon=True)
        self.metrics_queue = None
        self.es = None

        # Add extra fields to gather metrics for
        # self.METRIC_TYPES['alerter'].extend(self.config.core.metrics.extra_metrics.alerter)
        # self.METRIC_TYPES['dispatcher'].extend(self.config.core.metrics.extra_metrics.dispatcher)
        # self.METRIC_TYPES['expiry'].extend(self.config.core.metrics.extra_metrics.expiry)
        # self.METRIC_TYPES['ingester'].extend(self.config.core.metrics.extra_metrics.ingester)
        # self.METRIC_TYPES['services'].extend(self.config.core.metrics.extra_metrics.services)

        self.counters_lock = Lock()
        self.counters = {}

    def try_run(self):

        self.metrics_queue = CommsQueue(METRICS_QUEUE)
        self.es = elasticsearch.Elasticsearch([{'host': self.elastic_ip, 'port': self.elastic_port}])

        self.scheduler.add_job(
            self._create_aggregated_metrics, 'interval',
            seconds=60, kwargs={"log": self.log})

        self.scheduler.start()

        while True:
            for metrics in self.metrics_queue.listen():
                metrics_name = metrics.pop('name', None)
                metrics_type = metrics.pop('type', None)
                metrics_host = metrics.pop('host', None)
                self.log.debug(f"Compiling metrics for component: {metrics_type}")
                _ = metrics.pop('instance', None)
                if not metrics_name or not metrics_type or not metrics_host:
                    continue

                with self.counters_lock:
                    if (metrics_name, metrics_type, metrics_host) not in self.counters:
                        self.counters[(metrics_name, metrics_type, metrics_host)] = Counter(metrics)
                    else:
                        self.counters[(metrics_name, metrics_type, metrics_host)] += Counter(metrics)

    def _create_aggregated_metrics(self, log):
        log.info("Copying counters.")
        with self.counters_lock:
            counter_copy = copy.deepcopy(self.counters)
            self.counters = {}

        log.info("Aggregating metrics.")
        timestamp = now_as_iso()
        for component, counts in counter_copy.items():
            component_name, component_type, component_host = component
            output_metrics = {'name': component_name,
                              'type': component_type,
                              'host': component_host}
            if component_type in self.METRIC_TYPES and component_type not in self.TIMED_METRICS:
                output_metrics.update({k: counts.get(k, 0) for k in self.METRIC_TYPES[component_type]})
            elif component_type in self.METRIC_TYPES and component_type in self.TIMED_METRICS:
                output_metrics.update({k: counts.get(k + ".t", 0) / counts.get(k + ".c", 1)
                                       for k in self.METRIC_TYPES[component_type]})
                output_metrics.update({k + "_count": counts.get(k + ".c", 0)
                                       for k in self.METRIC_TYPES[component_type]})
            else:
                log.info("Skipping unknown component type: {cpt}".format(cpt=component_type))
                continue
            output_metrics['timestamp'] = timestamp
            output_metrics = cleanup_metrics(output_metrics)
            index_time = timestamp[:10].replace("-", ".")

            log.info(output_metrics)
            try:
                self.es.index(f"al_metrics_{component_type}-{index_time}", component_type, output_metrics)
            except Exception as e:
                log.exception(e)

        log.info("Metrics aggregated... Waiting for next run.")


if __name__ == '__main__':
    with MetricsServer() as metricsd:
        metricsd.serve_forever()
