#!/usr/bin/env python
import time

import elasticapm
import elasticsearch
import sys

from apscheduler.schedulers.background import BackgroundScheduler
from threading import RLock

from al_core.metrics.heartbeat_manager import HeartbeatManager
from al_core.server_base import ServerBase
from assemblyline.common import forge, metrics
from assemblyline.common.isotime import epoch_to_iso
from assemblyline.datastore import SearchException
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.counters import MetricCounter


class MetricCounterCache(object):
    def __init__(self, redis, log, apm_client, metrics_type):
        config = forge.get_config()
        self.scheduler = BackgroundScheduler(daemon=True)
        self.datastore = forge.get_datastore(config)
        self.metrics_counter_cache = {}
        self.redis = redis
        self.lock = RLock()
        self.log = log

        self.scheduler.add_job(self._update_metrics_counter_cache, 'interval', seconds=60)
        self.scheduler.start()

        self.apm_client = apm_client
        self.metrics_type = metrics_type

    def _update_metrics_counter_cache(self):
        self.log.info("Reloading metrics cache...")
        # APM Transaction start
        if self.apm_client:
            self.apm_client.begin_transaction(self.metrics_type)

        try:
            service_names = list(self.datastore.service_delta.keys())
        except SearchException:
            service_names = []

        with self.lock:
            old_keys = set(self.metrics_counter_cache.keys())
            new_keys = set()

            for mtype, mname_list in metrics.METRIC_TYPES.items():
                if "service" in mtype:
                    for srv in service_names:
                        new_keys.add((mtype, srv))
                        if (mtype, srv) in self.metrics_counter_cache:
                            continue

                        if mtype not in metrics.TIMED_METRICS:
                            self.metrics_counter_cache[(mtype, srv)] = {
                                name: MetricCounter(f"{mtype}.{srv}.{name}", host=self.redis) for name in mname_list
                            }
                        else:
                            self.metrics_counter_cache[(mtype, srv)] = {
                                f"{name}_count": MetricCounter(f"{mtype}.{srv}.{name}.c", host=self.redis) for name in
                                mname_list
                            }
                            self.metrics_counter_cache[(mtype, srv)].update({
                                name: MetricCounter(f"{mtype}.{srv}.{name}.t", host=self.redis) for name in mname_list
                            })
                else:
                    new_keys.add((mtype, mtype))
                    if (mtype, mtype) in self.metrics_counter_cache:
                        continue

                    self.metrics_counter_cache[(mtype, mtype)] = {
                        name: MetricCounter(f"{mtype}.{mtype}.{name}", host=self.redis) for name in mname_list
                    }

            for to_del in old_keys.difference(new_keys):
                self.log.warning(f"Not tracking metric {to_del[1]} of type {to_del[0]} anymore...")
                del self.metrics_counter_cache[to_del]

            self.log.info("Done updating metrics cache.")

        # APM Transaction end
        if self.apm_client:
            self.apm_client.end_transaction('update_metrics_cache', 'success')

    def get_metrics_counters(self):
        if not self.metrics_counter_cache:
            self._update_metrics_counter_cache()

        return self.metrics_counter_cache

    def get_lock(self):
        return self.lock


class HashMapMetricsServer(ServerBase):
    def __init__(self, config=None):
        super().__init__('assemblyline.metrics_aggregator', shutdown_timeout=60)
        self.config = config or forge.get_config()
        self.elastic_hosts = self.config.core.metrics.elasticsearch.hosts

        if not self.elastic_hosts:
            self.log.error("No elasticsearch cluster defined to store metrics. All gathered stats will be ignored...")
            sys.exit(1)

        self.datastore = forge.get_datastore(self.config)
        self.redis = get_client(
            db=self.config.core.redis.nonpersistent.db,
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )
        self.es = elasticsearch.Elasticsearch(hosts=self.elastic_hosts)

        self.metrics_type = 'metrics'
        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="metrics_aggregator")
        else:
            self.apm_client = None

        self.mcc = MetricCounterCache(self.redis, self.log, self.apm_client, self.metrics_type)

    def try_run(self):
        while self.running:
            # APM Transaction start
            if self.apm_client:
                self.apm_client.begin_transaction(self.metrics_type)

            start = time.time()
            timestamp = epoch_to_iso(start)

            self.log.info("Aggregating metrics ...")
            with self.mcc.get_lock():
                for counter_meta, counters in self.mcc.get_metrics_counters().items():
                    mtype, mname = counter_meta
                    output_metrics = {
                        'name': mname,
                        'type': mtype,
                        'timestamp': timestamp
                    }

                    # TODO: For now metrics are not reported at the timestamp they ran on...
                    #       this could be changed but it is much easier to sum them up
                    #       and assume metrics aggregator is always running
                    output_metrics.update({
                        ct_name: sum(ct.pop_expired().values()) or 0
                        for ct_name, ct in counters.items()
                    })

                    self.log.info(output_metrics)
                    try:
                        index_time = timestamp[:10].replace("-", ".")
                        self.es.index(f"al_metrics_{mtype}-{index_time}", mtype, output_metrics)
                    except Exception as e:
                        self.log.exception(e)

            # Run exactly every 60 seconds so we have to remove our execution time from our sleep
            next_run = max(60 - (time.time()-start), 1)

            self.log.info(f"Metrics aggregated. Waiting for next run in {int(next_run)} seconds...")

            # APM Transaction end
            if self.apm_client:
                self.apm_client.end_transaction('aggregate_metrics', 'success')

            time.sleep(next_run)


class HashMapHeartbeatManager(ServerBase):
    def __init__(self, config=None):
        super().__init__('assemblyline.heartbeat_manager', shutdown_timeout=60)
        self.config = config or forge.get_config()
        self.redis = get_client(
            db=self.config.core.redis.nonpersistent.db,
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )

        self.hm = HeartbeatManager("hashmap_heartbeat_manager", self.log, config=self.config, redis=self.redis)

        self.metrics_type = 'heartbeat'
        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="heartbeat_manager")
        else:
            self.apm_client = None

        self.mcc = MetricCounterCache(self.redis, self.log, self.apm_client, self.metrics_type)

    def try_run(self):
        while self.running:
            # APM Transaction start
            if self.apm_client:
                self.apm_client.begin_transaction(self.metrics_type)

            start = time.time()

            self.log.info("Generating heartbeats ...")
            with self.mcc.get_lock():
                for counter_meta, counters in self.mcc.get_metrics_counters().items():
                    # TODO: For now we do not have any way to know how many instances we have running of a given counter
                    minstances = 1

                    mtype, mname = counter_meta
                    with elasticapm.capture_span(name=f"{mtype}.{mname}", span_type="send_heartbeat"):
                        hb_metrics = {}
                        hb_metrics.update({
                            ct_name: ct.read()
                            for ct_name, ct in counters.items()
                        })

                        self.hm.send_heartbeat(mtype, mname, hb_metrics, minstances)

            # Run exactly every "export_interval" seconds so we have to remove our execution time from our sleep
            next_run = max(self.config.core.metrics.export_interval - (time.time()-start), 1)

            # APM Transaction end
            if self.apm_client:
                self.apm_client.end_transaction('send_heartbeats', 'success')

            time.sleep(next_run)
