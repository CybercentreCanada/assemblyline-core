#!/usr/bin/env python
import time

import elasticsearch
import sys

from apscheduler.schedulers.background import BackgroundScheduler

from al_core.server_base import ServerBase
from assemblyline.common import forge

METRICS_QUEUE = "assemblyline_metrics"
STATUS_QUEUE = "status"

class HashMapMetricsServer(ServerBase):
    def __init__(self, config=None):
        super().__init__('assemblyline.metrics_aggregator')
        self.config = config or forge.get_config()

        self.elastic_hosts = self.config.core.metrics.elasticsearch.hosts

        if not self.elastic_hosts:
            self.log.error("No elasticsearch cluster defined to store metrics. All gathered stats will be ignored...")
            sys.exit(1)

        self.scheduler = BackgroundScheduler(daemon=True)
        self.es = None

    def try_run(self):
        self.es = elasticsearch.Elasticsearch(hosts=self.elastic_hosts)

        self.scheduler.add_job(self._create_aggregated_metrics, 'interval', seconds=60)
        self.scheduler.start()

        while self.running:
            # TODO: Create heartbeats and dispatch them
            time.sleep(self.config.core.metrics.export_interval)

    def _create_aggregated_metrics(self):
        pass

class HashMapHeartbeatManager(ServerBase):
    def __init__(self, config=None):
        super().__init__('assemblyline.heartbeat_manager')
        self.config = config or forge.get_config()

    def try_run(self):
        pass
