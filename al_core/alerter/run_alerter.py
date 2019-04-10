#!/usr/bin/env python

import elasticapm

from al_core.server_base import ServerBase
from assemblyline.common import forge
from assemblyline.common.metrics import MetricsFactory
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.named import NamedQueue

ALERT_QUEUE_NAME = 'm-alert'
MAX_RETRIES = 10


class Alerter(ServerBase):
    def __init__(self):
        super().__init__('assemblyline.alerter')
        self.config = forge.get_config()
        # Publish counters to the metrics sink.
        self.counter = MetricsFactory('alerter')
        self.datastore = forge.get_datastore(self.config)
        self.persistent_redis = get_client(
            db=self.config.core.redis.persistent.db,
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )
        self.process_alert_message = forge.get_process_alert_message()
        self.running = False

        self.alert_queue = NamedQueue(ALERT_QUEUE_NAME, self.persistent_redis)
        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="alerter")
        else:
            self.apm_client = None

    def close(self):
        if self.counter:
            self.counter.stop()

        if self.apm_client:
            elasticapm.uninstrument()

    def try_run(self):
        while self.running:
            alert = self.alert_queue.pop(timeout=1)
            if not alert:
                continue
            # Start of process alert transaction
            if self.apm_client:
                self.apm_client.begin_transaction('Process alert message')

            self.counter.increment('received')
            try:
                alert_type = self.process_alert_message(self.counter, self.datastore, self.log, alert)

                # End of process alert transaction (success)
                if self.apm_client:
                    self.apm_client.end_transaction(alert_type, 'success')

            except Exception as ex:  # pylint: disable=W0703
                retries = alert['alert_retries'] = alert.get('alert_retries', 0) + 1
                if retries > MAX_RETRIES:
                    self.log.exception(f'Max retries exceeded for: {alert}')
                else:
                    self.alert_queue.push(alert)
                    if 'Submission not finalized' not in str(ex):
                        self.log.exception(f'Unhandled exception processing: {alert}')

                # End of process alert transaction (failure)
                if self.apm_client:
                    self.apm_client.end_transaction('unknown', 'exception')


if __name__ == "__main__":
    with Alerter() as alerter:
        alerter.serve_forever()
