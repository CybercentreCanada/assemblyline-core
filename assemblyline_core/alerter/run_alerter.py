#!/usr/bin/env python

import elasticapm
import time

from assemblyline.common import forge
from assemblyline.common.isotime import now
from assemblyline.common.metrics import MetricsFactory
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.odm.messages.alerter_heartbeat import Metrics

from assemblyline_core.alerter.processing import SubmissionNotFinalized
from assemblyline_core.server_base import ServerBase

ALERT_QUEUE_NAME = 'm-alert'
MAX_RETRIES = 10


class Alerter(ServerBase):
    def __init__(self):
        super().__init__('assemblyline.alerter')
        # Publish counters to the metrics sink.
        self.counter = MetricsFactory('alerter', Metrics)
        self.datastore = forge.get_datastore(self.config)
        self.persistent_redis = get_client(
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

    def stop(self):
        if self.counter:
            self.counter.stop()

        if self.apm_client:
            elasticapm.uninstrument()
        super().stop()

    def run_once(self):
        alert = self.alert_queue.pop(timeout=1)

        # If there is no alert bail out
        if not alert:
            return

        # If there is a wait_until time set and we have not reached it,
        # push the alert back in the queue and sleep a little to reduce
        # the pressure on Redis
        if 'wait_until' in alert and alert['wait_until'] > now():
            self.alert_queue.push(alert)
            time.sleep(0.1)
            return

        # Start of process alert transaction
        if self.apm_client:
            self.apm_client.begin_transaction('Process alert message')

        self.counter.increment('received')
        try:
            alert_type = self.process_alert_message(self.counter, self.datastore, self.log, alert)

            # End of process alert transaction (success)
            if self.apm_client:
                self.apm_client.end_transaction(alert_type, 'success')

            return alert_type
        except SubmissionNotFinalized:  # pylint: disable=W0703
            self.counter.increment('wait')

            # Wait another 15 secs for the submission to complete
            alert['wait_until'] = now(15)
            self.alert_queue.push(alert)

            # End of process alert transaction (wait)
            if self.apm_client:
                self.apm_client.end_transaction('unknown', 'wait')
        except Exception:  # pylint: disable=W0703
            retries = alert['alert_retries'] = alert.get('alert_retries', 0) + 1
            self.counter.increment('error')
            if retries > MAX_RETRIES:
                self.log.exception(f'Max retries exceeded for: {alert}')
            else:
                self.alert_queue.push(alert)
                self.log.exception(f'Unhandled exception processing: {alert}')

            # End of process alert transaction (failure)
            if self.apm_client:
                self.apm_client.end_transaction('unknown', 'exception')

    def try_run(self):
        while self.running:
            self.heartbeat()
            self.run_once()


if __name__ == "__main__":
    with Alerter() as alerter:
        alerter.serve_forever()
