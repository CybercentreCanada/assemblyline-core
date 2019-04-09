"""
The internal worker for ingester runs several routing processes

Things handled here:
 - Retrying after a delay
 - Processing timeouts

"""

import elasticapm
import time

from assemblyline.common import forge, isotime

from al_core.ingester.ingester import Ingester, _dup_prefix
from al_core.server_base import ServerBase


# noinspection PyBroadException
class IngesterInternals(ServerBase):
    def __init__(self, logger=None, datastore=None, redis=None, persistent_redis=None):
        super().__init__('assemblyline.ingester.internals', logger=logger)
        config = forge.get_config()
        # Connect to all sorts of things
        datastore = datastore or forge.get_datastore(config)
        classification_engine = forge.get_classification()

        # Initialize the ingester specific resources
        self.ingester = Ingester(datastore=datastore, classification=classification_engine, logger=self.log,
                                 redis=redis, persistent_redis=persistent_redis)

        if config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=config.core.metrics.apm_server.server_url,
                                                service_name="ingester")
        else:
            self.apm_client = None

    def close(self):
        if self.apm_client:
            elasticapm.uninstrument()

    def process_retries(self) -> int:
        # Start of ingest message
        if self.apm_client:
            self.apm_client.begin_transaction('ingest_msg')

        tasks = self.ingester.retry_queue.dequeue_range(upper_limit=isotime.now(), num=10)

        for task in tasks:
            self.ingester.ingest_queue.push(task)

        # End of ingest message (success)
        if self.apm_client:
            elasticapm.tag(retries=len(tasks))
            self.apm_client.end_transaction('ingest_retries', 'success')

        return len(tasks)

    def process_timeouts(self):
        # Start of ingest message
        if self.apm_client:
            self.apm_client.begin_transaction('ingest_msg')

        ingester = self.ingester
        timeouts = ingester.timeout_queue.dequeue_range(upper_limit=isotime.now(), num=10)

        for scan_key in timeouts:
            try:
                actual_timeout = False

                # Remove the entry from the hash of submissions in progress.
                entry = ingester.scanning.pop(scan_key)
                if entry:
                    actual_timeout = True
                    self.log.error("Submission timed out for %s: %s", scan_key, str(entry))

                dup = ingester.duplicate_queue.pop(_dup_prefix + scan_key, blocking=False)
                if dup:
                    actual_timeout = True

                while dup:
                    self.log.error("Submission timed out for %s: %s", scan_key, str(dup))
                    dup = ingester.duplicate_queue.pop(_dup_prefix + scan_key, blocking=False)

                if actual_timeout:
                    ingester.counter.increment('ingest_timeout')
            except Exception:
                self.log.exception("Problem timing out %s:", scan_key)

        # End of ingest message (success)
        if self.apm_client:
            elasticapm.tag(timeouts=len(timeouts))
            self.apm_client.end_transaction('ingest_timeouts', 'success')

        return len(timeouts)

    def try_run(self):
        while self.running:
            retries_done = self.process_retries()
            timeouts_done = self.process_timeouts()

            if retries_done == 0 and timeouts_done == 0:
                time.sleep(0.5)


if __name__ == '__main__':
    IngesterInternals().serve_forever()
