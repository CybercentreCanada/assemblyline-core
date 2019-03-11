"""
The internal worker for ingester runs several routing processes

Things handled here:
 - Retrying after a delay
 - Processing timeouts

"""

import time

from assemblyline.common import forge, isotime

from al_core.ingester.ingester import Ingester, _dup_prefix
from al_core.server_base import ServerBase


class IngesterInternals(ServerBase):
    def __init__(self, logger=None, datastore=None, redis=None, persistent_redis=None):
        super().__init__('assemblyline.ingester.internals', logger=logger)
        # Connect to all sorts of things
        datastore = datastore or forge.get_datastore()
        classification_engine = forge.get_classification()

        # Initialize the ingester specific resources
        self.ingester = Ingester(datastore=datastore, classification=classification_engine, logger=self.log,
                                 redis=redis, persistent_redis=persistent_redis)

    def process_retries(self) -> int:
        tasks = self.ingester.retry_queue.dequeue_range(upper_limit=isotime.now(), num=10)

        for task in tasks:
            self.ingester.ingest_queue.push(task)

        return len(tasks)

    def process_timeouts(self):
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
                    ingester.ingest_timeout_counter.increment()
            except:
                self.log.exception("Problem timing out %s:", scan_key)

        return len(timeouts)

    def try_run(self):
        while self.running:
            retries_done = self.process_retries()
            timeouts_done = self.process_timeouts()

            if retries_done == 0 and timeouts_done == 0:
                time.sleep(0.5)


if __name__ == '__main__':
    IngesterInternals().serve_forever()
