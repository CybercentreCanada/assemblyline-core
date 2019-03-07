"""
An ingest worker is responsible for processing input messages to the ingester.

These are:
 - Messages from dispatcher saying that jobs are complete.
 - Messages from THE OUTSIDE telling us to scan files.

"""
import json

from assemblyline.common import forge
from assemblyline.common import log

from al_core.ingester.ingester import Ingester, IngestTask
from al_core.server_base import ServerBase
from assemblyline.odm.models.submission import Submission
from assemblyline.odm.messages.submission import Submission as SubmissionInput, SubmissionMessage


class IngesterInput(ServerBase):
    def __init__(self, logger=None, datastore=None, redis=None, persistent_redis=None):
        super().__init__('assemblyline.ingester.ingester', logger)
        # Connect to all sorts of things
        datastore = datastore or forge.get_datastore()
        classification_engine = forge.get_classification()

        # Initialize the ingester specific resources
        self.ingester = Ingester(datastore=datastore, classification=classification_engine, logger=self.log,
                                 redis=redis, persistent_redis=persistent_redis)

    def try_run(self, volatile=False):
        ingester = self.ingester

        # Move from ingest to unique and waiting queues.
        # While there are entries in the ingest queue we consume chunk_size
        # entries at a time and move unique entries to uniqueq / queued and
        # duplicates to their own queues / waiting.
        while self.running:
            while True:
                result = ingester.complete_queue.pop(blocking=False)
                if not result:
                    break

                ingester.completed(Submission(result))

            message = ingester.ingest_queue.pop(timeout=1)
            if not message:
                continue

            try:
                sub = SubmissionInput(message)
                # Write all input to the traffic queue
                ingester.traffic_queue.publish(SubmissionMessage({
                    'msg': sub,
                    'msg_type': 'SubmissionIngested',
                    'sender': 'ingester',
                }).as_primitives())

                task = IngestTask(dict(
                    submission=sub,
                    ingest_id=sub.sid,
                ))
                task.submission.sid = None  # Reset to new random uuid

            except ValueError:
                self.log.warning(f"Dropped ingest submission {message}")
                if volatile:
                    raise
                continue

            if any(len(file.sha256) != 64 for file in task.submission.files):
                self.log.error(f"Invalid sha256: {[file.sha256 for file in task.submission.files]}")
                continue

            for file in task.submission.files:
                file.sha256 = file.sha256.lower()

            ingester.ingest(task)


if __name__ == '__main__':
    log.init_logging("ingester")
    IngesterInput().serve_forever()

