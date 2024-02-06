#!/usr/bin/env python
import elasticapm
import os
import tempfile

from assemblyline.common import forge
from assemblyline.common.archiving import ARCHIVE_QUEUE_NAME
from assemblyline.common.metrics import MetricsFactory
from assemblyline.datastore.collection import ESCollection, Index
from assemblyline.odm.messages.archive_heartbeat import Metrics
from assemblyline.odm.models.submission import Submission
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.named import NamedQueue

from assemblyline_core.server_base import ServerBase


class SubmissionNotFound(Exception):
    pass


class Archiver(ServerBase):
    def __init__(self):
        super().__init__('assemblyline.archiver')
        self.apm_client = None
        self.counter = None

        if self.config.datastore.archive.enabled:
            # Publish counters to the metrics sink.
            self.counter = MetricsFactory('archiver', Metrics)
            self.datastore = forge.get_datastore(self.config, archive_access=True)
            self.filestore = forge.get_filestore(config=self.config)
            self.archivestore = forge.get_archivestore(config=self.config)
            self.persistent_redis = get_client(
                host=self.config.core.redis.persistent.host,
                port=self.config.core.redis.persistent.port,
                private=False,
            )

            self.archive_queue: NamedQueue[dict] = NamedQueue(ARCHIVE_QUEUE_NAME, self.persistent_redis)
            if self.config.core.metrics.apm_server.server_url is not None:
                self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
                elasticapm.instrument()
                self.apm_client = forge.get_apm_client("archiver")
        else:
            self.log.warning("Archive is not enabled in the config, no need to run archiver.")
            exit()

    def stop(self):
        if self.counter:
            self.counter.stop()

        if self.apm_client:
            elasticapm.uninstrument()
        super().stop()

    def run_once(self):
        message = self.archive_queue.pop(timeout=1)

        # If there is no alert bail out
        if not message:
            return
        else:
            try:
                archive_type, type_id, delete_after = message
                self.counter.increment('received')
            except Exception:
                self.log.error(f"Invalid message received: {message}")
                return

        # Start of process alert transaction
        if self.apm_client:
            self.apm_client.begin_transaction('Process archive message')

        try:
            if archive_type == "submission":
                self.counter.increment('submission')
                # Load submission
                submission: Submission = self.datastore.submission.get_if_exists(type_id)
                if not submission:
                    raise SubmissionNotFound(type_id)

                self.datastore.submission.archive(type_id, delete_after=delete_after)
                if not delete_after:
                    self.datastore.submission.update(type_id, [(ESCollection.UPDATE_SET, 'archived', True)],
                                                     index_type=Index.HOT)

                # Gather list of files and archives them
                files = {(f.sha256, False) for f in submission.files}
                files.update(self.datastore.get_file_list_from_keys(submission.results))
                for sha256, supplementary in files:
                    self.counter.increment('file')

                    # Get the tags for this file
                    tags = self.datastore.get_tag_list_from_keys(
                        [r for r in submission.results if r.startswith(sha256)])
                    attributions = {x['value'] for x in tags if x['type'].startswith('attribution.')}
                    techniques = {x['type'].rsplit('.', 1)[1] for x in tags if x['type'].startswith('technique.')}
                    infos = {'ioc' for x in tags if x['type'] in self.config.submission.tag_types.ioc}
                    infos = infos.union({'password' for x in tags if x['type'] == 'info.password'})

                    # Create the archive file
                    self.datastore.file.archive(sha256, delete_after=delete_after, allow_missing=True)

                    # Auto-Labelling
                    operations = []

                    # Create default labels
                    operations += [(self.datastore.file.UPDATE_APPEND_IF_MISSING, 'labels', x) for x in attributions]
                    operations += [(self.datastore.file.UPDATE_APPEND_IF_MISSING, 'labels', x) for x in techniques]
                    operations += [(self.datastore.file.UPDATE_APPEND_IF_MISSING, 'labels', x) for x in infos]

                    # Create type specific labels
                    operations += [
                        (self.datastore.file.UPDATE_APPEND_IF_MISSING, 'label_categories.attribution', x)
                        for x in attributions]
                    operations += [
                        (self.datastore.file.UPDATE_APPEND_IF_MISSING, 'label_categories.technique', x)
                        for x in techniques]
                    operations += [
                        (self.datastore.file.UPDATE_APPEND_IF_MISSING, 'label_categories.info', x)
                        for x in infos]

                    # Set the is_supplementary property
                    operations += [(self.datastore.file.UPDATE_SET, 'is_supplementary', supplementary)]

                    # Apply auto-created labels
                    self.datastore.file.update(sha256, operations=operations, index_type=Index.ARCHIVE)
                    self.datastore.file.update(sha256, operations=operations, index_type=Index.HOT)

                    if self.filestore != self.archivestore:
                        with tempfile.NamedTemporaryFile() as buf:
                            try:
                                self.filestore.download(sha256, buf.name)
                                if os.path.getsize(buf.name):
                                    self.archivestore.upload(buf.name, sha256)
                            except Exception as e:
                                self.log.error(
                                    f"Could not copy file {sha256} from the filestore to the archivestore. ({e})")

                # Archive associated results (Skip emptys)
                for r in submission.results:
                    if not r.endswith(".e"):
                        self.counter.increment('result')
                        self.datastore.result.archive(r, delete_after=delete_after, allow_missing=True)

                # End of process alert transaction (success)
                self.log.info(f"Successfully archived submission '{type_id}'.")
                if self.apm_client:
                    self.apm_client.end_transaction(archive_type, 'success')

            # Invalid archiving type
            else:
                self.counter.increment('invalid')
                self.log.warning(f"'{archive_type}' is not a valid archive type.")
                # End of process alert transaction (success)
                if self.apm_client:
                    self.apm_client.end_transaction(archive_type, 'invalid')

        except SubmissionNotFound:
            self.counter.increment('not_found')
            self.log.warning(f"Could not archive {archive_type} '{type_id}'. It was not found in the system.")
            # End of process alert transaction (failure)
            if self.apm_client:
                self.apm_client.end_transaction(archive_type, 'not_found')

        except Exception:  # pylint: disable=W0703
            self.counter.increment('exception')
            self.log.exception(f'Unhandled exception processing {archive_type} ID: {type_id}')

            # End of process alert transaction (failure)
            if self.apm_client:
                self.apm_client.end_transaction(archive_type, 'exception')

    def try_run(self):
        while self.running:
            self.heartbeat()
            self.run_once()


if __name__ == "__main__":
    with Archiver() as archiver:
        archiver.serve_forever()
