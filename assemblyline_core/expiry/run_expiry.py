#!/usr/bin/env python

import concurrent.futures
import time

import elasticapm

from assemblyline.common.isotime import now_as_iso
from assemblyline_core.server_base import ServerBase
from assemblyline.common import forge
from assemblyline.common.metrics import MetricsFactory
from assemblyline.filestore import FileStore
from assemblyline.odm.messages.expiry_heartbeat import Metrics


class ExpiryManager(ServerBase):
    def __init__(self):
        self.config = forge.get_config()
        super().__init__('assemblyline.expiry', shutdown_timeout=self.config.core.expiry.sleep_time + 5)
        self.datastore = forge.get_datastore(config=self.config, archive_access=True)
        self.filestore = forge.get_filestore(config=self.config)
        self.cachestore = FileStore(*self.config.filestore.cache)
        self.expirable_collections = []
        self.archiveable_collections = []
        self.counter = MetricsFactory('expiry', Metrics)
        self.counter_archive = MetricsFactory('archive', Metrics)

        self.fs_hashmap = {
            'file': self.filestore.delete,
            'cached_file': self.cachestore.delete
        }

        for name, definition in self.datastore.ds.get_models().items():
            if hasattr(definition, 'archive_ts'):
                self.archiveable_collections.append(getattr(self.datastore, name))
            if hasattr(definition, 'expiry_ts'):
                self.expirable_collections.append(getattr(self.datastore, name))

        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="expiry")
        else:
            self.apm_client = None

    def close(self):
        if self.counter:
            self.counter.stop()

        if self.apm_client:
            elasticapm.uninstrument()

    def run_expiry_once(self):
        now = now_as_iso()
        delay = self.config.core.expiry.delay
        hour = self.datastore.ds.hour
        day = self.datastore.ds.day

        # Expire data
        for collection in self.expirable_collections:
            # Call heartbeat pre-dated by 5 minutes. If a collection takes more than
            # 5 minutes to expire, this container could be seen as unhealthy. The down
            # side is if it is stuck on something it will be more than 5 minutes before
            # the container is restarted.
            self.heartbeat(int(time.time() + 5 * 60))

            # Start of expiry transaction
            if self.apm_client:
                self.apm_client.begin_transaction("Delete expired documents")

            if self.config.core.expiry.batch_delete:
                delete_query = f"expiry_ts:[* TO {now}||-{delay}{hour}/{day}]"
            else:
                delete_query = f"expiry_ts:[* TO {now}||-{delay}{hour}]"

            number_to_delete = collection.search(delete_query, rows=0, as_obj=False)['total']

            if self.apm_client:
                elasticapm.tag(query=delete_query)
                elasticapm.tag(number_to_delete=number_to_delete)

            self.log.info(f"Processing collection: {collection.name}")
            if number_to_delete != 0:
                if self.config.core.expiry.delete_storage and collection.name in self.fs_hashmap:
                    with elasticapm.capture_span(name=f'FILESTORE [ThreadPoolExecutor] :: delete()',
                                                 labels={"num_files": number_to_delete,
                                                         "query": delete_query}):
                        # Delete associated files
                        with concurrent.futures.ThreadPoolExecutor(self.config.core.expiry.workers) as executor:
                            res = {item['id']: executor.submit(self.fs_hashmap[collection.name], item['id'])
                                   for item in collection.stream_search(delete_query, fl='id', as_obj=False)}
                        for v in res.values():
                            v.result()
                        self.log.info(f'    Deleted associated files from the '
                                      f'{"cachestore" if "cache" in collection.name else "filestore"}...')

                # Proceed with deletion
                collection.delete_matching(delete_query, workers=self.config.core.expiry.workers)
                self.counter.increment(f'{collection.name}', increment_by=number_to_delete)

                self.log.info(f"    Deleted {number_to_delete} items from the datastore...")

            else:
                self.log.debug("    Nothing to delete in this collection.")

            # End of expiry transaction
            if self.apm_client:
                self.apm_client.end_transaction(collection.name, 'deleted')

    def run_archive_once(self):
        if not self.config.datastore.ilm.enabled:
            return

        now = now_as_iso()
        # Archive data
        for collection in self.archiveable_collections:
            # Start of expiry transaction
            if self.apm_client:
                self.apm_client.begin_transaction("Archive older documents")

            archive_query = f"archive_ts:[* TO {now}]"

            number_to_archive = collection.search(archive_query, rows=0, as_obj=False, use_archive=False)['total']

            if self.apm_client:
                elasticapm.tag(query=archive_query)
                elasticapm.tag(number_to_archive=number_to_archive)

            self.log.info(f"Processing collection: {collection.name}")
            if number_to_archive != 0:
                # Proceed with archiving
                collection.archive(archive_query)
                self.counter_archive.increment(f'{collection.name}', increment_by=number_to_archive)

                self.log.info(f"    Archived {number_to_archive} items to the time sliced storage...")

            else:
                self.log.debug("    Nothing to archive in this collection.")

            # End of expiry transaction
            if self.apm_client:
                self.apm_client.end_transaction(collection.name, 'archived')

    def try_run(self):
        while self.running:
            try:
                self.run_expiry_once()
            except Exception as e:
                self.log.exception(str(e))

            try:
                self.run_archive_once()
            except Exception as e:
                self.log.exception(str(e))

            self.sleep_with_heartbeat(self.config.core.expiry.sleep_time)


if __name__ == "__main__":
    with ExpiryManager() as em:
        em.serve_forever()
