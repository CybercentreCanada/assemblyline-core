#!/usr/bin/env python

import concurrent.futures
import elasticapm
import time

from datemath import dm

from assemblyline.common.isotime import epoch_to_iso, now_as_iso
from assemblyline_core.server_base import ServerBase
from assemblyline.common import forge
from assemblyline.common.metrics import MetricsFactory
from assemblyline.filestore import FileStore
from assemblyline.odm.messages.expiry_heartbeat import Metrics

ARCHIVE_SIZE = 10000
EXPIRY_SIZE = 10000


class ExpiryManager(ServerBase):
    def __init__(self, force_ilm=False):
        self.config = forge.get_config()
        if force_ilm:
            self.config.datastore.ilm.enabled = True

        super().__init__('assemblyline.expiry', shutdown_timeout=self.config.core.expiry.sleep_time + 5)
        self.datastore = forge.get_datastore(config=self.config, archive_access=True)
        self.hot_datastore = forge.get_datastore(config=self.config, archive_access=False)
        self.filestore = forge.get_filestore(config=self.config)
        self.cachestore = FileStore(*self.config.filestore.cache)
        self.expirable_collections = []
        self.archiveable_collections = []
        self.counter = MetricsFactory('expiry', Metrics)
        self.counter_archive = MetricsFactory('archive', Metrics)

        if self.config.datastore.ilm.enabled:
            self.fs_hashmap = {
                'file': self.archive_filestore_delete,
                'cached_file': self.archive_cachestore_delete
            }
        else:
            self.fs_hashmap = {
                'file': self.filestore_delete,
                'cached_file': self.cachestore_delete
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

    def stop(self):
        if self.counter:
            self.counter.stop()

        if self.apm_client:
            elasticapm.uninstrument()
        super().stop()

    def filestore_delete(self, sha256, _):
        self.filestore.delete(sha256)

    def archive_filestore_delete(self, sha256, expiry_time):
        # If we are working with an archive, their may be a hot entry that expires later.
        doc = self.hot_datastore.file.get_if_exists(sha256, as_obj=False)
        if doc and doc['expiry_ts'] > expiry_time:
            return
        self.filestore.delete(sha256)

    def cachestore_delete(self, sha256, _):
        self.filestore.delete(sha256)

    def archive_cachestore_delete(self, sha256, expiry_time):
        doc = self.hot_datastore.cached_file.get_if_exists(sha256, as_obj=False)
        if doc and doc['expiry_ts'] > expiry_time:
            return
        self.cachestore.delete(sha256)

    def run_expiry_once(self):
        now = now_as_iso()
        reached_max = False

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
                computed_date = epoch_to_iso(dm(f"{now}||-{self.config.core.expiry.delay}h/d").float_timestamp)
            else:
                computed_date = epoch_to_iso(dm(f"{now}||-{self.config.core.expiry.delay}h").float_timestamp)

            delete_query = f"expiry_ts:[* TO {computed_date}]"

            if self.config.core.expiry.delete_storage and collection.name in self.fs_hashmap:
                file_delete = True
                sort = ["expiry_ts asc", "id asc"]
            else:
                file_delete = False
                sort = None

            number_to_delete = collection.search(delete_query, rows=0, as_obj=False, use_archive=True,
                                                 sort=sort, track_total_hits=EXPIRY_SIZE)['total']

            if self.apm_client:
                elasticapm.label(query=delete_query)
                elasticapm.label(number_to_delete=number_to_delete)

            self.log.info(f"Processing collection: {collection.name}")
            if number_to_delete != 0:
                if file_delete:
                    with elasticapm.capture_span(name='FILESTORE [ThreadPoolExecutor] :: delete()',
                                                 labels={"num_files": number_to_delete,
                                                         "query": delete_query}):
                        # Delete associated files
                        with concurrent.futures.ThreadPoolExecutor(self.config.core.expiry.workers,
                                                                   thread_name_prefix="file_delete") as executor:
                            for item in collection.search(delete_query, fl='id', rows=number_to_delete,
                                                          sort=sort, use_archive=True, as_obj=False)['items']:
                                executor.submit(self.fs_hashmap[collection.name], item['id'], computed_date)

                        self.log.info(f'    Deleted associated files from the '
                                      f'{"cachestore" if "cache" in collection.name else "filestore"}...')

                    # Proceed with deletion
                    collection.delete_by_query(delete_query, workers=self.config.core.expiry.workers,
                                               sort=sort, max_docs=number_to_delete)

                else:
                    # Proceed with deletion
                    collection.delete_by_query(delete_query, workers=self.config.core.expiry.workers)

                if number_to_delete == EXPIRY_SIZE:
                    reached_max = True

                self.counter.increment(f'{collection.name}', increment_by=number_to_delete)

                self.log.info(f"    Deleted {number_to_delete} items from the datastore...")

            else:
                self.log.debug("    Nothing to delete in this collection.")

            # End of expiry transaction
            if self.apm_client:
                self.apm_client.end_transaction(collection.name, 'deleted')

        return reached_max

    def run_archive_once(self):
        reached_max = False
        if not self.config.datastore.ilm.enabled:
            return reached_max

        now = now_as_iso()
        # Archive data
        for collection in self.archiveable_collections:
            # Call heartbeat pre-dated by 5 minutes. If a collection takes more than
            # 5 minutes to expire, this container could be seen as unhealthy. The down
            # side is if it is stuck on something it will be more than 5 minutes before
            # the container is restarted.
            self.heartbeat(int(time.time() + 5 * 60))

            # Start of expiry transaction
            if self.apm_client:
                self.apm_client.begin_transaction("Archive older documents")

            archive_query = f"archive_ts:[* TO {now}]"
            sort = ["archive_ts asc", "id asc"]

            number_to_archive = collection.search(archive_query, rows=0, as_obj=False,
                                                  use_archive=False, sort=sort,
                                                  track_total_hits=ARCHIVE_SIZE)['total']

            if number_to_archive == ARCHIVE_SIZE:
                reached_max = True

            if self.apm_client:
                elasticapm.label(query=archive_query)
                elasticapm.label(number_to_archive=number_to_archive)

            self.log.info(f"Processing collection: {collection.name}")
            if number_to_archive != 0:
                # Proceed with archiving
                if collection.archive(archive_query, max_docs=number_to_archive, sort=sort):
                    self.counter_archive.increment(f'{collection.name}', increment_by=number_to_archive)
                    self.log.info(f"    Archived {number_to_archive} documents...")
                else:
                    self.log.warning(f"    Failed to properly archive {number_to_archive} documents...")

            else:
                self.log.debug("    Nothing to archive in this collection.")

            # End of expiry transaction
            if self.apm_client:
                self.apm_client.end_transaction(collection.name, 'archived')

        return reached_max

    def try_run(self):
        while self.running:
            expiry_maxed_out = False
            archive_maxed_out = False
            try:
                expiry_maxed_out = self.run_expiry_once()
            except Exception as e:
                self.log.exception(str(e))

            try:
                archive_maxed_out = self.run_archive_once()
            except Exception as e:
                self.log.exception(str(e))

            if not expiry_maxed_out and not archive_maxed_out:
                self.sleep_with_heartbeat(self.config.core.expiry.sleep_time)


if __name__ == "__main__":
    with ExpiryManager() as em:
        em.serve_forever()
