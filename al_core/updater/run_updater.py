import os
import sched
import shutil
import tempfile
import time

from al_core.server_base import ServerBase
from al_core.updater.url import url_update
from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.hash import Hash

SERVICE_SYNC_INTERVAL = 30
UPDATE_CHECK_INTERVAL = 5


FILE_UPDATE_DIRECTORY = os.environ.get('FILE_UPDATE_DIRECTORY', None)


class ServiceUpdater(ServerBase):
    def __init__(self, persistent_redis=None, logger=None, datastore=None):
        super().__init__('assemblyline.service.updater', logger=logger)

        if not FILE_UPDATE_DIRECTORY:
           raise RuntimeError("The updater process must be run within the orchestration environment, "
                              "the update volume must be mounted, and the path to the volume must be "
                              "set in the environment variable FILE_UPDATE_DIRECTORY. Setting "
                              "FILE_UPDATE_DIRECTORY directly may be done for testing.")

        # The directory where we want working temporary directories to be created.
        # Building our temporary directories in the persistent update volume may
        # have some performance down sides, but may help us run into fewer docker FS overlay
        # cleanup issues. Try to flush it out every time we start. This service should
        # be a singleton anyway.
        self.temporary_directory = os.path.join(FILE_UPDATE_DIRECTORY, '.tmp')
        shutil.rmtree(self.temporary_directory, ignore_errors=True)
        os.mkdir(self.temporary_directory)

        self.config = forge.get_config()
        self.datastore = datastore or forge.get_datastore()
        self.persistent_redis = persistent_redis or get_client(
            db=self.config.core.redis.persistent.db,
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )

        self.services = Hash('service-updates', self.persistent_redis)

        # Prepare a single threaded scheduler
        self.scheduler = sched.scheduler()


    def sync_services(self):
        self.scheduler.enter(SERVICE_SYNC_INTERVAL, 0, self.sync_services)

        # Get all the service data
        for service in self.datastore.list_all_services(full=True):

            # Ensure that any disabled services are not being updated
            if not service.enabled and self.services.exists(service.name):
                self.log.info(f"Service updates disabled for {service.name}")
                self.services.pop(service.name)

            # Ensure that any enabled services with an update config are being updated
            if service.enabled and service.update_config and not self.services.exists(service.name):
                self.log.info(f"Service updates enabled for {service.name}")
                self.services.add(
                    service.name,
                    dict(
                        next_update=now_as_iso(),
                        sha256='',
                    )
                )

    def try_run(self):
        # Do an initial call to the main methods, who will then be registered with the scheduler
        self.sync_services()
        self.update_services()

        # Run as long as we need to
        while self.running:
            delay = self.scheduler.run(False)
            time.sleep(min(delay, 0.02))

    def update_services(self):
        """Check if we need to update any services."""
        self.scheduler.enter(UPDATE_CHECK_INTERVAL, 0, self.update_services)

        for service_name, data in self.services.items():

            # Check if its time to check for new service update
            if data['next_update'] <= now_as_iso():
                # TODO when this code is parallelized, everything from here down is part of what gets spun out

                # Check for new update with service specified update method
                service = self.datastore.get_service_with_delta(service_name)
                update_method = service.update_config.method
                working_directory = tempfile.mkdtemp(dir=self.temporary_directory)
                update_hash = None

                try:
                    if update_method == 'URL':
                        update_hash = url_update(service.update_config.source, data['sha256'], working_directory)
                    elif update_method == 'Dockerfile':
                        # TODO
                        pass
                    elif update_method == 'Function':
                        # TODO
                        pass

                    if update_hash:
                        # FILE_UPDATE_DIRECTORY/{service_name} is the directory mounted to the service,
                        # the service sees multiple directories in that directory, each with a timestamp
                        destination_dir = os.path.join(FILE_UPDATE_DIRECTORY, service_name, service_name + '_' + now_as_iso())
                        shutil.move(working_directory, destination_dir)

                        # Update the sha256 to that of the new update file
                        data['sha256'] = update_hash
                finally:
                    # Update the next service update check time
                    data['next_update'] = now_as_iso(service.update_config.update_interval_seconds)

                    # If the working directory is still there for any reason erase it
                    shutil.rmtree(working_directory, ignore_errors=True)


if __name__ == '__main__':
    ServiceUpdater().serve_forever()
