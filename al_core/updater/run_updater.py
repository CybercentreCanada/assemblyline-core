"""
A process that manages tracking and running update commands for the AL services.

TODO:
    - delete oldest update files when there are more than X
    - docker run updates
    - docker build updates
    - restarting system components that need to detect updates

"""
import os
import sched
import shutil
import tempfile
import time
from threading import Thread
from typing import Dict

from al_core.server_base import ServerBase
from al_core.updater.url import url_update
from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.hash import Hash
from assemblyline.odm.models.service import Service, DockerConfig

SERVICE_SYNC_INTERVAL = 30
UPDATE_CHECK_INTERVAL = 5


FILE_UPDATE_VOLUME = os.environ.get('FILE_UPDATE_VOLUME', None)
FILE_UPDATE_DIRECTORY = os.environ.get('FILE_UPDATE_DIRECTORY', None)




class DockerUpdateInterface:
    def __init__(self):

        raise NotImplementedError()

    def create_temporary(self, parent_volume, ):
        raise NotImplementedError()

    def launch(self, docker_config: DockerConfig, mounts, env, namespace, blocking):
        raise NotImplementedError()

    def restart(self, service_name, namespace):
        label = {'component': service_name}


class KubernetesUpdateInterface:
    pass



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
        self.running_updates: Dict[str, Thread] = {}

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
                        last_update=now_as_iso(-10**10),
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

        # Check for finished update threads
        self.running_updates = {name: thread for name, thread in self.running_updates.items() if thread.is_alive()}

        # Check if its time to try to update the service
        for service_name, data in self.services.items():
            if data['next_update'] <= now_as_iso() and service_name not in self.running_updates:
                self.running_updates[service_name] = Thread(target=self.run_update, kwargs=dict(service_name=service_name))
                self.running_updates[service_name].start()

    def run_update(self, service_name):
        """Common setup and tear down for all update types."""
        # noinspection PyBroadException
        try:
            # Check for new update with service specified update method
            service = self.datastore.get_service_with_delta(service_name)
            update_method = service.update_config.method
            update_data = self.services.get(service_name)
            update_hash = None

            try:
                # Actually run the update method
                if update_method == 'run':
                    update_hash = self.do_file_update(
                        service=service,
                        old_hash=update_data['sha256'],
                        last_update=update_data['last_update']
                    )
                elif update_method == 'build':
                    update_hash = self.do_build_update()

                # If we have performed an update, write that data
                if update_hash is not None:
                    update_data['sha256'] = update_hash
                    update_data['last_update'] = now_as_iso()

            finally:
                # Update the next service update check time
                update_data['next_update'] = now_as_iso(service.update_config.update_interval_seconds)
                self.services.set(service_name, update_data)

        except BaseException:
            self.log.exception("An error occurred while running an update.")

    def do_build_update(self):
        raise NotImplementedError()

    def do_file_update(self, service, old_hash, last_update):
        input_directory = tempfile.mkdtemp(dir=self.temporary_directory)
        output_directory = tempfile.mkdtemp(dir=self.temporary_directory)

        try:
            # Write out the parameters we want to pass to the update container
            with open(os.path.join(input_directory, 'config.yaml'), 'w') as update_config:
                update_config.write("ABC123")

            # Run the update container
            self.controller.launch(
                docker_config=service.update_config.run_options,
                mounts=[
                    {
                        'volume': FILE_UPDATE_VOLUME,
                        'source_path': os.path.relpath(input_directory, FILE_UPDATE_DIRECTORY),
                        'dest_path': '/mount/input_directory'
                    },
                    {
                        'volume': FILE_UPDATE_VOLUME,
                        'source_path': os.path.relpath(output_directory, FILE_UPDATE_DIRECTORY),
                        'dest_path': '/mount/output_directory'
                    },
                ],
                env={'UPDATE_CONFIGURATION_PATH': '/mount/input_directory/config.yaml'},
                blocking=True,
                namespace=self.config.core.scaler.core_namespace
            )

            # Read out the results from the output container
            results_meta_file = os.path.join(output_directory, 'update_summary.yaml')

            if not os.path.exists(results_meta_file) or not os.path.isfile(results_meta_file):
                return None

            with open(results_meta_file) as rf:
                results_meta = rf.read()

            if results_meta:
                pass

            # Erase the results meta file
            os.unlink(results_meta_file)

            # FILE_UPDATE_DIRECTORY/{service_name} is the directory mounted to the service,
            # the service sees multiple directories in that directory, each with a timestamp
            destination_dir = os.path.join(FILE_UPDATE_DIRECTORY, service.name, service.name + '_' + now_as_iso())
            shutil.move(output_directory, destination_dir)

            return update_hash
        finally:
            # If the working directory is still there for any reason erase it
            shutil.rmtree(input_directory, ignore_errors=True)
            shutil.rmtree(output_directory, ignore_errors=True)



if __name__ == '__main__':
    ServiceUpdater().serve_forever()
