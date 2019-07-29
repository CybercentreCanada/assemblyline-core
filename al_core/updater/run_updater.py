import sched
import time

from al_core.server_base import ServerBase
from al_core.updater.url import url_update
from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.hash import Hash

SERVICE_SYNC_INTERVAL = 30
UPDATE_CHECK_INTERVAL = 5


class ServiceUpdater(ServerBase):
    def __init__(self, persistent_redis=None, logger=None):
        super().__init__('assemblyline.service.updater', logger=logger)

        self.config = forge.get_config()
        self.datastore = forge.get_datastore()
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
                        next_update=now_as_iso(service.update_config.frequency),
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
                # Check for new update with service specified update method
                service = self.datastore.get_service_with_delta(service_name)
                update_method = service.update_config.source_type

                if update_method == 'URL':
                    url_update(service.update_config.source_value, data['sha256'])
                elif update_method == 'Dockerfile':
                    # TODO
                    pass
                elif update_method == 'Function':
                    # TODO
                    pass


if __name__ == '__main__':
    ServiceUpdater().serve_forever()
