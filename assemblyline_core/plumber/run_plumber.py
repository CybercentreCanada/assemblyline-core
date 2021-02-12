"""
A daemon that cleans up tasks from the service queues when a service is disabled/deleted.

When a service is turned off by the orchestrator or deleted by the user, the service task queue needs to be
emptied. The status of all the services will be periodically checked and any service that is found to be
disabled or deleted for which a service queue exists, the dispatcher will be informed that the task(s)
had an error.
"""
import time

from assemblyline.odm.models.error import Error
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.constants import service_queue_name

from assemblyline_core.dispatching.client import DispatchClient
from assemblyline_core.server_base import CoreBase, ServiceStage


class Plumber(CoreBase):
    def __init__(self, logger=None, shutdown_timeout: float = None, config=None,
                 redis=None, redis_persist=None, datastore=None, delay=60):
        super().__init__('plumber', logger, shutdown_timeout, config=config, redis=redis,
                         redis_persist=redis_persist, datastore=datastore)
        self.delay = float(delay)
        self.dispatch_client = DispatchClient(datastore=self.datastore, redis=self.redis,
                                              redis_persist=self.redis_persist, logger=self.log)

    def try_run(self):
        # Get an initial list of all the service queues
        service_queues = {queue.decode('utf-8').lstrip('service-queue-'): None
                          for queue in self.redis.keys(service_queue_name('*'))}

        while self.running:
            self.heartbeat()
            # Reset the status of the service queues
            service_queues = {service_name: None for service_name in service_queues}

            # Update the service queue status based on current list of services
            for service in self.datastore.list_all_services(full=True):
                service_queues[service.name] = service

            for service_name, service in service_queues.items():
                if not service or not service.enabled or self.get_service_stage(service_name) != ServiceStage.Running:
                    while True:
                        task = self.dispatch_client.request_work(None, service_name=service_name,
                                                                 service_version='0', blocking=False)
                        if task is None:
                            break

                        error = Error(dict(
                            archive_ts=now_as_iso(self.config.datastore.ilm.days_until_archive * 24 * 60 * 60),
                            created='NOW',
                            expiry_ts=now_as_iso(task.ttl * 24 * 60 * 60) if task.ttl else None,
                            response=dict(
                                message='The service was disabled while processing this task.',
                                service_name=task.service_name,
                                service_version='0',
                                status='FAIL_NONRECOVERABLE',
                            ),
                            sha256=task.fileinfo.sha256,
                            type="TASK PRE-EMPTED",
                        ))

                        error_key = error.build_key(task=task)
                        self.dispatch_client.service_failed(task.sid, error_key, error)

            # Wait a while before checking status of all services again
            time.sleep(self.delay)


if __name__ == '__main__':
    with Plumber() as server:
        server.serve_forever()
