"""
A daemon that cleans up tasks from the service queues when a service is disabled/deleted.

When a service is turned off by the orchestrator or deleted by the user, the service task queue needs to be
emptied. The status of all the services will be periodically checked and any service that is found to be
disabled or deleted for which a service queue exists, the dispatcher will be informed that the task(s)
had an error.
"""
import hashlib
import json
import time

from assemblyline.odm.models.error import Error
from assemblyline.common.isotime import now_as_iso
from assemblyline.common import forge
from assemblyline.common.constants import service_queue_name
from assemblyline.remote.datatypes import get_client

from assemblyline_core.dispatching.client import DispatchClient
from assemblyline_core.server_base import ServerBase, SHUTDOWN_SECONDS_LIMIT


class Plumber(ServerBase):
    def __init__(self, logger=None, shutdown_timeout: float = SHUTDOWN_SECONDS_LIMIT, config=None,
                 redis=None, redis_persist=None, datastore=None):
        super().__init__('plumber', logger, shutdown_timeout)
        self.config = config or forge.get_config()
        self.redis = redis or get_client(
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )
        self.redis_persist = redis_persist or get_client(
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )
        self.datastore = datastore or forge.get_datastore()
        self.dispatch_client = DispatchClient(datastore=self.datastore, redis=self.redis,
                                              redis_persist=self.redis_persist, logger=self.log)

    def try_run(self):
        # Get an initial list of all the service queues
        service_queues = {queue.decode('utf-8').lstrip('service-queue-'): None
                          for queue in self.redis.keys(service_queue_name('*'))}

        while self.running:
            # Reset the status of the service queues
            service_queues = {service_name: None for service_name in service_queues}

            # Update the service queue status based on current list of services
            for service in self.datastore.list_all_services(full=True):
                service_queues[service.name] = service

            for service_name, service in service_queues.items():
                if not service or not service.enabled:
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

                        service_tool_version_hash = ''
                        task_config_hash = hashlib.md5((json.dumps(sorted(task.service_config)).encode('utf-8'))).hexdigest()
                        conf_key = hashlib.md5((str(service_tool_version_hash + task_config_hash).encode('utf-8'))).hexdigest()
                        error_key = error.build_key(conf_key)

                        self.dispatch_client.service_failed(task.sid, error_key, error)

            # Wait a while before checking status of all services again
            time.sleep(60)


if __name__ == '__main__':
    with Plumber() as server:
        server.serve_forever()
