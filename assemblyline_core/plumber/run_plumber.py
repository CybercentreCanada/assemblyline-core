"""
A daemon that cleans up tasks from the service queues when a service is disabled/deleted.

When a service is turned off by the orchestrator or deleted by the user, the service task queue needs to be
emptied. The status of all the services will be periodically checked and any service that is found to be
disabled or deleted for which a service queue exists, the dispatcher will be informed that the task(s)
had an error.
"""
import threading
import warnings
from typing import Optional

from assemblyline.common.constants import service_queue_name
from assemblyline.common.forge import get_service_queue
from assemblyline.common.isotime import now_as_iso
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.service import Service
from assemblyline.remote.datatypes import retry_call
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline_core.dispatching.client import DispatchClient
from assemblyline_core.server_base import CoreBase, ServiceStage

DAY = 60 * 60 * 24
TASK_DELETE_CHUNK = 10000


class Plumber(CoreBase):
    def __init__(self, logger=None, shutdown_timeout: Optional[float] = None, config=None,
                 redis=None, redis_persist=None, datastore=None, delay=60):
        super().__init__('assemblyline.plumber', logger, shutdown_timeout, config=config, redis=redis,
                         redis_persist=redis_persist, datastore=datastore)
        self.delay = float(delay)
        self.datastore.ds.switch_user("plumber")
        self.dispatch_client = DispatchClient(datastore=self.datastore, redis=self.redis,
                                              redis_persist=self.redis_persist, logger=self.log)

        self.flush_threads: dict[str, threading.Thread] = {}
        self.stop_signals: dict[str, threading.Event] = {}
        self.service_limit: dict[str, int] = {}

    def stop(self):
        for sig in self.stop_signals.values():
            sig.set()
        super().stop()

    def try_run(self):
        # Start a task cleanup thread
        tc_thread = threading.Thread(target=self.cleanup_old_tasks, daemon=True, name="datastore_task_cleanup")
        tc_thread.start()

        # Start a notification queue cleanup thread
        nq_thread = threading.Thread(target=self.cleanup_notification_queues, daemon=True,
                                     name="redis_notification_queue_cleanup")
        nq_thread.start()

        self.service_queue_plumbing()

    def service_queue_plumbing(self):
        # Get an initial list of all the service queues
        service_queues: dict[str, Optional[Service]]
        service_queues = {queue.decode('utf-8').lstrip('service-queue-'): None
                          for queue in self.redis.keys(service_queue_name('*'))}

        while self.running:
            # Reset the status of the service queues
            service_queues = {service_name: None for service_name in service_queues}

            # Update the service queue status based on current list of services
            for service in self.datastore.list_all_services(full=True):
                service_queues[service.name] = service

            for service_name, service in service_queues.items():
                # For disabled or othewise unavailable services purge the queue
                current_stage = self.get_service_stage(service_name, ServiceStage.Running)
                if not service or not service.enabled or current_stage != ServiceStage.Running:
                    while True:
                        task = self.dispatch_client.request_work('plumber', service_name=service_name,
                                                                 service_version='0', blocking=False)
                        if task is None:
                            break

                        error = Error(dict(
                            archive_ts=None,
                            created='NOW',
                            expiry_ts=now_as_iso(task.ttl * DAY) if task.ttl else None,
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
                        self.heartbeat()

                # For services that are enabled but limited
                if not service or not service.enabled or service.max_queue_length == 0:
                    if service_name in self.stop_signals:
                        self.stop_signals[service_name].set()
                        self.service_limit.pop(service_name)
                        self.flush_threads.pop(service_name)
                elif service and service.enabled and service.max_queue_length > 0:
                    self.service_limit[service_name] = service.max_queue_length
                    thread = self.flush_threads.get(service_name)
                    if not thread or not thread.is_alive():
                        self.stop_signals[service_name] = threading.Event()
                        thread = threading.Thread(
                            target=self.watch_service, args=[service_name],
                            daemon=True, name=service_name)
                        self.flush_threads[service_name] = thread
                        thread.start()

            # Wait a while before checking status of all services again
            self.sleep_with_heartbeat(self.delay)

    def cleanup_notification_queues(self):
        self.log.info("Cleaning up notification queues for old messages...")
        while self.running:
            # Finding all possible notification queues
            keys = [k.decode() for k in retry_call(self.redis_persist.keys, "nq-*")]
            if not keys:
                self.log.info('There are no queues right now in the system')
            for k in keys:
                self.log.info(f'Checking for old message in queue: {k}')
                # Peek the first message of the queue
                q = NamedQueue(k, self.redis_persist)
                msg = q.peek_next()
                if msg:
                    current_time = now_as_iso(-1 * self.config.core.plumber.notification_queue_max_age)
                    task_time = msg.get('notify_time', None) or msg.get('ingest_time', None)

                    # If the message too old, cleanup
                    if task_time is None or task_time <= current_time:
                        self.log.warning(
                            f"Messages on queue {k} by "
                            f"{msg.get('submission', {}).get('params', {}).get('submitter', 'unknown')}"
                            f" are older then the maximum queue age ({task_time}), removing queue")
                        q.delete()
                    else:
                        self.log.info('All messages are recent enough')
                else:
                    self.log.info('There are no messages in the queue')

            # wait for next run
            self.sleep(self.config.core.plumber.notification_queue_interval)
        self.log.info("Done cleaning up notification queues")

    def cleanup_old_tasks(self):
        self.log.info("Cleaning up task index for old completed tasks...")
        while self.running:
            deleted = self.datastore.task_cleanup(deleteable_task_age=DAY, max_tasks=TASK_DELETE_CHUNK)
            if not deleted:
                self.sleep(self.delay)
            else:
                self.log.info(f"Cleaned up {deleted} tasks that were already completed")
        self.log.info("Done cleaning up task index")

    def watch_service(self, service_name):
        self.log.info(f"Watching {service_name} service queue...")
        service_queue = get_service_queue(service_name, self.redis)
        while self.running and not self.stop_signals[service_name].is_set():
            while service_queue.length() > self.service_limit[service_name]:
                task = self.dispatch_client.request_work('plumber', service_name=service_name,
                                                         service_version='0', blocking=False, low_priority=True)
                if task is None:
                    break

                error = Error(dict(
                    archive_ts=None,
                    created='NOW',
                    expiry_ts=now_as_iso(task.ttl * 24 * 60 * 60) if task.ttl else None,
                    response=dict(
                        message="Task canceled due to execesive queuing.",
                        service_name=task.service_name,
                        service_version='0',
                        status='FAIL_NONRECOVERABLE',
                    ),
                    sha256=task.fileinfo.sha256,
                    type="TASK PRE-EMPTED",
                ))

                error_key = error.build_key(task=task)
                self.dispatch_client.service_failed(task.sid, error_key, error)
            self.sleep(2)
        self.log.info(f"Done watching {service_name} service queue")


if __name__ == '__main__':
    with Plumber() as server:
        server.serve_forever()
