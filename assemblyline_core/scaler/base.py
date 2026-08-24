import os
import platform
import threading
import concurrent.futures
import functools
from typing import Optional
from contextlib import contextmanager

import elasticapm

from assemblyline.common.constants import (
    SCALER_TIMEOUT_QUEUE,
    SERVICE_STATE_HASH,
    ServiceStatus,
)
from assemblyline.common.forge import get_apm_client
from assemblyline.remote.datatypes.exporting_counter import export_metrics_once
from assemblyline.odm.messages.changes import Operation, ServiceChange
from assemblyline.odm.messages.scaler_heartbeat import Metrics
from assemblyline.odm.messages.scaler_status_heartbeat import Status
from assemblyline.remote.datatypes.events import EventSender, EventWatcher
from assemblyline.remote.datatypes.hash import ExpiringHash, Hash
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline_core.scaler.controllers.interface import ControllerInterface, ServiceControlError
from assemblyline_core.server_base import ServiceStage, ThreadedCoreBase


# How often (in seconds) to download new service data, try to scale managed services,
# and download more metrics data respectively
SERVICE_SYNC_INTERVAL = 60 * 30  # Every half hour
HEARTBEAT_INTERVAL = 5

CONTAINER_EVENTS_LOG_INTERVAL = 2
HOSTNAME = os.getenv('HOSTNAME', platform.node())
APM_SPAN_TYPE = 'scaler'


@contextmanager
def apm_span(client: Optional[elasticapm.Client], span_name: str):
    try:
        if client:
            client.begin_transaction(APM_SPAN_TYPE)
        yield None
        if client:
            client.end_transaction(span_name, 'success')
    except Exception:
        if client:
            client.end_transaction(span_name, 'exception')
        raise


class ProfileBase:
    def __init__(self, name: str, target_queue_length: int) -> None:
        self.name = name
        # How long does a backlog need to be before we are concerned
        self.target_queue_length = target_queue_length

        # Number of instances kubernetes expects to be running
        self.target_instances: int = 0

        # The number of instances running based on feedback metrics
        self.running_instances: int = 0

        # Last reported length of the service task queue
        self.queue_length: int = 0


class ScalerBase(ThreadedCoreBase):
    """Common scaler methods."""
    def __init__(self, config=None, datastore=None, redis=None, redis_persist=None) -> None:
        super().__init__('assemblyline.scaler', config=config, datastore=datastore,
                         redis=redis, redis_persist=redis_persist)

        self.scaler_timeout_queue: NamedQueue[dict] = NamedQueue(SCALER_TIMEOUT_QUEUE, host=self.redis_persist)
        self.status_table = ExpiringHash(SERVICE_STATE_HASH, host=self.redis, ttl=30*60)
        self.service_event_sender: EventSender[dict] = EventSender('changes.services', host=self.redis)
        self.service_watcher_wakeup = threading.Event()
        self.service_change_watcher = EventWatcher(self.redis, deserializer=ServiceChange.deserialize)
        self.service_change_watcher.register('changes.services.*', self._handle_service_change_event)

        self.controller: ControllerInterface

        # Information about services
        self.profiles: dict[str, ProfileBase] = {}
        self.profiles_lock = threading.RLock()

        # Load the APM connection if any
        self.apm_client = None
        if self.config.core.metrics.apm_server.server_url:
            elasticapm.instrument()
            self.apm_client = get_apm_client("scaler")

    def try_run(self):
        self.service_change_watcher.start()
        self.maintain_threads({
            'Log Container Events': self.log_container_events,
            'Process Timeouts': self.process_timeouts,
            'Service Configuration Sync': self.sync_services,
            'Service Adjuster': self.update_scaling,
            'Import Metrics': self.sync_metrics,
            'Export Metrics': self.export_metrics,
        })

    def stop(self):
        super().stop()
        self.service_change_watcher.stop()
        self.service_watcher_wakeup.set()
        self.controller.stop()

    def _handle_service_change_event(self, data: Optional[ServiceChange]):
        if data is None:
            self.service_watcher_wakeup.set()
        else:
            if data.operation == Operation.Removed:
                self.log.info(f'Service appears to be deleted, removing {data.name}')
                stage = self.get_service_stage(data.name)
                self.stop_service(data.name, stage)
            elif data.operation == Operation.Incompatible:
                return
            else:
                service = self.datastore.get_service_with_delta(data.name)
                if not service:
                    self.log.warning(f'Received change event for non-existent service: {data.name}. Ignoring..')
                    return
                self._sync_service(service)

    def sync_services(self) -> None:
        last_synced_profiles = None
        while self.running:
            with apm_span(self.apm_client, 'sync_services'):
                self.log.info('Synchronizing service configuration')
                with self.profiles_lock:
                    current_services = set(self.profiles.keys())

                    # Check to see if the service is progressing since it's last sync
                    if last_synced_profiles:
                        for service, profile in self.profiles.items():
                            # Assume there was no backlog initially if the service is new since last sync
                            last_synced_backlog = 0
                            if last_synced_profiles.get(service):
                                last_synced_backlog = last_synced_profiles[service].queue_length

                            # Check to see if the backlog has increased and if the service has been running since
                            backlog = profile.queue_length
                            trying_to_start = profile.running_instances == 0 and profile.target_instances > 0
                            if backlog and backlog >= last_synced_backlog and trying_to_start:
                                # Restart the service in an attempt to resolve intermittent issues with container/pod
                                self.controller.restart(profile)

                    # Update the last synced profiles for next time
                    last_synced_profiles = self.profiles

                discovered_services: list[str] = []

                # Get all the service data
                for service in self.datastore.list_all_services(full=True):
                    self._sync_service(service)
                    discovered_services.append(service.name)

                # Find any services we have running, that are no longer in the database and remove them
                for stray_service in current_services - set(discovered_services):
                    self.log.info('Service appears to be deleted, removing stray %s', stray_service)
                    stage = self.get_service_stage(stray_service)
                    self.stop_service(stray_service, stage)
                self.log.info('Finish synchronizing service configuration')

            # Wait for the interval or until someone wakes us up
            self.service_watcher_wakeup.wait(timeout=SERVICE_SYNC_INTERVAL)
            self.service_watcher_wakeup.clear()

    def _sync_service(self, service):
        raise NotImplementedError()

    @elasticapm.capture_span(span_type=APM_SPAN_TYPE)
    def stop_service(self, name: str, current_stage: ServiceStage):
        if current_stage != ServiceStage.Off:
            # Disable this service's dependencies
            self.controller.stop_containers(labels={
                'dependency_for': name
            })

            # Clear related dependency caching from Redis
            Hash(f'service-updates-{name}', self.redis_persist).delete()

            # Mark this service as not running in the shared record
            self._service_stage_hash.set(name, ServiceStage.Off)

        # Stop any running disabled services
        if name in self.profiles or self.controller.get_target(name) > 0:
            self.log.info(f'Removing {name} from scaling')
            with self.profiles_lock:
                self.profiles.pop(name, None)
            self.controller.set_target(name, 0)

    def log_crashes(self, fn):
        @functools.wraps(fn)
        def with_logs(*args, **kwargs):
            # noinspection PyBroadException
            try:
                fn(*args, **kwargs)
            except ServiceControlError as error:
                self.log.exception(f"Error while managing service: {error.service_name}")
            except Exception:
                self.log.exception(f'Crash in scaler: {fn.__name__}')
        return with_logs

    def _timeout_kill(self, service, container):
        with apm_span(self.apm_client, 'timeout_kill'):
            self.controller.stop_container(service, container)
            self.status_table.pop(container)

    def process_timeouts(self):
        with concurrent.futures.ThreadPoolExecutor(10) as pool:
            futures = []

            while self.running:
                message = self.scaler_timeout_queue.pop(blocking=True, timeout=1)
                if not message:
                    continue

                with apm_span(self.apm_client, 'process_timeouts'):
                    # Process new messages
                    self.log.info(f"Killing service container: {message['container']} running: {message['service']}")
                    futures.append(pool.submit(self._timeout_kill, message['service'], message['container']))

                    # Process finished
                    finished = [_f for _f in futures if _f.done()]
                    futures = [_f for _f in futures if _f not in finished]
                    for _f in finished:
                        exception = _f.exception()
                        if exception is not None:
                            self.log.error("Exception trying to stop timed out service container: %s", exception)

    def get_cpu_overallocation(self) -> float:
        node_limit = self.config.core.scaler.overallocation_node_limit
        if node_limit is not None and node_limit <= self.controller.node_count:
            return 1
        return self.config.core.scaler.cpu_overallocation

    def get_memory_overallocation(self) -> float:
        node_limit = self.config.core.scaler.overallocation_node_limit
        if node_limit is not None and node_limit <= self.controller.node_count:
            return 1
        return self.config.core.scaler.memory_overallocation

    def get_cpu_info(self, overallocation: bool) -> tuple[float, float]:
        # Get the raw used resource numbers
        free_cpu, total_cpu = self.controller.cpu_info()

        # Recalculate the amount of free resources expanding the total quantity by the overallocation
        if overallocation:
            used_cpu = total_cpu - free_cpu
            free_cpu = total_cpu * self.get_cpu_overallocation() - used_cpu

        # Include the service containers not counted in the raw numbers because they are pending
        for name, pending in self.controller.get_unavailable().items():
            profile = self.profiles.get(name)
            if not profile or not pending:
                continue

            free_cpu = free_cpu - profile.container_config.cpu_cores * pending

        return (free_cpu, total_cpu)

    def get_memory_info(self, overallocation: bool) -> tuple[float, float]:
        # Get the raw used resource numbers
        free_memory, total_memory = self.controller.memory_info()

        # Recalculate the amount of free resources expanding the total quantity by the overallocation
        if overallocation:
            used_memory = total_memory - free_memory
            free_memory = total_memory * self.get_memory_overallocation() - used_memory

        # Include the service containers not counted in the raw numbers because they are pending
        for name, pending in self.controller.get_unavailable().items():
            profile = self.profiles.get(name)
            if not profile or not pending:
                continue

            free_memory = free_memory - profile.container_config.ram_mb * pending

        return (free_memory, total_memory)

    def log_container_events(self):
        """The service status table may have references to containers that have crashed. Try to remove them all."""
        while self.sleep(CONTAINER_EVENTS_LOG_INTERVAL):
            with apm_span(self.apm_client, 'log_container_events'):
                for message in self.controller.new_events():
                    self.log.warning("Container Event :: " + message)

    def export_metrics(self):
        while self.sleep(self.config.logging.export_interval):
            with apm_span(self.apm_client, 'export_metrics'):
                service_metrics = {}
                with self.profiles_lock:
                    for service_name, profile in self.profiles.items():
                        service_metrics[service_name] = {
                            'running': profile.running_instances,
                            'target': profile.target_instances,
                            'minimum': profile.min_instances,
                            'maximum': profile.instance_limit,
                            'dynamic_maximum': profile.max_instances,
                            'queue': profile.queue_length,
                            'duty_cycle': profile.duty_cycle,
                            'pressure': profile.pressure,
                        }

                for service_name, metrics in service_metrics.items():
                    export_metrics_once(service_name, Status, metrics, host=HOSTNAME,
                                        counter_type='scaler_status', config=self.config, redis=self.redis)

                memory, memory_total = self.get_memory_info(overallocation=False)
                cpu, cpu_total = self.get_cpu_info(overallocation=False)
                metrics = {
                    'memory_total': memory_total,
                    'cpu_total': cpu_total,
                    'memory_free': memory,
                    'cpu_free': cpu
                }
                export_metrics_once('scaler', Metrics, metrics, host=HOSTNAME,
                                    counter_type='scaler', config=self.config, redis=self.redis)

    def update_scaling(self):
        raise NotImplementedError()

    def sync_metrics(self):
        raise NotImplementedError()
