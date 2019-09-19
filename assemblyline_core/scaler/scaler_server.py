"""
An auto-scaling service specific to Assemblyline.

TODO react to changes in memory/cpu limits
"""

import os
import time
import sched
from pprint import pprint

from assemblyline.remote.datatypes.hash import ExpiringHash

from assemblyline.common.constants import SCALER_TIMEOUT_QUEUE, SERVICE_STATE_HASH, ServiceStatus
from assemblyline.odm.models.service import Service
from assemblyline_core.dispatching.dispatcher import service_queue_name
from assemblyline_core.metrics.metrics_server import METRICS_QUEUE
from assemblyline_core.scaler.controllers import KubernetesController
from assemblyline_core.scaler.controllers.interface import ServiceControlError
from assemblyline.common import forge
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.comms import CommsQueue

from assemblyline_core.server_base import ServerBase
from assemblyline_core.metrics.heartbeat_formatter import STATUS_QUEUE
from assemblyline.remote.datatypes.queues.named import NamedQueue

from .scaling import ScalingGroup, ServiceProfile
from .controllers import DockerController
from . import collection

# How often (in seconds) to download new service data, try to scale managed services,
# and download more metrics data respectively
SERVICE_SYNC_INTERVAL = 30
PROCESS_TIMEOUT_INTERVAL = 30
SCALE_INTERVAL = 5
METRIC_SYNC_INTERVAL = 0.1

# How many times to let a service generate an error in this module before we disable it.
# This is only for analysis services, core services we keep retrying forever
MAXIMUM_SERVICE_ERRORS = 5
ERROR_EXPIRY_INTERVAL = 60*60  # how long we wait before we forgive an error. (seconds)

# An environment variable that should be set when we are started with kubernetes, tells us how to attach
# the global Assemblyline config to new things that we launch.
KUBERNETES_AL_CONFIG = os.environ.get('KUBERNETES_AL_CONFIG')


class ScalerServer(ServerBase):
    def __init__(self, config=None, datastore=None, redis=None, redis_persist=None):
        super().__init__('assemblyline.scaler')
        # Connect to the assemblyline system
        self.config = config or forge.get_config()
        self.datastore = datastore or forge.get_datastore()
        self.redis = redis or get_client(
            db=self.config.core.redis.nonpersistent.db,
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )

        self.redis_persist = redis_persist or get_client(
            db=self.config.core.redis.persistent.db,
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )
        self.status_queue = CommsQueue(STATUS_QUEUE, self.redis)
        self.metrics_queue = CommsQueue(METRICS_QUEUE, self.redis)
        self.scaler_timeout_queue = NamedQueue(SCALER_TIMEOUT_QUEUE, host=self.redis_persist)
        self._metrics_loop = self.get_metrics()
        self.error_count = {}
        self.status_table = ExpiringHash(SERVICE_STATE_HASH, host=self.redis, ttl=None)

        core_labels = {
            'app': 'assemblyline',
            'section': 'core',
        }

        service_labels = {
            'app': 'assemblyline',
            'section': 'service',
        }

        # TODO select the right controller by examining our environment
        if KUBERNETES_AL_CONFIG:
            self.log.info("Loading Kubernetes cluster interface.")
            service_controller = KubernetesController(logger=self.log, prefix='alsvc_', labels=service_labels,
                                                      namespace=self.config.core.scaler.service_namespace,
                                                      priority='al-service-priority')
            core_controller = KubernetesController(logger=self.log, namespace=self.config.core.scaler.core_namespace,
                                                   labels=core_labels, prefix='', priority='al-core-priority')

            # Mare sure core services get attached to the config
            core_controller.config_mount(
                name='al-config',
                config_map=KUBERNETES_AL_CONFIG,
                key='config',
                file_name='config.yml',
                target_path='/etc/assemblyline/'
            )
        else:
            self.log.info("Loading Docker cluster interface.")
            # TODO allocation parameters should be read from config
            service_controller = DockerController(logger=self.log, prefix=self.config.core.scaler.service_namespace,
                                                  cpu_overallocation=100, memory_overallocation=1.5,
                                                  labels=service_labels, network='svc')
            core_controller = DockerController(logger=self.log, prefix=self.config.core.scaler.core_namespace,
                                               cpu_overallocation=100, memory_overallocation=1,
                                               labels=core_labels, network='backend')

            # TODO move these lines to config
            core_controller.global_mounts.append(['/opt/alv4/alv4/dev/core/config/', '/etc/assemblyline/'])
            core_controller.global_mounts.append(['/opt/alv4/', '/opt/alv4/'])

        self.services = ScalingGroup(service_controller)
        self.core = ScalingGroup(core_controller)

        # Prepare a single threaded scheduler
        self.state = collection.Collection(period=self.config.core.metrics.export_interval)
        self.scheduler = sched.scheduler()

        # Add the core components to the scaler
        core_defaults = self.config.core.scaler.core_defaults
        for name, parameters in self.config.core.scaler.core_configs.items():
            self.core.add_service(ServiceProfile(name=name, **core_defaults.apply(parameters)))

    def try_run(self):
        # Do an initial call to the main methods, who will then be registered with the scheduler
        self.sync_services()
        self.sync_metrics()
        self.update_scaling()
        self.expire_errors()
        self.process_timeouts()

        # Run as long as we need to
        while self.running:
            delay = self.scheduler.run(False)
            time.sleep(min(delay, 0.02))

    def sync_services(self):
        self.scheduler.enter(SERVICE_SYNC_INTERVAL, 0, self.sync_services)
        # Get all the service data
        for service in self.datastore.list_all_services(full=True):
            service: Service = service
            # noinspection PyBroadException
            try:
                name = service.name

                # Stop any running disabled services
                if not service.enabled and (name in self.services.profiles or self.services.controller.get_target(name) > 0):
                    self.log.info(f'Removing {service.name} from scaling')
                    self.services.profiles.pop(name)
                    self.services.controller.set_target(name, 0)

                # Check that all enabled services are enabled
                if service.enabled and name not in self.services.profiles:
                    self.log.info(f'Adding {service.name} to scaling')
                    default_settings = self.config.core.scaler.service_defaults

                    # Apply global options to the docker configuration
                    docker_config = service.docker_config
                    set_keys = set(var.name for var in docker_config.environment)
                    for var in default_settings.environment:
                        if var.name not in set_keys:
                            docker_config.environment.append(var)

                    # Add the service to the list of services being scaled
                    self.services.add_service(ServiceProfile(
                        name=name,
                        min_instances=default_settings.min_instances,
                        growth=default_settings.growth,
                        shrink=default_settings.shrink,
                        backlog=default_settings.backlog,
                        max_instances=service.licence_count,
                        container_config=docker_config,
                        queue=service_queue_name(name),
                        shutdown_seconds=service.timeout + 30,  # Give service an extra 30 seconds to upload results
                    ), updates=service.update_config)

                # Update RAM, CPU, licence requirements for running services
                if service.enabled and name in self.services.profiles:
                    profile = self.services.profiles[name]
                    if profile.container_config.cpu_cores != service.docker_config.cpu_cores:
                        self.services.controller.set_cpu_limit(name, service.docker_config.cpu_cores)
                        profile.container_config.cpu_cores = service.docker_config.cpu_cores

                    if profile.container_config.ram_mb != service.docker_config.ram_mb:
                        self.services.controller.set_memory_limit(name, service.docker_config.ram_mb)
                        profile.container_config.ram_mb = service.docker_config.ram_mb

                    if service.licence_count == 0:
                        profile._max_instances = float('inf')
                    else:
                        profile._max_instances = service.licence_count
            except:
                self.log.exception(f"Error applying service settings from: {service.name}")
                self.handle_service_error(service.name)

    def update_scaling(self):
        """Check if we need to scale any services up or down."""
        self.scheduler.enter(SCALE_INTERVAL, 0, self.update_scaling)
        # noinspection PyBroadException
        try:
            self.core.update()
        except:
            self.log.exception("Error in core processes.")

        try:
            self.services.update()
        except ServiceControlError as error:
            self.log.exception("Error while scaling services.")
            self.handle_service_error(error.service_name)

    def handle_service_error(self, service_name):
        """Handle an error occurring in the *analysis* service.

        Errors for core systems should simply be logged, and a best effort to continue made.

        For analysis services, ignore the error a few times, then disable the service.
        """
        self.error_count[service_name] = self.error_count.get(service_name, 0) + 1

        if self.error_count[service_name] >= MAXIMUM_SERVICE_ERRORS:
            self.datastore.service_delta.update(service_name, [
                (self.datastore.service_delta.UPDATE_SET, 'enabled', False)
            ])
            del self.error_count[service_name]

    def get_metrics(self):
        """A generator that makes a non blocking check on the metrics queue each time it is called."""
        with self.metrics_queue as sq:
            for message in sq.listen(blocking=False):
                yield message

    def sync_metrics(self):
        """Check if there are any pubsub messages we need."""
        self.scheduler.enter(METRIC_SYNC_INTERVAL, 3, self.sync_metrics)
        for message in self._metrics_loop:
            # We will get none when we have cleared the buffer
            if message is None:
                break

            elif message.get('name', None) == 'dispatcher_submissions':
                self.state.update(
                    service=message['name'],
                    host=message['host'],
                    busy_seconds=message['busy_seconds.t'],
                    throughput=message['submissions_completed']
                )

            elif message.get('name', None) == 'dispatcher_files':
                self.state.update(
                    service=message['name'],
                    host=message['host'],
                    busy_seconds=message['busy_seconds.t'],
                    throughput=message['files_completed']
                )

        # Pull service metrics from redis
        service_data = self.status_table.items()
        for host, (service, state) in service_data.items():
            self.state.update(service=service, host=host, throughput=0,
                              busy_seconds=METRIC_SYNC_INTERVAL if state == ServiceStatus.Running else 0)

        # Check the set of services that might be sitting at zero instances, and if it is, we need to
        # manually check if it is offline
        export_interval = self.config.core.metrics.export_interval
        for group in (self.core, self.services):
            for profile_name, profile in group.profiles.items():
                # Pull out statistics from the metrics regularization
                update = self.state.read(profile_name)
                if update:
                    delta = time.time() - profile.last_update
                    profile.update(
                        delta=delta,
                        backlog=NamedQueue(profile.queue, self.redis).length(),
                        **update
                    )

                # Wait until we have missed two heartbeats before we take things into our own hands
                if time.time() - profile.last_update > 2 * export_interval:
                    # Check if we expect no messages, if so pull the queue length ourselves since there is no heartbeat
                    if group.controller.get_target(profile_name) == 0:
                        if profile.queue:
                            profile.update(
                                delta=export_interval,
                                instances=0,
                                backlog=NamedQueue(profile.queue, self.redis).length(),
                                duty_cycle=profile.target_duty_cycle
                            )

        # TODO maybe find another way of implementing this that is less aggressive
        # for profile_name, profile in self.services.profiles.items():
        #     # In the case that there should actually be instances running, but we haven't gotten
        #     # any heartbeat messages we might be waiting for a container that can't start properly
        #     if self.services.controller.get_target(profile_name) > 0:
        #         if time.time() - profile.last_update > profile.shutdown_seconds:
        #             self.log.error(f"Starting service {profile_name} has timed out "
        #                            f"({time.time() - profile.last_update} > {profile.shutdown_seconds} seconds)")
        #
        #             # Disable the the service
        #             self.datastore.service_delta.update(profile_name, [
        #                 (self.datastore.service_delta.UPDATE_SET, 'enabled', False)
        #             ])

    def expire_errors(self):
        self.scheduler.enter(ERROR_EXPIRY_INTERVAL, 0, self.expire_errors)
        self.error_count = {name: err - 1 for name, err in self.error_count.items() if err > 1}

    def process_timeouts(self):
        self.scheduler.enter(PROCESS_TIMEOUT_INTERVAL, 0, self.process_timeouts)
        while True:
            message = self.scaler_timeout_queue.pop(blocking=False)
            if not message:
                break
            try:
                self.log.info(f"Killing service container: {message['container']} running: {message['service']}")
                self.services.controller.stop_container(message['service'], message['container'])
            except Exception:
                self.log.exception(f"Exception trying to stop timed out service container: {message}")
