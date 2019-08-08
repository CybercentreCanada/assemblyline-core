"""
An auto-scaling service specific to Assemblyline.

TODO react to changes in memory/cpu limits
"""

import os
import time
import sched
from pprint import pprint

from al_core.dispatching.dispatcher import service_queue_name
from al_core.metrics.metrics_server import METRICS_QUEUE
from al_core.scaler.controllers import KubernetesController
from al_core.scaler.controllers.interface import ServiceControlError
from assemblyline.common import forge
from assemblyline.odm.models.service import DockerConfig
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.comms import CommsQueue

from al_core.server_base import ServerBase
from al_core.metrics.heartbeat_formatter import STATUS_QUEUE
from assemblyline.remote.datatypes.queues.named import NamedQueue

from .scaling import ScalingGroup, ServiceProfile
from .controllers import DockerController
from . import collection

# How often (in seconds) to download new service data, try to scale managed services,
# and download more metrics data respectively
SERVICE_SYNC_INTERVAL = 30
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
    def __init__(self, config=None, datastore=None, redis=None):
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
        self.status_queue = CommsQueue(STATUS_QUEUE, self.redis)
        self.metrics_queue = CommsQueue(METRICS_QUEUE, self.redis)
        self._metrics_loop = self.get_metrics()
        self.error_count = {}

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
                                                      namespace=self.config.core.scaler.service_namespace)
            core_controller = KubernetesController(logger=self.log, namespace=self.config.core.scaler.core_namespace,
                                                   labels=core_labels, prefix='')

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
            # TODO allocation parameters should be in config
            service_controller = DockerController(logger=self.log, prefix=self.config.core.scaler.service_namespace,
                                                  cpu_overallocation=100, memory_overallocation=1.5,
                                                  labels=service_labels)
            core_controller = DockerController(logger=self.log, prefix=self.config.core.scaler.core_namespace,
                                               cpu_overallocation=100, memory_overallocation=1,
                                               labels=core_labels)

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

        # Run as long as we need to
        while self.running:
            delay = self.scheduler.run(False)
            time.sleep(min(delay, 0.02))

    def sync_services(self):
        self.scheduler.enter(SERVICE_SYNC_INTERVAL, 0, self.sync_services)
        # Get all the service data
        for service in self.datastore.list_all_services(full=True):
            # noinspection PyBroadException
            try:
                # name = 'alsvc_' + service.name.lower()
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
                    docker_config.network = list(set(default_settings.network + docker_config.network))

                    # Add the service to the list of services being scaled
                    self.services.add_service(ServiceProfile(
                        name=name,
                        ram=service.ram_mb,
                        cpu=service.cpu_cores,
                        min_instances=default_settings.min_instances,
                        growth=default_settings.growth,
                        shrink=default_settings.shrink,
                        backlog=default_settings.backlog,
                        max_instances=service.licence_count,
                        container_config=docker_config,
                        queue=service_queue_name(name)
                    ))

                # Update RAM, CPU, licence requirements for running services
                if service.enabled and name in self.services.profiles:
                    profile = self.services.profiles[name]
                    if profile.cpu != service.cpu_cores:
                        self.services.controller.set_cpu_limit(name, service.cpu_cores)
                        profile.cpu = service.cpu_cores

                    if profile.ram != service.ram_mb:
                        self.services.controller.set_memory_limit(name, service.ram_mb)
                        profile.ram = service.ram_mb

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

            # Things that don't provide information we need
            if message.get('name', '') in {'expiry', 'ingester_submitter', 'alerter', 'ingester_internals',
                                           'ingester_input', 'watcher'}:
                pass

            elif message.get('type', None) == 'service':
                pass

            elif message.get('type', None) == 'service_timing':
                self.state.update(
                    service=message['name'],
                    host=message['host'],
                    busy_seconds=message['execution.t'],
                    throughput=0
                )

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

            else:
                pprint(message)

        export_interval = self.config.core.metrics.export_interval

        # Check the set of services that might be sitting at zero instances, and if it is, we need to
        # manually check if it is offline
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
                            self.log.info(f"{profile_name} down, pulling data directly")
                            profile.update(
                                delta=export_interval,
                                instances=0,
                                backlog=NamedQueue(profile.queue, self.redis).length(),
                                duty_cycle=profile.target_duty_cycle
                            )

                    # In the case that there should actually be instances running, but we haven't gotten
                    # any heartbeat messages we might be waiting for a container that can't start properly
                    else:
                    # elif time.time() - profile.last_update > profile.timeout:

                        # TODO do something about it
                        pass

                        # TODO delete next couple lines, debugging code
                        # if time.time() - profile.last_update > 15:
                        print(f"its been a bit since we heard from {profile_name}")
                        profile.last_update = time.time()

    def expire_errors(self):
        self.scheduler.enter(ERROR_EXPIRY_INTERVAL, 0, self.expire_errors)
        self.error_count = {name: err - 1 for name, err in self.error_count.items() if err > 1}