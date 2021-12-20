"""
An auto-scaling service specific to Assemblyline services.
"""
from __future__ import annotations
import functools
import threading
from collections import defaultdict
from string import Template
from typing import Optional, Any
import os
import math
import time
import platform
import concurrent.futures
import copy
from contextlib import contextmanager

import elasticapm
import yaml

from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.queues.priority import PriorityQueue, length as pq_length
from assemblyline.remote.datatypes.exporting_counter import export_metrics_once
from assemblyline.remote.datatypes.hash import ExpiringHash
from assemblyline.remote.datatypes.events import EventWatcher
from assemblyline.odm.models.service import Service, DockerConfig
from assemblyline.odm.messages.scaler_heartbeat import Metrics
from assemblyline.odm.messages.scaler_status_heartbeat import Status
from assemblyline.odm.messages.changes import ServiceChange, Operation
from assemblyline.common.forge import get_classification, get_service_queue
from assemblyline.common.constants import SCALER_TIMEOUT_QUEUE, SERVICE_STATE_HASH, ServiceStatus
from assemblyline.common.version import FRAMEWORK_VERSION, SYSTEM_VERSION
from assemblyline_core.scaler.controllers import KubernetesController
from assemblyline_core.scaler.controllers.interface import ServiceControlError
from assemblyline_core.server_base import ServiceStage, ThreadedCoreBase

from .controllers import DockerController
from . import collection

APM_SPAN_TYPE = 'scaler'

# How often (in seconds) to download new service data, try to scale managed services,
# and download more metrics data respectively
SERVICE_SYNC_INTERVAL = 60 * 30  # Every half hour
SCALE_INTERVAL = 5
METRIC_SYNC_INTERVAL = 0.5
CONTAINER_EVENTS_LOG_INTERVAL = 2
HEARTBEAT_INTERVAL = 5

# How many times to let a service generate an error in this module before we disable it.
# This is only for analysis services, core services we keep retrying forever
MAXIMUM_SERVICE_ERRORS = 5
ERROR_EXPIRY_TIME = 60*60  # how long we wait before we forgive an error. (seconds)

# The maximum containers we ask to be created in a single scaling iteration
# This is to give kubernetes a chance to update our view of resource usage before we ask for more containers
MAX_CONTAINER_ALLOCATION = 10

# An environment variable that should be set when we are started with kubernetes, tells us how to attach
# the global Assemblyline config to new things that we launch.
KUBERNETES_AL_CONFIG = os.environ.get('KUBERNETES_AL_CONFIG')

HOSTNAME = os.getenv('HOSTNAME', platform.node())
NAMESPACE = os.getenv('NAMESPACE', 'al')
CLASSIFICATION_HOST_PATH = os.getenv('CLASSIFICATION_HOST_PATH', None)
CLASSIFICATION_CONFIGMAP = os.getenv('CLASSIFICATION_CONFIGMAP', None)
CLASSIFICATION_CONFIGMAP_KEY = os.getenv('CLASSIFICATION_CONFIGMAP_KEY', 'classification.yml')

DOCKER_CONFIGURATION_PATH = os.getenv('DOCKER_CONFIGURATION_PATH', None)
DOCKER_CONFIGURATION_VOLUME = os.getenv('DOCKER_CONFIGURATION_VOLUME', None)
CONFIGURATION_CONFIGMAP = os.getenv('CONFIGURATION_CONFIGMAP', None)
CONFIGURATION_CONFIGMAP_KEY = os.getenv('CONFIGURATION_CONFIGMAP_KEY', 'config')


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


class Pool:
    """
    This is a light wrapper around ThreadPoolExecutor to run batches of
    jobs as a context manager, and wait for the batch to finish after
    the context ends.
    """

    def __init__(self, size: int = 10):
        self.pool = concurrent.futures.ThreadPoolExecutor(size)
        self.futures: list[concurrent.futures.Future[Any]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()

    def finish(self):
        while self.futures:
            self.futures.pop(0).result()

    def call(self, fn, *args, **kwargs):
        self.futures.append(self.pool.submit(fn, *args, **kwargs))


class ServiceProfile:
    """A profile, describing a currently running service.

    This includes how the service should be run, and conditions related to the scaling of the service.
    """

    def __init__(self, name: str, container_config: DockerConfig, config_blob: str = '',
                 min_instances: int = 0, max_instances: int = None, growth: float = 600,
                 shrink: Optional[float] = None, backlog: int = 500, queue=None,
                 shutdown_seconds: int = 30, dependency_blobs: dict[str, str] = None, privileged: bool = False):
        """
        :param name: Name of the service to manage
        :param container_config: Instructions on how to start this service
        :param min_instances: The minimum number of copies of this service keep running
        :param max_instances: The maximum number of copies permitted to be running
        :param growth: Delay before growing a service, unit-less, approximately seconds
        :param shrink: Delay before shrinking a service, unit-less, approximately seconds, defaults to -growth
        :param backlog: How long a queue backlog should be before it takes `growth` seconds to grow.
        :param queue: Queue name for monitoring
        :param privileged: Is this service able to interact with core directly?
        """
        self.name = name
        self.queue: PriorityQueue = queue
        self.container_config = container_config
        self.high_duty_cycle = 0.7
        self.low_duty_cycle = 0.5
        self.shutdown_seconds = shutdown_seconds
        self.config_blob = config_blob
        self.dependency_blobs = dependency_blobs or {}
        self.privileged = privileged

        # How many instances we want, and can have
        self.min_instances: int = max(0, int(min_instances))
        self._min_instances: int = self.min_instances
        self._max_instances: int = max(0, int(max_instances or 0))
        self.desired_instances: int = 0
        self.target_instances: int = 0
        self.running_instances: int = 0

        # Information tracking when we want to grow/shrink
        self.pressure: float = 0
        self.growth_threshold = abs(float(growth))
        self.shrink_threshold = -self.growth_threshold/2 if shrink is None else -abs(float(shrink))
        self.leak_rate: float = 0.1

        # How long does a backlog need to be before we are concerned
        self.backlog = int(backlog)
        self.queue_length = 0
        self.duty_cycle = 0.0
        self.last_update = 0.0

    @property
    def cpu(self):
        return self.container_config.cpu_cores

    @property
    def ram(self):
        return self.container_config.ram_mb

    @property
    def instance_limit(self):
        return self._max_instances

    @property
    def max_instances(self) -> int:
        # Adjust the max_instances based on the number that is already requested
        # this keeps the scaler from running way ahead with its demands when resource caps are reached
        if self._max_instances == 0:
            return self.target_instances + MAX_CONTAINER_ALLOCATION
        return min(self._max_instances, self.target_instances + MAX_CONTAINER_ALLOCATION)

    @max_instances.setter
    def max_instances(self, value: int):
        self._max_instances = max(0, value)

    def update(self, delta: float, instances: int, backlog: int, duty_cycle: float):
        self.last_update = time.time()
        self.running_instances = instances
        self.queue_length = backlog
        self.duty_cycle = duty_cycle

        # Adjust min instances based on queue (if something has min_instances == 0, bump it up to 1 when
        # there is anything in the queue) this should have no effect for min_instances > 0
        self.min_instances = max(self._min_instances, int(bool(backlog)))
        self.desired_instances = max(self.min_instances, min(self.max_instances, self.desired_instances))

        # Should we scale up because of backlog
        self.pressure += delta * math.sqrt(backlog/self.backlog)

        # Should we scale down due to duty cycle? (are some of the workers idle)
        if duty_cycle > self.high_duty_cycle:
            self.pressure -= delta * (self.high_duty_cycle - duty_cycle)/self.high_duty_cycle
        if duty_cycle < self.low_duty_cycle:
            self.pressure -= delta * (self.low_duty_cycle - duty_cycle)/self.low_duty_cycle

        # Apply the friction, tendency to do nothing, move the change pressure gradually to the center.
        leak = min(self.leak_rate * delta, abs(self.pressure))
        self.pressure = math.copysign(abs(self.pressure) - leak, self.pressure)

        # When we are already at the minimum number of instances, don't let negative values build up
        # otherwise this can cause irregularities in scaling response around the min_instances
        if self.desired_instances == self.min_instances:
            self.pressure = max(0.0, self.pressure)

        # Apply change with each step in the same direction being larger than the last by one
        if self.pressure >= self.growth_threshold:
            self.desired_instances = min(self.max_instances, self.desired_instances + 1)
            self.pressure = 0

        if self.pressure <= self.shrink_threshold:
            self.desired_instances = max(self.min_instances, self.desired_instances - 1)
            self.pressure = 0

    def __deepcopy__(self, memodict=None):
        """Copy properly, since the redis objects don't copy well."""
        prof = ServiceProfile(
            name=self.name,
            container_config=DockerConfig(self.container_config.as_primitives()),
            config_blob=self.config_blob,
            min_instances=self.min_instances,
            max_instances=self.max_instances,
            growth=self.growth_threshold,
            shrink=self.shrink_threshold,
            backlog=self.backlog,
            shutdown_seconds=self.shutdown_seconds,
            privileged=self.privileged
        )
        prof.desired_instances = self.desired_instances
        prof.running_instances = self.running_instances
        prof.pressure = self.pressure
        prof.queue_length = self.queue_length
        prof.duty_cycle = self.duty_cycle
        prof.last_update = self.last_update
        return prof


class ScalerServer(ThreadedCoreBase):
    def __init__(self, config=None, datastore=None, redis=None, redis_persist=None):
        super().__init__('assemblyline.scaler', config=config, datastore=datastore,
                         redis=redis, redis_persist=redis_persist)

        self.scaler_timeout_queue = NamedQueue(SCALER_TIMEOUT_QUEUE, host=self.redis_persist)
        self.error_count_lock = threading.Lock()
        self.error_count: dict[str, list[float]] = {}
        self.status_table = ExpiringHash(SERVICE_STATE_HASH, host=self.redis, ttl=30*60)
        self.service_change_watcher = EventWatcher(self.redis, deserializer=ServiceChange.deserialize)
        self.service_change_watcher.register('changes.services.*', self._handle_service_change_event)

        labels = {
            'app': 'assemblyline',
            'section': 'service',
            'privilege': 'service'
        }

        if self.config.core.scaler.additional_labels:
            labels.update({k: v for k, v in (_l.split("=") for _l in self.config.core.scaler.additional_labels)})

        if KUBERNETES_AL_CONFIG:
            self.log.info(f"Loading Kubernetes cluster interface on namespace: {NAMESPACE}")
            self.controller = KubernetesController(logger=self.log, prefix='alsvc_', labels=labels,
                                                   namespace=NAMESPACE, priority='al-service-priority',
                                                   cpu_reservation=self.config.services.cpu_reservation,
                                                   log_level=self.config.logging.log_level)
            # If we know where to find it, mount the classification into the service containers
            if CLASSIFICATION_CONFIGMAP:
                self.controller.config_mount('classification-config', config_map=CLASSIFICATION_CONFIGMAP,
                                             key=CLASSIFICATION_CONFIGMAP_KEY,
                                             target_path='/etc/assemblyline/classification.yml')
            if CONFIGURATION_CONFIGMAP:
                self.controller.core_config_mount('assemblyline-config', config_map=CONFIGURATION_CONFIGMAP,
                                                  key=CONFIGURATION_CONFIGMAP_KEY,
                                                  target_path='/etc/assemblyline/config.yml')
        else:
            self.log.info("Loading Docker cluster interface.")
            self.controller = DockerController(logger=self.log, prefix=NAMESPACE,
                                               labels=labels, log_level=self.config.logging.log_level)
            self._service_stage_hash.delete()

            if DOCKER_CONFIGURATION_PATH and DOCKER_CONFIGURATION_VOLUME:
                self.controller.core_mounts.append((DOCKER_CONFIGURATION_VOLUME, '/etc/assemblyline/'))

                with open(os.path.join(DOCKER_CONFIGURATION_PATH, 'config.yml'), 'w') as handle:
                    yaml.dump(self.config.as_primitives(), handle)

                with open(os.path.join(DOCKER_CONFIGURATION_PATH, 'classification.yml'), 'w') as handle:
                    yaml.dump(get_classification().original_definition, handle)

            # If we know where to find it, mount the classification into the service containers
            if CLASSIFICATION_HOST_PATH:
                self.controller.global_mounts.append((CLASSIFICATION_HOST_PATH, '/etc/assemblyline/classification.yml'))

        # Information about services
        self.profiles: dict[str, ServiceProfile] = {}
        self.profiles_lock = threading.RLock()

        # Prepare a single threaded scheduler
        self.state = collection.Collection(period=self.config.core.metrics.export_interval)
        self.stopping = threading.Event()
        self.main_loop_exit = threading.Event()

        # Load the APM connection if any
        self.apm_client = None
        if self.config.core.metrics.apm_server.server_url:
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="scaler")

    def log_crashes(self, fn):
        @functools.wraps(fn)
        def with_logs(*args, **kwargs):
            # noinspection PyBroadException
            try:
                fn(*args, **kwargs)
            except ServiceControlError as error:
                self.log.exception(f"Error while managing service: {error.service_name}")
                self.handle_service_error(error.service_name)
            except Exception:
                self.log.exception(f'Crash in scaler: {fn.__name__}')
        return with_logs

    @elasticapm.capture_span(span_type=APM_SPAN_TYPE)
    def add_service(self, profile: ServiceProfile):
        # We need to hold the lock the whole time we add the service,
        # we don't want the scaling thread trying to adjust the scale of a
        # deployment we haven't added to the system yet
        with self.profiles_lock:
            profile.desired_instances = max(self.controller.get_target(profile.name), profile.min_instances)
            profile.running_instances = profile.desired_instances
            profile.target_instances = profile.desired_instances
            self.log.debug(f'Starting service {profile.name} with a target of {profile.desired_instances}')
            profile.last_update = time.time()
            self.profiles[profile.name] = profile
            self.controller.add_profile(profile, scale=profile.desired_instances)

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
        self.controller.stop()

    def _handle_service_change_event(self, data: ServiceChange):
        if data.operation == Operation.Removed:
            self.log.info(f'Service appears to be deleted, removing {data.name}')
            stage = self.get_service_stage(data.name)
            self.stop_service(data.name, stage)
        else:
            self._sync_service(self.datastore.get_service_with_delta(data.name))

    def sync_services(self):
        while self.running:
            with apm_span(self.apm_client, 'sync_services'):
                with self.profiles_lock:
                    current_services = set(self.profiles.keys())
                discovered_services: list[str] = []

                # Get all the service data
                for service in self.datastore.list_all_services(full=True):
                    self._sync_service(service)
                    discovered_services.append(service.name)

                # Find any services we have running, that are no longer in the database and remove them
                for stray_service in current_services - set(discovered_services):
                    self.log.info(f'Service appears to be deleted, removing stray {stray_service}')
                    stage = self.get_service_stage(stray_service)
                    self.stop_service(stray_service, stage)

            self.sleep(SERVICE_SYNC_INTERVAL)

    def _sync_service(self, service: Service):
        name = service.name
        stage = self.get_service_stage(service.name)
        default_settings = self.config.core.scaler.service_defaults
        image_variables: defaultdict[str, str] = defaultdict(str)
        image_variables.update(self.config.services.image_variables)

        def prepare_container(docker_config: DockerConfig) -> DockerConfig:
            docker_config.image = Template(docker_config.image).safe_substitute(image_variables)
            set_keys = set(var.name for var in docker_config.environment)
            for var in default_settings.environment:
                if var.name not in set_keys:
                    docker_config.environment.append(var)
            return docker_config

        # noinspection PyBroadException
        try:
            # Is this service considered compatible to run on this version of Assemblyline?
            system_spec = f'{FRAMEWORK_VERSION}.{SYSTEM_VERSION}'
            if not service.version.startswith(system_spec):
                # Raise awareness to other components by editing document in datastore
                service.enabled = False
                if self.datastore.service.save(key=f'{service.name}_{service.version}', data=service):
                    self.stop_service(service.name, stage)
                    raise Exception("Service version isn't compatible with system.\n"
                                    f"Expected: '{system_spec}.X.{service.update_channel}Y'.\n"
                                    f"Got: {service.version}\n"
                                    "Service will be disabled.")

            if not service.enabled:
                self.stop_service(service.name, stage)
                return

            # Build the docker config for the dependencies. For now the dependency blob values
            # aren't set for the change key going to kubernetes because everything about
            # the dependency config should be captured in change key that the function generates
            # internally. A change key is set for the service deployment as that includes
            # things like the submission params
            dependency_config: dict[str, Any] = {}
            dependency_blobs: dict[str, str] = {}
            for _n, dependency in service.dependencies.items():
                dependency.container = prepare_container(dependency.container)
                dependency_config[_n] = dependency
                dependency_blobs[_n] = str(dependency) + str(service.version)

            # Check if the service dependencies have been deployed.
            dependency_keys = []
            updater_ready = stage == ServiceStage.Running
            if service.update_config:
                for _n, dependency in dependency_config.items():
                    key = self.controller.stateful_container_key(service.name, _n, dependency, '')
                    if key:
                        dependency_keys.append(_n + key)
                    else:
                        updater_ready = False

            # If stage is not set to running or a dependency container is missing start the setup process
            if not updater_ready:
                self.log.info(f'Preparing environment for {service.name}')
                # Move to the next service stage (do this first because the container we are starting may care)
                if service.update_config and service.update_config.wait_for_update:
                    self._service_stage_hash.set(name, ServiceStage.Update)
                    stage = ServiceStage.Update
                else:
                    self._service_stage_hash.set(name, ServiceStage.Running)
                    stage = ServiceStage.Running

                # Enable this service's dependencies before trying to launch the service containers
                dependency_internet = [(name, dependency.container.allow_internet_access)
                                       for name, dependency in dependency_config.items()]

                self.controller.prepare_network(
                    service.name, service.docker_config.allow_internet_access, dependency_internet)
                for _n, dependency in dependency_config.items():
                    self.log.info(f'Launching {service.name} dependency {_n}')
                    self.controller.start_stateful_container(
                        service_name=service.name,
                        container_name=_n,
                        spec=dependency,
                        labels={'dependency_for': service.name},
                        change_key=''
                    )

            # If the conditions for running are met deploy or update service containers
            if stage == ServiceStage.Running:
                # Compute a blob of service properties not include in the docker config, that
                # should still result in a service being restarted when changed
                config_blob = str(sorted(service.config.items()))
                config_blob += str(service.submission_params)
                config_blob += ''.join(sorted(dependency_keys))

                # Build the docker config for the service, we are going to either create it or
                # update it so we need to know what the current configuration is either way
                docker_config = prepare_container(service.docker_config)
                config_blob += str(docker_config)

                # Add the service to the list of services being scaled
                with self.profiles_lock:
                    if name not in self.profiles:
                        self.log.info(f"Adding "
                                      f"{f'privileged {service.name}' if service.privileged else service.name}"
                                      " to scaling")
                        self.add_service(ServiceProfile(
                            name=name,
                            min_instances=default_settings.min_instances,
                            growth=default_settings.growth,
                            shrink=default_settings.shrink,
                            config_blob=config_blob,
                            dependency_blobs=dependency_blobs,
                            backlog=default_settings.backlog,
                            max_instances=service.licence_count,
                            container_config=docker_config,
                            queue=get_service_queue(name, self.redis),
                            # Give service an extra 30 seconds to upload results
                            shutdown_seconds=service.timeout + 30,
                            privileged=service.privileged
                        ))

                    # Update RAM, CPU, licence requirements for running services
                    else:
                        profile = self.profiles[name]
                        profile.max_instances = service.licence_count

                        for dependency_name, dependency_blob in dependency_blobs.items():
                            if profile.dependency_blobs[dependency_name] != dependency_blob:
                                self.log.info(f"Updating deployment information for {name}/{dependency_name}")
                                profile.dependency_blobs[dependency_name] = dependency_blob
                                self.controller.start_stateful_container(
                                    service_name=service.name,
                                    container_name=dependency_name,
                                    spec=dependency_config[dependency_name],
                                    labels={'dependency_for': service.name},
                                    change_key=''
                                )

                        if profile.config_blob != config_blob:
                            self.log.info(f"Updating deployment information for {name}")
                            profile.container_config = docker_config
                            profile.config_blob = config_blob
                            self.controller.restart(profile)
                            self.log.info(f"Deployment information for {name} replaced")

        except Exception:
            self.log.exception(f"Error applying service settings from: {service.name}")
            self.handle_service_error(service.name)

    @elasticapm.capture_span(span_type=APM_SPAN_TYPE)
    def stop_service(self, name: str, current_stage: ServiceStage):
        if current_stage != ServiceStage.Off:
            # Disable this service's dependencies
            self.controller.stop_containers(labels={
                'dependency_for': name
            })

            # Mark this service as not running in the shared record
            self._service_stage_hash.set(name, ServiceStage.Off)

        # Stop any running disabled services
        if name in self.profiles or self.controller.get_target(name) > 0:
            self.log.info(f'Removing {name} from scaling')
            with self.profiles_lock:
                self.profiles.pop(name, None)
            self.controller.set_target(name, 0)

    def update_scaling(self):
        """Check if we need to scale any services up or down."""
        pool = Pool()
        while self.sleep(SCALE_INTERVAL):
            with apm_span(self.apm_client, 'update_scaling'):
                # Figure out what services are expected to be running and how many
                with elasticapm.capture_span('read_profiles'):
                    with self.profiles_lock:
                        all_profiles: dict[str, ServiceProfile] = copy.deepcopy(self.profiles)
                    raw_targets = self.controller.get_targets()
                    targets = {_p.name: raw_targets.get(_p.name, 0) for _p in all_profiles.values()}

                for name, profile in all_profiles.items():
                    self.log.debug(f'{name}')
                    self.log.debug(f'Instances \t{profile.min_instances} < {profile.desired_instances} | '
                                   f'{targets[name]} < {profile.max_instances}')
                    self.log.debug(f'Pressure \t{profile.shrink_threshold} < '
                                   f'{profile.pressure} < {profile.growth_threshold}')

                #
                #   1.  Any processes that want to release resources can always be approved first
                #
                with pool:
                    for name, profile in all_profiles.items():
                        if targets[name] > profile.desired_instances:
                            self.log.info(f"{name} wants less resources changing allocation "
                                          f"{targets[name]} -> {profile.desired_instances}")
                            pool.call(self.controller.set_target, name, profile.desired_instances)
                            targets[name] = profile.desired_instances

                #
                #   2.  Any processes that aren't reaching their min_instances target must be given
                #       more resources before anyone else is considered.
                #
                    for name, profile in all_profiles.items():
                        if targets[name] < profile.min_instances:
                            self.log.info(f"{name} isn't meeting minimum allocation "
                                          f"{targets[name]} -> {profile.min_instances}")
                            pool.call(self.controller.set_target, name, profile.min_instances)
                            targets[name] = profile.min_instances

                #
                #   3.  Try to estimate available resources, and based on some metric grant the
                #       resources to each service that wants them. While this free memory
                #       pool might be spread across many nodes, we are going to treat it like
                #       it is one big one, and let the orchestration layer sort out the details.
                #

                # Recalculate the amount of free resources expanding the total quantity by the overallocation
                free_cpu, total_cpu = self.controller.cpu_info()
                used_cpu = total_cpu - free_cpu
                free_cpu = total_cpu * self.config.core.scaler.cpu_overallocation - used_cpu

                free_memory, total_memory = self.controller.memory_info()
                used_memory = total_memory - free_memory
                free_memory = total_memory * self.config.core.scaler.memory_overallocation - used_memory

                #
                def trim(prof: list[ServiceProfile]):
                    prof = [_p for _p in prof if _p.desired_instances > targets[_p.name]]
                    drop = [_p for _p in prof if _p.cpu > free_cpu or _p.ram > free_memory]
                    if drop:
                        summary = {_p.name: (_p.cpu, _p.ram) for _p in drop}
                        self.log.debug(f"Can't make more because not enough resources {summary}")
                    prof = [_p for _p in prof if _p.cpu <= free_cpu and _p.ram <= free_memory]
                    return prof

                remaining_profiles: list[ServiceProfile] = trim(list(all_profiles.values()))
                # The target values up until now should be in sync with the container orchestrator
                # create a copy, so we can track which ones change in the following loop
                old_targets = dict(targets)

                while remaining_profiles:
                    # TODO do we need to add balancing metrics other than 'least running' for this? probably
                    remaining_profiles.sort(key=lambda _p: targets[_p.name])

                    # Add one for the profile at the bottom
                    free_memory -= remaining_profiles[0].container_config.ram_mb
                    free_cpu -= remaining_profiles[0].container_config.cpu_cores
                    targets[remaining_profiles[0].name] += 1

                    # Take out any services that should be happy now
                    remaining_profiles = trim(remaining_profiles)

                # Apply those adjustments we have made back to the controller
                with elasticapm.capture_span('write_targets'):
                    with pool:
                        for name, value in targets.items():
                            if name not in self.profiles:
                                # A service was probably added/removed while we were
                                # in the middle of this function
                                continue
                            self.profiles[name].target_instances = value
                            old = old_targets[name]
                            if value != old:
                                self.log.info(f"Scaling service {name}: {old} -> {value}")
                                pool.call(self.controller.set_target, name, value)

    @elasticapm.capture_span(span_type=APM_SPAN_TYPE)
    def handle_service_error(self, service_name: str):
        """Handle an error occurring in the *analysis* service.

        Errors for core systems should simply be logged, and a best effort to continue made.

        For analysis services, ignore the error a few times, then disable the service.
        """
        with self.error_count_lock:
            try:
                self.error_count[service_name].append(time.time())
            except KeyError:
                self.error_count[service_name] = [time.time()]

            self.error_count[service_name] = [
                _t for _t in self.error_count[service_name]
                if _t >= time.time() - ERROR_EXPIRY_TIME
            ]

            if len(self.error_count[service_name]) >= MAXIMUM_SERVICE_ERRORS:
                self.datastore.service_delta.update(service_name, [
                    (self.datastore.service_delta.UPDATE_SET, 'enabled', False)
                ])
                del self.error_count[service_name]

    def sync_metrics(self):
        """Check if there are any pub-sub messages we need."""
        while self.sleep(METRIC_SYNC_INTERVAL):
            with apm_span(self.apm_client, 'sync_metrics'):
                # Pull service metrics from redis
                service_data = self.status_table.items()
                for host, (service, state, time_limit) in service_data.items():
                    # If an entry hasn't expired, take it into account
                    if time.time() < time_limit:
                        self.state.update(service=service, host=host, throughput=0,
                                          busy_seconds=METRIC_SYNC_INTERVAL if state == ServiceStatus.Running else 0)

                    # If an entry expired a while ago, the host is probably not in use any more
                    if time.time() > time_limit + 600:
                        self.status_table.pop(host)

                # Download the current targets in the orchestrator while not holding the lock
                with self.profiles_lock:
                    targets = {name: profile.target_instances for name, profile in self.profiles.items()}

                # Check the set of services that might be sitting at zero instances, and if it is, we need to
                # manually check if it is offline
                export_interval = self.config.core.metrics.export_interval

                with self.profiles_lock:
                    queues = [profile.queue for profile in self.profiles.values() if profile.queue]
                    lengths_list = pq_length(*queues)
                    lengths = {_q: _l for _q, _l in zip(queues, lengths_list)}

                    for profile_name, profile in self.profiles.items():
                        queue_length = lengths.get(profile.queue, 0)

                        # Pull out statistics from the metrics regularization
                        update = self.state.read(profile_name)
                        if update:
                            delta = time.time() - profile.last_update
                            profile.update(
                                delta=delta,
                                backlog=queue_length,
                                **update
                            )

                        # Check if we expect no messages, if so pull the queue length ourselves
                        # since there is no heartbeat
                        if targets.get(profile_name) == 0 and profile.desired_instances == 0 and profile.queue:

                            if queue_length > 0:
                                self.log.info(f"Service at zero instances has messages: "
                                              f"{profile.name} ({queue_length} in queue)")
                            profile.update(
                                delta=export_interval,
                                instances=0,
                                backlog=queue_length,
                                duty_cycle=profile.high_duty_cycle
                            )

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
                            self.log.error(f"Exception trying to stop timed out service container: {exception}")

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
                            'pressure': profile.pressure
                        }

                for service_name, metrics in service_metrics.items():
                    export_metrics_once(service_name, Status, metrics, host=HOSTNAME,
                                        counter_type='scaler_status', config=self.config, redis=self.redis)

                memory, memory_total = self.controller.memory_info()
                cpu, cpu_total = self.controller.cpu_info()
                metrics = {
                    'memory_total': memory_total,
                    'cpu_total': cpu_total,
                    'memory_free': memory,
                    'cpu_free': cpu
                }
                export_metrics_once('scaler', Metrics, metrics, host=HOSTNAME,
                                    counter_type='scaler', config=self.config, redis=self.redis)

    def log_container_events(self):
        """The service status table may have references to containers that have crashed. Try to remove them all."""
        while self.sleep(CONTAINER_EVENTS_LOG_INTERVAL):
            with apm_span(self.apm_client, 'log_container_events'):
                for message in self.controller.new_events():
                    self.log.warning("Container Event :: " + message)
