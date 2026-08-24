"""
An auto-scaling service specific to Assemblyline services.
"""
from __future__ import annotations

import concurrent.futures
import copy
import math
import os
import threading
import time
from collections import defaultdict
from string import Template
from typing import Any, Dict, Optional

import elasticapm
import yaml
from assemblyline.common.constants import (
    SCALER_TIMEOUT_QUEUE,
    SERVICE_STATE_HASH,
    ServiceStatus,
)
from assemblyline.common.dict_utils import flatten, get_recursive_sorted_tuples
from assemblyline.common.forge import get_classification, get_service_queue
from assemblyline.common.uid import get_id_from_data
from assemblyline.common.version import FRAMEWORK_VERSION, SYSTEM_VERSION
from assemblyline.odm.messages.changes import Operation
from assemblyline.odm.models.config import Mount
from assemblyline.odm.models.service import DependencyConfig, DockerConfig, EnvironmentVariable, Service
from assemblyline.remote.datatypes.queues.priority import PriorityQueue
from assemblyline.remote.datatypes.queues.priority import length as pq_length

from assemblyline_core.scaler import collection
from assemblyline_core.scaler.base import ScalerBase, APM_SPAN_TYPE, apm_span, ProfileBase
from assemblyline_core.scaler.controllers import DockerController, KubernetesController
from assemblyline_core.scaler.controllers.interface import ServiceControlError
from assemblyline_core.server_base import ServiceStage, ThreadedCoreBase
from assemblyline_core.updater.helper import get_registry_config


SCALE_INTERVAL = 5
METRIC_SYNC_INTERVAL = 0.5

# The maximum containers we ask to be created in a single scaling iteration
# This is to give kubernetes a chance to update our view of resource usage before we ask for more containers
MAX_CONTAINER_ALLOCATION = 10

# An environment variable that should be set when we are started with kubernetes, tells us how to attach
# the global Assemblyline config to new things that we launch.
KUBERNETES_AL_CONFIG = os.environ.get('KUBERNETES_AL_CONFIG')

RELEASE_NAME = os.getenv('RELEASE_NAME', 'assemblyline')
NAMESPACE = os.getenv('NAMESPACE', 'al')
CLASSIFICATION_HOST_PATH = os.getenv('CLASSIFICATION_HOST_PATH', None)

DOCKER_CONFIGURATION_PATH = os.getenv('DOCKER_CONFIGURATION_PATH', None)
DOCKER_CONFIGURATION_VOLUME = os.getenv('DOCKER_CONFIGURATION_VOLUME', None)

SERVICE_API_HOST = os.getenv('SERVICE_API_HOST', None)
SERVICE_API_KEY = os.getenv('SERVICE_API_KEY', None)
INTERNAL_ENCRYPT = bool(SERVICE_API_HOST and SERVICE_API_HOST.startswith('https'))


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


class ServiceProfile(ProfileBase):
    """A profile, describing a currently running service.

    This includes how the service should be run, and conditions related to the scaling of the service.
    """

    def __init__(self, name: str, container_config: DockerConfig, config_blob: str = '',
                 min_instances: int = 0, max_instances: Optional[int] = None, growth: float = 600,
                 shrink: Optional[float] = None, target_queue_length: int = 500, queue: Optional[PriorityQueue] = None,
                 shutdown_seconds: int = 30, dependency_blobs: Optional[dict[str, str]] = None):
        """
        :param name: Name of the service to manage
        :param container_config: Instructions on how to start this service
        :param min_instances: The minimum number of copies of this service keep running
        :param max_instances: The maximum number of copies permitted to be running
        :param growth: Delay before growing a service, unit-less, approximately seconds
        :param shrink: Delay before shrinking a service, unit-less, approximately seconds, defaults to -growth
        :param target_queue_length: How long a queue backlog should be before it takes `growth` seconds to grow.
        :param queue: Queue object for monitoring
        :param privileged: Is this service able to interact with core directly?
        """
        super().__init__(name, target_queue_length)
        self.name = name
        self.queue: Optional[PriorityQueue] = queue
        self.container_config = container_config
        self.high_duty_cycle = 0.7
        self.low_duty_cycle = 0.5
        self.shutdown_seconds = shutdown_seconds
        self.config_blob = config_blob
        self.dependency_blobs = dependency_blobs or {}
        self.privileged = False

        # How many instances we want, and can have
        self._min_instances: int = max(0, int(min_instances))
        self._max_instances: int = max(0, int(max_instances or 0))
        self.desired_instances: int = 0

        # Information tracking when we want to grow/shrink
        self.pressure: float = 0
        self.growth_threshold = abs(float(growth))
        self.shrink_threshold = -self.growth_threshold/2 if shrink is None else -abs(float(shrink))
        self.leak_rate: float = 0.1

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

    @property
    def min_instances(self) -> int:
        return self._min_instances

    @min_instances.setter
    def min_instances(self, value: int):
        self._min_instances = max(0, value)

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
        self.pressure += delta * math.sqrt(backlog/self.target_queue_length)

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
            target_queue_length=self.target_queue_length,
            shutdown_seconds=self.shutdown_seconds,
        )
        prof.desired_instances = self.desired_instances
        prof.running_instances = self.running_instances
        prof.pressure = self.pressure
        prof.queue_length = self.queue_length
        prof.duty_cycle = self.duty_cycle
        prof.last_update = self.last_update
        return prof


class ScalerServer(ScalerBase):
    def __init__(self, config=None, datastore=None, redis=None, redis_persist=None) -> None:
        super().__init__(config=config, datastore=datastore, redis=redis, redis_persist=redis_persist)

        core_env: dict[str, str] = {}

        # If we have privileged services, we must be able to pass the necessary environment variables for them to
        # function properly.
        with open('/etc/assemblyline/config.yml', encoding="utf-8") as fh:
            flattened_config: Dict[str, Any] = flatten(yaml.safe_load(fh.read()))

        # Limit secrets to be shared to very specific configurations
        for cfg in ["datastore.hosts", "filestore.archive", "filestore.cache", "filestore.storage"]:
            for conn_str in flattened_config.get(cfg, []):
                # Look for any secrets that need to passed onto services via env
                for secret in Template(conn_str).get_identifiers():
                    try:
                        core_env[secret] = os.environ[secret]
                    except KeyError:
                        # Don't pass through variables that scaler doesn't have
                        # they are likely specific to other components and shouldn't
                        # be shared with privileged services.
                        pass

        # Create a configuration file specifically meant for core containers to consume
        # This should only contain the relevant information to connect to the databases
        privileged_config = yaml.dump({
            'datastore': self.config.datastore.as_primitives(),
            'filestore': self.config.filestore.as_primitives(),
            'core': {
                'redis': self.config.core.redis.as_primitives()
            }
        })

        labels = {
            'app': 'assemblyline',
            'section': 'service',
            'privilege': 'service'
        }
        priv_labels = {}

        service_defaults_config = self.config.core.scaler.service_defaults
        self.max_pending = max(1, self.config.core.scaler.max_pending)

        # If Scaler has envs that set service-server env, then that should override configured values
        if SERVICE_API_HOST:
            service_defaults_config.environment = \
                [EnvironmentVariable(dict(name="SERVICE_API_HOST", value=SERVICE_API_HOST))] + \
                [env for env in service_defaults_config.environment if env.name != "SERVICE_API_HOST"]

        if SERVICE_API_KEY:
            service_defaults_config.environment = \
                [EnvironmentVariable(dict(name="SERVICE_API_KEY", value=SERVICE_API_KEY))] + \
                [env for env in service_defaults_config.environment if env.name != "SERVICE_API_KEY"]

        if self.config.core.scaler.additional_labels:
            labels.update(dict(_l.split("=") for _l in self.config.core.scaler.additional_labels))

        additional_core_labels = self.config.core.scaler.privileged_services_additional_labels
        if additional_core_labels:
            priv_labels.update(dict(_l.split("=") for _l in additional_core_labels))

        if KUBERNETES_AL_CONFIG:
            self.log.info(f"Loading Kubernetes cluster interface on namespace: {NAMESPACE}")
            self.controller = KubernetesController(
                logger=self.log, prefix='alsvc_', labels=labels,
                namespace=NAMESPACE, priority='al-service-priority',
                dependency_priority='al-core-priority',
                cpu_reservation=self.config.services.cpu_reservation,
                cpu_slack=self.config.services.cpu_slack,
                linux_node_selector=self.config.core.scaler.linux_node_selector,
                log_level=self.config.logging.log_level,
                core_env=core_env,
                cluster_pod_list=self.config.core.scaler.cluster_pod_list,
                enable_pod_security=self.config.core.scaler.enable_pod_security,
                default_service_tolerations=service_defaults_config.tolerations,
                priv_labels=priv_labels
            )

            # Add global configuration for privileged services
            # Check if the ConfigMap already exists, if it does, update it
            self.controller.update_config_map(data={'config': privileged_config}, name='privileged-service-config')
            self.controller.add_config_mount(KUBERNETES_AL_CONFIG, config_map='privileged-service-config', key="config",
                                             target_path="/etc/assemblyline/config.yml", read_only=True, core=True)

            # If we're passed an override for server-server and it's defining an HTTPS connection, then add a global
            # mount for the Root CA that needs to be mounted
            if INTERNAL_ENCRYPT:
                service_defaults_config.mounts.append(Mount(dict(
                    name="root-ca",
                    path="/etc/assemblyline/ssl/al_root-ca.crt",
                    resource_type="secret",
                    resource_name=f"{RELEASE_NAME}.internal-generated-ca",
                    resource_key="tls.crt"
                )))

            # Add default mounts for (non-)privileged services
            for mount in service_defaults_config.mounts:
                if mount.resource_type == 'configmap':
                    # ConfigMap-based mount
                    self.controller.add_config_mount(mount.name, config_map=mount.resource_name, key=mount.resource_key,
                                                     target_path=mount.path, read_only=mount.read_only,
                                                     core=mount.privileged_only)
                elif mount.resource_type == 'secret':
                    # Secret-based mount
                    self.controller.add_secret_mount(mount.name, secret_name=mount.resource_name,
                                                     sub_path=mount.resource_key, target_path=mount.path,
                                                     read_only=mount.read_only, core=mount.privileged_only)
                elif mount.resource_type == 'volume':
                    # Add storage-based mount
                    self.controller.add_volume_mount(name=mount.name, target_path=mount.path, read_only=mount.read_only,
                                                     core=mount.privileged_only)
        else:
            self.log.info("Loading Docker cluster interface.")
            self.controller = DockerController(logger=self.log, prefix=NAMESPACE,
                                               labels=labels, log_level=self.config.logging.log_level,
                                               core_env=core_env)
            self._service_stage_hash.delete()

            if DOCKER_CONFIGURATION_PATH and DOCKER_CONFIGURATION_VOLUME:
                self.controller.core_mounts.append((DOCKER_CONFIGURATION_VOLUME, '/etc/assemblyline/'))

                with open(os.path.join(DOCKER_CONFIGURATION_PATH, 'config.yml'), 'w') as handle:
                    # Convert to JSON before converting to YAML to account for direct ODM representation errors
                    handle.write(privileged_config)

                with open(os.path.join(DOCKER_CONFIGURATION_PATH, 'classification.yml'), 'w') as handle:
                    yaml.dump(get_classification().original_definition, handle)

            # If we know where to find it, mount the classification into the service containers
            if CLASSIFICATION_HOST_PATH:
                self.controller.global_mounts.append((CLASSIFICATION_HOST_PATH, '/etc/assemblyline/classification.yml'))

            for mount in service_defaults_config.mounts:
                # Mounts are all storage-based since there's no equivalent to ConfigMaps in Docker
                if mount.privileged_only:
                    self.controller.core_mounts.append((mount.name, mount.path))
                else:
                    self.controller.global_mounts.append((mount.name, mount.path))

        # Prepare a single threaded scheduler
        self.state = collection.Collection(period=self.config.core.metrics.export_interval)
        self.stopping = threading.Event()
        self.main_loop_exit = threading.Event()

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

    def _sync_service(self, service: Service):
        """
        Synchronize the state of the service in the database with the orchestration environment.

        :param service: Service data from the database.
        """
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

            # Set authentication to registry to pull the image
            auth_config = get_registry_config(docker_config, self.config)
            docker_config.registry_username = auth_config['username']
            docker_config.registry_password = auth_config['password']

            return docker_config

        # noinspection PyBroadException
        try:
            def disable_incompatible_service():
                service.enabled = False
                if self.datastore.service_delta.update(service.name,
                                                       [(self.datastore.service_delta.UPDATE_SET, 'enabled', False)]):
                    # Raise awareness to other components by sending an event for the service
                    self.service_event_sender.send(service.name, {
                        'operation': Operation.Incompatible,
                        'name': service.name
                    })

            # Check if service considered compatible to run on Assemblyline?
            system_spec = f'{FRAMEWORK_VERSION}.{SYSTEM_VERSION}'
            if not service.version.startswith(system_spec):
                # If FW and SYS version don't prefix in the service version, we can't guarantee the
                # service is compatible. Disable and treat it as incompatible due to service version.
                self.log.warning(f"Disabling {service.name} with incompatible version. "
                                 f"[{service.version} != '{system_spec}.X.{service.update_channel}Y'].")
                disable_incompatible_service()
            elif service.update_config and service.update_config.wait_for_update and not service.update_config.sources:
                # All signatures sources from a signature-dependent service was removed
                # Disable and treat it as incompatible due to service configuration relative to source management
                self.log.warning("Disabling service with incompatible service configuration. "
                                 "Signature-dependent service has no signature sources.")
                disable_incompatible_service()

            if not service.enabled:
                self.stop_service(service.name, stage)
                return

            # Build the docker config for the dependencies. For now the dependency blob values
            # aren't set for the change key going to kubernetes because everything about
            # the dependency config should be captured in change key that the function generates
            # internally. A change key is set for the service deployment as that includes
            # things like the submission params
            dependency_config: dict[str, DependencyConfig] = {}
            dependency_blobs: dict[str, str] = {}
            for _n, dependency in service.dependencies.items():
                dependency.container = prepare_container(dependency.container)
                dependency_config[_n] = dependency
                dep_hash = get_id_from_data(dependency, length=16)
                dependency_blobs[_n] = f"dh={dep_hash}v={service.version}p={service.privileged}ssl={INTERNAL_ENCRYPT}"

            # Check if the service dependencies have been deployed.
            dependency_keys = dict()
            if service.update_config:
                for _n, dependency in dependency_config.items():
                    key = self.controller.stateful_container_key(service.name, _n, dependency,
                                                                 dependency_blobs.get(_n, ''))
                    if key:
                        dependency_keys[_n] = _n + key
            else:
                # Services without an update configuration are born ready
                self._service_stage_hash.set(name, ServiceStage.Running)
                stage = ServiceStage.Running

            self.log.info('Preparing environment for %s', service.name)

            # Configure the necessary network policies for the service and it's dependencies, if applicable
            dependency_internet = [(name, dependency.container.allow_internet_access)
                                   for name, dependency in dependency_config.items()]

            self.controller.prepare_network(service.name, service.docker_config.allow_internet_access,
                                            dependency_internet)

            # If dependency container(s) are missing, start the setup process
            if set(dependency_keys.keys()) != set(dependency_config.keys()):
                # Services that don't need to wait for an update can be declared ready
                if service.update_config and not service.update_config.wait_for_update:
                    self._service_stage_hash.set(name, ServiceStage.Running)
                    stage = ServiceStage.Running

                for _n, dependency in dependency_config.items():
                    if dependency_keys.get(_n):
                        # Dependency already exists, skip
                        continue
                    self.log.info('Launching %s dependency %s', service.name, _n)
                    self.controller.start_stateful_container(
                        service_name=service.name,
                        container_name=_n,
                        spec=dependency,
                        labels={'dependency_for': service.name},
                        change_key=dependency_blobs.get(_n, '')
                    )

            # If the conditions for running are met deploy or update service containers
            if stage == ServiceStage.Running:
                # Build the docker config for the service, we are going to either create it or
                # update it so we need to know what the current configuration is either way
                docker_config = prepare_container(service.docker_config)

                # Compute a blob of service properties not include in the docker config, that
                # should still result in a service being restarted when changed
                cfg_items = get_recursive_sorted_tuples(service.config)
                dep_keys = ''.join(sorted(dependency_keys.values()))

                min_instances = default_settings.min_instances
                if service.min_instances is not None:
                    # Use service-specific value if present
                    min_instances = service.min_instances
                target_queue_length = self.config.core.scaler.service_defaults.backlog
                if service.expected_queue_length:
                    target_queue_length = service.expected_queue_length

                config_blob = (f"c={cfg_items}sp={service.submission_params}"
                               f"dk={dep_keys}p={service.privileged}d={docker_config}ssl={INTERNAL_ENCRYPT}"
                               f"min={min_instances}ql={target_queue_length}")

                # Add the service to the list of services being scaled
                with self.profiles_lock:

                    if name not in self.profiles:
                        self.log.info("Adding %s to scaling", service.name)
                        self.add_service(ServiceProfile(
                            name=name,
                            min_instances=min_instances,
                            growth=default_settings.growth,
                            shrink=default_settings.shrink,
                            config_blob=config_blob,
                            dependency_blobs=dependency_blobs,
                            target_queue_length=target_queue_length,
                            max_instances=service.licence_count,
                            container_config=docker_config,
                            queue=get_service_queue(name, self.redis),
                            # Give service an extra 30 seconds to upload results
                            shutdown_seconds=service.timeout + 30,
                        ))

                    # Update RAM, CPU, licence requirements for running services
                    else:
                        profile = self.profiles[name]
                        profile.min_instances = min_instances
                        profile.max_instances = service.licence_count

                        for dependency_name, dependency_blob in dependency_blobs.items():
                            if profile.dependency_blobs.get(dependency_name, '') != dependency_blob:
                                self.log.info("Updating deployment information for %s/%s", name, dependency_name)
                                profile.dependency_blobs[dependency_name] = dependency_blob
                                self.controller.start_stateful_container(
                                    service_name=service.name,
                                    container_name=dependency_name,
                                    spec=dependency_config[dependency_name],
                                    labels={'dependency_for': service.name},
                                    change_key=dependency_blob
                                )

                        if profile.config_blob != config_blob:
                            self.log.info("Updating deployment information for %s", name)
                            profile.container_config = docker_config
                            profile.config_blob = config_blob
                            self.controller.restart(profile)
                            self.log.info("Deployment information for %s replaced", name)

            # If service has already been scaled but is not running, scale down until ready
            elif name in self.profiles:
                self.log.info("System has deemed %s not ready/running. Scaling down..", name)
                self.controller.set_target(name, 0)
        except Exception:
            self.log.exception("Error applying service settings from: %s", service.name)

    def update_scaling(self) -> None:
        """Check if we need to scale any services up or down."""
        pool = Pool()
        while self.sleep(SCALE_INTERVAL):
            with apm_span(self.apm_client, 'update_scaling'):
                # Figure out what services are expected to be running and how many
                with elasticapm.capture_span('read_profiles'):
                    with self.profiles_lock:
                        # We want to evaluate 'active' service profiles
                        all_profiles: dict[str, ServiceProfile] = {
                            _n: copy.deepcopy(_v)
                            for _n, _v in self.profiles.items()
                            if self.get_service_stage(_n) == ServiceStage.Running
                        }

                    raw_targets = self.controller.get_targets()
                    # This is the list of targets we will adjust
                    targets = {_p.name: raw_targets.get(_p.name, 0) for _p in all_profiles.values()}
                    # This represents what the environment is currently running
                    old_targets = dict(targets)

                for name, profile in all_profiles.items():
                    self.log.debug(name)
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

                # Get the processed resource numbers
                free_cpu, _ = self.get_cpu_info(overallocation=True)
                free_memory, _ = self.get_memory_info(overallocation=True)

                # Make adjustments to the targets until everything is satisified
                # or we don't have the resouces to make more adjustments
                def trim(prof: list[ServiceProfile]):
                    # only consider services where more are desired
                    prof = [_p for _p in prof if _p.desired_instances > targets[_p.name]]
                    # only consider services where there is room for more pending containers
                    prof = [_p for _p in prof if _p.running_instances + self.max_pending > targets[_p.name]]
                    drop = [_p for _p in prof if _p.cpu > free_cpu or _p.ram > free_memory]
                    if drop:
                        summary = {_p.name: (_p.cpu, _p.ram) for _p in drop}
                        self.log.debug("Can't make more because not enough resources %s", summary)
                    prof = [_p for _p in prof if _p.cpu <= free_cpu and _p.ram <= free_memory]
                    return prof

                remaining_profiles: list[ServiceProfile] = trim(list(all_profiles.values()))
                while remaining_profiles:
                    # Prioritize by adding resources to services with the least instances requested first.
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
                                self.log.info("Scaling service %s: %s -> %s", name, old, value)
                                pool.call(self.controller.set_target, name, value)

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
                    lengths = dict(zip(queues, lengths_list))

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

