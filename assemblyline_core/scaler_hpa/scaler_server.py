"""
An auto-scaling service specific to Assemblyline services.
"""
from __future__ import annotations

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
from assemblyline.common.forge import get_service_queue
from assemblyline.common.uid import get_id_from_data
from assemblyline.common.version import FRAMEWORK_VERSION, SYSTEM_VERSION
from assemblyline.odm.messages.changes import Operation
from assemblyline.odm.models.config import Mount
from assemblyline.odm.models.service import DependencyConfig, DockerConfig, EnvironmentVariable, Service
from assemblyline.remote.datatypes.queues.priority import PriorityQueue
from assemblyline.remote.datatypes.queues.priority import length as pq_length

from assemblyline_core.scaler import collection
from assemblyline_core.scaler.base import ScalerBase, APM_SPAN_TYPE, apm_span, ProfileBase
from assemblyline_core.scaler_hpa.kubernetes import KubernetesController
from assemblyline_core.server_base import ServiceStage
from assemblyline_core.updater.helper import get_registry_config

# How often (in seconds) to download new service data, try to scale managed services,
# and download more metrics data respectively
SERVICE_SYNC_INTERVAL = 60 * 30  # Every half hour
SCALE_INTERVAL = 5
METRIC_SYNC_INTERVAL = 0.5
HEARTBEAT_INTERVAL = 5

# An environment variable that should be set when we are started with kubernetes, tells us how to attach
# the global Assemblyline config to new things that we launch.
KUBERNETES_AL_CONFIG: str = os.environ.get('KUBERNETES_AL_CONFIG', '')
if not KUBERNETES_AL_CONFIG:
    raise RuntimeError("The KUBERNETES_AL_CONFIG must be set to use the HPA scaler.")

RELEASE_NAME = os.getenv('RELEASE_NAME', 'assemblyline')
NAMESPACE = os.getenv('NAMESPACE', 'al')
CLASSIFICATION_HOST_PATH = os.getenv('CLASSIFICATION_HOST_PATH', None)

SERVICE_API_HOST = os.getenv('SERVICE_API_HOST', None)
SERVICE_API_KEY = os.getenv('SERVICE_API_KEY', None)
INTERNAL_ENCRYPT = bool(SERVICE_API_HOST and SERVICE_API_HOST.startswith('https'))
SCALE_TO_ZERO_DELAY = 60


class ServiceProfile(ProfileBase):
    """A profile, describing a currently running service.

    This includes how the service should be run, and conditions related to the scaling of the service.

    The information here is combined from:
     - the service settings.
     - kubernetes resource info
     - metrics and readings from redis
    """

    def __init__(self, name: str, container_config: DockerConfig, config_blob: str,
                 min_instances: int, max_instances: int, queue: PriorityQueue, target_queue_length: int,
                 shutdown_seconds: int = 30, dependency_blobs: Optional[dict[str, str]] = None):
        """
        :param name: Name of the service to manage
        :param container_config: Instructions on how to start this service
        :param min_instances: The minimum number of copies of this service keep running
        :param max_instances: The maximum number of copies permitted to be running
        :param queue: Queue object for monitoring
        """
        super().__init__(name, target_queue_length)
        self.name = name
        self.queue: PriorityQueue = queue
        self.container_config = container_config
        self.shutdown_seconds = shutdown_seconds
        self.config_blob = config_blob
        self.dependency_blobs = dependency_blobs or {}
        self.privileged = False

        # How many instances we want, and can have
        self.min_instances: int = max(0, int(min_instances))
        self.max_instances: int = max(0, int(max_instances))

        # How busy the service is based on feedback metrics
        self.duty_cycle: float = 0.0

    @property
    def pressure(self):
        return 0

    @property
    def instance_limit(self):
        return self.max_instances


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
            self.log.debug('Starting service %s', profile.name)
            self.profiles[profile.name] = profile
            self.controller.add_profile(profile)

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
                max_instances = service.licence_count
                if not max_instances:
                    # Apply an abitrary high limit for unlimited services.
                    max_instances = 2 ** 20
                target_queue_length = self.config.core.scaler.service_defaults.backlog
                if service.expected_queue_length:
                    target_queue_length = service.expected_queue_length

                config_blob = (f"c={cfg_items}sp={service.submission_params}"
                               f"dk={dep_keys}p={service.privileged}d={docker_config}ssl={INTERNAL_ENCRYPT}"
                               f"min={min_instances}max={max_instances}ql={target_queue_length}")

                # Add the service to the list of services being scaled
                with self.profiles_lock:

                    if name not in self.profiles:
                        self.log.info("Adding %s%s to scaling",
                                      'privileged ' if service.privileged else '', service.name)
                        self.add_service(ServiceProfile(
                            name=name,
                            min_instances=min_instances,
                            max_instances=max_instances,
                            config_blob=config_blob,
                            dependency_blobs=dependency_blobs,
                            container_config=docker_config,
                            queue=get_service_queue(name, self.redis),
                            # Give service an extra 30 seconds to upload results
                            shutdown_seconds=service.timeout + 30,
                            target_queue_length=target_queue_length
                        ))

                    # Update RAM, CPU, licence requirements for running services
                    else:
                        profile = self.profiles[name]
                        profile.min_instances = min_instances
                        profile.max_instances = max_instances
                        profile.target_queue_length = target_queue_length

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
                # Set the scale in kubernetes to 0, but wipe the config_blob so we
                # definately reset that information once the service is running
                self.controller.set_target(name, 0)
                self.profiles[name].config_blob = ''

        except Exception:
            self.log.exception("Error applying service settings from: %s", service.name)

    def update_scaling(self) -> None:
        """Check if we need to scale any services up or down."""

        scale_down: dict[str, float] = {}

        while self.sleep(SCALE_INTERVAL):

            with apm_span(self.apm_client, 'update_scaling'):
                scale_up: list[str] = []
                active_names: list[str] = []

                with self.profiles_lock:
                    names = list(self.profiles.keys())

                # We want to evaluate active service profiles
                active_names = [
                    name for name in names
                    if self.get_service_stage(name) == ServiceStage.Running
                ]

                with self.profiles_lock:
                    for name in active_names:
                        profile = self.profiles.get(name, None)
                        scale_down_start = scale_down.pop(name, time.time())
                        if profile is None:
                            continue

                        if profile.min_instances != 0:
                            # Anything that has a min instance above zero, but is targeted to
                            # zero needs to have scaling enabled
                            if profile.target_instances == 0:
                                scale_up.append(name)
                            continue  # Otherwise ignore services with above zero min_instances

                        # We are only looking at services that have the ability to scale to zero
                        # Check for services that need to having scaling re-enabled
                        if profile.queue_length > 0 and profile.target_instances == 0:
                            scale_up.append(name)

                        # Check for services that can be scaled to zero
                        elif profile.queue_length == 0 and profile.duty_cycle == 0 and profile.target_instances == 1:
                            scale_down.setdefault(name, scale_down_start)

                # Apply the adjustments
                for service_name in scale_up:
                    self.controller.set_target(service_name, 1)
                for (service_name, timer) in scale_down.items():
                    if time.time() - timer > SCALE_TO_ZERO_DELAY:
                        self.controller.set_target(service_name, 0)

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

                with self.profiles_lock:
                    # Read the current targets
                    for profile in self.profiles.values():
                        profile.target_instances = self.controller.get_target(profile.name)

                    # Get ready to read the queue lengths
                    queues = [profile.queue for profile in self.profiles.values() if profile.queue]

                lengths_list = pq_length(*queues)

                with self.profiles_lock:
                    lengths = dict(zip(queues, lengths_list))

                    for profile_name, profile in self.profiles.items():
                        queue_length = lengths.get(profile.queue, 0)

                        # Pull out statistics from the metrics regularization
                        update = self.state.read(profile_name)
                        if update:
                            # delta = time.time() - profile.last_update
                            profile.queue_length = queue_length
                            profile.running_instances = update['instances']
                            profile.duty_cycle = update['duty_cycle']

                        # Check if we expect no messages, if so pull the queue length ourselves
                        # since there is no heartbeat
                        if profile.target_instances == 0 and profile.queue:
                            if queue_length > 0:
                                self.log.debug("Service at zero instances has messages: %s (%s in queue)",
                                               profile.name, queue_length)
                            profile.queue_length = queue_length
                            profile.running_instances = 0
                            profile.duty_cycle = 0.7

