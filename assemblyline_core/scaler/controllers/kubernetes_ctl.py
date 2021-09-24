from __future__ import annotations
import base64
from collections import defaultdict
import functools
import json
import uuid
import os
import threading
import weakref
from typing import Optional, Tuple

import urllib3

from kubernetes import client, config, watch
from kubernetes.client import V1Deployment, V1DeploymentSpec, V1PodTemplateSpec, \
    V1PodSpec, V1ObjectMeta, V1Volume, V1Container, V1VolumeMount, V1EnvVar, V1ConfigMapVolumeSource, \
    V1PersistentVolumeClaimVolumeSource, V1LabelSelector, V1ResourceRequirements, V1PersistentVolumeClaim, \
    V1PersistentVolumeClaimSpec, V1NetworkPolicy, V1NetworkPolicySpec, V1NetworkPolicyEgressRule, V1NetworkPolicyPeer, \
    V1NetworkPolicyIngressRule, V1Secret, V1LocalObjectReference, V1Service, V1ServiceSpec, V1ServicePort
from kubernetes.client.rest import ApiException
from assemblyline.odm.models.service import DockerConfig

from assemblyline_core.scaler.controllers.interface import ControllerInterface

# RESERVE_MEMORY_PER_NODE = os.environ.get('RESERVE_MEMORY_PER_NODE')

API_TIMEOUT = 90
WATCH_TIMEOUT = 10 * 60
WATCH_API_TIMEOUT = WATCH_TIMEOUT + 10
CHANGE_KEY_NAME = 'al_change_key'

_exponents = {
    'Ki': 2**10,
    'K': 2**10,
    'Mi': 2**20,
    'M': 2**20,
    'Gi': 2**30,
    'G': 2**30,
    'Ti': 2**40,
    'T': 2**40,
    'Pi': 2**50,
    'P': 2 ** 50,
}


class TypelessWatch(watch.Watch):
    """A kubernetes watch object that doesn't marshal the response."""

    def get_return_type(self, func):
        return None


def median(values: list[float]) -> float:
    if len(values) == 0:
        return 0
    return values[len(values)//2]


def get_resources(container) -> Tuple[float, float]:
    requests = container['resources'].get('requests', {})
    limits = container['resources'].get('limits', {})

    cpu_value = requests.get('cpu', limits.get('cpu', None))
    if cpu_value is not None:
        cpu_value = parse_cpu(cpu_value)

    memory_value = requests.get('memory', limits.get('memory', None))
    if memory_value is not None:
        memory_value = parse_memory(memory_value)

    return cpu_value, memory_value


def create_docker_auth_config(image: str, username: str, password: str) -> str:
    # Take the registry part of the image if set, use the default registry if no registry component is in the string
    if '/' in image:
        server_name = image.rpartition('/')[0]
        if not server_name.startswith('http://') and not server_name.startswith('https://'):
            server_name = 'https://' + server_name
    else:
        server_name = 'https://index.docker.io/v1/'

    # The docker auth string is the base64'd username and password with a : to separate them
    bin_u_pass = f"{username}:{password}".encode()
    auth_string = base64.b64encode(bin_u_pass).decode()

    # Return a string form that matches docker's config.json format
    return json.dumps({
        "auths": {
            server_name: {
                "auth": auth_string
            }
        }
    })


def parse_memory(string: str) -> float:
    """Convert a memory string to megabytes float"""
    # Maybe we have a number in bytes
    try:
        return float(string)/2**20
    except ValueError:
        pass

    # Try parsing a unit'd number then
    if string[-2:] in _exponents:
        return (float(string[:-2]) * _exponents[string[-2:]])/(2**20)
    if string[-1:] in _exponents:
        return (float(string[:-1]) * _exponents[string[-1:]])/(2**20)

    raise ValueError(string)


def parse_cpu(string: str) -> float:
    try:
        return float(string)
    except ValueError:
        pass

    if string.endswith('m'):
        return float(string[:-1])/1000.0

    raise ValueError('Un-parsable CPU string: ' + string)


class KubernetesController(ControllerInterface):
    def __init__(self, logger, namespace, prefix, priority, cpu_reservation, labels=None, log_level="INFO"):
        # Try loading a kubernetes connection from either the fact that we are running
        # inside of a cluster, or have a config file that tells us how
        try:
            config.load_incluster_config()
        except config.config_exception.ConfigException:
            # Load the configuration once to initialize the defaults
            config.load_kube_config()

            # Now we can actually apply any changes we want to make
            cfg = client.configuration.Configuration(client.configuration.Configuration)

            if 'HTTPS_PROXY' in os.environ:
                cfg.proxy = os.environ['HTTPS_PROXY']
                if not cfg.proxy.startswith("http"):
                    cfg.proxy = "https://" + cfg.proxy
                client.Configuration.set_default(cfg)

            # Load again with our settings set
            config.load_kube_config(client_configuration=cfg)

        self.running: bool = True
        self.prefix: str = prefix.lower()
        self.priority: str = priority
        self.cpu_reservation: float = max(0.0, min(cpu_reservation, 1.0))
        self.logger = logger
        self.log_level: str = log_level
        self._labels: dict[str, str] = labels or {}
        self.apps_api = client.AppsV1Api()
        self.api = client.CoreV1Api()
        self.net_api = client.NetworkingV1Api()
        self.namespace: str = namespace
        self.config_volumes: dict[str, V1Volume] = {}
        self.config_mounts: dict[str, V1VolumeMount] = {}
        self.core_config_volumes: dict[str, V1Volume] = {}
        self.core_config_mounts: dict[str, V1VolumeMount] = {}
        self._external_profiles = weakref.WeakValueDictionary()
        self._service_limited_env: dict[str, dict[str, str]] = defaultdict(dict)

        # A record of previously reported events so that we don't report the same message repeatedly, fill it with
        # existing messages so we don't have a huge dump of duplicates on restart
        self.events_window = {}
        response = self.api.list_namespaced_event(namespace='al', pretty='false',
                                                  field_selector='type=Warning', watch=False,
                                                  _request_timeout=API_TIMEOUT)
        for event in response.items:
            # Keep the scaler related events in case it helps us know why scaler was restarting
            if 'scaler' not in event.involved_object.name:
                self.events_window[event.metadata.uid] = event.count

        self._quota_cpu_limit: Optional[float] = None
        self._quota_cpu_used: Optional[float] = None
        self._quota_mem_limit: Optional[float] = None
        self._quota_mem_used: Optional[float] = None
        quota_background = threading.Thread(target=self._loop_forever(self._monitor_quotas), daemon=True)
        quota_background.start()

        self._node_pool_max_ram: float = 0
        self._node_pool_max_cpu: float = 0
        node_background = threading.Thread(target=self._loop_forever(self._monitor_node_pool), daemon=True)
        node_background.start()

        self._pod_used_ram: float = 0
        self._pod_used_cpu: float = 0
        self._pod_used_namespace_ram: float = 0
        self._pod_used_namespace_cpu: float = 0
        pod_background = threading.Thread(target=self._loop_forever(self._monitor_pods), daemon=True)
        pod_background.start()

        self._deployment_targets: dict[str, int] = {}
        deployment_background = threading.Thread(target=self._loop_forever(self._monitor_deployments), daemon=True)
        deployment_background.start()

    def stop(self):
        self.running = False

    def _deployment_name(self, service_name: str):
        return (self.prefix + service_name).lower().replace('_', '-')

    def _dependency_name(self, service_name: str, container_name: str):
        return f"{self._deployment_name(service_name)}-{container_name}".lower()

    def config_mount(self, name: str, config_map: str, key: str, target_path: str):
        if name not in self.config_volumes:
            self.config_volumes[name] = V1Volume(
                name=name,
                config_map=V1ConfigMapVolumeSource(
                    name=config_map,
                    optional=False
                )
            )

        self.config_mounts[target_path] = V1VolumeMount(
            name=name,
            mount_path=target_path,
            sub_path=key
        )

    def core_config_mount(self, name, config_map, key, target_path):
        if name not in self.core_config_volumes:
            self.core_config_volumes[name] = V1Volume(
                name=name,
                config_map=V1ConfigMapVolumeSource(
                    name=config_map,
                    optional=False
                )
            )

        self.core_config_mounts[target_path] = V1VolumeMount(
            name=name,
            mount_path=target_path,
            sub_path=key
        )

    def add_profile(self, profile, scale=0):
        """Tell the controller about a service profile it needs to manage."""
        self._create_deployment(profile.name, self._deployment_name(profile.name),
                                profile.container_config, profile.shutdown_seconds, scale,
                                change_key=profile.config_blob)
        self._external_profiles[profile.name] = profile

    def _loop_forever(self, function):
        @functools.wraps(function)
        def _function():
            while self.running:
                # noinspection PyBroadException
                try:
                    function()

                except (urllib3.exceptions.ProtocolError, urllib3.exceptions.ReadTimeoutError):
                    # Protocol errors are a product of api connections timing out, just retry silently.
                    pass

                except Exception:
                    self.logger.exception(f"Error in {function.__name__}")
        return _function

    def _monitor_node_pool(self):
        self._node_pool_max_cpu = 0
        self._node_pool_max_ram = 0
        watch = TypelessWatch()

        for event in watch.stream(func=self.api.list_node, timeout_seconds=WATCH_TIMEOUT,
                                  _request_timeout=WATCH_API_TIMEOUT):
            if not self.running:
                break

            if event['type'] == "ADDED":
                self._node_pool_max_cpu += parse_cpu(event['raw_object']['status']['allocatable']['cpu'])
                self._node_pool_max_ram += parse_memory(event['raw_object']['status']['allocatable']['memory'])
            elif event['type'] == "DELETED":
                self._node_pool_max_cpu -= parse_cpu(event['raw_object']['status']['allocatable']['cpu'])
                self._node_pool_max_ram -= parse_memory(event['raw_object']['status']['allocatable']['memory'])

    def _monitor_pods(self):
        watch = TypelessWatch()
        containers = {}
        namespaced_containers = {}
        self._pod_used_cpu = 0
        self._pod_used_ram = 0
        self._pod_used_namespace_cpu = 0
        self._pod_used_namespace_ram = 0

        for event in watch.stream(func=self.api.list_pod_for_all_namespaces, timeout_seconds=WATCH_TIMEOUT,
                                  _request_timeout=WATCH_API_TIMEOUT):
            if not self.running:
                break

            uid = event['raw_object']['metadata']['uid']
            namespace = event['raw_object']['metadata']['namespace']

            if event['type'] in ['ADDED', 'MODIFIED']:
                for container in event['raw_object']['spec']['containers']:
                    containers[f"{uid}-{container['name']}"] = get_resources(container)
                    if namespace == self.namespace:
                        namespaced_containers[f"{uid}-{container['name']}"] = get_resources(container)
            elif event['type'] == 'DELETED':
                for container in event['raw_object']['spec']['containers']:
                    containers.pop(f"{uid}-{container['name']}", None)
                    namespaced_containers.pop(f"{uid}-{container['name']}", None)
            else:
                continue

            memory_unrestricted = sum(1 for cpu, mem in containers.values() if mem is None)
            cpu_unrestricted = sum(1 for cpu, mem in containers.values() if cpu is None)

            memory_used = [mem for cpu, mem in containers.values() if mem is not None]
            cpu_used = [cpu for cpu, mem in containers.values() if cpu is not None]

            self._pod_used_cpu = sum(cpu_used) + cpu_unrestricted * median(cpu_used)
            self._pod_used_ram = sum(memory_used) + memory_unrestricted * median(memory_used)

            memory_unrestricted = sum(1 for cpu, mem in namespaced_containers.values() if mem is None)
            cpu_unrestricted = sum(1 for cpu, mem in namespaced_containers.values() if cpu is None)

            memory_used = [mem for cpu, mem in namespaced_containers.values() if mem is not None]
            cpu_used = [cpu for cpu, mem in namespaced_containers.values() if cpu is not None]

            self._pod_used_namespace_cpu = sum(cpu_used) + cpu_unrestricted * median(cpu_used)
            self._pod_used_namespace_ram = sum(memory_used) + memory_unrestricted * median(memory_used)

    def _monitor_quotas(self):
        watch = TypelessWatch()
        cpu_limits = {}
        cpu_used = {}
        mem_limits = {}
        mem_used = {}

        self._quota_cpu_limit = None
        self._quota_cpu_used = None
        self._quota_mem_limit = None
        self._quota_mem_used = None

        for event in watch.stream(func=self.api.list_namespaced_resource_quota, namespace=self.namespace,
                                  timeout_seconds=WATCH_TIMEOUT, _request_timeout=WATCH_API_TIMEOUT):
            if not self.running:
                break

            name = event['raw_object']['metadata']['name']
            if 'scope_selector' in event['raw_object']['spec'] or 'scopes' in event['raw_object']['spec']:
                continue

            if event['type'] in ['ADDED', 'MODIFIED']:
                status = event['raw_object']['status']

                if 'hard' in status:
                    if 'cpu' in status['hard']:
                        cpu_limits[name] = parse_cpu(status['hard']['cpu'])
                    if 'requests.cpu' in status['hard']:
                        cpu_limits[name] = parse_cpu(status['hard']['requests.cpu'])
                    if 'limits.cpu' in status['hard']:
                        cpu_limits[name] = parse_cpu(status['hard']['limits.cpu'])
                    if 'memory' in status['hard']:
                        mem_limits[name] = parse_memory(status['hard']['memory'])
                    if 'requests.memory' in status['hard']:
                        mem_limits[name] = parse_memory(status['hard']['requests.memory'])
                    if 'limits.memory' in status['hard']:
                        mem_limits[name] = parse_memory(status['hard']['limits.memory'])

                if 'used' in status:
                    if 'cpu' in status['used']:
                        cpu_used[name] = parse_cpu(status['used']['cpu'])
                    if 'requests.cpu' in status['used']:
                        cpu_used[name] = parse_cpu(status['used']['requests.cpu'])
                    if 'limits.cpu' in status['used']:
                        cpu_used[name] = parse_cpu(status['used']['limits.cpu'])
                    if 'memory' in status['used']:
                        mem_used[name] = parse_memory(status['used']['memory'])
                    if 'requests.memory' in status['used']:
                        mem_used[name] = parse_memory(status['used']['requests.memory'])
                    if 'limits.memory' in status['used']:
                        mem_used[name] = parse_memory(status['used']['limits.memory'])

            elif event['type'] == 'DELETED':
                cpu_limits.pop(name, None)
                cpu_used.pop(name, None)
                mem_limits.pop(name, None)
                mem_used.pop(name, None)
            else:
                continue

            if cpu_limits:
                self._quota_cpu_limit = min(cpu_limits.values())
            else:
                self._quota_cpu_limit = None

            if cpu_used:
                self._quota_cpu_used = max(cpu_used.values())
            else:
                self._quota_cpu_used = None

            if mem_limits:
                self._quota_mem_limit = min(mem_limits.values())
            else:
                self._quota_mem_limit = None

            if mem_used:
                self._quota_mem_used = max(mem_used.values())
            else:
                self._quota_mem_used = None

    def _monitor_deployments(self):
        watch = TypelessWatch()

        self._deployment_targets = {}
        label_selector = ','.join(f'{_n}={_v}' for _n, _v in self._labels.items())

        for event in watch.stream(func=self.apps_api.list_namespaced_deployment,
                                  namespace=self.namespace, label_selector=label_selector,
                                  timeout_seconds=WATCH_TIMEOUT, _request_timeout=WATCH_API_TIMEOUT):
            if event['type'] in ['ADDED', 'MODIFIED']:
                name = event['raw_object']['metadata']['labels'].get('component', None)
                if name is not None:
                    self._deployment_targets[name] = event['raw_object']['spec']['replicas']
            elif event['type'] == 'DELETED':
                name = event['raw_object']['metadata']['labels'].get('component', None)
                self._deployment_targets.pop(name, None)

    def cpu_info(self):
        if self._quota_cpu_limit:
            if self._quota_cpu_used:
                return self._quota_cpu_limit - self._quota_cpu_used, self._quota_cpu_limit
            return self._quota_cpu_limit - self._pod_used_namespace_cpu, self._quota_cpu_limit
        return self._node_pool_max_cpu - self._pod_used_cpu, self._node_pool_max_cpu

    def memory_info(self):
        if self._quota_mem_limit:
            if self._quota_mem_used:
                return self._quota_mem_limit - self._quota_mem_used, self._quota_mem_limit
            return self._quota_mem_limit - self._pod_used_namespace_ram, self._quota_mem_limit
        return self._node_pool_max_ram - self._pod_used_ram, self._node_pool_max_ram

    def _create_containers(self, service_name: str, deployment_name: str, container_config, mounts,
                           core_container=False):
        cores = container_config.cpu_cores
        memory = container_config.ram_mb
        min_memory = min(container_config.ram_mb_min, container_config.ram_mb)
        environment_variables: list[V1EnvVar] = []
        # If we are launching a core container, include environment variables related to authentication for DBs
        if core_container:
            environment_variables += [V1EnvVar(name=_n, value=_v) for _n, _v in os.environ.items()
                                      if any(term in _n for term in ['ELASTIC', 'FILESTORE', 'UI_SERVER'])]
        # Overwrite them with configured special environment variables
        environment_variables += [V1EnvVar(name=_e.name, value=_e.value) for _e in container_config.environment]
        # Overwrite those with special hard coded variables
        environment_variables += [
            V1EnvVar(name='AL_SERVICE_NAME', value=service_name),
            V1EnvVar(name='LOG_LEVEL', value=self.log_level)
        ]
        # Overwrite ones defined dynamically by dependency container launches
        for name, value in self._service_limited_env[service_name].items():
            environment_variables.append(V1EnvVar(name=name, value=value))
        return [V1Container(
            name=deployment_name,
            image=container_config.image,
            command=container_config.command,
            env=environment_variables,
            image_pull_policy='Always',
            volume_mounts=mounts,
            resources=V1ResourceRequirements(
                limits={'cpu': cores, 'memory': f'{memory}Mi'},
                requests={'cpu': cores*self.cpu_reservation, 'memory': f'{min_memory}Mi'},
            )
        )]

    def _create_deployment(self, service_name: str, deployment_name: str, docker_config: DockerConfig,
                           shutdown_seconds: int, scale: int, labels: dict[str, str] = None,
                           volumes: list[V1Volume] = None, mounts: list[V1VolumeMount] = None,
                           core_mounts: bool = False, change_key: str = ''):
        # Build a cache key to check for changes, just trying to only patch what changed
        # will still potentially result in a lot of restarts due to different kubernetes
        # systems returning differently formatted data
        change_key = (
            deployment_name + change_key + str(docker_config) + str(shutdown_seconds) +
            str(sorted((labels or {}).items())) + str(volumes) + str(mounts) + str(core_mounts)
        )

        # Check if a deployment already exists, and if it does check if it has the same change key set
        replace = None
        try:
            replace = self.apps_api.read_namespaced_deployment(
                deployment_name, namespace=self.namespace, _request_timeout=API_TIMEOUT)
            if replace.metadata.annotations.get(CHANGE_KEY_NAME) == change_key:
                if replace.spec.replicas != scale:
                    self.set_target(service_name, scale)
                return
        except ApiException as error:
            if error.status != 404:
                raise

        # If we have been given a username or password for the registry, we have to
        # update it, if we haven't been, make sure its been cleaned up in the system
        # so we don't leave passwords lying around
        pull_secret_name = f'{deployment_name}-container-pull-secret'
        use_pull_secret = False
        try:
            current_pull_secret = self.api.read_namespaced_secret(pull_secret_name, self.namespace,
                                                                  _request_timeout=API_TIMEOUT)
        except ApiException as error:
            if error.status != 404:
                raise
            current_pull_secret = None

        if docker_config.registry_username or docker_config.registry_password:
            use_pull_secret = True
            # Build the secret we want to make
            new_pull_secret = V1Secret(
                metadata=V1ObjectMeta(name=pull_secret_name, namespace=self.namespace),
                type='kubernetes.io/dockerconfigjson',
                string_data={
                    '.dockerconfigjson': create_docker_auth_config(
                        image=docker_config.image,
                        username=docker_config.registry_username,
                        password=docker_config.registry_password,
                    )
                }
            )

            # Send it to the server
            if current_pull_secret:
                self.api.patch_namespaced_secret(pull_secret_name, namespace=self.namespace, body=new_pull_secret,
                                                 _request_timeout=API_TIMEOUT)
            else:
                self.api.create_namespaced_secret(namespace=self.namespace, body=new_pull_secret,
                                                  _request_timeout=API_TIMEOUT)
        elif current_pull_secret:
            self.api.delete_namespaced_secret(pull_secret_name, self.namespace, _request_timeout=API_TIMEOUT)

        all_labels = dict(self._labels)
        all_labels['component'] = service_name
        if core_mounts:
            all_labels['section'] = 'core'
        all_labels.update(labels or {})

        # Build set of volumes, first the global mounts, then the core specific ones,
        # then the ones specific to this container only
        all_volumes: list[V1Volume] = []
        all_mounts: list[V1VolumeMount] = []
        all_volumes.extend(self.config_volumes.values())
        all_mounts.extend(self.config_mounts.values())
        if core_mounts:
            all_volumes.extend(self.core_config_volumes.values())
            all_mounts.extend(self.core_config_mounts.values())
        all_volumes.extend(volumes or [])
        all_mounts.extend(mounts or [])

        # Build metadata
        metadata = V1ObjectMeta(name=deployment_name, labels=all_labels, annotations={CHANGE_KEY_NAME: change_key})

        pod = V1PodSpec(
            volumes=all_volumes,
            containers=self._create_containers(service_name, deployment_name, docker_config,
                                               all_mounts, core_container=core_mounts),
            priority_class_name=self.priority,
            termination_grace_period_seconds=shutdown_seconds,
        )

        if use_pull_secret:
            pod.image_pull_secrets = [V1LocalObjectReference(name=pull_secret_name)]

        template = V1PodTemplateSpec(
            metadata=metadata,
            spec=pod,
        )

        spec = V1DeploymentSpec(
            replicas=int(scale),
            revision_history_limit=0,
            selector=V1LabelSelector(match_labels=all_labels),
            template=template,
        )

        deployment = V1Deployment(
            kind="Deployment",
            metadata=metadata,
            spec=spec,
        )

        if replace:
            self.logger.info("Requesting kubernetes replace deployment info for: " + metadata.name)
            self.apps_api.replace_namespaced_deployment(namespace=self.namespace, body=deployment,
                                                        name=metadata.name, _request_timeout=API_TIMEOUT)
        else:
            self.logger.info("Requesting kubernetes create deployment info for: " + metadata.name)
            self.apps_api.create_namespaced_deployment(namespace=self.namespace, body=deployment,
                                                       _request_timeout=API_TIMEOUT)

    def get_target(self, service_name: str) -> int:
        """Get the target for running instances of a service."""
        return self._deployment_targets.get(service_name, 0)

    def get_targets(self) -> dict[str, int]:
        """Get the target for running instances of all services."""
        return self._deployment_targets

    def set_target(self, service_name: str, target: int):
        """Set the target for running instances of a service."""
        for _ in range(10):
            try:
                name = self._deployment_name(service_name)
                scale = self.apps_api.read_namespaced_deployment_scale(name=name, namespace=self.namespace,
                                                                       _request_timeout=API_TIMEOUT)
                scale.spec.replicas = target
                self.apps_api.patch_namespaced_deployment_scale(name=name, namespace=self.namespace, body=scale,
                                                                _request_timeout=API_TIMEOUT)
                return
            except client.ApiException as error:
                # If the error is a conflict, it means multiple attempts to scale a deployment
                # were made at the same time and conflicted, we can retry
                if error.reason == 'Conflict':
                    self.logger.info(f"Conflict scaling {service_name} retrying.")
                    continue
                if error.status == 404:
                    profile = self._external_profiles.get(service_name, None)
                    if profile:
                        self.add_profile(profile, scale=target)
                    return
                raise

    def stop_container(self, service_name, container_id):
        try:
            pods = self.api.list_namespaced_pod(namespace=self.namespace,
                                                field_selector=f'metadata.name={container_id}',
                                                label_selector=f'component={service_name}',
                                                _request_timeout=API_TIMEOUT)
            for pod in pods.items:
                if pod.metadata.name == container_id:
                    self.api.delete_namespaced_pod(name=container_id, namespace=self.namespace, grace_period_seconds=0,
                                                   _request_timeout=API_TIMEOUT)
                    return
        except ApiException as error:
            if error.status != 404:
                raise

    def restart(self, service):
        self._create_deployment(service.name, self._deployment_name(service.name), service.container_config,
                                service.shutdown_seconds, self.get_target(service.name),
                                change_key=service.config_blob)

    def get_running_container_names(self):
        pods = self.api.list_pod_for_all_namespaces(field_selector='status.phase==Running',
                                                    _request_timeout=API_TIMEOUT)
        return [pod.metadata.name for pod in pods.items]

    def new_events(self):
        response = self.api.list_namespaced_event(namespace='al', pretty='false',
                                                  field_selector='type=Warning', watch=False,
                                                  _request_timeout=API_TIMEOUT)

        # Pull out events that are new, or have occurred again since last reporting
        new = []
        for event in response.items:
            if self.events_window.get(event.metadata.uid, 0) != event.count:
                self.events_window[event.metadata.uid] = event.count
                new.append(event.involved_object.name + ': ' + event.message)

        # Flush out events that have moved outside the window
        old = set(self.events_window.keys()) - {event.metadata.uid for event in response.items}
        for uid in old:
            self.events_window.pop(uid)

        return new

    def start_stateful_container(self, service_name: str, container_name: str,
                                 spec, labels: dict[str, str], change_key: str):
        # Setup PVC
        deployment_name = self._dependency_name(service_name, container_name)
        mounts, volumes = [], []
        for volume_name, volume_spec in spec.volumes.items():
            mount_name = deployment_name + volume_name

            # Check if the PVC exists, create if not
            self._ensure_pvc(mount_name, volume_spec.storage_class, volume_spec.capacity)

            # Create the volume info
            volumes.append(V1Volume(
                name=mount_name,
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(mount_name)
            ))
            mounts.append(V1VolumeMount(mount_path=volume_spec.mount_path, name=mount_name))

        # Read the key being used for the deployment instance or generate a new one
        try:
            instance_key = uuid.uuid4().hex
            old_deployment = self.apps_api.read_namespaced_deployment(deployment_name, self.namespace)
            for container in old_deployment.spec.template.spec.containers:
                for env in container.env:
                    if env.name == 'AL_INSTANCE_KEY':
                        instance_key = env.value
                        break
        except ApiException as error:
            if error.status != 404:
                raise

        # Setup the deployment itself
        labels['container'] = container_name
        spec.container.environment.append({'name': 'AL_INSTANCE_KEY', 'value': instance_key})
        self._create_deployment(service_name, deployment_name, spec.container,
                                30, 1, labels, volumes=volumes, mounts=mounts,
                                core_mounts=spec.run_as_core, change_key=change_key)

        # Setup a service to direct to the deployment
        try:
            service = self.api.read_namespaced_service(deployment_name, self.namespace)
            service.metadata.labels = labels
            service.spec.selector = labels
            service.spec.ports = [V1ServicePort(port=int(_p)) for _p in spec.container.ports]
            self.api.patch_namespaced_service(deployment_name, self.namespace, service)
        except ApiException as error:
            if error.status != 404:
                raise
            service = V1Service(
                metadata=V1ObjectMeta(name=deployment_name, labels=labels),
                spec=V1ServiceSpec(
                    cluster_ip='None',
                    selector=labels,
                    ports=[V1ServicePort(port=int(_p)) for _p in spec.container.ports]
                )
            )
            self.api.create_namespaced_service(self.namespace, service)

        # Add entries to the environment variable list to point to this container
        self._service_limited_env[service_name][f'{container_name}_host'] = deployment_name
        self._service_limited_env[service_name][f'{container_name}_key'] = instance_key
        if spec.container.ports:
            self._service_limited_env[service_name][f'{container_name}_port'] = spec.container.ports[0]

    def _ensure_pvc(self, name, storage_class, size):
        request = V1ResourceRequirements(requests={'storage': size})
        claim_spec = V1PersistentVolumeClaimSpec(storage_class_name=storage_class, resources=request)
        metadata = V1ObjectMeta(namespace=self.namespace, name=name)
        claim = V1PersistentVolumeClaim(metadata=metadata, spec=claim_spec)
        self.api.create_namespaced_persistent_volume_claim(namespace=self.namespace, body=claim,
                                                           _request_timeout=API_TIMEOUT)

    def stop_containers(self, labels):
        label_selector = ','.join(f'{_n}={_v}' for _n, _v in labels.items())
        deployments = self.apps_api.list_namespaced_deployment(namespace=self.namespace, label_selector=label_selector,
                                                               _request_timeout=API_TIMEOUT)
        for dep in deployments.items:
            self.apps_api.delete_namespaced_deployment(name=dep.metadata.name, namespace=self.namespace,
                                                       _request_timeout=API_TIMEOUT)

    def prepare_network(self, service_name, internet):
        safe_name = service_name.lower().replace('_', '-')

        # Allow access to containers with dependency_for
        try:
            self.net_api.delete_namespaced_network_policy(namespace=self.namespace, name=f'allow-{safe_name}-to-dep',
                                                          _request_timeout=API_TIMEOUT)
        except ApiException as error:
            if error.status != 404:
                raise
        self.net_api.create_namespaced_network_policy(namespace=self.namespace, body=V1NetworkPolicy(
            metadata=V1ObjectMeta(name=f'allow-{safe_name}-to-dep'),
            spec=V1NetworkPolicySpec(
                pod_selector=V1LabelSelector(match_labels={
                    'app': 'assemblyline',
                    'section': 'service',
                    'component': service_name,
                }),
                egress=[V1NetworkPolicyEgressRule(
                    to=[V1NetworkPolicyPeer(
                        pod_selector=V1LabelSelector(match_labels={
                            'app': 'assemblyline',
                            'dependency_for': service_name,
                        })
                    )]
                )],
            )
        ), _request_timeout=API_TIMEOUT)

        try:
            self.net_api.delete_namespaced_network_policy(namespace=self.namespace, name=f'allow-dep-from-{safe_name}',
                                                          _request_timeout=API_TIMEOUT)
        except ApiException as error:
            if error.status != 404:
                raise
        self.net_api.create_namespaced_network_policy(namespace=self.namespace, body=V1NetworkPolicy(
            metadata=V1ObjectMeta(name=f'allow-dep-from-{safe_name}'),
            spec=V1NetworkPolicySpec(
                pod_selector=V1LabelSelector(match_labels={
                    'app': 'assemblyline',
                    'dependency_for': service_name,
                }),
                ingress=[V1NetworkPolicyIngressRule(
                    _from=[V1NetworkPolicyPeer(
                        pod_selector=V1LabelSelector(match_labels={
                            'app': 'assemblyline',
                            'section': 'service',
                            'component': service_name,
                        })
                    )]
                )],
            )
        ), _request_timeout=API_TIMEOUT)

        # Allow outgoing
        try:
            self.net_api.delete_namespaced_network_policy(namespace=self.namespace, name=f'allow-{safe_name}-outgoing',
                                                          _request_timeout=API_TIMEOUT)
        except ApiException as error:
            if error.status != 404:
                raise
        if internet:
            self.net_api.create_namespaced_network_policy(namespace=self.namespace, body=V1NetworkPolicy(
                metadata=V1ObjectMeta(name=f'allow-{safe_name}-outgoing'),
                spec=V1NetworkPolicySpec(
                    pod_selector=V1LabelSelector(match_labels={
                        'app': 'assemblyline',
                        'section': 'service',
                        'component': service_name,
                    }),
                    egress=[V1NetworkPolicyEgressRule(to=[])],
                )
            ), _request_timeout=API_TIMEOUT)
