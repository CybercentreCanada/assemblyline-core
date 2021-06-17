import base64
import json
import os
import time
import threading
import weakref
from typing import Dict, List

from kubernetes import client, config
from kubernetes.client import ExtensionsV1beta1Deployment, ExtensionsV1beta1DeploymentSpec, V1PodTemplateSpec, \
    V1PodSpec, V1ObjectMeta, V1Volume, V1Container, V1VolumeMount, V1EnvVar, V1ConfigMapVolumeSource, \
    V1PersistentVolumeClaimVolumeSource, V1LabelSelector, V1ResourceRequirements, V1PersistentVolumeClaim, \
    V1PersistentVolumeClaimSpec, V1NetworkPolicy, V1NetworkPolicySpec, V1NetworkPolicyEgressRule, V1NetworkPolicyPeer, \
    V1NetworkPolicyIngressRule, V1Secret, V1LocalObjectReference
from kubernetes.client.rest import ApiException

from assemblyline_core.scaler.controllers.interface import ControllerInterface


# How to identify the update volume as a whole, in a way that the underlying container system recognizes.
FILE_UPDATE_VOLUME = os.environ.get('FILE_UPDATE_VOLUME', None)
CONTAINER_UPDATE_DIRECTORY = '/mount/updates/'

# Where to find the update directory inside this container.
FILE_UPDATE_DIRECTORY = os.environ.get('FILE_UPDATE_DIRECTORY', None)
# RESERVE_MEMORY_PER_NODE = os.environ.get('RESERVE_MEMORY_PER_NODE')

API_TIMEOUT = 90

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


def median(values: List[float]) -> float:
    if len(values) == 0:
        return 0
    return values[len(values)//2]


def create_docker_auth_config(image, username, password):
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


def parse_memory(string) -> float:
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


def parse_cpu(string):
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

        self.prefix = prefix.lower()
        self.priority = priority
        self.cpu_reservation = max(0.0, min(cpu_reservation, 1.0))
        self.logger = logger
        self.log_level = log_level
        self._labels = labels
        self.apps_api = client.AppsV1Api()
        self.api = client.CoreV1Api()
        self.net_api = client.NetworkingV1Api()
        self.auto_cloud = False  # TODO draw from config
        self.namespace = namespace
        self.config_volumes: Dict[str, V1Volume] = {}
        self.config_mounts: Dict[str, V1VolumeMount] = {}
        self._external_profiles = weakref.WeakValueDictionary()

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

        self._current_cpu = 0, 0
        self._current_ram = 0, 0
        self._get_system_info()
        background = threading.Thread(target=self._monitor_system_info, daemon=True)
        background.start()

    def _deployment_name(self, service_name):
        return (self.prefix + service_name).lower().replace('_', '-')

    def config_mount(self, name, config_map, key, target_path):
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

    def add_profile(self, profile, scale=0):
        """Tell the controller about a service profile it needs to manage."""
        self._create_deployment(profile.name, self._deployment_name(profile.name),
                                profile.container_config, profile.shutdown_seconds, scale,
                                mount_updates=profile.mount_updates)
        self._external_profiles[profile.name] = profile

    def _monitor_system_info(self):
        while True:
            try:
                start = time.time()
                self._get_system_info()
                remaining_sleep = 3 - (time.time() - start)
                if remaining_sleep > 0:
                    time.sleep(remaining_sleep)
            except Exception:
                self.logger.exception("Error in kubernetes system info loop:")

    def _get_system_info(self):
        # Try to get the limit from the namespace
        max_cpu = parse_cpu('inf')
        used_cpu = 0
        used_ram = 0

        max_ram = float('inf')

        found = False
        resources = self.api.list_namespaced_resource_quota(namespace=self.namespace, _request_timeout=API_TIMEOUT)
        for limit in resources.items:
            # Don't worry about specific quotas, just look for namespace wide ones
            if limit.spec.scope_selector or limit.spec.scopes:
                continue

            found = True  # At least one limit has been found
            if 'limits.cpu' in limit.status.hard:
                max_cpu = min(max_cpu, parse_cpu(limit.status.hard['limits.cpu']))

            if 'limits.cpu' in limit.status.used:
                used_cpu = max(used_cpu, parse_cpu(limit.status.used['limits.cpu']))

            if 'limits.memory' in limit.status.hard:
                max_ram = min(max_ram, parse_memory(limit.status.hard['limits.memory']))

            if 'limits.memory' in limit.status.used:
                used_ram = max(used_ram, parse_memory(limit.status.used['limits.memory']))

        if found:
            self._current_cpu = max_cpu - used_cpu, max_cpu
            self._current_ram = max_ram - used_ram, max_ram
            return

        # If the limit isn't set by the user, and we are on a cloud with auto-scaling
        # we don't have a real memory limit
        if self.auto_cloud:
            return parse_cpu('inf'), parse_cpu('inf')

        # Try to get the limit by looking at the host list
        cpu = 0
        memory = 0
        for node in self.api.list_node(_request_timeout=API_TIMEOUT).items:
            cpu += parse_cpu(node.status.allocatable['cpu'])
            memory += parse_memory(node.status.allocatable['memory'])
        max_cpu = cpu
        max_memory = memory

        memory_unrestricted = 0
        cpu_unrestricted = 0
        memory_restrictions = []
        cpu_restrictions = []

        for pod in self.api.list_pod_for_all_namespaces(_request_timeout=API_TIMEOUT).items:
            for container in pod.spec.containers:
                requests = container.resources.requests or {}
                limits = container.resources.limits or {}

                cpu_value = requests.get('cpu', limits.get('cpu', None))
                if cpu_value is None:
                    cpu_unrestricted += 1
                else:
                    cpu_restrictions.append(parse_cpu(cpu_value))

                memory_value = requests.get('memory', limits.get('memory', None))
                if memory_value is None:
                    memory_unrestricted += 1
                else:
                    memory_restrictions.append(parse_memory(memory_value))

        cpu -= sum(cpu_restrictions) + median(cpu_restrictions) * cpu_unrestricted
        memory -= sum(memory_restrictions) + median(memory_restrictions) * memory_unrestricted

        self._current_cpu = cpu, max_cpu
        self._current_ram = memory, max_memory

    def cpu_info(self):
        return self._current_cpu

    def memory_info(self):
        return self._current_ram

    @staticmethod
    def _create_metadata(deployment_name: str, labels: Dict[str, str]):
        return V1ObjectMeta(name=deployment_name, labels=labels)

    def _create_volumes(self, service_name, mount_updates=False):
        volumes, mounts = [], []

        # Attach the mount that provides the config file
        volumes.extend(self.config_volumes.values())
        mounts.extend(self.config_mounts.values())

        if mount_updates:
            # Attach the mount that provides the update
            volumes.append(V1Volume(
                name='update-directory',
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                    claim_name=FILE_UPDATE_VOLUME
                ),
            ))

            mounts.append(V1VolumeMount(
                name='update-directory',
                mount_path=CONTAINER_UPDATE_DIRECTORY,
                sub_path=service_name
            ))

        return volumes, mounts

    def _create_containers(self, deployment_name, container_config, mounts):
        cores = container_config.cpu_cores
        memory = container_config.ram_mb
        min_memory = min(container_config.ram_mb_min, container_config.ram_mb)
        environment_variables = [V1EnvVar(name=_e.name, value=_e.value) for _e in container_config.environment]
        environment_variables += [
            V1EnvVar(name='UPDATE_PATH', value=CONTAINER_UPDATE_DIRECTORY),
            V1EnvVar(name='FILE_UPDATE_DIRECTORY', value=CONTAINER_UPDATE_DIRECTORY),
            V1EnvVar(name='LOG_LEVEL', value=self.log_level)
        ]
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

    def _create_deployment(self, service_name: str, deployment_name: str, docker_config,
                           shutdown_seconds, scale: int, labels=None, volumes=None, mounts=None, mount_updates=True):

        replace = False

        if not os.path.exists(os.path.join(FILE_UPDATE_DIRECTORY, service_name)):
            os.makedirs(os.path.join(FILE_UPDATE_DIRECTORY, service_name), 0x777)

        resources = self.apps_api.list_namespaced_deployment(namespace=self.namespace, _request_timeout=API_TIMEOUT)
        for dep in resources.items:
            if dep.metadata.name == deployment_name:
                replace = True
                break

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
                self.api.replace_namespaced_secret(pull_secret_name, namespace=self.namespace, body=new_pull_secret,
                                                   _request_timeout=API_TIMEOUT)
            else:
                self.api.create_namespaced_secret(namespace=self.namespace, body=new_pull_secret,
                                                  _request_timeout=API_TIMEOUT)
        elif current_pull_secret:
            self.api.delete_namespaced_secret(pull_secret_name, self.namespace, _request_timeout=API_TIMEOUT)

        all_labels = dict(self._labels)
        all_labels['component'] = service_name
        all_labels.update(labels or {})

        all_volumes, all_mounts = self._create_volumes(service_name, mount_updates)
        all_volumes.extend(volumes or [])
        all_mounts.extend(mounts or [])
        metadata = self._create_metadata(deployment_name=deployment_name, labels=all_labels)

        pod = V1PodSpec(
            volumes=all_volumes,
            containers=self._create_containers(deployment_name, docker_config, all_mounts),
            priority_class_name=self.priority,
            termination_grace_period_seconds=shutdown_seconds,
        )

        if use_pull_secret:
            pod.image_pull_secrets = [V1LocalObjectReference(name=pull_secret_name)]

        template = V1PodTemplateSpec(
            metadata=metadata,
            spec=pod,
        )

        spec = ExtensionsV1beta1DeploymentSpec(
            replicas=int(scale),
            revision_history_limit=0,
            selector=V1LabelSelector(match_labels=all_labels),
            template=template,
        )

        deployment = ExtensionsV1beta1Deployment(
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
        try:
            scale = self.apps_api.read_namespaced_deployment_scale(self._deployment_name(service_name),
                                                                   namespace=self.namespace,
                                                                   _request_timeout=API_TIMEOUT)
            return int(scale.spec.replicas or 0)
        except ApiException as error:
            # If we get a 404 it means the resource doesn't exist, which we treat the same as
            # scheduled to run zero instances since we create deployments on demand
            if error.status == 404:
                return 0
            raise

    def set_target(self, service_name: str, target: int):
        """Set the target for running instances of a service."""
        for _ in range(10):
            try:
                name = self._deployment_name(service_name)
                scale = self.apps_api.read_namespaced_deployment_scale(name=name, namespace=self.namespace,
                                                                       _request_timeout=API_TIMEOUT)
                scale.spec.replicas = target
                self.apps_api.replace_namespaced_deployment_scale(name=name, namespace=self.namespace, body=scale,
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
            pods = self.api.list_namespaced_pod(namespace=self.namespace, label_selector=f'component={service_name}',
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
                                mount_updates=service.mount_updates)

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

    def start_stateful_container(self, service_name, container_name, spec, labels, mount_updates=False):
        # Setup PVC
        deployment_name = service_name + '-' + container_name
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

        self._create_deployment(service_name, deployment_name, spec.container,
                                30, 1, labels, volumes=volumes, mounts=mounts, mount_updates=mount_updates)

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
