import os
from typing import Dict, Tuple, List

from kubernetes import client, config
from kubernetes.client import ExtensionsV1beta1Deployment, ExtensionsV1beta1DeploymentSpec, V1PodTemplateSpec, \
    V1PodSpec, V1ObjectMeta, V1Volume, V1Container, V1VolumeMount, V1EnvVar, V1KeyToPath, V1ConfigMapVolumeSource
from kubernetes.client.rest import ApiException

from al_core.scaler.controllers.interface import ControllerInterface


def parse_memory(string):
    # TODO use a library for parsing this?
    #      I tried a couple, and they weren't compatable with the formats kubernetes uses
    #      if we can't find one, this needs to be improved
    # Maybe we have a number in bytes
    try:
        return float(string)/2**20
    except ValueError:
        pass

    # Try parsing a unit'd number then
    if string.endswith('Ki'):
        byte_count = float(string[:-2]) * 2**10
    elif string.endswith('Mi'):
        byte_count = float(string[:-2]) * 2**20
    elif string.endswith('Gi'):
        byte_count = float(string[:-2]) * 2**30
    else:
        raise ValueError(string)

    return byte_count/2**20


class KubernetesController(ControllerInterface):
    def __init__(self, logger, namespace, prefix, labels=None):
        # Try loading a kubernetes connection from either the fact that we are running
        # inside of a cluster,
        try:
            config.load_incluster_config()
        except config.config_exception.ConfigException:
            # Load the configuration once to initialize the defaults
            config.load_kube_config()

            # Now we can actually apply any changes we want to make
            cfg = client.configuration.Configuration()

            if 'HTTPS_PROXY' in os.environ:
                cfg.proxy = os.environ['HTTPS_PROXY']
                if not cfg.proxy.startswith("http"):
                    cfg.proxy = "https://" + cfg.proxy
                client.Configuration.set_default(cfg)

            # Load again with our settings set
            config.load_kube_config(client_configuration=cfg)

        self.prefix = prefix
        self.logger = logger
        self._labels = labels
        self.b1api = client.AppsV1beta1Api()
        self.api = client.CoreV1Api()
        self.auto_cloud = False  #  TODO draw from config
        self.namespace = namespace
        self.config_mounts: List[Tuple[V1Volume, V1VolumeMount]] = []

    def config_mount(self, name, config_map, key, file_name, target_path):
        volume = V1Volume(
            name='al-config',
            config_map=V1ConfigMapVolumeSource(
                name=config_map,
                items=[V1KeyToPath(key=key, path=file_name)],
                optional=False
            ),
        )

        mount = V1VolumeMount(
            name=name,
            mount_path=target_path,
            read_only=True,
        )

        self.config_mounts.append((volume, mount))

    def add_profile(self, profile):
        """Tell the controller about a service profile it needs to manage."""
        self._create_deployment(profile, 0)

    def free_cpu(self):
        """Number of cores available for reservation."""
        # Try to get the limit from the namespace
        max_cpu = float('inf')
        used = 0
        found = False
        for limit in self.api.list_namespaced_resource_quota(namespace=self.namespace).items:
            # Don't worry about specific quotas, just look for namespace wide ones
            if limit.spec.scope_selector or limit.spec.scopes:
                continue

            found = True  # At least one limit has been found
            if 'limits.cpu' in limit.status.hard:
                max_cpu = min(max_cpu, float(limit.status.hard['limits.cpu']))

            if 'limits.cpu' in limit.status.used:
                used = max(used, float(limit.status.used['limits.cpu']))

        if found:
            return max_cpu - used

        # If the limit isn't set by the user, and we are on a cloud with auto-scaling
        # we don't have a real memory limit
        if self.auto_cloud:
            return float('inf')

        # Try to get the limit by looking at the host list
        cpu = 0
        for node in self.api.list_node().items:
            cpu += float(node.status.allocatable['cpu'])
        return cpu

    def free_memory(self):
        """Megabytes of RAM that has not been reserved."""
        # Try to get the limit from the namespace
        max_ram = float('inf')
        used = 0
        found = False
        for limit in self.api.list_namespaced_resource_quota(namespace=self.namespace).items:
            # Don't worry about specific quotas, just look for namespace wide ones
            if limit.spec.scope_selector or limit.spec.scopes:
                continue

            found = True  # At least one limit has been found
            if 'limits.memory' in limit.status.hard:
                max_ram = min(max_ram, parse_memory(limit.status.hard['limits.memory']))

            if 'limits.memory' in limit.status.used:
                used = max(used, parse_memory(limit.status.used['limits.memory']))

        if found:
            return max_ram - used

        # If the limit isn't set by the user, and we are on a cloud with auto-scaling
        # we don't have a real memory limit
        if self.auto_cloud:
            return float('inf')

        # Read the memory that is free from the node list
        memory = 0
        for node in self.api.list_node().items:
            memory += parse_memory(node.status.allocatable['memory'])
        return memory

    def _create_labels(self, service_name) -> Dict[str, str]:
        x = dict(self._labels)
        x['component'] = service_name
        return x

    def _create_metadata(self, service_name):
        return V1ObjectMeta(name=self.prefix + service_name, labels=self._create_labels(service_name))

    def _create_selector(self, service_name) -> Dict[str, str]:
        return self._create_labels(service_name)

    def _create_volumes(self, profile):
        volumes, mounts = [], []
        for _v, _m in self.config_mounts:
            volumes.append(_v)
            mounts.append(_m)
        return volumes, mounts

    def _create_containers(self, profile, mounts):
        return [V1Container(
            name=self.prefix + profile.name,
            image=profile.container_config.image,
            command=profile.container_config.command,
            env=[V1EnvVar(name=_e.name, value=_e.value) for _e in profile.container_config.environment],
            image_pull_policy='Always',
            volume_mounts=mounts,
        )]

    def _create_deployment(self, profile, scale):
        volumes, mounts = self._create_volumes(profile)
        metadata = self._create_metadata(profile.name)

        pod = V1PodSpec(
            volumes=self._create_volumes(profile),
            containers=self._create_containers(profile, mounts),
        )

        template = V1PodTemplateSpec(
            metadata=metadata,
            spec=pod,
        )

        spec = ExtensionsV1beta1DeploymentSpec(
            replicas=scale,
            selector=self._create_selector(profile.name),
            template=template,
        )

        deployment = ExtensionsV1beta1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=metadata,
            spec=spec,
        )

        self.b1api.create_namespaced_deployment(namespace=self.namespace, body=deployment)

    def get_target(self, service_name):
        """Get the target for running instances of a service."""
        try:
            scale = self.b1api.read_namespaced_deployment_scale(self.prefix + service_name, namespace=self.namespace)
            return scale.spec.replicas
        except ApiException as error:
            # If we get a 404 it means the resource doesn't exist, which we treat the same as
            # scheduled to run zero instances since we create deployments on demand
            if error.status == 404:
                return 0
            raise

    def set_target(self, service_name, target):
        """Set the target for running instances of a service."""
        name = self.prefix + service_name
        scale = self.b1api.read_namespaced_deployment_scale(name=name, namespace=self.namespace)
        scale.spec.replicas = target
        self.b1api.replace_namespaced_deployment_scale(name=name, namespace=self.namespace, body=scale)

