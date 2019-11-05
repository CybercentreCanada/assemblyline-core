import os
from typing import Dict, Tuple, List

from kubernetes import client, config
from kubernetes.client import ExtensionsV1beta1Deployment, ExtensionsV1beta1DeploymentSpec, V1PodTemplateSpec, \
    V1PodSpec, V1ObjectMeta, V1Volume, V1Container, V1VolumeMount, V1EnvVar, V1KeyToPath, V1ConfigMapVolumeSource, \
    V1PersistentVolumeClaimVolumeSource, V1LabelSelector, V1ResourceRequirements
from kubernetes.client.rest import ApiException

from assemblyline_core.scaler.controllers.interface import ControllerInterface


# How to identify the update volume as a whole, in a way that the underlying container system recognizes.
FILE_UPDATE_VOLUME = os.environ.get('FILE_UPDATE_VOLUME', None)
CONTAINER_UPDATE_DIRECTORY = '/mount/updates/'

# Where to find the update directory inside this container.
FILE_UPDATE_DIRECTORY = os.environ.get('FILE_UPDATE_DIRECTORY', None)


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


def parse_cpu(string):
    try:
        return float(string)
    except ValueError:
        pass

    if string.endswith('m'):
        return float(string[:-1])/1000.0

    raise ValueError('Un-parsable CPU string: ' + string)


class KubernetesController(ControllerInterface):
    def __init__(self, logger, namespace, prefix, priority, labels=None):
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

        self.prefix = prefix.lower()
        self.priority = priority
        self.logger = logger
        self._labels = labels
        self.b1api = client.AppsV1beta1Api()
        self.api = client.CoreV1Api()
        self.auto_cloud = False  #  TODO draw from config
        self.namespace = namespace
        self.config_mounts: List[Tuple[V1Volume, V1VolumeMount]] = []

        # A record of previously reported events so that we don't report the same message repeatedly, fill it with
        # existing messages so we don't have a huge dump of duplicates on restart
        self.events_window = {}
        response = self.api.list_namespaced_event(namespace='al', pretty='false',
                                                  field_selector='type=Warning', watch=False)
        for event in response.items:
            # Keep the scaler related events in case it helps us know why scaler was restarting
            if 'scaler' not in event.involved_object.name:
                self.events_window[event.metadata.uid] = event.count

    def _deployment_name(self, service_name):
        return (self.prefix + service_name).lower().replace('_', '-')

    def config_mount(self, name, config_map, key, file_name, target_path):
        volume = V1Volume(
            name=name,
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
        max_cpu = parse_cpu('inf')
        used = 0
        found = False
        for limit in self.api.list_namespaced_resource_quota(namespace=self.namespace).items:
            # Don't worry about specific quotas, just look for namespace wide ones
            if limit.spec.scope_selector or limit.spec.scopes:
                continue

            found = True  # At least one limit has been found
            if 'limits.cpu' in limit.status.hard:
                max_cpu = min(max_cpu, parse_cpu(limit.status.hard['limits.cpu']))

            if 'limits.cpu' in limit.status.used:
                used = max(used, parse_cpu(limit.status.used['limits.cpu']))

        if found:
            return max_cpu - used

        # If the limit isn't set by the user, and we are on a cloud with auto-scaling
        # we don't have a real memory limit
        if self.auto_cloud:
            return parse_cpu('inf')

        # Try to get the limit by looking at the host list
        cpu = 0
        for node in self.api.list_node().items:
            cpu += parse_cpu(node.status.allocatable['cpu'])
        for pod in self.api.list_pod_for_all_namespaces().items:
            for container in pod.spec.containers:
                requests = container.resources.requests or {}
                limits = container.resources.limits or {}
                cpu -= parse_cpu(requests.get('cpu', limits.get('cpu', '0.1')))
        return cpu

    def free_memory(self):
        """Megabytes of RAM that has not been reserved."""
        max_ram = float('inf')

        # Try to get the limit from the namespace, if a specific quota has been set use
        # that over any other options.
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
        for pod in self.api.list_pod_for_all_namespaces().items:
            for container in pod.spec.containers:
                requests = container.resources.requests or {}
                limits = container.resources.limits or {}
                memory -= parse_memory(requests.get('memory', limits.get('memory', '16Mi')))
        return memory

    def _create_labels(self, service_name) -> Dict[str, str]:
        x = dict(self._labels)
        x['component'] = service_name
        return x

    def _create_metadata(self, service_name):
        return V1ObjectMeta(name=self._deployment_name(service_name), labels=self._create_labels(service_name))

    def _create_selector(self, service_name) -> V1LabelSelector:
        return V1LabelSelector(match_labels=self._create_labels(service_name))

    def _create_volumes(self, profile):
        volumes, mounts = [], []

        # Attach the mount that provides the config file
        for _v, _m in self.config_mounts:
            volumes.append(_v)
            mounts.append(_m)

        # Attach the mount that provides the update
        volumes.append(V1Volume(
            name='update-directory',
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                claim_name=FILE_UPDATE_VOLUME,
                read_only=True
            ),
        ))

        mounts.append(V1VolumeMount(
            name='update-directory',
            mount_path=CONTAINER_UPDATE_DIRECTORY,
            sub_path=profile.name,
            read_only=True,
        ))

        return volumes, mounts

    def _create_containers(self, profile, mounts):
        cores = profile.container_config.cpu_cores
        memory = profile.container_config.ram_mb
        environment_variables = [V1EnvVar(name=_e.name, value=_e.value) for _e in profile.container_config.environment]
        environment_variables += [V1EnvVar(name='UPDATE_PATH', value=CONTAINER_UPDATE_DIRECTORY)]
        return [V1Container(
            name=self._deployment_name(profile.name),
            image=profile.container_config.image,
            command=profile.container_config.command,
            env=environment_variables,
            image_pull_policy='Always',
            volume_mounts=mounts,
            resources=V1ResourceRequirements(
                limits={'cpu': cores, 'memory': f'{memory}Mi'},
                requests={'cpu': cores/4, 'memory': f'{int(memory/4)}Mi'},
            )
        )]

    def _create_deployment(self, profile, scale: int):

        replace = False

        if not os.path.exists(os.path.join(FILE_UPDATE_DIRECTORY, profile.name)):
            os.makedirs(os.path.join(FILE_UPDATE_DIRECTORY, profile.name), 0x777)

        for dep in self.b1api.list_namespaced_deployment(namespace=self.namespace).items:
            if dep.metadata.name == self._deployment_name(profile.name):
                replace = True

        volumes, mounts = self._create_volumes(profile)
        metadata = self._create_metadata(profile.name)

        pod = V1PodSpec(
            volumes=volumes,
            containers=self._create_containers(profile, mounts),
            priority_class_name=self.priority,
            termination_grace_period_seconds=profile.shutdown_seconds,
        )

        template = V1PodTemplateSpec(
            metadata=metadata,
            spec=pod,
        )

        spec = ExtensionsV1beta1DeploymentSpec(
            replicas=int(scale),
            selector=self._create_selector(profile.name),
            template=template,
        )

        deployment = ExtensionsV1beta1Deployment(
            kind="Deployment",
            metadata=metadata,
            spec=spec,
        )

        if replace:
            self.logger.info("Requesting kubernetes replace deployment info for: " + metadata.name)
            self.b1api.replace_namespaced_deployment(namespace=self.namespace, body=deployment, name=metadata.name)
        else:
            self.logger.info("Requesting kubernetes create deployment info for: " + metadata.name)
            self.b1api.create_namespaced_deployment(namespace=self.namespace, body=deployment)

    def get_target(self, service_name: str) -> int:
        """Get the target for running instances of a service."""
        try:
            scale = self.b1api.read_namespaced_deployment_scale(self._deployment_name(service_name), namespace=self.namespace)
            return int(scale.spec.replicas or 0)
        except ApiException as error:
            # If we get a 404 it means the resource doesn't exist, which we treat the same as
            # scheduled to run zero instances since we create deployments on demand
            if error.status == 404:
                return 0
            raise

    def set_target(self, service_name: str, target: int):
        """Set the target for running instances of a service."""
        name = self._deployment_name(service_name)
        scale = self.b1api.read_namespaced_deployment_scale(name=name, namespace=self.namespace)
        scale.spec.replicas = target
        self.b1api.replace_namespaced_deployment_scale(name=name, namespace=self.namespace, body=scale)

    def stop_container(self, service_name, container_id):
        pods = self.api.list_namespaced_pod(namespace=self.namespace, label_selector=f'component={service_name}')
        for pod in pods.items:
            if pod.metadata.name == container_id:
                self.api.delete_namespaced_pod(name=container_id, namespace=self.namespace)
                return

    def restart(self, service):
        self._create_deployment(service, self.get_target(service.name))

    def get_running_container_names(self):
        pods = self.api.list_pod_for_all_namespaces(field_selector='status.phase==Running')
        return [pod.metadata.name for pod in pods.items]

    def new_events(self):
        response = self.api.list_namespaced_event(namespace='al', pretty='false',
                                                  field_selector='type=Warning', watch=False)

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
