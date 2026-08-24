from __future__ import annotations

from typing import Optional, TYPE_CHECKING
from collections import OrderedDict, defaultdict
import os
import json
import base64
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from dateutil.tz import tzlocal
from time import sleep
import uuid
import weakref

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from kubernetes import client, config, watch
import kubernetes.client.configuration
from kubernetes.watch import Watch
from kubernetes.client import (
    CustomObjectsApi,
    V1Affinity,
    V1Capabilities,
    V1ConfigMap,
    V1ConfigMapVolumeSource,
    V1Container,
    V1Deployment,
    V1DeploymentSpec,
    V1DeploymentStrategy,
    V1EnvVar,
    V1ExecAction,
    V1HorizontalPodAutoscalerSpec,
    V1LabelSelector,
    V1LocalObjectReference,
    V1NetworkPolicy,
    V1NetworkPolicyEgressRule,
    V1NetworkPolicyIngressRule,
    V1NetworkPolicyPeer,
    V1NetworkPolicySpec,
    V1NodeAffinity,
    V1NodeSelector,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
    V1ObjectMeta,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PersistentVolumeClaimVolumeSource,
    V1PodSecurityContext,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Probe,
    V1ResourceRequirements,
    V1SeccompProfile,
    V1Secret,
    V1SecretVolumeSource,
    V1SecurityContext,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
    V1Toleration,
    V1Volume,
    V1VolumeMount,
    V2CrossVersionObjectReference,
    V2ExternalMetricSource,
    V2HorizontalPodAutoscaler,
    V2HorizontalPodAutoscalerSpec,
    V2MetricIdentifier,
    V2MetricSpec,
    V2MetricTarget,
    V2ResourceMetricSource,
)
from kubernetes.client.rest import ApiException

from assemblyline.odm.models.config import Selector
from assemblyline.odm.models.service import DependencyConfig, DockerConfig, PersistentVolume

if TYPE_CHECKING:
    from assemblyline_core.scaler.base import ProfileBase


API_TIMEOUT = 90
CHANGE_KEY_NAME = 'al_change_key'
CERTIFICATE_VALIDITY_PERIOD = int(os.environ.get('CERTIFICATE_VALIDITY_PERIOD', '36500'))
SERVICE_LIVENESS_PERIOD = int(os.environ.get('SERVICE_LIVENESS_PERIOD', 300))
SERVICE_LIVENESS_TIMEOUT = int(os.environ.get('SERVICE_LIVENESS_TIMEOUT', 60))
DEV_MODE = os.environ.get('DEV_MODE', 'false').lower() == 'true'
UNPRIVILEGED_SERVICE_ACCOUNT_NAME = os.environ.get('UNPRIVILEGED_SERVICE_ACCOUNT_NAME', None)
PRIVILEGED_SERVICE_ACCOUNT_NAME = os.environ.get('PRIVILEGED_SERVICE_ACCOUNT_NAME', None)

AL_ROOT_CA = os.environ.get('AL_ROOT_CA', '/etc/assemblyline/ssl/al_root-ca.crt')
AL_ROOT_CA_PK = os.environ.get('AL_ROOT_CA_PK', '/etc/assemblyline/ssl/al_root-ca.key')

RESTRICTED_POD_SECURITY_CONTEXT = V1SecurityContext(
    run_as_user=1000,
    run_as_group=1000,
    capabilities=V1Capabilities(drop=["ALL"]),
    run_as_non_root=True,
    allow_privilege_escalation=False,
    seccomp_profile=V1SeccompProfile(type="RuntimeDefault")
)

_exponents = {
    'ki': 2**10,
    'k': 2**10,
    'mi': 2**20,
    'm': 2**20,
    'gi': 2**30,
    'g': 2**30,
    'ti': 2**40,
    't': 2**40,
    'pi': 2**50,
    'p': 2 ** 50,
}


class CacheDict(OrderedDict):
    """Dict with a limited length, ejecting LRUs as needed."""

    def __init__(self, *args, cache_len: int = 1000, **kwargs):
        assert cache_len > 0
        self.cache_len = cache_len

        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        super().move_to_end(key)

        while len(self) > self.cache_len:
            oldkey = next(iter(self))
            super().__delitem__(oldkey)

    def __getitem__(self, key):
        val = super().__getitem__(key)
        super().move_to_end(key)

        return val


class TypelessWatch(watch.Watch):
    """A kubernetes watch object that doesn't marshal the response."""

    def get_return_type(self, func):
        return None


def median(values: list[float]) -> float:
    if len(values) == 0:
        return 0
    return values[len(values)//2]


def mean(values: list[float]) -> float:
    if len(values) == 0:
        return 0
    return sum(values)/len(values)


def selector_to_list_filters(selector: Selector) -> tuple[Optional[str], Optional[str]]:
    """Return the field and label selector strings described by selector."""
    # Field selector only supports equal and not equal
    field_parts = []
    for part in selector.field:
        op = '==' if part.equal else '!='
        field_parts.append(f"{part.key}{op}{part.value}")
    field_selector = None
    if field_parts:
        field_selector = ','.join(field_parts)

    # label selector is a bit more complicated
    label_parts = []
    for part in selector.label:
        if part.operator == 'In':
            label_parts.append(f"{part.key} in ({','.join(part.values)})")
        elif part.operator == 'NotIn':
            label_parts.append(f"{part.key} notin ({','.join(part.values)})")
        elif part.operator == 'Exists':
            label_parts.append(part.key)
        elif part.operator == 'DoesNotExist':
            label_parts.append(f"!{part.key}")
        else:
            raise ValueError("Unknown selector operator: " + str(part.operator))
    label_selector = None
    if label_parts:
        label_selector = ','.join(label_parts)

    return field_selector, label_selector


def selector_to_node_affinity(selector: Selector) -> Optional[V1Affinity]:
    """Return the selector as a kubernetes affinity."""

    label_expressions = []
    for label in selector.label:
        label_expressions.append(V1NodeSelectorRequirement(
            key=label.key,
            operator=label.operator,
            values=label.values,
        ))

    field_expressions = []
    for field in selector.field:
        field_expressions.append(V1NodeSelectorRequirement(
            key=field.key,
            operator='In' if field.equal else 'NotIn',
            values=[field.value]
        ))

    if not label_expressions and not field_expressions:
        return None

    return V1Affinity(
        node_affinity=V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=V1NodeSelector(
                node_selector_terms=[V1NodeSelectorTerm(
                    match_expressions=label_expressions,
                    match_fields=field_expressions,
                )]
            )
        )
    )


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
    lower = string.lower()
    # Try parsing a unit'd number then
    if lower[-2:] in _exponents:
        return (float(string[:-2]) * _exponents[lower[-2:]])/(2**20)
    if lower[-1:] in _exponents:
        return (float(string[:-1]) * _exponents[lower[-1:]])/(2**20)

    raise ValueError(string)


def parse_cpu(string: str) -> float:
    try:
        return float(string)
    except ValueError:
        pass

    if string.endswith('n'):
        return float(string[:-1])/1_000_000_000.0

    if string.endswith('u'):
        return float(string[:-1])/1_000_000.0

    if string.endswith('m'):
        return float(string[:-1])/1_000.0

    raise ValueError('Un-parsable CPU string: ' + string)


class KubernetesMethods:
    def __init__(self, logger, namespace: str, prefix: str, core_env, enable_pod_security: bool, log_level: str,
                 linux_node_selector: Selector, cpu_reservation: float, cpu_slack: float, labels, priv_labels,
                 priority, dependency_priority, default_service_tolerations) -> None:
        self.logger = logger
        self.log_level: str = log_level
        self.namespace: str = namespace
        self.linux_node_selector = linux_node_selector
        self.prefix: str = prefix.lower()
        self.cpu_reservation: float = max(0.0, min(cpu_reservation, 1.0))
        self.cpu_slack: float = max(1.0, cpu_slack + 1.0)
        self._labels: dict[str, str] = labels or {}
        self._priv_labels: dict[str, str] = priv_labels or {}
        self.default_service_tolerations = [V1Toleration(**toleration.as_primitives())
                                            for toleration in default_service_tolerations]
        self.priority: str = priority
        self.dependency_priority: str = dependency_priority
        self._external_profiles: weakref.WeakValueDictionary[str, ProfileBase] = weakref.WeakValueDictionary()

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

        self.apps_api = client.AppsV1Api()
        self.api = client.CoreV1Api()
        self.api_client = client.ApiClient()
        self.net_api = client.NetworkingV1Api()
        self.scale_api = client.AutoscalingV2Api()
        self.volumes: dict[str, V1Volume] = {}
        self.mounts: dict[str, V1VolumeMount] = {}
        self.core_env: dict[str, str] = core_env or {}
        self.core_secret_env: list[V1EnvVar] = []
        self.core_volumes: dict[str, V1Volume] = {}
        self.core_mounts: dict[str, V1VolumeMount] = {}
        self._service_limited_env: dict[str, dict[str, str]] = defaultdict(dict)
        self.security_policy = RESTRICTED_POD_SECURITY_CONTEXT if enable_pod_security else None

        self._deployment_targets: dict[str, int] = {}
        self._deployment_unavailable: dict[str, int] = {}

        # A record of previously reported events so that we don't report the same message repeatedly, fill it with
        # existing messages so we don't have a huge dump of duplicates on restart
        self.events_window = {}
        response = self.api.list_namespaced_event(namespace=self.namespace, pretty='false',
                                                  field_selector='type=Warning', watch=False,
                                                  _request_timeout=API_TIMEOUT)
        for event in response.items:
            # Keep the scaler related events in case it helps us know why scaler was restarting
            if 'scaler' not in event.involved_object.name:
                self.events_window[event.metadata.uid] = event.count

    def _deployment_name(self, service_name: str):
        return (self.prefix + service_name).lower().replace('_', '-')

    def _dependency_name(self, service_name: str, container_name: str):
        return f"{self._deployment_name(service_name)}-{container_name}".lower()

    def add_config_mount(self, name: str, config_map: str, key: Optional[str], target_path: str,
                         read_only: bool = True, core: bool = False):
        volumes, mounts = self.volumes, self.mounts
        if core:
            volumes, mounts = self.core_volumes, self.core_mounts
        if name not in volumes:
            volumes[name] = V1Volume(
                name=name,
                config_map=V1ConfigMapVolumeSource(
                    name=config_map,
                    optional=False
                )
            )

        mounts[target_path] = V1VolumeMount(
            name=name,
            mount_path=target_path,
            sub_path=key,
            read_only=read_only
        )

    def add_secret_mount(self, name, secret_name, target_path, sub_path=None, read_only=True, core=False):
        volumes, mounts = self.volumes, self.mounts
        if core:
            volumes, mounts = self.core_volumes, self.core_mounts

        if name not in volumes:
            volumes[name] = V1Volume(name=name, secret=V1SecretVolumeSource(secret_name=secret_name))

        mounts[target_path] = V1VolumeMount(
            name=name,
            mount_path=target_path,
            read_only=read_only,
            sub_path=sub_path
        )

    def add_volume_mount(self, name: str, target_path: str, read_only=True, core=False):
        volumes, mounts = self.volumes, self.mounts
        if core:
            volumes, mounts = self.core_volumes, self.core_mounts

        if name not in volumes:
            volumes[name] = (V1Volume(name=name))

        mounts[target_path] = V1VolumeMount(
            name=name,
            mount_path=target_path,
            read_only=read_only
        )

    def _create_containers(self, service_name: str, deployment_name: str, container_config, mounts, security_context,
                           core_container=False):
        cores = container_config.cpu_cores
        memory = container_config.ram_mb
        min_memory = min(container_config.ram_mb_min, container_config.ram_mb)
        environment_variables: list[V1EnvVar] = []

        # Use custom health check located in service base
        health_probe = V1Probe(
            _exec=V1ExecAction(command=["python3", "-m", "assemblyline_v4_service.healthz"]),
            timeout_seconds=SERVICE_LIVENESS_TIMEOUT,
            period_seconds=SERVICE_LIVENESS_PERIOD)

        if 'assemblyline' not in container_config.image:
            # We can't assign an Assemblyline-based probe to a non-Assemblyline-based container image
            health_probe = None

        # If we are launching a core container, include environment variables related to authentication for DBs
        if core_container:
            environment_variables += [V1EnvVar(name=_n, value=_v) for _n, _v in self.core_env.items()]
            environment_variables.extend(self.core_secret_env)
            environment_variables.append(V1EnvVar(name='PRIVILEGED', value='true'))
        # Overwrite those with special hard coded variables
        environment_variables += [
            V1EnvVar(name='AL_SERVICE_NAME', value=service_name),
            V1EnvVar(name='LOG_LEVEL', value=self.log_level)
        ]
        # Overwrite ones defined dynamically by dependency container launches
        for name, value in self._service_limited_env[service_name].items():
            environment_variables.append(V1EnvVar(name=name, value=value))
        # Overwrite them with configured special environment variables
        environment_variables += [V1EnvVar(name=_e.name, value=_e.value) for _e in container_config.environment]
        image_pull_policy = 'Always' if DEV_MODE else 'IfNotPresent'
        return [V1Container(
            name=deployment_name,
            image=container_config.image,
            command=container_config.command,
            env=environment_variables,
            image_pull_policy=image_pull_policy,
            volume_mounts=mounts,
            security_context=security_context,
            resources=V1ResourceRequirements(
                limits={'cpu': cores*self.cpu_slack, 'memory': f'{memory}Mi'},
                requests={'cpu': cores*self.cpu_reservation, 'memory': f'{min_memory}Mi'},
            ),
            liveness_probe=health_probe,
            readiness_probe=health_probe
        )]

    def _create_deployment(self, service_name: str, deployment_name: str, docker_config: DockerConfig,
                           shutdown_seconds: int, scale: int, labels: dict[str, str] | None = None,
                           volumes: list[V1Volume] | None = None, mounts: list[V1VolumeMount] | None = None,
                           core_mounts: bool = False, change_key: str = '', high_priority: bool = False,
                           deployment_strategy: V1DeploymentStrategy = V1DeploymentStrategy(),
                           security_context: V1SecurityContext | None = None):
        # Build a cache key to check for changes, just trying to only patch what changed
        # will still potentially result in a lot of restarts due to different kubernetes
        # systems returning differently formatted data
        field_selector, label_selector = selector_to_list_filters(self.linux_node_selector)
        key_labels = sorted((labels or {}).items())
        svc_env = sorted(self._service_limited_env[service_name].items())
        deployment_labels = {_v.name: _v.value for _v in docker_config.labels}
        key_labels += sorted(deployment_labels.items())
        change_key = str(f"n={deployment_name}{change_key}dc={docker_config}ss={shutdown_seconds}"
                         f"l={key_labels}v={volumes}m={mounts}cm={core_mounts}senv={svc_env}"
                         f"nodes={field_selector or ''}{label_selector or ''}"
                         f"security_context={security_context or ''}")
        self.logger.debug(f"{deployment_name} actual change_key: {change_key}")
        change_key = str(hash(change_key))

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

        all_labels = deployment_labels
        all_labels.update(self._labels)
        all_labels['component'] = service_name
        if core_mounts:
            all_labels['privilege'] = 'core'
            all_labels.update(self._priv_labels)
        all_labels.update(labels or {})

        # Build set of volumes, first the global mounts, then the core specific ones,
        # then the ones specific to this container only
        all_volumes: list[V1Volume] = []
        all_mounts: list[V1VolumeMount] = []
        all_volumes.extend(self.volumes.values())
        all_mounts.extend(self.mounts.values())
        if core_mounts:
            all_volumes.extend(self.core_volumes.values())
            all_mounts.extend(self.core_mounts.values())
        all_volumes.extend(volumes or [])
        all_mounts.extend(mounts or [])

        # Build metadata
        metadata = V1ObjectMeta(name=deployment_name, labels=all_labels, annotations={CHANGE_KEY_NAME: change_key})

        # Figure out which (if any) service account to use
        service_account = PRIVILEGED_SERVICE_ACCOUNT_NAME if core_mounts else UNPRIVILEGED_SERVICE_ACCOUNT_NAME
        if docker_config.service_account:
            service_account = docker_config.service_account

        # Prepare initContainers, if necessary
        init_containers: list[V1Container] = []

        # Ensure AL user has the right access on volume mounts
        # Ignore ownership changes involving Secret/ConfigMap mounts
        chown_mounts: list[V1VolumeMount] = [m for i, m in enumerate(all_mounts)
                                             if not (all_volumes[i].config_map or all_volumes[i].secret)]
        if chown_mounts:
            # Ensure AL user has the right access
            init_containers.append(V1Container(
                name="chown-mounts",
                image=docker_config.image,
                command=['chown', '-R', '1000:1000'] + [m.mount_path for m in chown_mounts],
                security_context=V1SecurityContext(run_as_user=0),
                volume_mounts=chown_mounts
            ))

        pod = V1PodSpec(
            init_containers=init_containers,
            volumes=all_volumes,
            containers=self._create_containers(service_name, deployment_name, docker_config,
                                               all_mounts, security_context, core_container=core_mounts),
            priority_class_name=self.dependency_priority if high_priority else self.priority,
            termination_grace_period_seconds=shutdown_seconds,
            security_context=V1PodSecurityContext(fs_group=1000),
            service_account_name=service_account,
            affinity=selector_to_node_affinity(self.linux_node_selector),
            tolerations=self.default_service_tolerations,
            automount_service_account_token=False,
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
            strategy=deployment_strategy
        )

        deployment = V1Deployment(
            kind="Deployment",
            metadata=metadata,
            spec=spec,
        )

        if replace:
            self.logger.info("Requesting kubernetes replace deployment info for: %s", metadata.name)
            try:
                self.apps_api.replace_namespaced_deployment(namespace=self.namespace, body=deployment,
                                                            name=metadata.name, _request_timeout=API_TIMEOUT)
                return
            except ApiException as error:
                if error.status == 422:
                    # Replacement of an immutable field (ie. labels) attempted
                    existing_labels = self.apps_api.read_namespaced_deployment(name=metadata.name,
                                                                               namespace=self.namespace).metadata.labels
                    # Delete deployments with the same labels and re-create
                    self.stop_containers(labels=existing_labels, exact_label_match=True)

        else:
            self.logger.info("Requesting kubernetes create deployment info for: %s", metadata.name)
        self.apps_api.create_namespaced_deployment(namespace=self.namespace, body=deployment,
                                                   _request_timeout=API_TIMEOUT)

    def _create_hpa(self, profile: ProfileBase, min_instances: int | None = None, max_instances: int | None = None):
        """Set the target for running instances of a service."""

        name = self._deployment_name(profile.name)
        if min_instances is None:
            min_instances = profile.min_instances
        if max_instances is None:
            max_instances = profile.max_instances
        min_instances = max(1, min_instances)
        max_instances = max(min_instances, max_instances)

        for _ in range(10):
            try:
                hpa: V2HorizontalPodAutoscaler = self.scale_api.read_namespaced_horizontal_pod_autoscaler(
                    name=name, namespace=self.namespace, _request_timeout=API_TIMEOUT)

                changed = any((
                    hpa.spec.max_replicas != max_instances,
                    hpa.spec.min_replicas != min_instances,
                    hpa.spec.metrics[0].external.target.average_value != profile.target_queue_length
                ))

                if not changed:
                    return
                hpa.spec.max_replicas = max_instances
                hpa.spec.min_replicas = min_instances
                hpa.spec.metrics[0].external.target.average_value = profile.target_queue_length
                self.scale_api.patch_namespaced_horizontal_pod_autoscaler(name=name, namespace=self.namespace, body=hpa,
                                                                          _request_timeout=API_TIMEOUT)
                return
            except client.ApiException as error:
                # If the error is a conflict, it means multiple attempts to scale a deployment
                # were made at the same time and conflicted, we can retry
                if error.reason == 'Conflict':
                    self.logger.info("Conflict scaling %s retrying.", profile.name)
                    continue
                if error.status == 404:
                    break
                raise

        all_labels = {_v.name: _v.value for _v in profile.container_config.labels}
        all_labels.update(self._labels)
        all_labels['component'] = profile.name

        # Build metadata
        metadata = V1ObjectMeta(name=name, labels=all_labels)

        spec = V2HorizontalPodAutoscalerSpec(
            max_replicas=max(max_instances, 1),
            min_replicas=max(min_instances, 1),
            # behavior=,
            metrics=[
                V2MetricSpec(
                    type='External',
                    external=V2ExternalMetricSource(
                        metric=V2MetricIdentifier(
                            name='al-service-queue-length-' + profile.name.lower(),
                        ),
                        target=V2MetricTarget(
                            average_value=str(profile.target_queue_length),
                            type='AverageValue'
                        ),
                    )
                ),
                V2MetricSpec(
                    type='Resource',
                    resource=V2ResourceMetricSource(
                        name='cpu',
                        target=V2MetricTarget(
                            average_utilization=70,
                            type='Utilization',
                        )
                    )
                ),
            ],
            scale_target_ref=V2CrossVersionObjectReference(
                kind='Deployment',
                api_version='apps/v1',
                name=name,
            ),
        )

        body = V2HorizontalPodAutoscaler(
            kind="HorizontalPodAutoscaler",
            api_version="autoscaling/v2",
            metadata=metadata,
            spec=spec,
        )

        self.scale_api.create_namespaced_horizontal_pod_autoscaler(
            namespace=self.namespace, body=body, _request_timeout=API_TIMEOUT)

    def get_target(self, service_name: str) -> int:
        """Get the target for running instances of a service."""
        return self._deployment_targets.get(service_name, 0)

    def get_targets(self) -> dict[str, int]:
        """Get the target for running instances of all services."""
        return self._deployment_targets

    def get_unavailable(self) -> dict[str, int]:
        """Get the number of containers the orchestration layer could not start."""
        return self._deployment_unavailable

    def set_target(self, service_name: str, target: int):
        """Set the target for running instances of a service."""
        for _ in range(10):
            try:
                # If we scale the deployment to zero the hpa will stop managing it until we clear this zero later
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
        self.logger.error("Repeated conflict scaling %s will not retry.", service_name)

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

    def get_running_container_names(self):
        pods = self.api.list_pod_for_all_namespaces(field_selector='status.phase==Running',
                                                    _request_timeout=API_TIMEOUT)
        return [pod.metadata.name for pod in pods.items]

    def new_events(self):
        response = self.api.list_namespaced_event(namespace=self.namespace, pretty='false',
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

    def stateful_container_key(self, service_name: str, container_name: str, spec: DependencyConfig,
                               change_key: str) -> Optional[str]:
        container_key = None
        deployment_name = self._dependency_name(service_name, container_name)
        try:
            old_deployment: V1Deployment = self.apps_api.read_namespaced_deployment(deployment_name, self.namespace)
            for container in old_deployment.spec.template.spec.containers:
                for env in container.env:
                    if env.name == 'AL_INSTANCE_KEY':
                        self._service_limited_env[service_name][f'{container_name}_host'] = deployment_name
                        self._service_limited_env[service_name][f'{container_name}_key'] = env.value
                        if spec.container.ports:
                            self._service_limited_env[service_name][f'{container_name}_port'] = spec.container.ports[0]
                        container_key = env.value
                        break
        except ApiException as error:
            if error.status != 404:
                raise

        if not container_key:
            # No existing instance found
            return None

        # Generate the expected change key
        senv = sorted(self._service_limited_env[service_name].items())
        labels = [('container', container_name), ('dependency_for', service_name)]
        labels += sorted([(_v.name, _v.value) for _v in spec.container.labels])
        temp_spec = DependencyConfig(spec.as_primitives())
        volumes, mounts, _ = self._get_volumes_mounts_strategy(deployment_name, container_name, temp_spec)
        temp_spec.container.environment.append(dict(name='AL_INSTANCE_KEY', value=container_key))
        change_key = str(f"n={deployment_name}{change_key}dc={temp_spec.container}ss={30}"
                         f"l={labels}v={volumes}m={mounts}cm={True}senv={senv}")

        self.logger.debug(f"{deployment_name} expected change_key: {change_key}")
        if old_deployment.metadata.annotations.get(CHANGE_KEY_NAME) != str(hash(change_key)):
            # A change occurred, declare dependency not ready yet.
            return None

        return container_key

    def _get_volumes_mounts_strategy(self, deployment_name: str, container_name: str, spec: DependencyConfig
                                     ) -> tuple[list[V1Volume], list[V1VolumeMount], V1DeploymentStrategy]:
        volumes, mounts = [], []
        deployment_strategy = V1DeploymentStrategy()  # Default strategy should be RollingUpdate

        # Since we reserved containers named 'updates' to be service updaters, they will always 'Recreate'
        deployment_strategy = V1DeploymentStrategy(type='Recreate')

        if os.path.exists(AL_ROOT_CA):
            # Specifically for service updaters when internal encryption is enabled on the cluster
            dep_cert_dir = f"/etc/assemblyline/ssl/al_{container_name}"
            cert_secret_name = f"{deployment_name}-cert"

            def generate_certificate_secret() -> V1Secret:
                # Certificate pair doesn't exist or is invalid for this dependency, create it
                with open(AL_ROOT_CA, 'rb') as root_ca:
                    rootca_cert = x509.load_pem_x509_certificate(root_ca.read())
                with open(AL_ROOT_CA_PK, 'rb') as root_ca_pk:
                    rootca_pk = serialization.load_pem_private_key(root_ca_pk.read(), None)

                cert_key = rsa.generate_private_key(65537, 2048)
                cert = x509.CertificateBuilder(
                    issuer_name=rootca_cert.issuer,
                    subject_name=x509.Name([x509.NameAttribute(x509.OID_COMMON_NAME, deployment_name)]),
                    not_valid_before=(datetime.utcnow() - timedelta(days=1)),
                    not_valid_after=(datetime.utcnow() + timedelta(days=CERTIFICATE_VALIDITY_PERIOD)),
                    public_key=cert_key.public_key(),
                    serial_number=x509.random_serial_number()).add_extension(
                        x509.SubjectAlternativeName([x509.DNSName(deployment_name)]),
                    critical=False).sign(rootca_pk, hashes.SHA256())

                return V1Secret(metadata=V1ObjectMeta(name=cert_secret_name, namespace=self.namespace),
                                type='kubernetes.io/tls',
                                data={
                                    'tls.crt': b64encode(cert.public_bytes(serialization.Encoding.PEM)).decode(),
                                    'tls.key': b64encode(cert_key.private_bytes(
                                        encoding=serialization.Encoding.PEM,
                                        format=serialization.PrivateFormat.PKCS8,
                                        encryption_algorithm=serialization.NoEncryption())).decode()})

            try:
                # Ensure that the certificate isn't close to expiring within a week, if it exists
                cert_secret: V1Secret = self.api.read_namespaced_secret(
                    name=cert_secret_name, namespace=self.namespace, _request_timeout=API_TIMEOUT)
                expiration_date = cert_secret.metadata.managed_fields[0].time + timedelta(
                    days=CERTIFICATE_VALIDITY_PERIOD)
                current_date = datetime.now(tzlocal())
                if current_date - timedelta(days=7) < expiration_date < current_date:
                    # If this certificate is set to expire within a week, rotate it
                    self.logger.warning(
                        f"Certificate '{cert_secret_name}' is set to expire within a week. Beginning rotation..")
                    self.api.patch_namespaced_secret(cert_secret_name, namespace=self.namespace,
                                                     body=generate_certificate_secret(),
                                                     _request_timeout=API_TIMEOUT)

            except (ApiException, ValueError) as error:
                if isinstance(error, ApiException) and error.status != 404:
                    raise

                self.api.create_namespaced_secret(namespace=self.namespace,
                                                  body=generate_certificate_secret(),
                                                  _request_timeout=API_TIMEOUT)

            finally:
                volumes.append(V1Volume(name=cert_secret_name,
                                        secret=V1SecretVolumeSource(secret_name=cert_secret_name)))
                mounts.append(V1VolumeMount(name=cert_secret_name, mount_path=dep_cert_dir, read_only=True))

            # Pass gunicorn settings via env
            spec.container.environment.append({'name': 'CERTFILE', 'value': os.path.join(dep_cert_dir, 'tls.crt')})
            spec.container.environment.append({'name': 'KEYFILE', 'value': os.path.join(dep_cert_dir, 'tls.key')})

        for volume_name, volume_spec in spec.volumes.items():
            mount_name = f'{deployment_name}-{volume_name}'

            if volume_spec.access_mode == 'ReadWriteOnce':
                # RollingUpdate strategy isn't appropriate for Deployments with RWO-attached volumes
                deployment_strategy = V1DeploymentStrategy(type='Recreate')

            # Check if the PVC exists, create if not
            self._ensure_pvc(mount_name, volume_spec, deployment_name)

            # Create the volume info
            volumes.append(V1Volume(
                name=mount_name,
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(mount_name)
            ))
            mounts.append(V1VolumeMount(mount_path=volume_spec.mount_path, name=mount_name))
        return volumes, mounts, deployment_strategy

    def start_stateful_container(self, service_name: str, container_name: str,
                                 spec, labels: dict[str, str], change_key: str):
        # Setup PVC
        deployment_name = self._dependency_name(service_name, container_name)
        volumes, mounts, deployment_strategy = self._get_volumes_mounts_strategy(deployment_name, container_name, spec)

        # Read the key being used for the deployment instance or generate a new one
        instance_key = uuid.uuid4().hex
        try:
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
                                30, 1, labels, volumes=volumes, mounts=mounts, high_priority=True,
                                core_mounts=spec.run_as_core, change_key=change_key,
                                deployment_strategy=deployment_strategy, security_context=self.security_policy)

        # Setup a service to direct to the deployment
        try:
            service = self.api.read_namespaced_service(deployment_name, self.namespace)
            service.metadata.labels = labels
            service.spec.selector = labels
            service.spec.ports = [V1ServicePort(port=int(_p), name=f"port-{_p}") for _p in spec.container.ports]
            self.api.patch_namespaced_service(deployment_name, self.namespace, service)
        except ApiException as error:
            if error.status != 404:
                raise
            service = V1Service(
                metadata=V1ObjectMeta(name=deployment_name, labels=labels),
                spec=V1ServiceSpec(
                    cluster_ip='None',
                    selector=labels,
                    ports=[V1ServicePort(port=int(_p), name=f"port-{_p}") for _p in spec.container.ports]
                )
            )
            self.api.create_namespaced_service(self.namespace, service)

        # Add entries to the environment variable list to point to this container
        self._service_limited_env[service_name][f'{container_name}_host'] = deployment_name
        self._service_limited_env[service_name][f'{container_name}_key'] = instance_key
        if spec.container.ports:
            self._service_limited_env[service_name][f'{container_name}_port'] = spec.container.ports[0]

    def _ensure_pvc(self, name: str, volume_spec: PersistentVolume, deployment_name: str):
        size_Mi = f'{max(round(int(volume_spec.capacity)/1024), 1024)}Mi'
        size_Gi = f'{max(round(int(volume_spec.capacity)/1048576), 1)}Gi'
        request = V1ResourceRequirements(requests={'storage': size_Mi})
        claim_spec = V1PersistentVolumeClaimSpec(storage_class_name=volume_spec.storage_class, resources=request,
                                                 volume_mode='Filesystem', access_modes=[volume_spec.access_mode])
        metadata = V1ObjectMeta(namespace=self.namespace, name=name)
        claim = V1PersistentVolumeClaim(metadata=metadata, spec=claim_spec)

        def remove_pvc(deployment_name, pvc_name):
            self.logger.info(f'Deleting old {deployment_name} deployment to release {name} PVC to be recreated..')
            # Remove deployment
            self.apps_api.delete_namespaced_deployment(name=deployment_name, namespace=self.namespace,
                                                       _request_timeout=API_TIMEOUT)
            # Remove PVC
            self.api.delete_namespaced_persistent_volume_claim(name=pvc_name, namespace=self.namespace,
                                                               _request_timeout=API_TIMEOUT)
            # Poll to see if PVC has been removed
            try:
                while self.api.read_namespaced_persistent_volume_claim_status(name=pvc_name, namespace=self.namespace):
                    sleep(15)
            except ApiException as e:
                if e.status == 404:
                    return
                self.logger.error(e.reason)

        # Check to see if a PVC with the same name exists
        for pvc in self.api.list_namespaced_persistent_volume_claim(namespace=self.namespace).items:
            if pvc.metadata.name == metadata.name:
                pvc_requests = pvc.spec.resources.requests
                # Check for significant changes, if so replace
                if (pvc_requests['storage'].endswith('Mi') and pvc_requests['storage'] != size_Mi) or \
                    (pvc_requests['storage'].endswith('Gi') and pvc_requests['storage'] != size_Gi) or \
                        pvc.spec.storage_class_name != claim.spec.storage_class_name:
                    # If PVC is currently in use, terminate associated deployments to proceed with replacement
                    remove_pvc(deployment_name, name)
                    break
                # Otherwise no need to create a PVC that already exists unchanged
                return
        self.api.create_namespaced_persistent_volume_claim(namespace=self.namespace, body=claim,
                                                           _request_timeout=API_TIMEOUT)

    def stop_containers(self, labels, fields={}, exact_label_match=False):
        label_selector = ','.join(f'{_n}={_v}' for _n, _v in labels.items())
        deployments = self.apps_api.list_namespaced_deployment(namespace=self.namespace, label_selector=label_selector,
                                                               _request_timeout=API_TIMEOUT)
        for dep in deployments.items:
            if exact_label_match and dep.metadata.labels != labels:
                # We're only interested in deployments with exact label matches
                continue

            # Remove deployments with matching labels
            self.apps_api.delete_namespaced_deployment(name=dep.metadata.name, namespace=self.namespace,
                                                       _request_timeout=API_TIMEOUT)
            # Remove PV/C related to the deployment
            for vol in dep.spec.template.spec.volumes or []:
                if vol._persistent_volume_claim:
                    self.api.delete_namespaced_persistent_volume_claim(name=vol._persistent_volume_claim.claim_name,
                                                                       namespace=self.namespace,
                                                                       _request_timeout=API_TIMEOUT)

    def prepare_network(self, service_name: str, internet: bool, dependency_internet: list[Tuple[str, bool]]):
        safe_name = service_name.lower().replace('_', '-')
        service_labels = {
            'app': 'assemblyline',
            'section': 'service',
            'component': service_name,
        }
        # Gather all existing network policies pertaining to the service
        existing_netpol = {netpol.metadata.name for netpol in self.net_api.list_namespaced_network_policy(
            namespace=self.namespace,
            label_selector=','.join([f"{k}={v}" for k, v in service_labels.items()])).items}

        def create_or_patch_network_policy(netpol_body: V1NetworkPolicy):
            netpol_body.metadata.labels = service_labels
            try:
                # Patch the network policy, if it exists
                self.net_api.patch_namespaced_network_policy(name=netpol_body.metadata.name, namespace=self.namespace,
                                                             body=netpol_body, _request_timeout=API_TIMEOUT)
            except ApiException as error:
                if error.status == 404:
                    # Object doesn't exist, therefore create it
                    self.net_api.create_namespaced_network_policy(namespace=self.namespace, body=netpol_body,
                                                                  _request_timeout=API_TIMEOUT)
                else:
                    raise

        # Create a list of network policies that must exist for this service
        # By default, we allow services to be able to interact with their dependencies and vice-versa
        network_policies = [
            V1NetworkPolicy(
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
            ),
            V1NetworkPolicy(
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
            )
        ]

        # service → anywhere
        if internet:
            network_policies.append(V1NetworkPolicy(
                metadata=V1ObjectMeta(name=f'allow-{safe_name}-outgoing'),
                spec=V1NetworkPolicySpec(
                    pod_selector=V1LabelSelector(match_labels={
                        'app': 'assemblyline',
                        'section': 'service',
                        'component': service_name,
                    }),
                    egress=[V1NetworkPolicyEgressRule(to=[])],
                )
            ))

        # dependencies → anywhere
        for dep_name, dep_internet in dependency_internet:
            safe_dep_name = dep_name.lower().replace('_', '-')
            if dep_internet:
                network_policies.append(V1NetworkPolicy(
                    metadata=V1ObjectMeta(name=f'allow-{safe_dep_name}-{safe_name}-outgoing'),
                    spec=V1NetworkPolicySpec(
                        pod_selector=V1LabelSelector(match_labels={
                            'app': 'assemblyline',
                            'section': 'service',
                            'dependency_for': service_name,
                            'container': dep_name,
                        }),
                        egress=[V1NetworkPolicyEgressRule(to=[])],
                    )
                ))

        # Create or patch the network policies based on what's required for the service
        [create_or_patch_network_policy(netpol) for netpol in network_policies]

        # Cleanup any network policies that aren't in-use
        for np in (existing_netpol - {np.metadata.name for np in network_policies}):
            self.net_api.delete_namespaced_network_policy(namespace=self.namespace, name=np,
                                                          _request_timeout=API_TIMEOUT)

    def update_config_map(self, data: dict, name: str):
        """Update or create a ConfigMap in Kubernetes."""
        config_map = V1ConfigMap(
            metadata=V1ObjectMeta(name=name, namespace=self.namespace),
            data=data
        )
        try:
            self.api.patch_namespaced_config_map(name=name, namespace=self.namespace, body=config_map,
                                                 _request_timeout=API_TIMEOUT)
        except ApiException as error:
            if error.status == 404:
                self.api.create_namespaced_config_map(namespace=self.namespace, body=config_map,
                                                      _request_timeout=API_TIMEOUT)
            else:
                raise
