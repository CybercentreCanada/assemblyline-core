"""
A process that manages tracking and running update commands for the AL services.
"""
from __future__ import annotations

import os
import re
import time
import uuid

from concurrent.futures import ThreadPoolExecutor
from typing import Any, List

import docker

from kubernetes.client import V1Job, V1ObjectMeta, V1JobSpec, V1PodTemplateSpec, V1PodSpec, V1Volume, \
    V1VolumeMount, V1EnvVar, V1Container, V1ResourceRequirements, \
    V1ConfigMapVolumeSource, V1Secret, V1SecretVolumeSource, V1LocalObjectReference
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from assemblyline.common import isotime
from assemblyline.odm.messages.changes import Operation, ServiceChange
from assemblyline.odm.models.config import Mount
from assemblyline.odm.models.service import DockerConfig, Service
from assemblyline.remote.datatypes.events import EventSender, EventWatcher
from assemblyline.remote.datatypes.hash import Hash
from assemblyline_core.scaler.controllers.kubernetes_ctl import create_docker_auth_config
from assemblyline_core.server_base import ThreadedCoreBase
from assemblyline_core.updater.helper import get_latest_tag_for_service

# How many seconds per check for outstanding updates
UPDATE_CHECK_INTERVAL = int(os.getenv("UPDATE_CHECK_INTERVAL", "60"))
# How many seconds to wait for checking for new service versions
CONTAINER_CHECK_INTERVAL = int(os.getenv("CONTAINER_CHECK_INTERVAL", "300"))

API_TIMEOUT = 90
NAMESPACE = os.getenv('NAMESPACE', None)
INHERITED_VARIABLES: list[str] = ['HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'http_proxy', 'https_proxy', 'no_proxy'] + \
    [
    secret.strip("${}")
    for secret in re.findall(r'\${\w+}', open('/etc/assemblyline/config.yml', 'r').read()) + ['UI_SERVER']]

CONFIGURATION_HOST_PATH = os.getenv('CONFIGURATION_HOST_PATH', 'service_config')
CONFIGURATION_CONFIGMAP = os.getenv('KUBERNETES_AL_CONFIG', None)
AL_CORE_NETWORK = os.environ.get("AL_CORE_NETWORK", 'core')

SERVICE_API_HOST = os.getenv('SERVICE_API_HOST')
UI_SERVER = os.getenv('UI_SERVER')
RELEASE_NAME = os.getenv('RELEASE_NAME')


class DockerUpdateInterface:
    """Wrap docker interface for the commands used in the update process."""

    def __init__(self, log_level="INFO"):
        self.client = docker.from_env()
        self._external_network = None
        self.log_level = log_level

    @property
    def external_network(self):
        """Lazy load the external network information from docker.

        In testing environments it may not exists, and we may not have docker socket anyway.
        """
        if not self._external_network:
            for network in self.client.networks.list(names=['external']):
                self._external_network = network
                break
            else:
                self._external_network = self.client.networks.create(name='external', internal=False)
        return self._external_network

    def launch(self, name, docker_config: DockerConfig, mounts, env, blocking: bool = True):
        """Run a container to completion."""
        docker_mounts = dict()
        # Add the configuration file if path is given
        if CONFIGURATION_HOST_PATH:
            docker_mounts[CONFIGURATION_HOST_PATH] = {
                'bind': '/etc/assemblyline/',
                'mode': 'ro'
            }

        # Pull the image before we try to use it locally.
        # This lets us override the auth_config on a per image basis.
        # Split the image string into "[registry/]image_name" and "tag"
        repository, _, tag = docker_config.image.rpartition(':')
        if '/' in tag:
            # if there is a '/' in the tag it is invalid. We have split ':' on a registry
            # port not a tag, there can't be a tag in this image string. Put the registry
            # string back together, and use a default tag
            repository += ':' + tag
            tag = 'latest'

        # Add auth info if we have it
        auth_config = None
        if docker_config.registry_username or docker_config.registry_password:
            auth_config = {
                'username': docker_config.registry_username,
                'password': docker_config.registry_password
            }

        self.client.images.pull(repository, tag, auth_config=auth_config)

        docker_mounts.update({os.path.join(row['volume'], row['source_path']): {'bind': row['dest_path'],
                                                                                'mode': row.get('mode', 'ro')}
                              for row in mounts})
        # Launch container
        container = self.client.containers.run(
            image=docker_config.image,
            name='update_' + name + '_' + uuid.uuid4().hex,
            labels={'update_for': name, 'updater_launched': 'true'},
            network=AL_CORE_NETWORK,
            restart_policy={'Name': 'no'},
            command=docker_config.command,
            volumes=docker_mounts,
            environment=[f'{_e.name}={_e.value}' for _e in docker_config.environment] +
                        [f'{k}={v}' for k, v in env.items()] +
                        [f'{name}={os.environ[name]}' for name in INHERITED_VARIABLES if name in os.environ] +
                        [f'LOG_LEVEL={self.log_level}'],
            detach=True,
        )

        # Connect to extra networks
        if docker_config.allow_internet_access:
            self.external_network.connect(container)

        # Wait for the container to terminate
        if blocking:
            container.wait()

    def cleanup_stale(self):
        # We want containers that are updater managed, already finished, and exited five minutes ago.
        # The reason for the delay is in development systems people may want to check the output
        # of failed update containers they are working on launching with the updater.
        filters = {'label': 'updater_launched=true', 'status': 'exited'}
        time_mark = isotime.now_as_iso(-60*5)

        for container in self.client.containers.list(all=True, ignore_removed=True, filters=filters):
            if container.attrs['State'].get('FinishedAt', '9999') < time_mark:
                container.remove()

    def restart(self, service_name):
        for container in self.client.containers.list(filters={'label': f'component={service_name}'}):
            container.kill()


class KubernetesUpdateInterface:
    def __init__(self, prefix, namespace, priority_class, extra_labels, log_level="INFO", default_service_account=None):
        # Try loading a kubernetes connection from either the fact that we are running
        # inside of a cluster, or we have a configuration in the normal location
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
        self.apps_api = client.AppsV1Api()
        self.api = client.CoreV1Api()
        self.batch_api = client.BatchV1Api()
        self.namespace = namespace
        self.priority_class = priority_class
        self.extra_labels = extra_labels
        self.log_level = log_level
        self.default_service_account = default_service_account
        self.secret_env = []

        # Get the deployment of this process. Use that information to fill out the secret info
        deployment = self.apps_api.read_namespaced_deployment(name='updater', namespace=self.namespace)
        for env_name in list(INHERITED_VARIABLES):
            for container in deployment.spec.template.spec.containers:
                for env_def in container.env:
                    if env_def.name == env_name:
                        self.secret_env.append(env_def)
                        INHERITED_VARIABLES.remove(env_name)

    def launch(self, name, docker_config: DockerConfig, mounts: List[Mount], env, blocking: bool = True):
        name = (self.prefix + 'update-' + name.lower()).replace('_', '-')

        # If we have been given a username or password for the registry, we have to
        # update it, if we haven't been, make sure its been cleaned up in the system
        # so we don't leave passwords lying around
        pull_secret_name = f'{name}-job-pull-secret'
        use_pull_secret = False
        try:
            # Check if there is already a username/password defined for this job
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
                self.api.replace_namespaced_secret(pull_secret_name, namespace=self.namespace,
                                                   body=new_pull_secret, _request_timeout=API_TIMEOUT)
            else:
                self.api.create_namespaced_secret(namespace=self.namespace, body=new_pull_secret,
                                                  _request_timeout=API_TIMEOUT)
        elif current_pull_secret:
            # If there is a password set in kubernetes, but not in our configuration clear it out
            self.api.delete_namespaced_secret(pull_secret_name, self.namespace, _request_timeout=API_TIMEOUT)

        try:
            self.batch_api.delete_namespaced_job(name=name, namespace=self.namespace,
                                                 propagation_policy='Background', _request_timeout=API_TIMEOUT)
            while True:
                self.batch_api.read_namespaced_job(namespace=self.namespace, name=name,
                                                   _request_timeout=API_TIMEOUT)
                time.sleep(1)
        except ApiException:
            pass

        volumes = []
        volume_mounts = []

        for mount in mounts:
            # Initialize with required set of params
            vol_kwargs = dict(name=mount.name)
            vol_mount_kwargs = dict(
                name=mount.name,
                mount_path=mount.path,
                read_only=mount.read_only,
            )

            if mount.config_map:
                # Deprecated configuration for mounting ConfigMap
                # TODO: Deprecate code on next major change
                self.log.warning(
                    "DEPRECATED: Migrate default service mounts using ConfigMaps to use: "
                    f"resource_type='configmap', resource_name={mount.config_map}, resource_key={mount.key or ''}. "
                    "Continuing deprecated mounting.."
                )
                vol_kwargs.update(dict(config_map=V1ConfigMapVolumeSource(name=mount.config_map, optional=False)))
                vol_mount_kwargs.update(dict(sub_path=mount.key))

            elif mount.resource_type == 'secret':
                # Secret-based source
                vol_kwargs.update(dict(secret=V1SecretVolumeSource(secret_name=mount.resource_name)))
                vol_mount_kwargs.update(dict(sub_path=mount.resource_key))

            elif mount.resource_type == 'configmap':
                # ConfigMap-based source
                vol_kwargs.update(dict(config_map=V1ConfigMapVolumeSource(name=mount.resource_name, optional=False)))
                vol_mount_kwargs.update(dict(sub_path=mount.resource_key))

            volumes.append(V1Volume(**vol_kwargs))
            volume_mounts.append(V1VolumeMount(**vol_mount_kwargs))

        if CONFIGURATION_CONFIGMAP:
            volumes.append(V1Volume(
                name='mount-configuration',
                config_map=V1ConfigMapVolumeSource(
                    name=CONFIGURATION_CONFIGMAP
                ),
            ))

            volume_mounts.append(V1VolumeMount(
                name='mount-configuration',
                mount_path='/etc/assemblyline/config.yml',
                sub_path="config",
                read_only=True,
            ))

        section = 'service'
        labels = {
            'app': 'assemblyline',
            'section': section,
            'privilege': 'core',
            'component': 'update-script',
        }
        labels.update(self.extra_labels)

        metadata = V1ObjectMeta(
            name=name,
            labels=labels
        )

        environment_variables = [V1EnvVar(name=_e.name, value=_e.value) for _e in docker_config.environment]
        environment_variables.extend([V1EnvVar(name=k, value=v) for k, v in env.items()])
        environment_variables.extend([V1EnvVar(name=k, value=os.environ[k])
                                      for k in INHERITED_VARIABLES if k in os.environ])
        environment_variables.extend(self.secret_env)
        environment_variables.append(V1EnvVar(name="LOG_LEVEL", value=self.log_level))

        cores = docker_config.cpu_cores
        memory = docker_config.ram_mb
        memory_min = min(docker_config.ram_mb_min, memory)

        container = V1Container(
            name=name,
            image=docker_config.image,
            command=docker_config.command,
            env=environment_variables,
            image_pull_policy='Always',
            volume_mounts=volume_mounts,
            resources=V1ResourceRequirements(
                limits={'cpu': cores, 'memory': f'{memory}Mi'},
                requests={'cpu': cores / 4, 'memory': f'{memory_min}Mi'},
            )
        )

        pod = V1PodSpec(
            volumes=volumes,
            restart_policy='Never',
            containers=[container],
            priority_class_name=self.priority_class,
            service_account_name=docker_config.service_account or self.default_service_account
        )

        if use_pull_secret:
            pod.image_pull_secrets = [V1LocalObjectReference(name=pull_secret_name)]

        job = V1Job(
            metadata=metadata,
            spec=V1JobSpec(
                backoff_limit=1,
                completions=1,
                template=V1PodTemplateSpec(
                    metadata=metadata,
                    spec=pod
                )
            )
        )

        status = self.batch_api.create_namespaced_job(namespace=self.namespace, body=job,
                                                      _request_timeout=API_TIMEOUT).status

        if blocking:
            try:
                while not (status.failed or status.succeeded):
                    time.sleep(3)
                    status = self.batch_api.read_namespaced_job(namespace=self.namespace, name=name,
                                                                _request_timeout=API_TIMEOUT).status

                self.batch_api.delete_namespaced_job(name=name, namespace=self.namespace,
                                                     propagation_policy='Background', _request_timeout=API_TIMEOUT)
            except ApiException as error:
                if error.status != 404:
                    raise

    def cleanup_stale(self):
        # Clear up any finished jobs.
        # This can happen when the updater was restarted while an update was running.
        labels = 'app=assemblyline,component=update-script'
        jobs = self.batch_api.list_namespaced_job(namespace=self.namespace, _request_timeout=API_TIMEOUT,
                                                  label_selector=labels)
        for job in jobs.items:
            if job.status.failed or job.status.succeeded:
                try:
                    self.batch_api.delete_namespaced_job(name=job.metadata.name, namespace=self.namespace,
                                                         propagation_policy='Background', _request_timeout=API_TIMEOUT)
                except ApiException as error:
                    if error.status != 404:
                        raise

    def restart(self, service_name):
        for _ in range(10):
            try:
                name = (self.prefix + service_name.lower()).replace('_', '-')
                scale = self.apps_api.read_namespaced_deployment_scale(name=name, namespace=self.namespace,
                                                                       _request_timeout=API_TIMEOUT)
                scale.spec.replicas = 0
                self.apps_api.replace_namespaced_deployment_scale(name=name, namespace=self.namespace, body=scale,
                                                                  _request_timeout=API_TIMEOUT)
                return
            except ApiException as error:
                # If the error is a 404, it means the resource we want to restart
                # doesn't exist, which means we don't need to restart it, so we can
                # safely ignore this error
                if error.status == 404:
                    return
                # Conflict means two different servers were trying to replace the
                # deployment at the same time, we should retry
                if error.reason == 'Conflict':
                    continue
                raise


class ServiceUpdater(ThreadedCoreBase):
    def __init__(self, redis_persist=None, redis=None, logger=None, datastore=None):
        super().__init__('assemblyline.service.updater', logger=logger, datastore=datastore,
                         redis_persist=redis_persist, redis=redis)

        self.container_update: Hash[dict[str, Any]] = Hash('container-update', self.redis_persist)
        self.container_install: Hash[dict[str, Any]] = Hash('container-install', self.redis_persist)
        self.latest_service_tags: Hash[dict[str, str]] = Hash('service-tags', self.redis_persist)
        self.service_events = EventSender('changes.services', host=self.redis)

        self.incompatible_services = set()
        self.service_change_watcher = EventWatcher(self.redis, deserializer=ServiceChange.deserialize)
        self.service_change_watcher.register('changes.services.*', self._handle_service_change_event)
        self.mounts = []

        if 'KUBERNETES_SERVICE_HOST' in os.environ and NAMESPACE:
            extra_labels = {}
            if self.config.core.scaler.additional_labels:
                extra_labels = {k: v for k, v in (_l.split("=") for _l in self.config.core.scaler.additional_labels)}

            # If Updater has envs that set the service-server to use HTTPS, then assume a Root CA needs to be mounted
            if SERVICE_API_HOST and SERVICE_API_HOST.startswith('https'):
                self.config.core.scaler.service_defaults.mounts.append(dict(
                    name="root-ca",
                    path="/etc/assemblyline/ssl/al_root-ca.crt",
                    resource_type="secret",
                    resource_name=f"{RELEASE_NAME}.internal-generated-ca",
                    resource_key="tls.crt"
                ))

            self.controller = KubernetesUpdateInterface(prefix='alsvc_', namespace=NAMESPACE,
                                                        priority_class='al-core-priority',
                                                        extra_labels=extra_labels,
                                                        log_level=self.config.logging.log_level,
                                                        default_service_account=self.config.services.service_account)
            # Add all additional mounts to privileged services
            self.mounts = self.config.core.scaler.service_defaults.mounts
        else:
            self.controller = DockerUpdateInterface(log_level=self.config.logging.log_level)

    def _handle_service_change_event(self, data: ServiceChange):
        if data.operation == Operation.Incompatible:
            self.incompatible_services.add(data.name)

    def container_installs(self):
        """Go through the list of services and check what are the latest tags for it"""
        while self.running:
            self.log.info("[CI] Installing all services marked for install...")

            # Install function for services
            def install_service(service_name: str, install_data: dict) -> str:
                if self.config.services.preferred_update_channel == 'stable':
                    tag = 'stable'
                else:
                    tag = 'latest'
                service_key = None
                try:
                    service = Service(
                        {'name': service_name,
                         'update_channel': self.config.services.preferred_update_channel,
                         'version': tag,
                         'docker_config': {'image': install_data.get('image')}})

                    image_name, tag_name, auth = get_latest_tag_for_service(
                        service,  self.config, self.log, prefix="[CI] ")

                    docker_config = DockerConfig(dict(
                        # remove allow internet access
                        allow_internet_access=True,
                        cpu_cores=1,
                        environment=[],
                        image=image_name + ":" + tag_name,
                        ports=[]
                    ))

                    if auth:
                        docker_config.registry_username = auth['username']
                        docker_config.registry_password = auth['password']

                    self.log.info(f"[CI] Service {service_name} is being installed to version {tag_name}...")

                    self.controller.launch(
                        name=service_name,
                        docker_config=docker_config,
                        mounts=self.mounts,
                        env={
                            "SERVICE_TAG": tag_name,
                            "REGISTER_ONLY": 'true',
                            "PRIVILEGED": 'true',
                        },
                        blocking=True
                    )

                    service_key = f"{service_name}_{tag_name.replace('stable', '')}"

                except Exception as e:
                    self.log.error(
                        f"[CI] Service {service_name} has failed to install. Install procedure cancelled... [{str(e)}]")
                return service_key

            # Start up installs for services in parallel
            install_threads = []
            with ThreadPoolExecutor() as service_installs_exec:
                for service_name, install_data in self.container_install.items().items():
                    install_threads.append(service_installs_exec.submit(install_service, service_name, install_data))

            # Once all threads are completed, check the status of the installs
            for thread in install_threads:
                service_key = thread.result()
                service_name, latest_tag = service_key.split("_")

                if self.datastore.service.get_if_exists(service_key):
                    operations = [(self.datastore.service_delta.UPDATE_SET, 'version', latest_tag)]

                    # Check if a service was previously disabled and re-enable it
                    if service_name in self.incompatible_services:
                        self.incompatible_services.remove(service_name)
                        operations.append((self.datastore.service_delta.UPDATE_SET, 'enabled', True))

                    if self.datastore.service_delta.update(service_name, operations):
                        # Update completed, cleanup
                        self.service_events.send(service_name, {
                            'operation': Operation.Added,
                            'name': service_name
                        })
                        self.log.info(f"[CI] Service {service_name}_{latest_tag} install successful!")
                    else:
                        self.log.error(f"[CI] Service {service_name} has failed to install because it cannot set "
                                       f"{latest_tag} as the new version. Install procedure cancelled...")
                else:
                    self.log.error(f"[CI] Service {service_name} has failed to install because resulting "
                                   f"service key ({service_key}) does not exist. Install procedure cancelled...")
                self.container_install.pop(service_name)

            # Clear out any old dead containers
            self.controller.cleanup_stale()

            self.log.info(f"[CI] Done installing services, waiting {UPDATE_CHECK_INTERVAL} seconds for next install...")
            time.sleep(UPDATE_CHECK_INTERVAL)

    def container_updates(self):
        """Go through the list of services and check what are the latest tags for it"""
        while self.running:
            self.log.info("[CU] Updating all services marked for update...")

            # Update function for services
            def update_service(service_name: str, update_data: dict) -> str:
                self.log.info(f"[CU] Service {service_name} is being updated to version {update_data['latest_tag']}...")

                # Load authentication params
                username = None
                password = None
                auth = update_data['auth'] or {}
                if auth:
                    username = auth.get('username', None)
                    password = auth.get('password', None)

                latest_tag = update_data['latest_tag'].replace('stable', '')
                service_key = f"{service_name}_{latest_tag}"
                try:
                    self.controller.launch(
                        name=service_name,
                        docker_config=DockerConfig(dict(
                            allow_internet_access=True,
                            registry_username=username,
                            registry_password=password,
                            cpu_cores=1,
                            environment=[],
                            image=update_data['image'],
                            ports=[]
                        )),
                        mounts=self.mounts,
                        env={
                            "SERVICE_TAG": update_data['latest_tag'],
                            "REGISTER_ONLY": 'true',
                            "PRIVILEGED": 'true',
                        },
                        blocking=True
                    )
                except Exception as e:
                    self.log.error(
                        f"[CU] Service {service_name} has failed to update. Update procedure cancelled... [{str(e)}]")
                return service_key

            # Start up updates for services in parallel
            update_threads = []
            with ThreadPoolExecutor() as service_updates_exec:
                for service_name, update_data in self.container_update.items().items():
                    update_threads.append(service_updates_exec.submit(update_service, service_name, update_data))

            # Once all threads are completed, check the status of the updates
            for thread in update_threads:
                service_key = thread.result()
                service_name, latest_tag = service_key.split("_")

                if self.datastore.service.get_if_exists(service_key):
                    operations = [(self.datastore.service_delta.UPDATE_SET, 'version', latest_tag)]

                    # Check if a service waas previously disabled and re-enable it
                    if service_name in self.incompatible_services:
                        self.incompatible_services.remove(service_name)
                        operations.append((self.datastore.service_delta.UPDATE_SET, 'enabled', True))

                    if self.datastore.service_delta.update(service_name, operations):
                        # Update completed, cleanup
                        self.service_events.send(service_name, {
                            'operation': Operation.Modified,
                            'name': service_name
                        })
                        self.log.info(f"[CU] Service {service_name} update successful!")
                    else:
                        self.log.error(f"[CU] Service {service_name} has failed to update because it cannot set "
                                       f"{latest_tag} as the new version. Update procedure cancelled...")
                else:
                    self.log.error(f"[CU] Service {service_name} has failed to update because resulting "
                                   f"service key ({service_key}) does not exist. Update procedure cancelled...")
                self.container_update.pop(service_name)

            # Clear out any old dead containers
            self.controller.cleanup_stale()

            self.log.info(f"[CU] Done updating services, waiting {UPDATE_CHECK_INTERVAL} seconds for next update...")
            time.sleep(UPDATE_CHECK_INTERVAL)

    def container_versions(self):
        """Go through the list of services and check what are the latest tags for it"""
        while self.running:
            self.log.info("[CV] Checking for new versions of all service containers...")
            existing_services = set(self.container_update.keys()) | set(self.latest_service_tags.keys())
            discovered_services: list[str] = []

            for service in self.datastore.list_all_services(full=True):
                discovered_services.append(service.name)
                image_name, tag_name, auth = get_latest_tag_for_service(service, self.config, self.log, prefix="[CV] ")
                self.latest_service_tags.set(service.name,
                                             {'auth': auth, 'image': image_name, service.update_channel: tag_name})

            # Remove services we have locally or in redis that have been deleted from the database
            for stray_service in existing_services - set(discovered_services):
                self.log.info(f"[CV] Service updates disabled for {stray_service}")
                self._service_stage_hash.pop(stray_service)
                self.container_update.pop(stray_service)
                self.latest_service_tags.pop(stray_service)

            self.log.info("[CV] Done checking for new container versions, "
                          f"waiting {CONTAINER_CHECK_INTERVAL} seconds for next run...")
            time.sleep(CONTAINER_CHECK_INTERVAL)

    def try_run(self):
        # Load and maintain threads
        threads = {
            'Container version check': self.container_versions,
            'Container updates': self.container_updates,
            'Container installs': self.container_installs
        }
        self.maintain_threads(threads)


if __name__ == '__main__':
    ServiceUpdater().serve_forever()
