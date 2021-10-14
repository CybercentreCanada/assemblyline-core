"""
A process that manages tracking and running update commands for the AL services.
"""
from __future__ import annotations

import os
import uuid
import sched
import time
from typing import Any

import docker

from assemblyline.common import isotime
from kubernetes.client import V1Job, V1ObjectMeta, V1JobSpec, V1PodTemplateSpec, V1PodSpec, V1Volume, \
    V1PersistentVolumeClaimVolumeSource, V1VolumeMount, V1EnvVar, V1Container, V1ResourceRequirements, \
    V1ConfigMapVolumeSource, V1Secret, V1LocalObjectReference
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from assemblyline.odm.messages.changes import Operation

from assemblyline.odm.models.service import DockerConfig
from assemblyline.remote.datatypes.events import EventSender
from assemblyline.remote.datatypes.hash import Hash
from assemblyline_core.scaler.controllers.kubernetes_ctl import create_docker_auth_config
from assemblyline_core.server_base import CoreBase
from assemblyline_core.updater.helper import get_latest_tag_for_service

UPDATE_CHECK_INTERVAL = 60  # How many seconds per check for outstanding updates
CONTAINER_CHECK_INTERVAL = 60 * 5  # How many seconds to wait for checking for new service versions
API_TIMEOUT = 90
HEARTBEAT_INTERVAL = 5

# How many past updates to keep for file based updates
NAMESPACE = os.getenv('NAMESPACE', None)
INHERITED_VARIABLES = ['HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'http_proxy', 'https_proxy', 'no_proxy']
CLASSIFICATION_HOST_PATH = os.getenv('CLASSIFICATION_HOST_PATH', None)
CLASSIFICATION_CONFIGMAP = os.getenv('CLASSIFICATION_CONFIGMAP', None)
CLASSIFICATION_CONFIGMAP_KEY = os.getenv('CLASSIFICATION_CONFIGMAP_KEY', 'classification.yml')


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

    def launch(self, name, docker_config: DockerConfig, mounts, env, network, blocking: bool = True):
        """Run a container to completion."""
        # Add the classification file if path is given
        if CLASSIFICATION_HOST_PATH:
            mounts.append({
                'volume': CLASSIFICATION_HOST_PATH,
                'source_path': '',
                'dest_path': '/etc/assemblyline/classification.yml',
                'mode': 'ro'
            })

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

        # Launch container
        container = self.client.containers.run(
            image=docker_config.image,
            name='update_' + name + '_' + uuid.uuid4().hex,
            labels={'update_for': name, 'updater_launched': 'true'},
            network=network,
            restart_policy={'Name': 'no'},
            command=docker_config.command,
            volumes={os.path.join(row['volume'], row['source_path']): {'bind': row['dest_path'],
                                                                       'mode': row.get('mode', 'rw')}
                     for row in mounts},
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
    def __init__(self, prefix, namespace, priority_class, extra_labels, log_level="INFO"):
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

    def launch(self, name, docker_config: DockerConfig, mounts, env, network, blocking: bool = True):
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

        for index, mnt in enumerate(mounts):
            volumes.append(V1Volume(
                name=f'mount-{index}',
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                    claim_name=mnt['volume'],
                    read_only=False
                ),
            ))

            volume_mounts.append(V1VolumeMount(
                name=f'mount-{index}',
                mount_path=mnt['dest_path'],
                sub_path=mnt['source_path'],
                read_only=False,
            ))

        if CLASSIFICATION_CONFIGMAP:
            volumes.append(V1Volume(
                name='mount-classification',
                config_map=V1ConfigMapVolumeSource(
                    name=CLASSIFICATION_CONFIGMAP
                ),
            ))

            volume_mounts.append(V1VolumeMount(
                name='mount-classification',
                mount_path='/etc/assemblyline/classification.yml',
                sub_path=CLASSIFICATION_CONFIGMAP_KEY,
                read_only=True,
            ))

        section = 'core'
        if network == 'al_registration':
            section = 'service'
            labels = {
                'app': 'assemblyline',
                'section': section,
                'component': 'update-script',
            }
            labels.update(self.extra_labels)

            metadata = V1ObjectMeta(
                name=name,
                labels=labels
            )

            environment_variables = [V1EnvVar(name=_e.name, value=_e.value) for _e in docker_config.environment]
            environment_variables.extend([V1EnvVar(name=k, value=v) for k, v in env.items()])
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


class ServiceUpdater(CoreBase):
    def __init__(self, redis_persist=None, redis=None, logger=None, datastore=None):
        super().__init__('assemblyline.service.updater', logger=logger, datastore=datastore,
                         redis_persist=redis_persist, redis=redis)

        self.container_update: Hash[dict[str, Any]] = Hash('container-update', self.redis_persist)
        self.latest_service_tags: Hash[dict[str, str]] = Hash('service-tags', self.redis_persist)
        self.service_events = EventSender('changes.services', host=self.redis)

        # Prepare a single threaded scheduler
        self.scheduler = sched.scheduler()

        #
        if 'KUBERNETES_SERVICE_HOST' in os.environ and NAMESPACE:
            extra_labels = {}
            if self.config.core.scaler.additional_labels:
                extra_labels = {k: v for k, v in (_l.split("=") for _l in self.config.core.scaler.additional_labels)}
            self.controller = KubernetesUpdateInterface(prefix='alsvc_', namespace=NAMESPACE,
                                                        priority_class='al-core-priority',
                                                        extra_labels=extra_labels,
                                                        log_level=self.config.logging.log_level)
        else:
            self.controller = DockerUpdateInterface(log_level=self.config.logging.log_level)

    def container_updates(self):
        """Go through the list of services and check what are the latest tags for it"""
        self.scheduler.enter(UPDATE_CHECK_INTERVAL, 0, self.container_updates)
        for service_name, update_data in self.container_update.items().items():
            self.log.info(f"Service {service_name} is being updated to version {update_data['latest_tag']}...")

            # Load authentication params
            username = None
            password = None
            auth = update_data['auth'] or {}
            if auth:
                username = auth.get('username', None)
                password = auth.get('password', None)

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
                    mounts=[],
                    env={
                        "SERVICE_TAG": update_data['latest_tag'],
                        "SERVICE_API_HOST": os.environ.get('SERVICE_API_HOST', "http://al_service_server:5003"),
                        "SERVICE_API_KEY": os.environ.get('SERVICE_API_KEY', 'ThisIsARandomAuthKey...ChangeMe!'),
                        "REGISTER_ONLY": 'true'
                    },
                    network='al_registration',
                    blocking=True
                )

                latest_tag = update_data['latest_tag'].replace('stable', '')

                service_key = f"{service_name}_{latest_tag}"

                if self.datastore.service.get_if_exists(service_key):
                    operations = [(self.datastore.service_delta.UPDATE_SET, 'version', latest_tag)]
                    if self.datastore.service_delta.update(service_name, operations):
                        # Update completed, cleanup
                        self.service_events.send(service_name, {
                            'operation': Operation.Modified,
                            'name': service_name
                        })
                        self.log.info(f"Service {service_name} update successful!")
                    else:
                        self.log.error(f"Service {service_name} has failed to update because it cannot set "
                                       f"{latest_tag} as the new version. Update procedure cancelled...")
                else:
                    self.log.error(f"Service {service_name} has failed to update because resulting "
                                   f"service key ({service_key}) does not exist. Update procedure cancelled...")
            except Exception as e:
                self.log.error(f"Service {service_name} has failed to update. Update procedure cancelled... [{str(e)}]")

            self.container_update.pop(service_name)

        # Clear out any old dead containers
        self.controller.cleanup_stale()

    def container_versions(self):
        """Go through the list of services and check what are the latest tags for it"""
        self.scheduler.enter(CONTAINER_CHECK_INTERVAL, 0, self.container_versions)
        existing_services = set(self.container_update.keys()) | set(self.latest_service_tags.keys())
        discovered_services: list[str] = []

        for service in self.datastore.list_all_services(full=True):
            discovered_services.append(service.name)
            image_name, tag_name, auth = get_latest_tag_for_service(service, self.config, self.log)
            self.latest_service_tags.set(service.name,
                                         {'auth': auth, 'image': image_name, service.update_channel: tag_name})

        # Remove services we have locally or in redis that have been deleted from the database
        for stray_service in existing_services - set(discovered_services):
            self.log.info(f"Service updates disabled for {stray_service}")
            self._service_stage_hash.pop(stray_service)
            self.container_update.pop(stray_service)
            self.latest_service_tags.pop(stray_service)

    def try_run(self):
        """Run the scheduler loop until told to stop."""
        # Do an initial call to the main methods, who will then be registered with the scheduler
        self.container_versions()
        self.container_updates()
        self.heartbeat()

        # Run as long as we need to
        while self.running:
            delay = self.scheduler.run(False)
            if delay:
                time.sleep(min(delay, 0.1))

    def heartbeat(self, timestamp: int = None):
        """Periodically touch a file on disk.

        Since tasks are run serially, the delay between touches will be the maximum of
        HEARTBEAT_INTERVAL and the longest running task.
        """
        if self.config.logging.heartbeat_file:
            self.scheduler.enter(HEARTBEAT_INTERVAL, 0, self.heartbeat)
            super().heartbeat(timestamp)


if __name__ == '__main__':
    ServiceUpdater().serve_forever()
