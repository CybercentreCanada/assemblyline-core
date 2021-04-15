"""
A process that manages tracking and running update commands for the AL services.

TODO:
    - docker build updates
    - If the service update interval changes in datastore move the next update time

"""
from collections import defaultdict

import os
import json
import uuid
import random
import sched
import shutil
import string
import tempfile
import time
from contextlib import contextmanager
from threading import Thread
from typing import Dict

import docker
import yaml

from assemblyline.common import isotime
from assemblyline.remote.datatypes.lock import Lock
from kubernetes.client import V1Job, V1ObjectMeta, V1JobSpec, V1PodTemplateSpec, V1PodSpec, V1Volume, \
    V1PersistentVolumeClaimVolumeSource, V1VolumeMount, V1EnvVar, V1Container, V1ResourceRequirements, \
    V1ConfigMapVolumeSource, V1Secret, V1LocalObjectReference
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from passlib.hash import bcrypt

from assemblyline.common.isotime import now_as_iso
from assemblyline.common.security import get_random_password, get_password_hash
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.models.service import DockerConfig
from assemblyline.odm.models.user import User
from assemblyline.odm.models.user_settings import UserSettings
from assemblyline.remote.datatypes.hash import Hash
from assemblyline_core.scaler.controllers.kubernetes_ctl import create_docker_auth_config
from assemblyline_core.server_base import CoreBase, ServiceStage
from assemblyline_core.updater.helper import get_latest_tag_for_service

SERVICE_SYNC_INTERVAL = 30  # How many seconds between checking for new services, or changes in service status
UPDATE_CHECK_INTERVAL = 60  # How many seconds per check for outstanding updates
CONTAINER_CHECK_INTERVAL = 60 * 5  # How many seconds to wait for checking for new service versions
API_TIMEOUT = 90
HEARTBEAT_INTERVAL = 5
UPDATE_STAGES = [ServiceStage.Update, ServiceStage.Running]

# Where to find the update directory inside this container.
FILE_UPDATE_DIRECTORY = os.environ.get('FILE_UPDATE_DIRECTORY', None)
# How to identify the update volume as a whole, in a way that the underlying container system recognizes.
FILE_UPDATE_VOLUME = os.environ.get('FILE_UPDATE_VOLUME', FILE_UPDATE_DIRECTORY)

# How many past updates to keep for file based updates
UPDATE_FOLDER_LIMIT = 5
NAMESPACE = os.getenv('NAMESPACE', None)
UI_SERVER = os.getenv('UI_SERVER', 'https://nginx')
INHERITED_VARIABLES = ['HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY', 'http_proxy', 'https_proxy', 'no_proxy']
CLASSIFICATION_HOST_PATH = os.getenv('CLASSIFICATION_HOST_PATH', None)
CLASSIFICATION_CONFIGMAP = os.getenv('CLASSIFICATION_CONFIGMAP', None)
CLASSIFICATION_CONFIGMAP_KEY = os.getenv('CLASSIFICATION_CONFIGMAP_KEY', 'classification.yml')


@contextmanager
def temporary_api_key(ds: AssemblylineDatastore, user_name: str, permissions=('R', 'W')):
    """Creates a context where a temporary API key is available."""
    with Lock(f'user-{user_name}', timeout=10):
        name = ''.join(random.choices(string.ascii_lowercase, k=20))
        random_pass = get_random_password(length=48)
        user = ds.user.get(user_name)
        user.apikeys[name] = {
            "password": bcrypt.hash(random_pass),
            "acl": permissions
        }
        ds.user.save(user_name, user)

    try:
        yield f"{name}:{random_pass}"
    finally:
        with Lock(f'user-{user_name}', timeout=10):
            user = ds.user.get(user_name)
            user.apikeys.pop(name)
            ds.user.save(user_name, user)


def chmod(directory, mask):
    try:
        os.chmod(directory, mask)
    except PermissionError as error:
        # If we are using an azure file share, we may not be able to change the permissions
        # on files, everything might be set to a pre defined permission level by the file share
        if "Operation not permitted" in str(error):
            return
        raise


class DockerUpdateInterface:
    """Wrap docker interface for the commands used in the update process.

    Volumes used for file updating on the docker interface are simply a host directory that gets
    mounted at different depths and locations in each container.

    FILE_UPDATE_VOLUME gives us the path of the directory on the host, so that we can mount
    it properly on new containers we launch. FILE_UPDATE_DIRECTORY gives us the path
    that it is mounted at in the update manager container.
    """
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
                name=f'mount-classification',
                config_map=V1ConfigMapVolumeSource(
                    name=CLASSIFICATION_CONFIGMAP
                ),
            ))

            volume_mounts.append(V1VolumeMount(
                name=f'mount-classification',
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
            while not (status.failed or status.succeeded):
                time.sleep(3)
                status = self.batch_api.read_namespaced_job(namespace=self.namespace, name=name,
                                                            _request_timeout=API_TIMEOUT).status

            try:
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

        if not FILE_UPDATE_DIRECTORY:
            raise RuntimeError("The updater process must be run within the orchestration environment, "
                               "the update volume must be mounted, and the path to the volume must be "
                               "set in the environment variable FILE_UPDATE_DIRECTORY. Setting "
                               "FILE_UPDATE_DIRECTORY directly may be done for testing.")

        # The directory where we want working temporary directories to be created.
        # Building our temporary directories in the persistent update volume may
        # have some performance down sides, but may help us run into fewer docker FS overlay
        # cleanup issues. Try to flush it out every time we start. This service should
        # be a singleton anyway.
        self.temporary_directory = os.path.join(FILE_UPDATE_DIRECTORY, '.tmp')
        shutil.rmtree(self.temporary_directory, ignore_errors=True)
        os.makedirs(self.temporary_directory)

        self.container_update = Hash('container-update', self.redis_persist)
        self.services = Hash('service-updates', self.redis_persist)
        self.latest_service_tags = Hash('service-tags', self.redis_persist)
        self.running_updates: Dict[str, Thread] = {}

        # Prepare a single threaded scheduler
        self.scheduler = sched.scheduler()

        #
        if 'KUBERNETES_SERVICE_HOST' in os.environ and NAMESPACE:
            extra_labels = {}
            if self.config.core.scaler.additional_labels:
                extra_labels = {k: v for k, v in (l.split("=") for l in self.config.core.scaler.additional_labels)}
            self.controller = KubernetesUpdateInterface(prefix='alsvc_', namespace=NAMESPACE,
                                                        priority_class='al-core-priority',
                                                        extra_labels=extra_labels,
                                                        log_level=self.config.logging.log_level)
        else:
            self.controller = DockerUpdateInterface(log_level=self.config.logging.log_level)

    def sync_services(self):
        """Download the service list and make sure our settings are up to date"""
        self.scheduler.enter(SERVICE_SYNC_INTERVAL, 0, self.sync_services)
        existing_services = (set(self.services.keys()) |
                             set(self.container_update.keys()) |
                             set(self.latest_service_tags.keys()))
        discovered_services = []

        # Get all the service data
        for service in self.datastore.list_all_services(full=True):
            discovered_services.append(service.name)

            # Ensure that any disabled services are not being updated
            if not service.enabled and self.services.exists(service.name):
                self.log.info(f"Service updates disabled for {service.name}")
                self.services.pop(service.name)

            if not service.enabled:
                continue

            # Ensure that any enabled services with an update config are being updated
            stage = self.get_service_stage(service.name)
            record = self.services.get(service.name)

            if stage in UPDATE_STAGES and service.update_config:
                # Stringify and hash the the current update configuration
                config_hash = hash(json.dumps(service.update_config.as_primitives()))

                # If we can update, but there is no record, create one
                if not record:
                    self.log.info(f"Service updates enabled for {service.name}")
                    self.services.add(
                        service.name,
                        dict(
                            next_update=now_as_iso(),
                            previous_update=now_as_iso(-10**10),
                            config_hash=config_hash,
                            sha256=None,
                        )
                    )
                else:
                    # If there is a record, check that its configuration hash is still good
                    # If an update is in progress, it may overwrite this, but we will just come back
                    # and reapply this again in the iteration after that
                    if record.get('config_hash', None) != config_hash:
                        record['next_update'] = now_as_iso()
                        record['config_hash'] = config_hash
                        self.services.set(service.name, record)

            if stage == ServiceStage.Update:
                if (record and record.get('sha256', None) is not None) or not service.update_config:
                    self._service_stage_hash.set(service.name, ServiceStage.Running)

        # Remove services we have locally or in redis that have been deleted from the database
        for stray_service in existing_services - set(discovered_services):
            self.log.info(f"Service updates disabled for {stray_service}")
            self.services.pop(stray_service)
            self._service_stage_hash.pop(stray_service)
            self.container_update.pop(stray_service)
            self.latest_service_tags.pop(stray_service)

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

        for service in self.datastore.list_all_services(full=True):
            if not service.enabled:
                continue

            image_name, tag_name, auth = get_latest_tag_for_service(service, self.config, self.log)

            self.latest_service_tags.set(service.name,
                                         {'auth': auth, 'image': image_name, service.update_channel: tag_name})

    def try_run(self):
        """Run the scheduler loop until told to stop."""
        # Do an initial call to the main methods, who will then be registered with the scheduler
        self.sync_services()
        self.update_services()
        self.container_versions()
        self.container_updates()
        self.heartbeat()

        # Run as long as we need to
        while self.running:
            delay = self.scheduler.run(False)
            time.sleep(min(delay, 0.1))

    def heartbeat(self):
        """Periodically touch a file on disk.

        Since tasks are run serially, the delay between touches will be the maximum of
        HEARTBEAT_INTERVAL and the longest running task.
        """
        if self.config.logging.heartbeat_file:
            self.scheduler.enter(HEARTBEAT_INTERVAL, 0, self.heartbeat)
            super().heartbeat()

    def update_services(self):
        """Check if we need to update any services.

        Spin off a thread to actually perform any updates. Don't allow multiple threads per service.
        """
        self.scheduler.enter(UPDATE_CHECK_INTERVAL, 0, self.update_services)

        # Check for finished update threads
        self.running_updates = {name: thread for name, thread in self.running_updates.items() if thread.is_alive()}

        # Check if its time to try to update the service
        for service_name, data in self.services.items().items():
            if data['next_update'] <= now_as_iso() and service_name not in self.running_updates:
                self.log.info(f"Time to update {service_name}")
                self.running_updates[service_name] = Thread(
                    target=self.run_update,
                    kwargs=dict(service_name=service_name)
                )
                self.running_updates[service_name].start()

    def run_update(self, service_name):
        """Common setup and tear down for all update types."""
        # noinspection PyBroadException
        try:
            # Check for new update with service specified update method
            service = self.datastore.get_service_with_delta(service_name)
            update_method = service.update_config.method
            update_data = self.services.get(service_name)
            update_hash = None

            try:
                # Actually run the update method
                if update_method == 'run':
                    update_hash = self.do_file_update(
                        service=service,
                        previous_hash=update_data['sha256'],
                        previous_update=update_data['previous_update']
                    )
                elif update_method == 'build':
                    update_hash = self.do_build_update()

                # If we have performed an update, write that data
                if update_hash is not None and update_hash != update_data['sha256']:
                    update_data['sha256'] = update_hash
                    update_data['previous_update'] = now_as_iso()
                else:
                    update_hash = None

            finally:
                # Update the next service update check time, don't update the config_hash,
                # as we don't want to disrupt being re-run if our config has changed during this run
                update_data['next_update'] = now_as_iso(service.update_config.update_interval_seconds)
                self.services.set(service_name, update_data)

            if update_hash:
                self.log.info(f"New update applied for {service_name}. Restarting service.")
                self.controller.restart(service_name=service_name)

        except BaseException:
            self.log.exception("An error occurred while running an update for: " + service_name)

    def do_build_update(self):
        """Update a service by building a new container to run."""
        raise NotImplementedError()

    def do_file_update(self, service, previous_hash, previous_update):
        """Update a service by running a container to get new files."""
        temp_directory = tempfile.mkdtemp(dir=self.temporary_directory)
        chmod(temp_directory, 0o777)
        input_directory = os.path.join(temp_directory, 'input_directory')
        output_directory = os.path.join(temp_directory, 'output_directory')
        service_dir = os.path.join(FILE_UPDATE_DIRECTORY, service.name)
        image_variables = defaultdict(str)
        image_variables.update(self.config.services.image_variables)

        try:
            # Use chmod directly to avoid effects of umask
            os.makedirs(input_directory)
            chmod(input_directory, 0o755)
            os.makedirs(output_directory)
            chmod(output_directory, 0o777)

            username = self.ensure_service_account()

            with temporary_api_key(self.datastore, username) as api_key:

                # Write out the parameters we want to pass to the update container
                with open(os.path.join(input_directory, 'config.yaml'), 'w') as fh:
                    yaml.safe_dump({
                        'previous_update': previous_update,
                        'previous_hash': previous_hash,
                        'sources': [x.as_primitives() for x in service.update_config.sources],
                        'api_user': username,
                        'api_key': api_key,
                        'ui_server': UI_SERVER
                    }, fh)

                # Run the update container
                run_options = service.update_config.run_options
                run_options.image = string.Template(run_options.image).safe_substitute(image_variables)
                self.controller.launch(
                    name=service.name,
                    docker_config=run_options,
                    mounts=[
                        {
                            'volume': FILE_UPDATE_VOLUME,
                            'source_path': os.path.relpath(temp_directory, start=FILE_UPDATE_DIRECTORY),
                            'dest_path': '/mount/'
                        },
                    ],
                    env={
                        'UPDATE_CONFIGURATION_PATH': '/mount/input_directory/config.yaml',
                        'UPDATE_OUTPUT_PATH': '/mount/output_directory/'
                    },
                    network=f'service-net-{service.name}',
                    blocking=True,
                )

                # Read out the results from the output container
                results_meta_file = os.path.join(output_directory, 'response.yaml')

                if not os.path.exists(results_meta_file) or not os.path.isfile(results_meta_file):
                    self.log.warning(f"Update produced no output for {service.name}")
                    return None

                with open(results_meta_file) as rf:
                    results_meta = yaml.safe_load(rf)
                update_hash = results_meta.get('hash', None)

                # Erase the results meta file
                os.unlink(results_meta_file)

                # Get a timestamp for now, and switch it to basic format representation of time
                # Still valid iso 8601, and : is sometimes a restricted character
                timestamp = now_as_iso().replace(":", "")

                # FILE_UPDATE_DIRECTORY/{service_name} is the directory mounted to the service,
                # the service sees multiple directories in that directory, each with a timestamp
                destination_dir = os.path.join(service_dir, service.name + '_' + timestamp)
                shutil.move(output_directory, destination_dir)

                # Remove older update files, due to the naming scheme, older ones will sort first lexically
                existing_folders = []
                for folder_name in os.listdir(service_dir):
                    folder_path = os.path.join(service_dir, folder_name)
                    if os.path.isdir(folder_path) and folder_name.startswith(service.name):
                        existing_folders.append(folder_name)
                existing_folders.sort()

                self.log.info(f'There are {len(existing_folders)} update folders for {service.name} in cache.')
                if len(existing_folders) > UPDATE_FOLDER_LIMIT:
                    extra_count = len(existing_folders) - UPDATE_FOLDER_LIMIT
                    self.log.info(f'We will only keep {UPDATE_FOLDER_LIMIT} updates, deleting {extra_count}.')
                    for extra_folder in existing_folders[:extra_count]:
                        # noinspection PyBroadException
                        try:
                            shutil.rmtree(os.path.join(service_dir, extra_folder))
                        except Exception:
                            self.log.exception('Failed to delete update folder')

                return update_hash
        finally:
            # If the working directory is still there for any reason erase it
            shutil.rmtree(temp_directory, ignore_errors=True)

    def ensure_service_account(self):
        """Check that the update service account exists, if it doesn't, create it."""
        uname = 'update_service_account'

        if self.datastore.user.get_if_exists(uname):
            return uname

        user_data = User({
            "agrees_with_tos": "NOW",
            "classification": "RESTRICTED",
            "name": "Update Account",
            "password": get_password_hash(''.join(random.choices(string.ascii_letters, k=20))),
            "uname": uname,
            "type": ["signature_importer"]
        })
        self.datastore.user.save(uname, user_data)
        self.datastore.user_settings.save(uname, UserSettings())
        return uname


if __name__ == '__main__':
    ServiceUpdater().serve_forever()
