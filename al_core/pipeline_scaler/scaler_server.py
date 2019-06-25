import time
import sched
from pprint import pprint

from al_core.dispatching.dispatcher import SUBMISSION_QUEUE, FILE_QUEUE, service_queue_name
from al_core.pipeline_scaler.controllers.interface import ControllerInterface
from assemblyline.common import forge
from assemblyline.odm.messages.dispatcher_heartbeat import DispatcherMessage
from assemblyline.odm.messages.service_timing_heartbeat import ServiceTimingMessage
from assemblyline.odm.models.service import DockerConfig
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.comms import CommsQueue

from al_core.server_base import ServerBase
from al_core.metrics.heartbeat_formatter import STATUS_QUEUE
from assemblyline.remote.datatypes.queues.named import NamedQueue

from .scaling import ScalingGroup, ServiceProfile
from .controllers import DockerController


SERVICE_SYNC_INTERVAL = 30
SCALE_INTERVAL = 5
METRIC_SYNC_INTERVAL = 0.1



class ScalerServer(ServerBase):
    def __init__(self, config=None, datastore=None, redis=None):
        super().__init__('assemblyline.pipeline_scaler')
        # Connect to the assemblyline system
        self.config = config or forge.get_config()
        self.datastore = datastore or forge.get_datastore()
        self.redis = redis or get_client(
            db=self.config.core.redis.nonpersistent.db,
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )
        self.status_queue = CommsQueue(STATUS_QUEUE, self.redis)
        self._metrics_loop = self.get_metrics()

        # TODO select the right controller by examining our environment
        service_controller = DockerController(datastore=self.datastore, logger=self.log, label='alsvc', limit_cpu=False, limit_memory=False)
        core_controller = DockerController(datastore=self.datastore, logger=self.log, label='al', limit_cpu=False, limit_memory=False)

        self.services = ScalingGroup(service_controller)
        self.core = ScalingGroup(core_controller)

        # Prepare a single threaded scheduler
        self.scheduler = sched.scheduler()

        # TODO move everything below here to configuration
        core_controller.global_mounts.append(['/opt/alv4/alv4/dev/core/config/', '/etc/assemblyline/'])
        core_controller.global_mounts.append(['/opt/alv4/', '/opt/alv4/'])

        # Add the core components to the scaler
        self.core.add_service(ServiceProfile(
            name='dispatcher_files',
            cpu=0, ram=0,
            min_instances=1,
            container_config=DockerConfig(dict(
                image='sgaroncse/assemblyline_dev:4.0.5',
                command='python3 /opt/alv4/alv4_core/al_core/dispatching/run_files.py',
                network=['backend'],
            )),
            growth=60,
            backlog=50,
        ))

        self.core.add_service(ServiceProfile(
            name='dispatcher_submissions',
            cpu=0, ram=0,
            min_instances=1,
            container_config=DockerConfig(dict(
                image='sgaroncse/assemblyline_dev:4.0.5',
                command='python3 /opt/alv4/alv4_core/al_core/dispatching/run_submissions.py',
                network=['backend'],
            )),
            growth=60,
            backlog=50,
        ))

        self.direct_monitoring = [
            (self.core, 'dispatcher_submissions', SUBMISSION_QUEUE),
            (self.core, 'dispatcher_files', FILE_QUEUE),
        ]

    def try_run(self):
        # Do an initial call to the main methods, who will then be registered with the scheduler
        self.sync_services()
        self.sync_metrics()
        self.update_scaling()

        # Run as long as we need to
        while self.running:
            delay = self.scheduler.run(False)
            time.sleep(min(delay, 0.02))

    def sync_services(self):
        self.scheduler.enter(SERVICE_SYNC_INTERVAL, 0, self.sync_services)
        # Get all the service data
        for service in self.datastore.list_all_services(full=True):
            # name = 'alsvc_' + service.name.lower()
            name = service.name

            # Stop any running disabled services
            if not service.enabled and name in self.services.profiles:
                self.log.info(f'Removing {service.name} from scaling')
                del self.services.profiles[name]
                self.services.controller.set_target(name, 0)
                self.direct_monitoring = [
                    row for row in self.direct_monitoring
                    if row[1] != name
                ]

            # Check that all enabled services are enabled
            if service.enabled and name not in self.services.profiles:
                self.log.info(f'Adding {service.name} to scaling')
                docker_config = service.docker_config
                if not docker_config.network:
                    docker_config.network.append('svc')  # TODO also move to config
                if not docker_config.environment:
                    docker_config.environment.append({'name': 'SERVICE_API_HOST', 'value': 'http://al_service_server:5003'})
                self.services.add_service(ServiceProfile(
                    name=name,
                    ram=service.ram_mb,
                    cpu=service.cpu_cores,
                    min_instances=0,  # TODO move to config
                    growth=20,        #      also
                    backlog=10,       #      also
                    max_instances=service.licence_count,
                    container_config=docker_config
                ))
                self.direct_monitoring.append((self.services, name, service_queue_name(name)))

            # Update RAM, CPU, licence requirements
            profile = self.services.profiles[name]
            if profile.cpu != service.cpu_cores:
                self.services.controller.set_cpu_limit(name, service.cpu_cores)
                profile.cpu = service.cpu_cores

            if profile.ram != service.ram_mb:
                self.services.controller.set_memory_limit(name, service.ram_mb)
                profile.ram = service.ram_mb

            profile.max_instances = service.licence_count

    def get_metrics(self):
        with self.status_queue as sq:
            for message in sq.listen(blocking=False):
                yield message

    def sync_metrics(self):
        """Check if there are any pubsub messages we need."""
        export_interval = self.config.core.metrics.export_interval
        self.scheduler.enter(METRIC_SYNC_INTERVAL, 3, self.sync_metrics)
        for message in self._metrics_loop:
            if message is None:
                break

            # Things that don't need us to pay attention to them
            if message.get('msg_type', '') in {'', 'ExpiryHeartbeat', 'ServiceHeartbeat'}:
                continue

            # Things that might need scaling, but don't have the tooling in place
            if message['msg_type'] in {'AlerterHeartbeat', ''}:
                continue

            # Things that should be ready for scaling but not done right this second
            # Both ingest and dispatcher need something telling me which role they have
            if message['msg_type'] in {'IngestHeartbeat'}:
                continue

            if message['msg_type'] == 'DispatcherHeartbeat':
                msg = DispatcherMessage(message).msg
                if msg.component == 'dispatcher_submissions':
                    delta = time.time() - self.core.profiles['dispatcher_submissions'].last_update
                    busy_seconds = msg.metrics.busy_seconds * msg.metrics.busy_seconds_count
                    self.core.profiles['dispatcher_submissions'].update(
                        delta=min(delta, 2 * export_interval),
                        instances=msg.instances,
                        backlog=msg.queues.files,
                        duty_cycle=(busy_seconds/msg.instances)/delta
                    )

                elif msg.component == 'dispatcher_files':
                    delta = time.time() - self.core.profiles['dispatcher_files'].last_update
                    busy_seconds = msg.metrics.busy_seconds * msg.metrics.busy_seconds_count
                    self.core.profiles['dispatcher_files'].update(
                        delta=min(delta, 2 * export_interval),
                        instances=msg.instances,
                        backlog=msg.queues.files,
                        duty_cycle=(busy_seconds/msg.instances)/delta
                    )

                else:
                    pprint(message)
                continue

            # We have a service message, write it into the right service profile
            if message['msg_type'] == 'ServiceTimingHeartbeat':
                msg = ServiceTimingMessage(message).msg
                if msg.service_name not in self.services.profiles:
                    continue
                delta = time.time() - self.services.profiles[msg.service_name].last_update
                busy_seconds = msg.metrics.execution * msg.metrics.execution_count
                self.services.profiles[msg.service_name].update(
                    delta=min(delta, 2 * export_interval),
                    instances=msg.instances,
                    backlog=msg.queue,
                    duty_cycle=(busy_seconds / msg.instances) / delta
                )
                continue

            pprint(message)
            exit()

        # Check the set of services that might be sitting at zero instances, and if it is, we need to
        # manually check if it is offline
        for group, profile_name, queue_name in self.direct_monitoring:
            # Wait until we have missed two heartbeats before we take things into our own hands
            profile = group.profiles[profile_name]
            if time.time() - profile.last_update > 2 * export_interval:
                # Check if we expect no messages, if so pull the queue length ourselves since there is no heartbeat
                if group.controller.get_target(profile_name) == 0:
                    self.log.info(f"{profile_name} down, pulling data directly")
                    profile.update(
                        delta=export_interval,
                        instances=0,
                        backlog=NamedQueue(queue_name, self.redis).length(),
                        duty_cycle=profile.target_duty_cycle
                    )

                # In the case that there should actually be instances running, but we haven't gotten
                # any heartbeat messages we might be waiting for a container that can't start properly
                else:
                    # TODO do something about it
                    pass

                    # TODO delete next couple lines, debugging code
                    # if time.time() - profile.last_update > 15:
                    print(f"its been a bit since we heard from {profile_name}")
                    profile.last_update = time.time()


    def update_scaling(self):
        """Check if we need to scale any services up or down."""
        self.scheduler.enter(SCALE_INTERVAL, 0, self.update_scaling)
        self.core.update()
        self.services.update()
