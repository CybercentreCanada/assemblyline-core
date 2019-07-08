"""
An auto-scaling service specific to Assemblyline.

TODO react to changes in memory/cpu limits
"""

import time
import sched
from pprint import pprint

from al_core.dispatching.dispatcher import SUBMISSION_QUEUE, FILE_QUEUE, service_queue_name
from al_core.metrics.metrics_server import METRICS_QUEUE
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
from . import collection


SERVICE_SYNC_INTERVAL = 30
SCALE_INTERVAL = 5
METRIC_SYNC_INTERVAL = 0.1


class ScalerServer(ServerBase):
    def __init__(self, config=None, datastore=None, redis=None):
        super().__init__('assemblyline.scaler')
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
        self.metrics_queue = CommsQueue(METRICS_QUEUE, self.redis)
        self._metrics_loop = self.get_metrics()

        # TODO select the right controller by examining our environment
        service_controller = DockerController(logger=self.log, label='alsvc',
                                              cpu_overallocation=100, memory_overallocation=1.5)
        core_controller = DockerController(logger=self.log, label='al',
                                           cpu_overallocation=100, memory_overallocation=1)

        self.services = ScalingGroup(service_controller)
        self.core = ScalingGroup(core_controller)

        # Prepare a single threaded scheduler
        self.state = collection.Collection(period=self.config.core.metrics.export_interval)
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
            backlog=100,
            queue=FILE_QUEUE
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
            backlog=100,
            queue=SUBMISSION_QUEUE
        ))


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
                    min_instances=0,   # TODO move to config
                    growth=40,         #      also
                    backlog=100,       #      also
                    max_instances=service.licence_count,
                    container_config=docker_config,
                    queue=service_queue_name(name)
                ))

            # Update RAM, CPU, licence requirements for running services
            if service.enabled and name in self.services.profiles:
                profile = self.services.profiles[name]
                if profile.cpu != service.cpu_cores:
                    self.services.controller.set_cpu_limit(name, service.cpu_cores)
                    profile.cpu = service.cpu_cores

                if profile.ram != service.ram_mb:
                    self.services.controller.set_memory_limit(name, service.ram_mb)
                    profile.ram = service.ram_mb

                if service.licence_count == 0:
                    profile._max_instances = float('inf')
                else:
                    profile._max_instances = service.licence_count

    def update_scaling(self):
        """Check if we need to scale any services up or down."""
        self.scheduler.enter(SCALE_INTERVAL, 0, self.update_scaling)

        self.core.update()
        self.services.update()

    def get_metrics(self):
        """A generator that makes a non blocking check on the metrics queue each time it is called."""
        with self.metrics_queue as sq:
            for message in sq.listen(blocking=False):
                yield message

    def sync_metrics(self):
        """Check if there are any pubsub messages we need."""
        self.scheduler.enter(METRIC_SYNC_INTERVAL, 3, self.sync_metrics)
        for message in self._metrics_loop:
            # We will get none when we have cleared the buffer
            if message is None:
                break

            # Things that don't provide information we need
            if message.get('name', '') in {'expiry', 'ingester_submitter', 'alerter', 'ingester_internals',
                                           'ingester_input'}:
                pass

            elif message.get('type', None) == 'service':
                pass

            elif message.get('type', None) == 'service_timing':
                self.state.update(
                    service=message['name'],
                    host=message['host'],
                    busy_seconds=message['execution.t'],
                    throughput=0
                )

            elif message.get('name', None) == 'dispatcher_submissions':
                self.state.update(
                    service=message['name'],
                    host=message['host'],
                    busy_seconds=message['busy_seconds.t'],
                    throughput=message['submissions_completed']
                )

            elif message.get('name', None) == 'dispatcher_files':
                self.state.update(
                    service=message['name'],
                    host=message['host'],
                    busy_seconds=message['busy_seconds.t'],
                    throughput=message['files_completed']
                )

            else:
                pprint(message)

        export_interval = self.config.core.metrics.export_interval

        # Check the set of services that might be sitting at zero instances, and if it is, we need to
        # manually check if it is offline
        for group in (self.core, self.services):
            for profile_name, profile in group.profiles.items():
                # Pull out statistics from the metrics regularization
                update = self.state.read(profile_name)
                if update:
                    delta = time.time() - profile.last_update
                    profile.update(
                        delta=delta,
                        backlog=NamedQueue(profile.queue, self.redis).length(),
                        **update
                    )

                # Wait until we have missed two heartbeats before we take things into our own hands
                if time.time() - profile.last_update > 2 * export_interval:
                    # Check if we expect no messages, if so pull the queue length ourselves since there is no heartbeat
                    if group.controller.get_target(profile_name) == 0:
                        if profile.queue:
                            self.log.info(f"{profile_name} down, pulling data directly")
                            profile.update(
                                delta=export_interval,
                                instances=0,
                                backlog=NamedQueue(profile.queue, self.redis).length(),
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



    # def get_heartbeat(self):
    #     """A generator that makes a non blocking check on the status queue each time it is called."""
    #     with self.status_queue as sq:
    #         for message in sq.listen(blocking=False):
    #             yield message
    #
    # def sync_heartbeats(self):
    #     """Check if there are any pubsub messages we need."""
    #     export_interval = self.config.core.metrics.export_interval
    #     self.scheduler.enter(METRIC_SYNC_INTERVAL, 3, self.sync_heartbeats)
    #     for message in self._metrics_loop:
    #         # We will get none when we have cleared the buffer
    #         if message is None:
    #             break
    #
    #         # Things that don't need us to pay attention to them
    #         if message.get('msg_type', '') in {'', 'ExpiryHeartbeat', 'ServiceHeartbeat'}:
    #             continue
    #
    #         # Things that might need scaling, but don't have the tooling in place
    #         if message['msg_type'] in {'AlerterHeartbeat', ''}:
    #             continue
    #
    #         # Things that should be ready for scaling but not done right this second
    #         # Both ingest and dispatcher need something telling me which role they have
    #         if message['msg_type'] in {'IngestHeartbeat'}:
    #             continue
    #
    #         if message['msg_type'] == 'DispatcherHeartbeat':
    #             msg = DispatcherMessage(message).msg
    #             if msg.component == 'dispatcher_submissions':
    #                 delta = time.time() - self.core.profiles['dispatcher_submissions'].last_update
    #                 busy_seconds = msg.metrics.busy_seconds * msg.metrics.busy_seconds_count
    #                 self.core.profiles['dispatcher_submissions'].update(
    #                     delta=min(delta, 2 * export_interval),
    #                     instances=msg.instances,
    #                     backlog=msg.queues.files,
    #                     duty_cycle=(busy_seconds/msg.instances)/delta
    #                 )
    #
    #             elif msg.component == 'dispatcher_files':
    #                 delta = time.time() - self.core.profiles['dispatcher_files'].last_update
    #                 busy_seconds = msg.metrics.busy_seconds * msg.metrics.busy_seconds_count
    #                 print(busy_seconds, msg.metrics.busy_seconds, msg.metrics.busy_seconds_count)
    #                 busy_fraction = (busy_seconds / msg.instances) / delta
    #                 print(busy_fraction, busy_seconds, msg.instances, delta)
    #                 self.core.profiles['dispatcher_files'].update(
    #                     delta=min(delta, 2 * export_interval),
    #                     instances=msg.instances,
    #                     backlog=msg.queues.files,
    #                     duty_cycle=busy_fraction,
    #                 )
    #
    #             else:
    #                 pprint(message)
    #             continue
    #
    #         # We have a service message, write it into the right service profile
    #         if message['msg_type'] == 'ServiceTimingHeartbeat':
    #             msg = ServiceTimingMessage(message).msg
    #             if msg.service_name not in self.services.profiles:
    #                 continue
    #             delta = time.time() - self.services.profiles[msg.service_name].last_update
    #             busy_seconds = msg.metrics.execution * msg.metrics.execution_count
    #             print(busy_seconds, msg.metrics.execution, msg.metrics.execution_count)
    #             busy_fraction = (busy_seconds / msg.instances) / delta
    #             print(busy_fraction, busy_seconds, msg.instances, delta)
    #             print('service update', busy_fraction)
    #             self.services.profiles[msg.service_name].update(
    #                 delta=min(delta, 2 * export_interval),
    #                 instances=msg.instances,
    #                 backlog=msg.queue,
    #                 duty_cycle=busy_fraction/60
    #             )
    #             continue
    #
    #         pprint(message)
    #         exit()
    #
    #     # Check the set of services that might be sitting at zero instances, and if it is, we need to
    #     # manually check if it is offline
    #     for group in (self.core, self.services):
    #         for profile_name, profile in group.profiles.items():
    #             # Wait until we have missed two heartbeats before we take things into our own hands
    #             if time.time() - profile.last_update > 2 * export_interval:
    #                 # Check if we expect no messages, if so pull the queue length ourselves since there is no heartbeat
    #                 if group.controller.get_target(profile_name) == 0:
    #                     if profile.queue:
    #                         self.log.info(f"{profile_name} down, pulling data directly")
    #                         profile.update(
    #                             delta=export_interval,
    #                             instances=0,
    #                             backlog=NamedQueue(profile.queue, self.redis).length(),
    #                             duty_cycle=profile.target_duty_cycle
    #                         )
    #
    #                 # In the case that there should actually be instances running, but we haven't gotten
    #                 # any heartbeat messages we might be waiting for a container that can't start properly
    #                 else:
    #                     # TODO do something about it
    #                     pass
    #
    #                     # TODO delete next couple lines, debugging code
    #                     # if time.time() - profile.last_update > 15:
    #                     print(f"its been a bit since we heard from {profile_name}")
    #                     profile.last_update = time.time()

