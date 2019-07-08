"""
The objective of this module is to use the information provided by the collection module
to determine what changes to the state of the system might be needed, and make the appropriate
calls to the controller objects.
"""
import time
import math
import logging
from typing import List
from assemblyline.odm.models.service import DockerConfig


logger = logging.getLogger('assemblyline.scaler')


class ServiceProfile:
    """Describe how to scale/start/stop a service."""
    def __init__(self, name, ram, cpu, container_config: DockerConfig, min_instances=0, max_instances=None,
                 growth=600, shrink=None, backlog=500, queue=None):
        """
        :param name: Name of the service to manage
        :param ram: Memory limit in MB
        :param cpu: CPU limit in cores
        :param container_config: Instructions on how to start this service
        :param min_instances: The minimum number of copies of this service keep running
        :param max_instances: The maximum number of copies permitted to be running
        :param growth: Delay before growing a service, unit-less, approximately seconds
        :param shrink: Delay before shrinking a service, unit-less, approximately seconds, defaults to -growth
        :param backlog: How long a queue backlog should be before it takes `growth` seconds to grow.
        :param queue: Queue name for monitoring
        """
        self.name = name
        self.ram = ram
        self.cpu = cpu
        self.queue = queue
        self.container_config = container_config
        self.target_duty_cycle = 0.9

        # How many instances we want, and can have
        self.min_instances = self._min_instances = max(0, int(min_instances))
        self._max_instances = max(0, int(max_instances)) if max_instances else float('inf')
        self.desired_instances = None
        self.running_instances = None

        # Information tracking when we want to grow/shrink
        self.pressure = 0
        self.growth_threshold = abs(float(growth))
        self.shrink_threshold = -self.growth_threshold if shrink is None else -abs(float(shrink))
        self.leak_rate = 0.1

        # How long does a backlog need to be before we are concerned
        self.backlog = int(backlog)
        self.last_update = 0

    @property
    def max_instances(self):
        # Adjust the max_instances based on the number that is already running
        # this keeps the scaler from running way ahead with its demands when resource caps are reached
        return min(self._max_instances, self.running_instances + 2)

    def update(self, delta, instances, backlog, duty_cycle):
        self.last_update = time.time()
        self.running_instances = instances

        # Adjust min instances based on queue (if something has min_instances == 0, bump it up to 1 when
        # there is anything in the queue) this should have no effect for min_instances > 0
        self.min_instances = max(self._min_instances, int(bool(backlog)))
        self.desired_instances = max(self.min_instances, min(self.max_instances, self.desired_instances))
        # print(self.name, self.pressure)

        # Should we scale up because of backlog
        self.pressure += delta * math.sqrt(backlog/self.backlog)
        # print('queue', delta * math.sqrt(backlog / self.backlog), delta, backlog, self.backlog)

        # Should we scale down due to duty cycle? (are some of the workers idle)
        self.pressure -= delta * (self.target_duty_cycle - duty_cycle)
        # print('duty', -delta * (self.target_duty_cycle - duty_cycle), delta, self.target_duty_cycle, duty_cycle)

        # Should we scale up/down because the input rate vs our expected rate
        # expected_max_throughput = data.per_unit_throughput / data.duty_cycle * self.desired_instances
        # rate_pressure = (self.input_rate - self.max_expected_throughput)

        # Apply the friction, tendency to do nothing, tendency of the 'what to do' bar
        # to move to nothing over time when there is no strong up or down pressure
        leak = min(self.leak_rate * delta, abs(self.pressure))
        # print('leak', leak)
        self.pressure = math.copysign(abs(self.pressure) - leak, self.pressure)

        # When we are already at the minimum number of instances, don't let negative values build up
        # otherwise this can cause irregularities in scaling response around the min_instances
        if self.desired_instances == self.min_instances:
            self.pressure = max(0.0, self.pressure)

        if self.pressure >= self.growth_threshold:
            if self.pressure >= 2*self.growth_threshold:
                logger.warning(f"Unexpectedly fast growth pressure on {self.name}")
            self.desired_instances = min(self.max_instances, self.desired_instances + 1)
            self.pressure = 0

        if self.pressure <= self.shrink_threshold:
            if self.pressure <= 2*self.shrink_threshold:
                logger.warning(f"Unexpectedly fast shrink pressure on {self.name}")
            self.desired_instances = max(self.min_instances, self.desired_instances - 1)
            self.pressure = 0


class ScalingGroup:
    """A controller that oversees a collection of scaled services that share resources."""

    def __init__(self, controller):
        self.controller = controller
        self.profiles = {}

    def add_service(self, profile: ServiceProfile):
        profile.desired_instances = max(self.controller.get_target(profile.name), profile.min_instances)
        profile.running_instances = profile.desired_instances
        logger.debug(f'Starting service {profile.name} with a target of {profile.desired_instances}')
        profile.last_update = time.time()
        self.profiles[profile.name] = profile
        self.controller.add_profile(profile)

    def update(self):
        # Figure out what services are expected to be running and how many
        profiles: List[ServiceProfile] = list(self.profiles.values())
        targets = {_p.name: self.controller.get_target(_p.name) for _p in profiles}

        for name, profile in self.profiles.items():
            logger.debug(f'{name}')
            logger.debug(f'Instances \t{profile.min_instances} < {profile.desired_instances} | {targets[name]} < {profile.max_instances}')
            logger.debug(f'Pressure \t{profile.shrink_threshold} < {profile.pressure} < {profile.growth_threshold}')

        #
        #   1.  Any processes that want to release resources can always be approved first
        #
        for name, profile in self.profiles.items():
            if targets[name] > profile.desired_instances:
                logger.debug(f"{name} wants less resources changing allocation {targets[name]} -> {profile.desired_instances}")
                self.controller.set_target(name, profile.desired_instances)
                targets[name] = profile.desired_instances

        #
        #   2.  Any processes that aren't reaching their min_instances target must be given
        #       more resources before anyone else is considered.
        #
        for name, profile in self.profiles.items():
            if targets[name] < profile.min_instances:
                logger.debug(f"{name} isn't meeting minimum allocation {targets[name]} -> {profile.min_instances}")
                self.controller.set_target(name, profile.min_instances)
                targets[name] = profile.min_instances

        #
        #   3.  Try to estimate available resources, and based on some metric grant the
        #       resources to each service that wants them. While this free memory
        #       pool might be spread across many nodes, we are going to treat it like
        #       it is one big one, and let the orchestration layer sort out the details.
        #
        free_cpu = self.controller.free_cpu()
        free_memory = self.controller.free_memory()

        #
        def trim(prof):
            prof = [_p for _p in prof if _p.desired_instances > targets[_p.name]]
            drop = [_p for _p in prof if _p.cpu > free_cpu or _p.ram > free_memory]
            if drop:
                drop = {_p.name: (_p.cpu, _p.ram) for _p in drop}
                logger.debug(f"Can't make more because not enough resources {drop}")
            prof = [_p for _p in prof if _p.cpu <= free_cpu and _p.ram <= free_memory]
            return prof
        profiles = trim(profiles)

        while profiles:
            # TODO do we need to add balancing metrics other than 'least running' for this? probably
            if True:
                profiles.sort(key=lambda _p: self.controller.get_target(_p.name))

            # Add one for the profile at the bottom
            free_memory -= profiles[0].ram
            free_cpu -= profiles[0].cpu
            targets[profiles[0].name] += 1

            # profiles = [_p for _p in profiles if _p.desired_instances > targets[_p.name]]
            # profiles = [_p for _p in profiles if _p.cpu < free_cpu and _p.ram < free_memory]
            profiles = trim(profiles)

        # Apply those adjustments we have made back to the controller
        for name, value in targets.items():
            if value != self.controller.get_target(name):
                self.controller.set_target(name, value)
