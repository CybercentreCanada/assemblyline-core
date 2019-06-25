from pprint import pprint
import time
from typing import List

from assemblyline.common.forge import CachedObject
from assemblyline.odm.models.service import Service

from .interface import ControllerInterface


class DockerController(ControllerInterface):
    """A controller for *non* swarm mode docker."""
    def __init__(self, datastore, logger, label='', limit_cpu=True, limit_memory=True):
        # Connect to the host docker port
        import docker
        self.client = docker.from_env()
        self.log = logger
        self.datastore = datastore
        self.global_mounts = []
        self._label = label

        # CPU and memory reserved for the host
        self._reserved_cpu = 0.3
        self._reserved_mem = 500
        self.limit_cpu = limit_cpu
        self.limit_memory = limit_memory
        self._profiles = {}

        # Prefetch some info that shouldn't change while we are running
        self._info = self.client.info()
        # noinspection PyTypeChecker
        # self.services: List[Service] = CachedObject(datastore.list_all_services, kwargs={'full': True})

        # We aren't checking for swarm nodes
        assert not self._info['Swarm']['NodeID']

    def add_profile(self, profile):
        self._profiles[profile.name] = profile

    def _start(self, service_name):
        container_name = self._name_container(service_name)
        prof = self._profiles[service_name]
        cfg = prof.container_config
        self.client.containers.run(
            image=cfg.image,
            name=container_name,
            cpu_period=100000,
            cpu_quota=int(100000*prof.cpu),
            mem_limit=f'{prof.ram}m',
            labels={'al_service': service_name, 'al_label': self._label},
            restart_policy={'Name': 'always'},
            command=cfg.command,
            volumes={row[0]: {'bind': row[1], 'mode': 'ro'} for row in self.global_mounts},
            # TODO This is the right network/URL for our dev compose, should come from config
            network=cfg.network[0],
            environment=[f'{_e.name}={_e.value}' for _e in cfg.environment],
            detach=True,
        )
        # TODO network needs to be adjusted

    def _name_container(self, service_name):
        used_names = []
        for container in self.client.containers.list(all=True):
            used_names.append(container.name)

        used_names = set(used_names)
        index = 0
        while True:
            name = f'{service_name}_{index}'
            if self._label:
                name = self._label + '_' + name
            if name not in used_names:
                return name
            index += 1

    def free_cpu(self):
        if not self.limit_cpu:
            return float('inf')

        cpu = self._info['NCPU'] - self._reserved_cpu
        for container in self.client.containers.list():
            if container.attrs['HostConfig']['CpuPeriod']:
                cpu -= container.attrs['HostConfig']['CpuQuota']/container.attrs['HostConfig']['CpuPeriod']
        self.log.debug(f'Total CPU available {cpu}/{self._info["NCPU"]}')
        return cpu

    def free_memory(self):
        if not self.limit_memory:
            return float('inf')

        mem = self._info['MemTotal']/1024 - self._reserved_mem
        for container in self.client.containers.list():
            mem -= container.attrs['HostConfig']['Memory']/1024
        self.log.debug(f'Total Memory available {mem}/{self._info["MemTotal"]/2**10}')
        return mem

    def get_target(self, service_name):
        running = 0
        for container in self.client.containers.list(filters={'label': f'al_service={service_name}'}):
            if container.status in {'restarting', 'running'}:
                running += 1
            elif container.status in {'created', 'removing', 'paused', 'exited', 'dead'}:
                pass
            else:
                self.log.warning(f"Unknown docker status string: {container.status}")
        # self.log.debug(f"Found {running} in {service_name}")
        return running

    def set_target(self, service_name, target):
        running = self.get_target(service_name)
        self.log.debug(f"New target for {service_name}: {running} -> {target}")
        delta = target - running

        if delta < 0:
            # Kill off delta instances of of the service
            running = [container for container in self.client.containers.list(filters={'label': f'al_service={service_name}'})
                       if container.status in {'restarting', 'running'}]
            running = running[0:-delta]
            for container in running:
                container.kill()

        if delta > 0:
            # Start delta instances of the service
            for _ in range(delta):
                self._start(service_name)

        self.client.containers.prune()
        self.client.volumes.prune()
