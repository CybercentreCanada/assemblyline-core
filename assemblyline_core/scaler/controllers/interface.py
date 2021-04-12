from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from assemblyline_core.scaler.scaler_server import ServiceProfile


class ServiceControlError(RuntimeError):
    def __init__(self, message, service_name):
        super().__init__(message)
        self.service_name = service_name


class ControllerInterface:
    def add_profile(self, profile, scale=0):
        """Tell the controller about a service profile it needs to manage."""
        raise NotImplementedError()

    def memory_info(self):
        """Return free and total memory in the system."""
        raise NotImplementedError()

    def cpu_info(self):
        """Return free and total memory in the system."""
        raise NotImplementedError()

    def free_cpu(self):
        """Number of cores available for reservation."""
        return self.cpu_info()[0]

    def free_memory(self):
        """Megabytes of RAM that has not been reserved."""
        return self.memory_info()[0]

    def get_target(self, service_name):
        """Get the target for running instances of a service."""
        raise NotImplementedError()

    def set_target(self, service_name, target):
        """Set the target for running instances of a service."""
        raise NotImplementedError()

    def restart(self, service: ServiceProfile):
        raise NotImplementedError()

    def get_running_container_names(self):
        raise NotImplementedError()

    def new_events(self):
        return []

    def start_stateful_container(self, service_name, container_name, spec, labels):
        raise NotImplementedError()

    def stop_containers(self, labels):
        raise NotImplementedError()

    def prepare_network(self, service_name, internet):
        raise NotImplementedError()

    def stop(self):
        pass
