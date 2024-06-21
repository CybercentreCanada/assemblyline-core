from __future__ import annotations
from typing import TYPE_CHECKING, Optional


if TYPE_CHECKING:
    from assemblyline_core.scaler.scaler_server import ServiceProfile


class ServiceControlError(RuntimeError):
    def __init__(self, message, service_name):
        super().__init__(message)
        self.service_name = service_name


class ContainerEvent:
    def __init__(self, object_name: str, message: str, service_name=None, updater=None) -> None:
        self.object_name = object_name
        self.message = message
        self.service_name = service_name
        self.updater = updater


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

    def free_cpu(self) -> float:
        """Number of cores available for reservation."""
        return self.cpu_info()[0]

    def free_memory(self) -> float:
        """Megabytes of RAM that has not been reserved."""
        return self.memory_info()[0]

    def get_target(self, service_name):
        """Get the target for running instances of a service."""
        raise NotImplementedError()

    def get_targets(self):
        """Get the target for running instances of all services."""
        raise NotImplementedError()

    def set_target(self, service_name, target):
        """Set the target for running instances of a service."""
        raise NotImplementedError()

    def restart(self, service: ServiceProfile):
        raise NotImplementedError()

    def get_running_container_names(self):
        raise NotImplementedError()

    def new_events(self) -> list[ContainerEvent]:
        return []

    def stateful_container_key(self, service_name: str, container_name: str, spec, change_key: str) -> Optional[str]:
        raise NotImplementedError()

    def start_stateful_container(self, service_name: str, container_name: str, spec, labels, change_key):
        raise NotImplementedError()

    def stop_containers(self, labels):
        raise NotImplementedError()

    def prepare_network(self, service_name, internet, dependency_internet):
        raise NotImplementedError()

    def stop(self):
        pass
