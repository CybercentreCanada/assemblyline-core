from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from assemblyline_core.scaler.scaling import ServiceProfile


class ServiceControlError(RuntimeError):
    def __init__(self, message, service_name):
        super().__init__(message)
        self.service_name = service_name


class ControllerInterface:
    def add_profile(self, profile):
        """Tell the controller about a service profile it needs to manage."""
        raise NotImplementedError()

    def free_cpu(self):
        """Number of cores available for reservation."""
        raise NotImplementedError()

    def free_memory(self):
        """Megabytes of RAM that has not been reserved."""
        raise NotImplementedError()

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
