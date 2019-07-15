import os

from al_core.scaler.controllers.interface import ControllerInterface


def parse_memory(string):
    # TODO use a library for parsing this?
    #      I tried a couple, and they weren't compatable with the formats kubernetes uses
    #      if we can't find one, this needs to be improved
    # Maybe we have a number in bytes
    try:
        return float(string)/2**20
    except ValueError:
        pass

    # Try parsing a unit'd number then
    if string.endswith('Ki'):
        bytes = float(string[:-2]) * 2**10
    elif string.endswith('Mi'):
        bytes = float(string[:-2]) * 2**20
    elif string.endswith('Gi'):
        bytes = float(string[:-2]) * 2**30
    else:
        raise ValueError(string)

    return bytes/2**20


class KubernetesController(ControllerInterface):
    def __init__(self):
        from kubernetes import client, config
        # Try loading a kubernetes connection from either the fact that we are running
        # inside of a cluster,
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

        self.api = client.CoreV1Api()
        self.auto_cloud = False  #  TODO draw from config
        self.namespace = 'al'  # TODO draw from config

    def add_profile(self, profile):
        """Tell the controller about a service profile it needs to manage."""
        raise NotImplementedError()

    def free_cpu(self):
        """Number of cores available for reservation."""
        # Try to get the limit from the namespace
        max_cpu = float('inf')
        used = 0
        found = False
        for limit in self.api.list_namespaced_resource_quota(namespace=self.namespace).items:
            # Don't worry about specific quotas, just look for namespace wide ones
            if limit.spec.scope_selector or limit.spec.scopes:
                continue

            found = True  # At least one limit has been found
            if 'limits.cpu' in limit.status.hard:
                max_cpu = min(max_cpu, float(limit.status.hard['limits.cpu']))

            if 'limits.cpu' in limit.status.used:
                used = max(used, float(limit.status.used['limits.cpu']))

        if found:
            return max_cpu - used

        # If the limit isn't set by the user, and we are on a cloud with auto-scaling
        # we don't have a real memory limit
        if self.auto_cloud:
            return float('inf')

        # Try to get the limit by looking at the host list
        cpu = 0
        for node in self.api.list_node().items:
            cpu += float(node.status.allocatable['cpu'])
        return cpu

    def free_memory(self):
        """Megabytes of RAM that has not been reserved."""
        # Try to get the limit from the namespace
        max_ram = float('inf')
        used = 0
        found = False
        for limit in self.api.list_namespaced_resource_quota(namespace=self.namespace).items:
            # Don't worry about specific quotas, just look for namespace wide ones
            if limit.spec.scope_selector or limit.spec.scopes:
                continue

            found = True  # At least one limit has been found
            if 'limits.memory' in limit.status.hard:
                max_ram = min(max_ram, parse_memory(limit.status.hard['limits.memory']))

            if 'limits.memory' in limit.status.used:
                used = max(used, parse_memory(limit.status.used['limits.memory']))

        if found:
            return max_ram - used

        # If the limit isn't set by the user, and we are on a cloud with auto-scaling
        # we don't have a real memory limit
        if self.auto_cloud:
            return float('inf')

        # Read the memory that is free from the node list
        memory = 0
        for node in self.api.list_node().items:
            memory += parse_memory(node.status.allocatable['memory'])
        return memory

    def get_target(self, service_name):
        """Get the target for running instances of a service."""
        raise NotImplementedError()

    def set_target(self, service_name, target):
        """Set the target for running instances of a service."""
        raise NotImplementedError()


ctl = KubernetesController()

print(ctl.free_cpu())
print(ctl.free_memory())
exit()