
class ControllerInterface:
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

    # def get_memory_limit(self, service_name):
    #     """Return memory limit for this service in megabytes."""
    #     raise NotImplementedError()
    #
    # def get_cpu_limit(self, service_name):
    #     """Return processor limit for this service in cpu cores."""
    #     raise NotImplementedError()


# from pprint import pprint
# import os
#
#
#
# class KubeController:
#     def __init__(self, incluster=True):
#         from kubernetes import client, config
#
#         if incluster:
#             config.load_incluster_config()
#         else:
#             config.load_kube_config()
#
#         proxy_url = os.getenv('HTTP_PROXY', None)
#         if proxy_url:
#             conf = client.Configuration()
#             conf.proxy = 'http://' + proxy_url if not proxy_url.startswith('http') else proxy_url
#             client.Configuration.set_default(conf)
#
#         self.api = client.CoreV1Api()
#
#     def read_target_scale(self, name):
#         raise NotImplementedError()
#
#     def set_target_scale(self, name, scale):
#         raise NotImplementedError()
#
#     def list_running(self):
#         print("Listing pods with their IPs:")
#         ret = self.api.list_pod_for_all_namespaces(watch=False)
#         for i in ret.items:
#             print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
#
#
#
# con = KubeController(incluster=False)
# con.list_running()