from __future__ import annotations

import functools
import os
import threading
import time
from typing import Optional, TYPE_CHECKING

import urllib3
from assemblyline.odm.models.config import Selector
from kubernetes.client import CustomObjectsApi

from assemblyline_core.scaler.controllers.interface import ControllerInterface
from assemblyline_core.scaler.controllers.kubernetes import (KubernetesMethods, TypelessWatch, parse_cpu,
                                                             parse_memory, selector_to_list_filters)

if TYPE_CHECKING:
    from assemblyline_core.scaler_hpa.scaler_server import ServiceProfile


# RESERVE_MEMORY_PER_NODE = os.environ.get('RESERVE_MEMORY_PER_NODE')

API_TIMEOUT = 90
WATCH_TIMEOUT = 10 * 60
WATCH_API_TIMEOUT = WATCH_TIMEOUT + 10
CHANGE_KEY_NAME = 'al_change_key'
CONTAINER_RESTART_THRESHOLD = int(os.environ.get('CONTAINER_RESTART_THRESHOLD', 1))


class NodeState:
    def __init__(self, cpu, ram):
        self.cpu = cpu
        self.ram = ram
        self.cpu_utilization = 0.0
        self.ram_utilization = 0.0


# def get_resources(container) -> Tuple[float, float]:
#     requests = container['resources'].get('requests', {})
#     limits = container['resources'].get('limits', {})

#     cpu_value = requests.get('cpu', limits.get('cpu', None))
#     if cpu_value is not None:
#         cpu_value = parse_cpu(cpu_value)

#     memory_value = requests.get('memory', limits.get('memory', None))
#     if memory_value is not None:
#         memory_value = parse_memory(memory_value)

#     return cpu_value, memory_value


class KubernetesController(KubernetesMethods, ControllerInterface):
    def __init__(self, logger, namespace: str, prefix: str, priority: str, dependency_priority: str,
                 cpu_reservation: float, cpu_slack: float, linux_node_selector: Selector, labels=None,
                 log_level="INFO", core_env=None, cluster_pod_list=True, enable_pod_security=False,
                 default_service_tolerations=[], priv_labels=None):
        super().__init__(logger, namespace, prefix, core_env, enable_pod_security, log_level, linux_node_selector,
                         cpu_reservation, cpu_slack, labels, priv_labels, priority, dependency_priority,
                         default_service_tolerations)

        self.running: bool = True
        self.cluster_pod_list = cluster_pod_list

        self._quota_cpu_limit: Optional[float] = None
        self._quota_cpu_used: Optional[float] = None
        self._quota_mem_limit: Optional[float] = None
        self._quota_mem_used: Optional[float] = None
        quota_background = threading.Thread(target=self._loop_forever(self._monitor_quotas), daemon=True)
        quota_background.start()

        self.node_count = 0
        self.ready_nodes: dict[str, NodeState] = {}
        self._node_pool_max_ram: float = 0
        self._node_pool_max_cpu: float = 0
        node_background = threading.Thread(target=self._loop_forever(self._monitor_node_pool), daemon=True)
        node_background.start()
        node_metrics_background = threading.Thread(target=self._loop_forever(self._monitor_node_metrics), daemon=True)
        node_metrics_background.start()

        # hpa_background = threading.Thread(target=self._loop_forever(self._monitor_hpas), daemon=True)
        # hpa_background.start()

        deployment_background = threading.Thread(target=self._loop_forever(self._monitor_deployments), daemon=True)
        deployment_background.start()

        # Get the deployment of this process. Use that information to fill out the secret info
        deployment = self.apps_api.read_namespaced_deployment(name='scaler', namespace=self.namespace)
        for env_name in list(self.core_env.keys()):
            for container in deployment.spec.template.spec.containers:
                for env_def in container.env:
                    if env_def.name == env_name:
                        self.core_secret_env.append(env_def)
                        self.core_env.pop(env_name)

    def stop(self):
        self.running = False

    def add_profile(self, profile):
        """Tell the controller about a service profile it needs to manage."""
        self._external_profiles[profile.name] = profile
        self.restart(profile)

    def _loop_forever(self, function):
        @functools.wraps(function)
        def _function():
            while self.running:
                # noinspection PyBroadException
                try:
                    function()

                except (urllib3.exceptions.ProtocolError, urllib3.exceptions.ReadTimeoutError):
                    # Protocol errors are a product of api connections timing out, just retry silently.
                    pass

                except Exception:
                    self.logger.exception(f"Error in {function.__name__}")
        return _function

    # def _monitor_hpas(self):
    #     watch = TypelessWatch()
    #     label_selector = ','.join(f'{_n}={_v}' for _n, _v in self._labels.items() if _n != 'privilege')

    #     for event in watch.stream(func=self.scale_api.list_namespaced_horizontal_pod_autoscaler,
    #                               namespace=self.namespace, timeout_seconds=WATCH_TIMEOUT,
    #                               label_selector=label_selector, _request_timeout=WATCH_API_TIMEOUT):
    #         if not self.running:
    #             break
    #         if event is None:
    #             continue

    #         name: str = event['raw_object']['metadata']['name']
    #         self.logger.warn("%s %s", name, event['raw_object'])

    #         if event['type'] in ["ADDED", "MODIFIED"]:
    #             # Check for node ready condition
    #             ready = False
    #             for condition in event['raw_object']['status']['conditions']:
    #                 if condition['type'] == 'Ready':
    #                     ready = condition['status'] == 'True'
    #                     break

    #             if ready:
    #                 cpu = parse_cpu(event['raw_object']['status']['allocatable']['cpu'])
    #                 ram = parse_memory(event['raw_object']['status']['allocatable']['memory'])
    #                 self.ready_nodes[name] = (cpu, ram)
    #             else:
    #                 self.ready_nodes.pop(name, None)

    #         elif event['type'] == "DELETED":
    #             # Remove deleted nodes
    #             self.ready_nodes.pop(name, None)

    def _monitor_node_pool(self) -> None:
        self._node_pool_max_cpu = 0
        self._node_pool_max_ram = 0
        self.node_count = 0
        watch = TypelessWatch()
        self.ready_nodes: dict[str, NodeState] = {}
        field_selector, label_selector = selector_to_list_filters(self.linux_node_selector)

        for event in watch.stream(func=self.api.list_node, timeout_seconds=WATCH_TIMEOUT,
                                  field_selector=field_selector, label_selector=label_selector,
                                  _request_timeout=WATCH_API_TIMEOUT):
            if not self.running:
                break
            if not isinstance(event, dict):
                continue

            name: str = event['raw_object']['metadata']['name']

            if event['type'] in ["ADDED", "MODIFIED"]:
                # Check for node ready condition
                ready = False
                for condition in event['raw_object']['status']['conditions']:
                    if condition['type'] == 'Ready':
                        ready = condition['status'] == 'True'
                        break

                if ready:
                    cpu = parse_cpu(event['raw_object']['status']['allocatable']['cpu'])
                    ram = parse_memory(event['raw_object']['status']['allocatable']['memory'])
                    self.ready_nodes[name] = NodeState(cpu, ram)
                else:
                    self.ready_nodes.pop(name, None)

            elif event['type'] == "DELETED":
                # Remove deleted nodes
                self.ready_nodes.pop(name, None)

            # Update the totals
            self.node_count = len(self.ready_nodes)
            max_cpu = 0.0
            max_ram = 0.0
            for state in self.ready_nodes.values():
                max_cpu += state.cpu
                max_ram += state.ram
            self._node_pool_max_cpu = max_cpu
            self._node_pool_max_ram = max_ram

    def _monitor_node_metrics(self) -> None:
        METRICS_API_GROUP = "metrics.k8s.io"
        METRICS_API_VERSION = "v1beta1"
        METRICS_INTERVAL = 15.0

        api = CustomObjectsApi(self.api_client)

        while self.running:
            iteration_start = time.time()

            metrics: dict = api.list_cluster_custom_object(
                group=METRICS_API_GROUP,
                version=METRICS_API_VERSION,
                plural="nodes"
            )

            for item in metrics['items']:
                name = item['metadata']['name']
                node = self.ready_nodes.get(name)
                if node:
                    node.cpu_utilization = parse_cpu(item['usage']['cpu'])
                    node.ram_utilization = parse_memory(item['usage']['memory'])

            duration = time.time() - iteration_start
            remaining_interval = METRICS_INTERVAL - duration
            if remaining_interval > 0:
                time.sleep(remaining_interval)

    def _monitor_quotas(self):
        watch = TypelessWatch()
        cpu_limits = {}
        cpu_used = {}
        mem_limits = {}
        mem_used = {}

        self._quota_cpu_limit = None
        self._quota_cpu_used = None
        self._quota_mem_limit = None
        self._quota_mem_used = None

        for event in watch.stream(func=self.api.list_namespaced_resource_quota, namespace=self.namespace,
                                  timeout_seconds=WATCH_TIMEOUT, _request_timeout=WATCH_API_TIMEOUT):
            if not self.running:
                break
            if not isinstance(event, dict):
                continue

            name = event['raw_object']['metadata']['name']
            if 'scope_selector' in event['raw_object']['spec'] or 'scopes' in event['raw_object']['spec']:
                continue

            if event['type'] in ['ADDED', 'MODIFIED']:
                status = event['raw_object']['status']

                if 'hard' in status:
                    if 'cpu' in status['hard']:
                        cpu_limits[name] = parse_cpu(status['hard']['cpu'])
                    if 'requests.cpu' in status['hard']:
                        cpu_limits[name] = parse_cpu(status['hard']['requests.cpu'])
                    if 'limits.cpu' in status['hard']:
                        cpu_limits[name] = parse_cpu(status['hard']['limits.cpu'])
                    if 'memory' in status['hard']:
                        mem_limits[name] = parse_memory(status['hard']['memory'])
                    if 'requests.memory' in status['hard']:
                        mem_limits[name] = parse_memory(status['hard']['requests.memory'])
                    if 'limits.memory' in status['hard']:
                        mem_limits[name] = parse_memory(status['hard']['limits.memory'])

                if 'used' in status:
                    if 'cpu' in status['used']:
                        cpu_used[name] = parse_cpu(status['used']['cpu'])
                    if 'requests.cpu' in status['used']:
                        cpu_used[name] = parse_cpu(status['used']['requests.cpu'])
                    if 'limits.cpu' in status['used']:
                        cpu_used[name] = parse_cpu(status['used']['limits.cpu'])
                    if 'memory' in status['used']:
                        mem_used[name] = parse_memory(status['used']['memory'])
                    if 'requests.memory' in status['used']:
                        mem_used[name] = parse_memory(status['used']['requests.memory'])
                    if 'limits.memory' in status['used']:
                        mem_used[name] = parse_memory(status['used']['limits.memory'])

            elif event['type'] == 'DELETED':
                cpu_limits.pop(name, None)
                cpu_used.pop(name, None)
                mem_limits.pop(name, None)
                mem_used.pop(name, None)
            else:
                continue

            if cpu_limits:
                self._quota_cpu_limit = min(cpu_limits.values())
            else:
                self._quota_cpu_limit = None

            if cpu_used:
                self._quota_cpu_used = max(cpu_used.values())
            else:
                self._quota_cpu_used = None

            if mem_limits:
                self._quota_mem_limit = min(mem_limits.values())
            else:
                self._quota_mem_limit = None

            if mem_used:
                self._quota_mem_used = max(mem_used.values())
            else:
                self._quota_mem_used = None

    def _monitor_deployments(self) -> None:
        watch = TypelessWatch()

        self._deployment_targets = {}
        self._deployment_unavailable = {}
        label_selector = ','.join(f'{_n}={_v}' for _n, _v in self._labels.items() if _n != 'privilege')

        for event in watch.stream(func=self.apps_api.list_namespaced_deployment,
                                  namespace=self.namespace, label_selector=label_selector,
                                  timeout_seconds=WATCH_TIMEOUT, _request_timeout=WATCH_API_TIMEOUT):
            if not isinstance(event, dict):
                continue

            if 'dependency_for' in event['raw_object']['metadata']['labels']:
                continue

            if event['type'] in ['ADDED', 'MODIFIED']:
                name = event['raw_object']['metadata']['labels'].get('component', None)
                if name is not None:
                    self._deployment_targets[name] = event['raw_object']['spec']['replicas']
                    self._deployment_unavailable[name] = event['raw_object']['status'].get('unavailableReplicas', 0)
            elif event['type'] == 'DELETED':
                name = event['raw_object']['metadata']['labels'].get('component', None)
                self._deployment_targets.pop(name, None)
                self._deployment_unavailable.pop(name, None)

    # def _get_pod_used_namespace_cpu(self) -> float:
    #     count = 0.0
    #     for name in self.ready_nodes.keys():
    #         count += self._pod_used_namespace_cpu[name]
    #     return count

    def _get_pod_used_cpu(self) -> float:
        count = 0.0
        for node in self.ready_nodes.values():
            count += node.cpu_utilization
        return count

    def cpu_info(self):
        if self._quota_cpu_limit:
            if self._quota_cpu_used:
                return self._quota_cpu_limit - self._quota_cpu_used, self._quota_cpu_limit
            # return self._quota_cpu_limit - self._get_pod_used_namespace_cpu(), self._quota_cpu_limit
        return self._node_pool_max_cpu - self._get_pod_used_cpu(), self._node_pool_max_cpu

    # def _get_pod_used_namespace_ram(self) -> float:
    #     count = 0.0
    #     for name in self.ready_nodes.keys():
    #         count += self._pod_used_namespace_ram[name]
    #     return count

    def _get_pod_used_ram(self) -> float:
        count = 0.0
        for node in self.ready_nodes.values():
            count += node.ram_utilization
        return count

    def memory_info(self):
        if self._quota_mem_limit:
            if self._quota_mem_used:
                return self._quota_mem_limit - self._quota_mem_used, self._quota_mem_limit
            # return self._quota_mem_limit - self._get_pod_used_namespace_ram(), self._quota_mem_limit
        return self._node_pool_max_ram - self._get_pod_used_ram(), self._node_pool_max_ram


    def restart(self, service: ServiceProfile):
        scale = max(self.get_target(service.name), service.min_instances)
        self._create_deployment(service.name, self._deployment_name(service.name), service.container_config,
                                service.shutdown_seconds, scale, core_mounts=False,
                                change_key=service.config_blob, security_context=self.security_policy)
        self._create_hpa(service)
