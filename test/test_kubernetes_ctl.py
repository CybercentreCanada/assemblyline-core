import math
from unittest.mock import Mock, patch

import pytest
from kubernetes.client import V1Toleration

from assemblyline.odm.models.config import Selector
from assemblyline_core.scaler.controllers.kubernetes_ctl import (
    KubernetesController,
    node_is_schedulable,
    toleration_matches_taint,
)


def make_node(name='node-a', ready=True, unschedulable=False, taints=None, cpu='4', memory='8Gi', pods='110'):
    spec = {}
    if unschedulable:
        spec['unschedulable'] = True
    if taints is not None:
        spec['taints'] = taints
    return {
        'metadata': {'name': name},
        'spec': spec,
        'status': {
            'conditions': [{'type': 'Ready', 'status': 'True' if ready else 'False'}],
            'allocatable': {'cpu': cpu, 'memory': memory, 'pods': pods},
        },
    }


CONTROL_PLANE = {'key': 'node-role.kubernetes.io/control-plane', 'effect': 'NoSchedule'}
DEDICATED = {'key': 'dedicated', 'value': 'system', 'effect': 'NoSchedule'}


@pytest.mark.parametrize('toleration, taint, expected', [
    (V1Toleration(key='dedicated', operator='Equal', value='system', effect='NoSchedule'), DEDICATED, True),
    (V1Toleration(key='dedicated', value='system'), DEDICATED, True),
    (V1Toleration(key='dedicated', operator='Equal', value='other'), DEDICATED, False),
    (V1Toleration(key='dedicated', operator='Exists'), DEDICATED, True),
    (V1Toleration(key='other', operator='Exists'), DEDICATED, False),
    (V1Toleration(key='dedicated', operator='Exists', effect='NoExecute'), DEDICATED, False),
    (V1Toleration(operator='Exists'), DEDICATED, True),
    (V1Toleration(key='node-role.kubernetes.io/control-plane', operator='Exists'), CONTROL_PLANE, True),
    (V1Toleration(key='node-role.kubernetes.io/control-plane', operator='Equal'), CONTROL_PLANE, True),
])
def test_toleration_matches_taint(toleration, taint, expected):
    assert toleration_matches_taint(toleration, taint) is expected


def test_node_is_schedulable():
    assert node_is_schedulable(make_node(), [])
    assert node_is_schedulable(make_node(taints=[]), [])
    assert not node_is_schedulable(make_node(unschedulable=True), [])
    assert not node_is_schedulable(make_node(unschedulable=True), [V1Toleration(operator='Exists')])

    # Only hard taints keep service pods off a node
    assert not node_is_schedulable(make_node(taints=[CONTROL_PLANE]), [])
    assert not node_is_schedulable(make_node(taints=[{**DEDICATED, 'effect': 'NoExecute'}]), [])
    assert node_is_schedulable(make_node(taints=[{**DEDICATED, 'effect': 'PreferNoSchedule'}]), [])

    # Every hard taint must be tolerated
    tolerate_dedicated = [V1Toleration(key='dedicated', operator='Exists')]
    assert node_is_schedulable(make_node(taints=[DEDICATED]), tolerate_dedicated)
    assert not node_is_schedulable(make_node(taints=[DEDICATED, CONTROL_PLANE]), tolerate_dedicated)


def monitor_node_pool(events, tolerations=()):
    ctl = KubernetesController.__new__(KubernetesController)
    ctl.running = True
    ctl.api = Mock()
    ctl.linux_node_selector = Selector({})
    ctl.default_service_tolerations = list(tolerations)

    watch = Mock()
    watch.stream.return_value = [{'type': kind, 'raw_object': node} for kind, node in events]
    with patch('assemblyline_core.scaler.controllers.kubernetes_ctl.Watch', return_value=watch):
        ctl._monitor_node_pool()
    return ctl


def test_monitor_node_pool_counts_only_schedulable_nodes():
    ctl = monitor_node_pool([
        ('ADDED', make_node('worker', cpu='4', memory='8Gi')),
        ('ADDED', make_node('control-plane', taints=[CONTROL_PLANE], cpu='2', memory='4Gi')),
        ('ADDED', make_node('cordoned', unschedulable=True, cpu='8', memory='16Gi')),
        ('ADDED', make_node('not-ready', ready=False, cpu='8', memory='16Gi')),
    ])

    assert set(ctl.ready_nodes) == {'worker'}
    assert ctl.node_count == 1
    assert ctl._node_pool_max_cpu == 4
    assert ctl._node_pool_max_ram == 8 * 1024


def test_monitor_node_pool_includes_tolerated_nodes():
    ctl = monitor_node_pool([
        ('ADDED', make_node('worker', cpu='4', memory='8Gi')),
        ('ADDED', make_node('system', taints=[DEDICATED], cpu='2', memory='4Gi')),
    ], tolerations=[V1Toleration(key='dedicated', operator='Equal', value='system', effect='NoSchedule')])

    assert set(ctl.ready_nodes) == {'worker', 'system'}
    assert ctl._node_pool_max_cpu == 6


def test_monitor_node_pool_drops_node_when_cordoned():
    ctl = monitor_node_pool([
        ('ADDED', make_node('worker-a')),
        ('ADDED', make_node('worker-b')),
        ('MODIFIED', make_node('worker-b', unschedulable=True)),
    ])

    assert set(ctl.ready_nodes) == {'worker-a'}
    assert ctl._node_pool_max_cpu == 4

    ctl = monitor_node_pool([
        ('ADDED', make_node('worker-a', unschedulable=True)),
        ('MODIFIED', make_node('worker-a')),
    ])

    assert set(ctl.ready_nodes) == {'worker-a'}


def test_monitor_node_pool_sums_pod_capacity():
    ctl = monitor_node_pool([
        ('ADDED', make_node('worker-a', pods='110')),
        ('ADDED', make_node('worker-b', pods='30')),
        ('ADDED', make_node('control-plane', taints=[CONTROL_PLANE], pods='110')),
    ])

    assert ctl._node_pool_max_pods == 140

    node = make_node('virtual')
    del node['status']['allocatable']['pods']
    ctl = monitor_node_pool([('ADDED', make_node('worker-a')), ('ADDED', node)])
    assert ctl._node_pool_max_pods == math.inf


def make_pod(uid, node, phase='Running', namespace='al'):
    return {
        'metadata': {'name': f'pod-{uid}', 'uid': uid, 'namespace': namespace},
        'spec': {
            'nodeName': node,
            'containers': [{'name': 'main', 'resources': {'requests': {'cpu': '100m', 'memory': '64Mi'}}}],
        },
        'status': {'phase': phase},
    }


def pod_controller(node_pods, cluster_pod_list=True):
    ctl = KubernetesController.__new__(KubernetesController)
    ctl.running = True
    ctl.api = Mock()
    ctl.logger = Mock()
    ctl.namespace = 'al'
    ctl.cluster_pod_list = cluster_pod_list
    ctl.ready_nodes = {name: (4.0, 8192.0, pods) for name, pods in node_pods.items()}
    ctl._node_pool_max_pods = sum(node_pods.values())
    return ctl


def monitor_pods(ctl, events):
    watch = Mock()
    watch.stream.return_value = [{'type': kind, 'raw_object': pod} for kind, pod in events]
    with patch('assemblyline_core.scaler.controllers.kubernetes_ctl.Watch', return_value=watch):
        ctl._monitor_pods()
    return ctl


def test_pods_info_counts_active_pods_on_schedulable_nodes():
    ctl = monitor_pods(pod_controller({'worker-a': 10, 'worker-b': 5}), [
        ('ADDED', make_pod('1', 'worker-a')),
        ('ADDED', make_pod('2', 'worker-a', namespace='kube-system')),
        ('ADDED', make_pod('3', 'worker-b', phase='Pending')),
        # Terminal pods don't occupy a slot
        ('ADDED', make_pod('4', 'worker-b', phase='Succeeded')),
        ('ADDED', make_pod('5', 'worker-b', phase='Failed')),
        # Pods on nodes outside the pool, or not yet bound to a node, aren't counted
        ('ADDED', make_pod('6', 'control-plane')),
        ('ADDED', make_pod('7', None, phase='Pending')),
    ])

    assert ctl.pods_info() == (12, 15)


def test_pods_info_tracks_pod_lifecycle():
    ctl = monitor_pods(pod_controller({'worker-a': 10}), [
        ('ADDED', make_pod('1', None, phase='Pending')),
        ('MODIFIED', make_pod('1', 'worker-a', phase='Pending')),
        ('MODIFIED', make_pod('1', 'worker-a', phase='Running')),
        ('ADDED', make_pod('2', 'worker-a')),
        ('MODIFIED', make_pod('2', 'worker-a', phase='Succeeded')),
        ('ADDED', make_pod('3', 'worker-a')),
        ('DELETED', make_pod('3', 'worker-a')),
    ])

    assert ctl.pods_info() == (9, 10)


def test_pods_info_unbounded_without_cluster_pod_list():
    ctl = monitor_pods(pod_controller({'worker-a': 10}, cluster_pod_list=False), [
        ('ADDED', make_pod('1', 'worker-a')),
    ])

    assert ctl.pods_info() == (math.inf, math.inf)
