import math
import threading

from assemblyline.odm.models.service import DockerConfig
from pytest import approx
from unittest.mock import Mock, patch
from assemblyline_core.scaler.collection import Collection
from assemblyline_core.scaler.controllers.interface import ControllerInterface
from assemblyline_core.scaler.scaler_server import ScalerServer, ServiceProfile
from assemblyline_core.server_base import ServiceStage

mock_time = Mock()


@patch('time.time', mock_time)
def test_collection():
    mock_time.return_value = 0
    collection = Collection(60, ttl=61)

    # Insert some sample data
    collection.update('service-a', 'host-a', 30, 1)
    collection.update('service-b', 'host-a', 60, 1)
    mock_time.return_value = 30
    collection.update('service-a', 'host-b', 30, 1)

    assert collection.read('service-c') is None
    assert collection.read('service-a')['instances'] == 2
    assert collection.read('service-a')['duty_cycle'] == approx(0.5)

    assert collection.read('service-b')['instances'] == 1
    assert collection.read('service-b')['duty_cycle'] == approx(1)

    # Move forward enough that the first two messages expire, send another message from the second
    # service, now both should have one active message/host
    mock_time.return_value = 62
    collection.update('service-b', 'host-a', 30, 1)

    assert collection.read('service-a')['instances'] == 1
    assert collection.read('service-a')['duty_cycle'] == approx(0.5)

    assert collection.read('service-b')['instances'] == 1
    assert collection.read('service-b')['duty_cycle'] == approx(0.5)

    # Move forward that the last of the original group of messages expire, but the update for the second
    # service is still in effect
    mock_time.return_value = 100

    assert collection.read('service-a') is None
    assert collection.read('service-b')['instances'] == 1
    assert collection.read('service-b')['duty_cycle'] == approx(0.5)


def test_default_bucket_rates():
    service = ServiceProfile('service', DockerConfig(dict(image='redis')))
    before = service.pressure
    service.update(5, 1, 1, 1)
    print(service.pressure, before)
    assert service.pressure > before


def make_profile(name, desired):
    profile = ServiceProfile(name, DockerConfig(dict(image='redis', cpu_cores=1, ram_mb=1024)))
    profile.desired_instances = desired
    return profile


def run_scaling_pass(profiles, pods, targets=None, unavailable=None, cpu_overallocation=1.0,
                     memory_overallocation=1.0, overallocation_node_limit=None, node_count=3):
    """Run one iteration of ScalerServer.update_scaling against a mocked controller with ample CPU/RAM."""
    server = ScalerServer.__new__(ScalerServer)
    server.log = Mock()
    server.apm_client = None
    server.max_pending = 50
    server.profiles = {profile.name: profile for profile in profiles}
    server.profiles_lock = threading.RLock()
    server.get_service_stage = Mock(return_value=ServiceStage.Running)
    server.sleep = Mock(side_effect=[True, False])

    server.config = Mock()
    server.config.core.scaler.cpu_overallocation = cpu_overallocation
    server.config.core.scaler.memory_overallocation = memory_overallocation
    server.config.core.scaler.overallocation_node_limit = overallocation_node_limit

    server.controller = Mock()
    server.controller.node_count = node_count
    server.controller.get_targets.return_value = dict(targets or {})
    server.controller.get_unavailable.return_value = dict(unavailable or {})
    server.controller.cpu_info.return_value = (1000.0, 1000.0)
    server.controller.memory_info.return_value = (1024.0 * 1000, 1024.0 * 1000)
    server.controller.pods_info.return_value = pods

    server.update_scaling()
    return {profile.name: profile.target_instances for profile in profiles}


def test_controller_pods_info_defaults_to_unbounded():
    assert ControllerInterface().pods_info() == (math.inf, math.inf)
    assert ControllerInterface().free_pods() == math.inf


def test_scaling_unbounded_by_default_pod_info():
    targets = run_scaling_pass([make_profile('a', 10), make_profile('b', 10)], pods=(math.inf, math.inf))
    assert targets == {'a': 10, 'b': 10}


def test_scaling_stops_when_pod_slots_run_out():
    targets = run_scaling_pass([make_profile('a', 10), make_profile('b', 10)], pods=(5, 100))
    assert sum(targets.values()) == 5
    assert abs(targets['a'] - targets['b']) <= 1


def test_scaling_reserves_pod_slots_for_pending_containers():
    targets = run_scaling_pass([make_profile('a', 10)], pods=(5, 100), targets={'a': 2}, unavailable={'a': 2})
    assert targets == {'a': 5}


def test_scaling_ignores_pod_slots_while_overallocating():
    # Overallocation relies on pending pods to drive a cluster autoscaler, so pod slots can't cap it
    targets = run_scaling_pass([make_profile('a', 10)], pods=(5, 100), cpu_overallocation=2.0)
    assert targets == {'a': 10}

    targets = run_scaling_pass([make_profile('a', 10)], pods=(5, 100), memory_overallocation=1.5)
    assert targets == {'a': 10}


def test_scaling_respects_pod_slots_past_overallocation_node_limit():
    targets = run_scaling_pass([make_profile('a', 10)], pods=(5, 100), cpu_overallocation=2.0,
                               overallocation_node_limit=3, node_count=3)
    assert targets == {'a': 5}
