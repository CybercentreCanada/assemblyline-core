from assemblyline.odm.models.service import DockerConfig
from pytest import approx
from unittest.mock import Mock, patch
from assemblyline_core.scaler.collection import Collection
from assemblyline_core.scaler.scaler_server import ServiceProfile

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
