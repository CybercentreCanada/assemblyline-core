from unittest.mock import Mock

from assemblyline_core.scaler.controllers.docker_ctl import DockerController


def test_find_service_server_accepts_container_list():
    controller = DockerController.__new__(DockerController)
    controller.log = Mock()

    service_servers = [
        Mock(name='service-server-1', id='id-1'),
        Mock(name='service-server-2', id='id-2'),
    ]
    service_servers[0].name = 'assemblyline-service_server-1'
    service_servers[0].id = 'id-1'
    service_servers[1].name = 'assemblyline-service_server-2'
    service_servers[1].id = 'id-2'

    assert controller.find_service_server(service_servers) == service_servers
    assert controller.log.info.call_count == 2


def test_connect_service_servers_to_network():
    controller = DockerController.__new__(DockerController)
    controller._connect_to_network = Mock()

    service_server_1 = Mock()
    service_server_1.name = 'assemblyline-service_server-1'
    service_server_2 = Mock()
    service_server_2.name = 'assemblyline-service_server-2'
    controller.service_servers = [service_server_1, service_server_2]

    connected_server = Mock()
    connected_server.name = service_server_1.name
    network = Mock()
    network.containers = [connected_server]

    controller._connect_service_servers_to_network(network)

    controller._connect_to_network.assert_called_once_with(
        service_server_2, network, aliases=['service-server']
    )
