from assemblyline_core.tasking_client import TaskingClient

from assemblyline.datastore.helper import AssemblylineDatastore as Datastore
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.result import Heuristic as SectionHeuristic
from assemblyline.odm.models.result import Result, Section
from assemblyline.odm.models.service import Service
from assemblyline.odm.randomizer import random_minimal_obj


def test_register_service(datastore_connection: Datastore):
    client = TaskingClient(datastore_connection, register_only=True)

    # Test service registration
    service = random_minimal_obj(Service).as_primitives()
    heuristics = [random_minimal_obj(Heuristic).as_primitives() for _ in range(2)]
    service['heuristics'] = heuristics
    assert client.register_service(service)
    assert all([datastore_connection.heuristic.exists(h['heur_id']) for h in heuristics])

    # Test registration with heuristics that were removed but still have related results
    heuristic = heuristics.pop(0)
    result = random_minimal_obj(Result)
    section = random_minimal_obj(Section)
    section.heuristic = SectionHeuristic(heuristic)
    result.result.sections = [section]
    datastore_connection.result.save('test_result', result)
    datastore_connection.result.commit()

    # Heuristics that were removed should still reside in the system if there are still associated data to it
    service['heuristics'] = heuristics
    assert client.register_service(service)
    assert datastore_connection.heuristic.exists(heuristic['heur_id'])

    # Test registration with removed heuristics that have no related results
    datastore_connection.result.delete('test_result')
    datastore_connection.result.commit()
    assert client.register_service(service)
    assert not datastore_connection.heuristic.exists(heuristic['heur_id'])

def test_service_update(datastore_connection: Datastore):
    client = TaskingClient(datastore_connection, register_only=True)

    # Test service registration
    service = random_minimal_obj(Service).as_primitives()
    assert client.register_service(service)

    # Test registering a service update where there's a new submission parameter and configuration
    service['submission_params'].append({"name": 'new_param', 'type': 'str', 'default': 'default_value', 'value': 'default_value'})
    service['version'] = "new_version"
    service['config']  = {'new_config': 'value'}
    assert client.register_service(service)

    # Pretend I'm the updater acknowledging the new version has been registered
    datastore_connection.service_delta.update(service['name'], [(datastore_connection.service_delta.UPDATE_SET, 'version', service['version'])])

    # We should see the new submission parameter in the service while applying delta changes
    delta = datastore_connection.get_service_with_delta(service['name'])
    assert delta['submission_params'][-1]['name'] == 'new_param'
    assert delta['config']['new_config'] == 'value'

    # Test registering a service update where the user has changed a submission parameter prior to the update and new parameter was added
    assert datastore_connection.service_delta.update(service['name'], [(datastore_connection.service_delta.UPDATE_APPEND, 'submission_params', {'name': 'new_param', 'type': 'str', 'default': 'custom_value', 'value': 'custom_value'})])
    datastore_connection.service_delta.commit()

    service['submission_params'].append({"name": 'new_new_param', 'type': 'str', 'default': 'default_value', 'value': 'default_value'})
    service['version'] = "new_new_version"

    assert client.register_service(service)

    # Pretend I'm the updater acknowledging the new version has been registered
    datastore_connection.service_delta.update(service['name'], [(datastore_connection.service_delta.UPDATE_SET, 'version', service['version'])])

    # We expect to see both the updated submission parameter and the newly added one (while still keeping the custom value changes)
    delta = datastore_connection.get_service_with_delta(service['name'])
    assert delta['submission_params'][-2]['name'] == 'new_param' and delta['submission_params'][-2]['value'] == 'custom_value'
    assert delta['submission_params'][-1]['name'] == 'new_new_param'
