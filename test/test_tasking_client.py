
import pytest
from assemblyline_core.tasking_client import TaskingClient

from assemblyline.odm.models.service import Service
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.result import Result, Section, Heuristic as SectionHeuristic
from assemblyline.odm.random_data import (
    create_badlists,
    create_users,
    wipe_badlist,
    wipe_users,
)
from assemblyline.odm.randomizer import random_minimal_obj


@pytest.fixture(scope="module")
def client(datastore_connection):
    try:
        create_users(datastore_connection)
        create_badlists(datastore_connection)
        yield TaskingClient(datastore_connection)
    finally:
        wipe_users(datastore_connection)
        wipe_badlist(datastore_connection)

def test_register_service(client, datastore_connection):
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
