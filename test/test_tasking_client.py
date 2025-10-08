from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.result import Heuristic as SectionHeuristic
from assemblyline.odm.models.result import Result, Section
from assemblyline.odm.models.service import Service
from assemblyline.odm.randomizer import random_minimal_obj, random_model_obj
from assemblyline_core.tasking_client import TaskingClient


def test_register_service(datastore_connection):
    client = TaskingClient(datastore_connection, register_only=True)

    # Test service registration
    service = random_model_obj(Service).as_primitives()
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

    # Test registration with new and removed sources, submission parameters and configurations
    datastore_connection.service_delta.save(service['name'], {
        "version": service['version'],
        # Let's imagine a user added a new configuration parameter between updates
        "config": {"user_config": "user_value"},
        "submission_params": [
            {
                # Let's imagine a user added a new submission parameter between updates
                "name": "user_param",
                "type": "str",
                "default": "user_value",
                "value": "user_value",
            }
        ],
        "update_config": {
            # Let's imagine a user removed a sources between updates
            "sources": service['update_config']['sources'][1:],
        }
    })

    # Now let's update the service with a new version that has some changes
    service['config']['new_config'] = 'new_value'
    service['submission_params'].append({
        "name": "new_param",
        "type": "str",
        "default": "new_value",
        "value": "new_value",
    })
    service['update_config']['sources'].append({"name": "new_source", "uri": "http://new_source"})
    service['version'] = "new_version"
    assert client.register_service(service)

    datastore_connection.service_delta.update(service['name'],
                                              [(datastore_connection.service_delta.UPDATE_SET, 'version', service['version'])])

    merged_service = datastore_connection.get_service_with_delta(service['name'], as_obj=False)
    # Update sources that have been removed should stay removed
    sources = [s['name'] for s in merged_service['update_config']['sources']]
    assert service['update_config']['sources'][0]['name'] not in sources

    # Update sources that are new should be added
    assert 'new_source' in sources

    # Submission parameters that are new should be added and old user parameters should stay
    submission_param_names = [p['name'] for p in merged_service['submission_params']]
    assert 'new_param' in submission_param_names
    assert 'user_param' in submission_param_names

    # Configurations that are new should be added and old user configurations should stay
    assert merged_service['config']['new_config'] == 'new_value'
    assert merged_service['config']['user_config'] == 'user_value'
