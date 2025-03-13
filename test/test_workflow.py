import pytest
import random
from assemblyline_core.workflow.run_workflow import WorkflowManager

from assemblyline.common.isotime import now_as_iso
from assemblyline.odm.models.workflow import Workflow
from assemblyline.odm.random_data import create_alerts, wipe_alerts, wipe_workflows
from assemblyline.odm.randomizer import random_minimal_obj


@pytest.fixture(scope="module")
def manager(datastore_connection):
    try:
        create_alerts(datastore_connection)
        wipe_workflows(datastore_connection)
        datastore_connection.alert.update_by_query("*", [(datastore_connection.alert.UPDATE_SET, 'reporting_ts', now_as_iso())])
        datastore_connection.alert.commit()
        yield WorkflowManager()
    finally:
        wipe_alerts(datastore_connection)

def test_workflow(manager, datastore_connection):
    # Create workflow that targets alerts based on YARA rule association
    workflow = random_minimal_obj(Workflow)

    yara_rule = random.choice(list(datastore_connection.alert.facet("al.yara").keys()))   
    workflow.query = f'al.yara:"{yara_rule}"'
    workflow.workflow_id = "AL_TEST"
    workflow.labels = ["AL_TEST"]
    workflow.priority = "LOW"
    workflow.status = "MALICIOUS"    
    datastore_connection.workflow.save(workflow.workflow_id, workflow)
    datastore_connection.workflow.commit()

    # Run Workflow manager to process new workflow against existing alerts
    manager.running = True
    manager.get_last_reporting_ts = lambda x: "now/d+1d"
    manager.try_run(run_once=True)
    datastore_connection.alert.commit()
    
    # Assert that custom labels were applied to alerts
    assert datastore_connection.alert.search("label:AL_TEST", track_total_hits=True)['total']

    # Assert that the change has been record in the alerts' event history
    assert datastore_connection.alert.search(f"events.entity_id:{workflow.workflow_id}", track_total_hits=True)['total']
