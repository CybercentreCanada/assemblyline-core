import collections
import json
import os
import random

import pytest

from assemblyline.common import forge
from assemblyline.odm.models.badlist import Badlist
from assemblyline.odm.models.safelist import Safelist
from assemblyline.odm.models.workflow import Workflow
from assemblyline.odm.random_data import create_alerts, wipe_alerts, wipe_submissions, create_submission, create_badlists, create_safelists, create_workflows, wipe_badlist, wipe_safelist, wipe_workflows
from assemblyline_core.replay.creator.run import ReplayCreator
from assemblyline_core.replay.creator.run_worker import ReplayCreatorWorker
from assemblyline_core.replay.loader.run import ReplayLoader
from assemblyline_core.replay.loader.run_worker import ReplayLoaderWorker

NUM_ALERTS = 1
NUM_BADLIST_ITEMS = 1
NUM_SAFELIST_ITEMS = 1
NUM_SUBMISSIONS = 1
NUM_WORKFLOWS = 1

all_alerts = []
all_badlists = []
all_safelists = []
all_submissions = []
all_workflows = []


@pytest.fixture(scope='module')
def fs(config):
    return forge.get_filestore(config)


def recursive_extend(d, u):
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = recursive_extend(d.get(k, {}), v)
        else:
            if k not in d:
                d[k] = []
            d[k].extend(v)

    return d


@pytest.fixture(scope="module")
def datastore(request, datastore_connection, fs):
    wipe_alerts(datastore_connection)
    wipe_badlist(datastore_connection)
    wipe_safelist(datastore_connection)
    wipe_submissions(datastore_connection, fs)
    wipe_workflows(datastore_connection)

    for _ in range(NUM_SUBMISSIONS):
        all_submissions.append(create_submission(datastore_connection, fs))
    create_alerts(datastore_connection, alert_count=NUM_ALERTS,
                  submission_list=all_submissions)
    create_safelists(datastore_connection, count=NUM_SAFELIST_ITEMS)
    create_badlists(datastore_connection, count=NUM_BADLIST_ITEMS)
    create_workflows(datastore_connection, count=NUM_WORKFLOWS)
    for alert in datastore_connection.alert.stream_search("id:*", fl="*"):
        all_alerts.append(alert)
    for bl_i in datastore_connection.badlist.stream_search("id:*", fl="id,*"):
        all_badlists.append(bl_i)
    for sl_i in datastore_connection.safelist.stream_search("id:*", fl="id,*"):
        all_safelists.append(sl_i)
    for wf in datastore_connection.workflow.stream_search("id:*", fl="*"):
        all_workflows.append(wf)

    try:
        yield datastore_connection
    finally:
        wipe_alerts(datastore_connection)
        wipe_badlist(datastore_connection)
        wipe_safelist(datastore_connection)
        wipe_submissions(datastore_connection, fs)
        wipe_workflows(datastore_connection)


@pytest.fixture(scope="module")
def creator():
    # Initialize Replay Creator
    replay_creator = ReplayCreator()
    replay_creator.running = True
    replay_creator.client.queues['alert'].delete()
    output_dir = replay_creator.replay_config.creator.output_filestore.replace('file://', '')
    if os.path.exists(output_dir):
        for f in os.listdir(output_dir):
            os.unlink(os.path.join(output_dir, f))

    return replay_creator


@pytest.fixture(scope="module")
def creator_worker():
    # Initialize Replay Creator worker
    replay_creator_worker = ReplayCreatorWorker()
    replay_creator_worker.running = True

    return replay_creator_worker


@pytest.fixture(scope="module")
def loader():
    # Initialize Replay Loader
    replay_loader = ReplayLoader()
    replay_loader.running = True
    replay_loader.client.queues['file'].delete()
    input_dir = replay_loader.replay_config.loader.input_directory
    if os.path.exists(input_dir):
        for f in os.listdir(input_dir):
            os.unlink(os.path.join(input_dir, f))

    return replay_loader


@pytest.fixture(scope="module")
def loader_worker():
    # Initialize Replay Loader worker
    replay_loader_worker = ReplayLoaderWorker()
    replay_loader_worker.running = True

    return replay_loader_worker


def test_replay_single_alert(config, datastore, creator, creator_worker, loader, loader_worker):
    output_dir = creator.replay_config.creator.output_filestore.replace('file://', '')
    input_dir = loader.replay_config.loader.input_directory

    # Make sure the alert get picked up by the creator
    alert = random.choice(all_alerts)
    alert.extended_scan = 'completed'
    alert.workflows_completed = True
    datastore.alert.save(alert.alert_id, alert)
    datastore.alert.commit()
    filename = os.path.join(output_dir, f'alert_{alert.alert_id}.al_bundle')

    # Test replay creator
    creator.client.setup_alert_input_queue(once=True)
    assert creator.client.queues['alert'].length() == 1
    assert creator.client.queues['alert'].peek_next()[
        'alert_id'] == alert.alert_id

    # Test replay creator worker
    creator_worker.process_alerts(once=True)
    datastore.alert.commit()
    assert creator_worker.client.queues['alert'].length() == 0
    assert datastore.alert.get(alert.alert_id, as_obj=False)['metadata']['replay'] == 'done'
    assert os.path.exists(filename)

    # Delete the alert to test the loading process
    datastore.alert.delete(alert.alert_id)
    datastore.alert.commit()

    # In case the replay.yaml config creator output is not the same as loader input
    new_filename = filename.replace(output_dir, input_dir)
    if filename != new_filename:
        os.rename(filename, new_filename)
        filename = new_filename

    # Test replay loader
    loader.load_files(once=True)
    assert loader.client.queues['file'].length() == 1
    assert loader.client.queues['file'].peek_next() == filename

    # Test replay loader worker
    loader_worker.process_file(once=True)
    assert loader_worker.client.queues['file'].length() == 0
    assert not os.path.exists(filename)

    loaded_alert = datastore.alert.get(alert.alert_id, as_obj=False)
    assert 'bundle.loaded' in loaded_alert['metadata']
    assert alert.alert_id == loaded_alert['alert_id']
    assert alert.workflows_completed != loaded_alert['workflows_completed']


def test_replay_single_submission(config, datastore, creator, creator_worker, loader, loader_worker):
    output_dir = creator.replay_config.creator.output_filestore.replace('file://', '')
    input_dir = loader.replay_config.loader.input_directory

    # Make sure the submission get picked up by the creator
    sub = random.choice(all_submissions).as_primitives()
    sub['metadata']['replay'] = 'requested'
    datastore.submission.save(sub['sid'], sub)
    datastore.submission.commit()
    filename = os.path.join(output_dir, f"submission_{sub['sid']}.al_bundle")

    # Test replay creator
    creator.client.setup_submission_input_queue(once=True)
    assert creator.client.queues['submission'].length() == 1
    assert creator.client.queues['submission'].peek_next()['sid'] == sub['sid']

    # Test replay creator worker
    creator_worker.process_submissions(once=True)
    datastore.submission.commit()
    assert creator_worker.client.queues['submission'].length() == 0
    assert datastore.submission.get(sub['sid'], as_obj=False)['metadata']['replay'] == 'done'
    assert os.path.exists(filename)

    # Delete the alert to test the loading process
    datastore.submission.delete(sub['sid'])
    datastore.submission.commit()

    # In case the replay.yaml config creator output is not the same as loader input
    new_filename = filename.replace(output_dir, input_dir)
    if filename != new_filename:
        os.rename(filename, new_filename)
        filename = new_filename

    # Test replay loader
    loader.load_files(once=True)
    assert loader.client.queues['file'].length() == 1
    assert loader.client.queues['file'].peek_next() == filename

    # Test replay loader worker
    loader_worker.process_file(once=True)
    assert loader_worker.client.queues['file'].length() == 0
    assert not os.path.exists(filename)

    loaded_submission = datastore.submission.get(sub['sid'], as_obj=False)
    assert 'bundle.loaded' in loaded_submission['metadata']
    assert sub['sid'] == loaded_submission['sid']
    assert 'replay' not in loaded_submission['metadata']


def test_replay_single_workflow(config, datastore, creator, creator_worker, loader, loader_worker):
    output_dir = creator.replay_config.creator.output_filestore.replace('file://', '')
    input_dir = loader.replay_config.loader.input_directory

    # Make sure the workflow gets picked up by the creator
    workflow: Workflow = random.choice(all_workflows)

    # Test replay creator
    creator.client.setup_workflow_input_queue(once=True)
    assert creator.client.queues['workflow'].length() == 1
    assert creator.client.queues['workflow'].peek_next(
    )['workflow_id'] == workflow['workflow_id']

    # Test replay creator worker
    creator_worker.process_workflows(once=True)
    assert creator_worker.client.queues['workflow'].length() == 0
    filename = os.path.join(output_dir,
                            ([f for f in os.listdir(output_dir) if f.startswith(
                                'workflow') and f.endswith('.al_json')] + ["not_found"])[0])
    assert os.path.exists(filename)

    # Delete the workflow to test the loading process
    datastore.submission.delete(workflow.workflow_id)
    datastore.submission.commit()

    # In case the replay.yaml config creator output is not the same as loader input
    new_filename = filename.replace(output_dir, input_dir)
    if filename != new_filename:
        os.rename(filename, new_filename)
        filename = new_filename

    # Test replay loader
    loader.load_files(once=True)
    assert loader.client.queues['file'].length() == 1
    assert loader.client.queues['file'].peek_next() == filename

    # Test replay loader worker
    loader_worker.process_file(once=True)
    assert loader_worker.client.queues['file'].length() == 0
    assert not os.path.exists(filename)

    loaded_workflow = datastore.workflow.get_if_exists(workflow.workflow_id)
    assert loaded_workflow == workflow

    # Test updating the workflow but by a different author (reverse the author's name) and change the enabled state
    workflow.edited_by = workflow.edited_by[::-1]
    workflow.enabled = not workflow.enabled

    new_workflow_fn = os.path.join(input_dir, "workflow_blah.al_json")
    with open(new_workflow_fn, 'w') as fp:
        json_blob = {"id": workflow.id}
        json_blob.update(workflow.as_primitives())
        json.dump([json_blob], fp)

    # Load file to be processed
    loader.load_files(once=True)
    loader_worker.process_file(once=True)

    # If the workflow was edited by someone else, they shouldn't have control over the enabled state
    loaded_workflow = datastore.workflow.get(workflow.workflow_id)
    assert loaded_workflow.enabled != workflow.enabled and loaded_workflow.edited_by == workflow.edited_by
