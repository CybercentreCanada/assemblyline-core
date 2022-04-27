import collections
import os
import random

import pytest

from assemblyline.common import forge
from assemblyline.odm.random_data import create_alerts, wipe_alerts, wipe_submissions, create_submission
from assemblyline_core.replay.creator.run import ReplayCreator
from assemblyline_core.replay.creator.run_worker import ReplayCreatorWorker
from assemblyline_core.replay.loader.run import ReplayLoader
from assemblyline_core.replay.loader.run_worker import ReplayLoaderWorker

NUM_ALERTS = 1
NUM_SUBMISSIONS = 1
all_submissions = []
all_alerts = []


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
    wipe_submissions(datastore_connection, fs)

    for _ in range(NUM_SUBMISSIONS):
        all_submissions.append(create_submission(datastore_connection, fs))
    create_alerts(datastore_connection, alert_count=NUM_ALERTS, submission_list=all_submissions)
    for alert in datastore_connection.alert.stream_search("id:*", fl="*"):
        all_alerts.append(alert)

    try:
        yield datastore_connection
    finally:
        wipe_alerts(datastore_connection)
        wipe_submissions(datastore_connection, fs)
        datastore_connection.alert.wipe()


@pytest.fixture(scope="module")
def creator():
    # Initialize Replay Creator
    replay_creator = ReplayCreator()
    replay_creator.running = True
    replay_creator.client.alert_queue.delete()
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
    replay_loader.client.file_queue.delete()
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
    assert creator.client.alert_queue.length() == 1
    assert creator.client.alert_queue.peek_next()['alert_id'] == alert.alert_id

    # Test replay creator worker
    creator_worker.process_alerts(once=True)
    datastore.alert.commit()
    assert creator_worker.client.alert_queue.length() == 0
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
    assert loader.client.file_queue.length() == 1
    assert loader.client.file_queue.peek_next() == filename

    # Test replay loader worker
    loader_worker.process_file(once=True)
    assert loader_worker.client.file_queue.length() == 0
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
    assert creator.client.submission_queue.length() == 1
    assert creator.client.submission_queue.peek_next()['sid'] == sub['sid']

    # Test replay creator worker
    creator_worker.process_submissions(once=True)
    datastore.submission.commit()
    assert creator_worker.client.submission_queue.length() == 0
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
    assert loader.client.file_queue.length() == 1
    assert loader.client.file_queue.peek_next() == filename

    # Test replay loader worker
    loader_worker.process_file(once=True)
    assert loader_worker.client.file_queue.length() == 0
    assert not os.path.exists(filename)

    loaded_submission = datastore.submission.get(sub['sid'], as_obj=False)
    assert 'bundle.loaded' in loaded_submission['metadata']
    assert sub['sid'] == loaded_submission['sid']
    assert 'replay' not in loaded_submission['metadata']
