import collections
import random
import uuid

import pytest

from assemblyline_core.alerter.run_alerter import Alerter
from assemblyline_core.ingester.ingester import IngestTask
from assemblyline.common import forge
from assemblyline.common.uid import get_random_id
from assemblyline.odm.models.submission import Submission
from assemblyline.odm.models.tagging import Tagging
from assemblyline.odm.random_data import wipe_submissions, create_submission
from assemblyline.odm.randomizer import random_model_obj, get_random_tags
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.named import NamedQueue

NUM_SUBMISSIONS = 2
all_submissions = []


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
    for _ in range(NUM_SUBMISSIONS):
        all_submissions.append(create_submission(datastore_connection, fs))

    try:
        yield datastore_connection
    finally:
        wipe_submissions(datastore_connection, fs)
        datastore_connection.alert.wipe()


def test_create_single_alert(config, datastore):
    persistent_redis = get_client(
        host=config.core.redis.persistent.host,
        port=config.core.redis.persistent.port,
        private=False,
    )
    alerter = Alerter()
    # Swap our alerter onto a private queue so our test doesn't get intercepted
    alerter.alert_queue = alert_queue = NamedQueue(uuid.uuid4().hex, persistent_redis)

    # Get a random submission
    submission = random.choice(all_submissions)
    all_submissions.remove(submission)

    # Generate a task for the submission
    ingest_msg = random_model_obj(IngestTask)
    ingest_msg.submission.sid = submission.sid
    ingest_msg.submission.metadata = submission.metadata
    ingest_msg.submission.params = submission.params
    ingest_msg.submission.files = submission.files

    alert_queue.push(ingest_msg.as_primitives())
    alert_type = alerter.run_once()
    assert alert_type == 'create'
    datastore.alert.commit()

    res = datastore.alert.search("id:*", as_obj=False)
    assert res['total'] == 1

    alert = datastore.alert.get(res['items'][0]['alert_id'])
    assert alert.sid == submission.sid


def test_update_single_alert(config, datastore):
    persistent_redis = get_client(
        host=config.core.redis.persistent.host,
        port=config.core.redis.persistent.port,
        private=False,
    )
    alerter = Alerter()
    # Swap our alerter onto a private queue so our test doesn't get intercepted
    alerter.alert_queue = alert_queue = NamedQueue(uuid.uuid4().hex, persistent_redis)

    # Get a random submission
    submission = random.choice(all_submissions)
    all_submissions.remove(submission)

    # Generate a task for the submission
    ingest_msg = random_model_obj(IngestTask)
    ingest_msg.submission.sid = submission.sid
    ingest_msg.submission.metadata = submission.metadata
    ingest_msg.submission.params = submission.params
    ingest_msg.submission.files = submission.files

    alert_queue.push(ingest_msg.as_primitives())
    alert_type = alerter.run_once()
    assert alert_type == 'create'
    datastore.alert.commit()

    original_alert = datastore.alert.get(datastore.alert.search(f"sid:{submission.sid}", fl="id",
                                                                as_obj=False)['items'][0]['id'])
    assert original_alert is not None

    # Generate a children task
    child_submission = Submission(submission.as_primitives())
    child_submission.sid = get_random_id()
    child_submission.params.psid = submission.sid

    # Alter the result of one of the services
    r = None
    while r is None:
        r = datastore.result.get(random.choice(child_submission.results))

    for s in r.result.sections:
        old_tags = s.tags.as_primitives(strip_null=True)
        s.tags = Tagging(recursive_extend(old_tags, get_random_tags()))

    datastore.result.save(r.build_key(), r)
    datastore.result.commit()

    datastore.submission.save(child_submission.sid, child_submission)
    datastore.submission.commit()

    child_ingest_msg = random_model_obj(IngestTask)
    child_ingest_msg.submission.sid = child_submission.sid
    child_ingest_msg.submission.metadata = child_submission.metadata
    child_ingest_msg.submission.params = child_submission.params
    child_ingest_msg.submission.files = child_submission.files
    child_ingest_msg.submission.time = ingest_msg.submission.time
    child_ingest_msg.ingest_id = ingest_msg.ingest_id

    alert_queue.push(child_ingest_msg.as_primitives())
    alert_type = alerter.run_once()
    assert alert_type == 'update'
    datastore.alert.commit()

    updated_alert = datastore.alert.get(datastore.alert.search(f"sid:{child_submission.sid}",
                                                               fl="id", as_obj=False)['items'][0]['id'])
    assert updated_alert is not None

    assert updated_alert != original_alert
