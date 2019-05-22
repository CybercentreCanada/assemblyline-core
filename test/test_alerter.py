import hashlib
import random

import pytest

from al_core.alerter.run_alerter import Alerter, ALERT_QUEUE_NAME
from al_core.ingester.ingester import IngestTask
from assemblyline.common import forge
from assemblyline.common.uid import get_random_id
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.file import File
from assemblyline.odm.models.result import Result, Tag
from assemblyline.odm.models.submission import Submission
from assemblyline.odm.randomizer import random_model_obj, SERVICES, get_random_phrase
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.named import NamedQueue


NUM_SUBMISSIONS = 2
config = forge.get_config()
ds = forge.get_datastore(config)
all_submissions = []
desired_tag_types = [
    'THREAT_ACTOR',
    'NET_IP',
    'NET_DOMAIN_NAME',
    'AV_VIRUS_NAME',
    'IMPLANT_NAME',
    'FILE_ATTRIBUTION',
    'FILE_YARA_RULE',
    'FILE_SUMMARY',
    'EXPLOIT_NAME'
]


def purge_data():
    ds.alert.wipe()
    ds.error.wipe()
    ds.file.wipe()
    ds.result.wipe()
    ds.submission.wipe()
    ds.submission_tags.wipe()
    ds.submission_tree.wipe()


def create_errors_for_file(f, services_done):
    e_list = []
    for _ in range(random.randint(0, 1)):
        e = random_model_obj(Error)

        # Only one error per service per file
        while e.response.service_name in services_done:
            e.response.service_name = random.choice(list(SERVICES.keys()))
        services_done.append(e.response.service_name)

        # Set the sha256
        e.sha256 = f

        e_key = e.build_key()
        e_list.append(e_key)
        ds.error.save(e_key, e)

    return e_list


def create_results_for_file(f, possible_childs=None):
    r_list = []
    services_done = []
    for _ in range(random.randint(2, 5)):
        r = random_model_obj(Result)

        # Only one result per service per file
        while r.response.service_name in services_done:
            r.response.service_name = random.choice(list(SERVICES.keys()))
        services_done.append(r.response.service_name)

        # Set the sha256
        r.sha256 = f

        # Set random extracted files that are not top level
        if not possible_childs:
            r.response.extracted = []
        else:
            for e in r.response.extracted:
                e.sha256 = random.choice(possible_childs)

        # Set random supplementary files that are not top level
        if not possible_childs:
            r.response.supplementary = []
        else:
            for s in r.response.supplementary:
                s.sha256 = random.choice(possible_childs)

        r_key = r.build_key()
        r_list.append(r_key)
        for t in r.result.tags:
            if t.type not in desired_tag_types:
                t.type = random.choice(desired_tag_types)
        ds.result.save(r_key, r)

    return r_list


def create_submission():
    f_list = []
    r_list = []
    e_list = []

    first_level_files = []
    for _ in range(random.randint(4, 8)):
        f = random_model_obj(File)
        byte_str = get_random_phrase(wmin=8, wmax=20).encode()
        sha256 = hashlib.sha256(byte_str).hexdigest()
        f.sha256 = sha256
        ds.file.save(sha256, f)
        f_list.append(sha256)

    for _ in range(random.randint(1, 2)):
        first_level_files.append(f_list.pop())

    for f in first_level_files:
        r_list.extend(create_results_for_file(f, possible_childs=f_list))
        e_list.extend(create_errors_for_file(f, [x.split('.')[1] for x in r_list if x.startswith(f)]))

    for f in f_list:
        r_list.extend(create_results_for_file(f))
        e_list.extend(create_errors_for_file(f, [x.split('.')[1] for x in r_list if x.startswith(f)]))

    s = random_model_obj(Submission)

    s.results = r_list
    s.errors = e_list

    s.error_count = len(e_list)
    s.file_count = len({x[:64] for x in r_list})

    s.files = s.files[:len(first_level_files)]

    fid = 0
    for f in s.files:
        f.sha256 = first_level_files[fid]
        fid += 1

    s.state = "completed"
    s.params.psid = None
    ds.submission.save(s.sid, s)

    return s


@pytest.fixture(scope="module")
def datastore(request):
    for _ in range(NUM_SUBMISSIONS):
        all_submissions.append(create_submission())

    ds.error.commit()
    ds.file.commit()
    ds.result.commit()
    ds.submission.commit()

    request.addfinalizer(purge_data)
    return ds


def test_create_single_alert(datastore):
    persistent_redis = get_client(
        db=config.core.redis.persistent.db,
        host=config.core.redis.persistent.host,
        port=config.core.redis.persistent.port,
        private=False,
    )
    alert_queue = NamedQueue(ALERT_QUEUE_NAME, persistent_redis)
    alerter = Alerter()

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


def test_update_single_alert(datastore):
    persistent_redis = get_client(
        db=config.core.redis.persistent.db,
        host=config.core.redis.persistent.host,
        port=config.core.redis.persistent.port,
        private=False,
    )
    alert_queue = NamedQueue(ALERT_QUEUE_NAME, persistent_redis)
    alerter = Alerter()

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
    r = datastore.result.get(random.choice(child_submission.results))
    for _ in range(random.randint(1, 3)):
        t = random_model_obj(Tag)
        t.type = random.choice(desired_tag_types)
        r.result.tags.append(t)

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

    alert_queue.push(child_ingest_msg.as_primitives())
    alert_type = alerter.run_once()
    assert alert_type == 'update'
    datastore.alert.commit()

    updated_alert = datastore.alert.get(datastore.alert.search(f"sid:{child_submission.sid}",
                                                               fl="id", as_obj=False)['items'][0]['id'])
    assert updated_alert is not None

    assert updated_alert != original_alert
