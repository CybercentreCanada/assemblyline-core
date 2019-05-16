import hashlib
import random

import pytest

from al_core.alerter.run_alerter import Alerter, ALERT_QUEUE_NAME
from al_core.ingester.ingester import IngestTask
from assemblyline.common import forge
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.file import File
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.submission import Submission
from assemblyline.odm.randomizer import random_model_obj, SERVICES, get_random_phrase
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.named import NamedQueue


NUM_SUBMISSIONS = 1
config = forge.get_config()
ds = forge.get_datastore(config)
fs = forge.get_filestore(config)
full_file_list = []
all_submissions = []


def purge_submission():
    ds.error.wipe()
    ds.file.wipe()
    ds.result.wipe()
    ds.submission.wipe()
    ds.submission_tags.wipe()
    ds.submission_tree.wipe()

    for f in full_file_list:
        fs.delete(f)

def create_errors_for_file(ds, f, services_done):
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


def create_results_for_file(ds, f, possible_childs=None):
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
        ds.result.save(r_key, r)

    return r_list


def create_submission(ds, fs):
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
        fs.put(sha256, byte_str)
        f_list.append(sha256)
        full_file_list.append(sha256)

    for _ in range(random.randint(1, 2)):
        first_level_files.append(f_list.pop())

    for f in first_level_files:
        r_list.extend(create_results_for_file(ds, f, possible_childs=f_list))
        e_list.extend(create_errors_for_file(ds, f, [x.split('.')[1] for x in r_list if x.startswith(f)]))

    for f in f_list:
        r_list.extend(create_results_for_file(ds, f))
        e_list.extend(create_errors_for_file(ds, f, [x.split('.')[1] for x in r_list if x.startswith(f)]))

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
    ds.submission.save(s.sid, s)

    return s


@pytest.fixture(scope="module")
def datastore(request):
    for _ in range(NUM_SUBMISSIONS):
        all_submissions.append(create_submission(ds, fs))

    ds.error.commit()
    ds.file.commit()
    ds.result.commit()
    ds.submission.commit()

    request.addfinalizer(purge_submission)
    return ds


def test_single(datastore):
    persistent_redis = get_client(
        db=config.core.redis.persistent.db,
        host=config.core.redis.persistent.host,
        port=config.core.redis.persistent.port,
        private=False,
    )
    alert_queue = NamedQueue(ALERT_QUEUE_NAME, persistent_redis)

    alerter = Alerter()
    alerter.start()

    # Generate a task for the submission
    ingest_msg = random_model_obj(IngestTask)
    submission = random.choice(all_submissions)
    ingest_msg.submission.sid = submission.sid
    ingest_msg.submission.metadata = submission.metadata
    ingest_msg.submission.params = submission.params
    ingest_msg.submission.files = submission.files

    alert_queue.push(ingest_msg.as_primitives())
    alerter.join(timeout=10)
    alerter.stop()

