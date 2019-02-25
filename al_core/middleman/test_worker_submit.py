from unittest import mock
import pytest
import time


from assemblyline.odm.models.submission import SubmissionParams
from assemblyline.odm.models.filescore import FileScore

from al_core.middleman.client import MiddlemanClient
from .test_worker_ingest import AssemblylineDatastore, MockDatastore, TrueCountTimes
from al_core.middleman.run_submit import MiddlemanSubmitter
from al_core.middleman.middleman import IngestTask, _dup_prefix
from al_core.submission_client import SubmissionClient

from al_core.mocking import clean_redis


@pytest.fixture
@mock.patch('al_core.middleman.middleman.SubmissionClient', new=mock.MagicMock(spec=SubmissionClient))
def submit_harness(clean_redis):
    """Setup a test environment just file for the ingest tests"""
    datastore = AssemblylineDatastore(MockDatastore())
    submitter = MiddlemanSubmitter(datastore=datastore, redis=clean_redis, persistent_redis=clean_redis)
    client = MiddlemanClient(redis=clean_redis, persistent_redis=clean_redis)
    submitter.running = TrueCountTimes(1)
    return datastore, submitter, client


def test_submit_simple(submit_harness):
    datastore, submitter, client = submit_harness

    # Push a normal ingest task
    submitter.middleman.unique_queue.push(0, IngestTask({
        'params': SubmissionParams({
            'classification': 'U',
            'services': {
                'selected': [],
                'excluded': [],
                'resubmit': [],
            },
            'submitter': 'user',
        }),
        'ingest_time': 0,
        'sha256': '0'*64,
        'file_size': 100,
        'classification': 'U',
        'metadata': {}
    }).json())
    submitter.try_run(volatile=True)

    # The task has been passed to the submit tool and there are no other submissions
    mm = submitter.middleman
    mm.submit_client.submit.assert_called()
    assert mm.unique_queue.pop() is None


def test_submit_duplicate(submit_harness):
    datastore, submitter, client = submit_harness

    # a normal ingest task
    task = IngestTask({
        'params': SubmissionParams({
            'classification': 'U',
            'services': {
                'selected': [],
                'excluded': [],
                'resubmit': [],
            },
            'submitter': 'user',
        }),
        'ingest_time': 0,
        'sha256': '0'*64,
        'file_size': 100,
        'classification': 'U',
        'metadata': {}
    })
    # Make sure the scan key is correct, this is normally done on ingest
    task.scan_key = task.params.create_filescore_key(task.sha256, [])

    # Add this file to the scanning table, so it looks like it has already been submitted + ingest again
    submitter.middleman.scanning.add(task.scan_key, task.as_primitives())
    submitter.middleman.unique_queue.push(0, task.json())

    submitter.try_run(volatile=True)

    # No tasks should be left in the queue
    mm = submitter.middleman
    assert mm.unique_queue.pop() is None
    # The task should have been pushed to the duplicates queue
    assert mm.duplicate_queue.length(_dup_prefix + task.scan_key) == 1


def test_existing_score(submit_harness):
    datastore, submitter, client = submit_harness

    # Set everything to have an existing filestore
    datastore.filescore.get = mock.MagicMock(return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=time.time())))

    # add task to internal queue
    submitter.middleman.unique_queue.push(0, IngestTask({
        'params': SubmissionParams({
            'classification': 'U',
            'services': {
                'selected': [],
                'excluded': [],
                'resubmit': [],
            },
            'submitter': 'user',
        }),
        'ingest_time': 0,
        'sha256': '0'*64,
        'file_size': 100,
        'classification': 'U',
        'metadata': {},
        'notification_queue': 'our_queue'
    }).json())

    submitter.try_run(volatile=True)

    mm = submitter.middleman
    # No tasks should be left in the queue
    assert mm.unique_queue.pop() is None
    # We should have received a notification about our task, since it was already 'done'
    assert mm.notification_queues['our_queue'].length() == 1

