from unittest import mock
import pytest
import time

from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.models.submission import SubmissionParams
from assemblyline.odm.models.filescore import FileScore

from assemblyline_core.ingester.ingester import IngestTask, _dup_prefix, Ingester

from mocking import TrueCountTimes, MockDatastore


@pytest.fixture
def submit_harness(clean_redis):
    """Setup a test environment just file for the ingest tests"""
    datastore = AssemblylineDatastore(MockDatastore())
    submitter = Ingester(datastore=datastore, redis=clean_redis, persistent_redis=clean_redis)
    submitter.running = TrueCountTimes(1)
    submitter.counter.increment = mock.MagicMock()
    submitter.submit_client.submit = mock.MagicMock()
    return datastore, submitter


def test_submit_simple(submit_harness):
    datastore, submitter = submit_harness

    # Push a normal ingest task
    submitter.unique_queue.push(0, IngestTask({
        'submission': {
            'params': SubmissionParams({
                'classification': 'U',
                'description': 'file abc',
                'services': {
                    'selected': [],
                    'excluded': [],
                    'resubmit': [],
                },
                'submitter': 'user',
            }),
            'files': [{
                'sha256': '0' * 64,
                'size': 100,
                'name': 'abc',
            }],
            'metadata': {}
        },
        'ingest_id': '123abc'
    }).as_primitives())
    submitter.handle_submit()

    # The task has been passed to the submit tool and there are no other submissions
    submitter.submit_client.submit.assert_called()
    assert submitter.unique_queue.pop() is None


def test_submit_duplicate(submit_harness):
    datastore, submitter = submit_harness

    # a normal ingest task
    task = IngestTask({
        'submission': {
            'params': SubmissionParams({
                'classification': 'U',
                'description': 'file abc',
                'services': {
                    'selected': [],
                    'excluded': [],
                    'resubmit': [],
                },
                'submitter': 'user',
            }),
            'files': [{
                'sha256': '0' * 64,
                'size': 100,
                'name': 'abc',
            }],
            'metadata': {}
        },
        'ingest_id': 'abc123'
    })
    # Make sure the scan key is correct, this is normally done on ingest
    task.scan_key = task.params.create_filescore_key(task.submission.files[0].sha256, [])

    # Add this file to the scanning table, so it looks like it has already been submitted + ingest again
    submitter.scanning.add(task.scan_key, task.as_primitives())
    submitter.unique_queue.push(0, task.as_primitives())

    submitter.handle_submit()

    # No tasks should be left in the queue
    assert submitter.unique_queue.pop() is None
    # The task should have been pushed to the duplicates queue
    assert submitter.duplicate_queue.length(_dup_prefix + task.scan_key) == 1


def test_existing_score(submit_harness):
    datastore, submitter = submit_harness

    # Set everything to have an existing filestore
    datastore.filescore.get = mock.MagicMock(return_value=FileScore(
        dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=time.time())))

    # add task to internal queue
    submitter.unique_queue.push(0, IngestTask({
        'submission': {
            'params': SubmissionParams({
                'classification': 'U',
                'description': 'file abc',
                'services': {
                    'selected': [],
                    'excluded': [],
                    'resubmit': [],
                },
                'submitter': 'user',
            }),
            'files': [{
                'sha256': '0' * 64,
                'size': 100,
                'name': 'abc',
            }],
            'metadata': {},
            'notification': {
                'queue': 'our_queue'
            }
        },
        'ingest_id': 'abc123'
    }).as_primitives())

    submitter.handle_submit()

    # No tasks should be left in the queue
    assert submitter.unique_queue.pop() is None
    # We should have received a notification about our task, since it was already 'done'
    assert submitter.notification_queues['nq-our_queue'].length() == 1
