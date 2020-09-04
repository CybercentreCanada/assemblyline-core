import pytest
from unittest import mock
import time

from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.common.metrics import MetricsFactory
from assemblyline.odm.models.user import User
from assemblyline.odm.randomizer import random_minimal_obj

from assemblyline_core.ingester.run_ingest import IngesterInput
from assemblyline_core.ingester.ingester import IngestTask
from assemblyline_core.submission_client import SubmissionClient

from .mocking import MockDatastore, TrueCountTimes, clean_redis


def make_message(message=None, files=None, params=None):
    """A helper function to fill in some fields that are largely invariant across tests."""
    send = dict(
        # describe the file being ingested
        files=[{
            'sha256': '0'*64,
            'size': 100,
            'name': 'abc'
        }],
        metadata={},

        # Information about who wants this file ingested
        params={
            'description': 'file abc',
            'submitter': 'user',
            'groups': ['users'],
        }
    )
    send.update(**(message or {}))
    send['files'][0].update(files or {})
    send['params'].update(**(params or {}))
    return send


@pytest.fixture
@mock.patch('assemblyline_core.ingester.ingester.SubmissionClient', new=mock.MagicMock(spec=SubmissionClient))
@mock.patch('assemblyline_core.ingester.ingester.MetricsFactory', new=mock.MagicMock(spec=MetricsFactory))
def ingest_harness(clean_redis):
    """"Setup a test environment.

    By using a fake redis and datastore, we:
        a) ensure that this test runs regardless of any code errors in replaced modules
        b) ensure that the datastore and redis is EMPTY every time the test runs
           isolating this test from any other test run at the same time
    """
    datastore = AssemblylineDatastore(MockDatastore())
    ingester = IngesterInput(datastore=datastore, redis=clean_redis, persistent_redis=clean_redis)
    ingester.running = TrueCountTimes(1)
    return datastore, ingester, ingester.ingester.ingest_queue


def test_ingest_simple(ingest_harness):
    datastore, ingester, in_queue = ingest_harness

    user = random_minimal_obj(User)
    user.name = 'user'
    custom_user_groups = ['users', 'the_user']
    user.groups = list(custom_user_groups)
    datastore.user.save('user', user)

    # Let the ingest loop run an extra time because we send two messages
    ingester.running.counter += 1

    # Send a message with a garbled sha, this should be dropped
    in_queue.push(make_message(
        files={'sha256': '1'*10}
    ))

    with pytest.raises(ValueError):
        # Process garbled message
        ingester.try_run(volatile=True)

    # Send a message that is fine, but has an illegal metadata field
    in_queue.push(make_message(dict(
        metadata={
            'tobig': 'a' * (ingester.ingester.config.submission.max_metadata_length + 2),
            'small': '100'
        }
    ), params={'submitter': 'user', 'groups': []}))

    # Process those ok message
    ingester.try_run(volatile=True)

    mm = ingester.ingester
    # The only task that makes it through though fit these parameters
    task = mm.unique_queue.pop()
    assert task
    task = IngestTask(task)
    assert task.submission.files[0].sha256 == '0' * 64  # Only the valid sha passed through
    assert 'tobig' not in task.submission.metadata  # The bad metadata was stripped
    assert task.submission.metadata['small'] == '100'  # The valid metadata is unchanged
    assert task.submission.params.submitter == 'user'
    assert task.submission.params.groups == custom_user_groups

    # None of the other tasks should reach the end
    assert mm.unique_queue.length() == 0
    assert mm.ingest_queue.length() == 0


def test_ingest_stale_score_exists(ingest_harness):
    datastore, ingester, in_queue = ingest_harness

    # Add a stale file score to the database for every file always
    from assemblyline.odm.models.filescore import FileScore
    datastore.filescore.get = mock.MagicMock(
        return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=0))
    )

    # Process a message that hits the stale score
    in_queue.push(make_message())
    ingester.try_run()

    # The stale filescore was retrieved
    datastore.filescore.get.assert_called_once()

    # but message was ingested as a cache miss
    mm = ingester.ingester
    task = mm.unique_queue.pop()
    assert task
    task = IngestTask(task)
    assert task.submission.files[0].sha256 == '0' * 64

    assert mm.unique_queue.length() == 0
    assert mm.ingest_queue.length() == 0


def test_ingest_score_exists(ingest_harness):
    datastore, ingester, in_queue = ingest_harness

    # Add a valid file score for all files
    from assemblyline.odm.models.filescore import FileScore
    datastore.filescore.get = mock.MagicMock(
        return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=time.time()))
    )

    # Ingest a file
    in_queue.push(make_message())
    ingester.try_run()

    # No file has made it into the internal buffer => cache hit and drop
    datastore.filescore.get.assert_called_once()
    assert ingester.ingester.unique_queue.length() == 0
    assert ingester.ingester.ingest_queue.length() == 0


def test_ingest_groups_custom(ingest_harness):
    datastore, ingester, in_queue = ingest_harness

    user = random_minimal_obj(User)
    user.name = 'user'
    custom_user_groups = ['users', 'the_user']
    user.groups = list(custom_user_groups)
    datastore.user.save('user', user)

    in_queue.push(make_message(params={'submitter': 'user', 'groups': ['group_b']}))
    ingester.try_run()

    mm = ingester.ingester
    task = mm.unique_queue.pop()
    assert task
    task = IngestTask(task)
    assert task.submission.params.submitter == 'user'
    assert task.submission.params.groups == ['group_b']


def test_ingest_size_error(ingest_harness):
    datastore, ingester, in_queue = ingest_harness
    mm = ingester.ingester
    mm._notify_drop = mock.MagicMock()

    # Send a rather big file
    in_queue.push(make_message(files={'size': 10**10}))
    ingester.try_run(volatile=True)

    # No files in the internal buffer
    assert mm.unique_queue.length() == 0
    assert mm.ingest_queue.length() == 0

    # A file was dropped
    mm._notify_drop.assert_called_once()
