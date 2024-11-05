import pytest
from unittest import mock
import time

from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.models.user import User
from assemblyline.odm.models.file import File
from assemblyline.odm.randomizer import random_minimal_obj

from assemblyline_core.ingester.ingester import IngestTask, _notification_queue_prefix, Ingester

from mocking import TrueCountTimes


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
def ingest_harness(clean_redis, clean_datastore: AssemblylineDatastore):
    datastore = clean_datastore
    ingester = Ingester(datastore=datastore, redis=clean_redis, persistent_redis=clean_redis)
    ingester.running = TrueCountTimes(1)
    ingester.counter.increment = mock.MagicMock()
    ingester.submit_client.submit = mock.MagicMock()
    return datastore, ingester, ingester.ingest_queue


def test_ingest_simple(ingest_harness):
    datastore, ingester, in_queue = ingest_harness

    user = random_minimal_obj(User)
    user.name = 'user'
    custom_user_groups = ['users', 'the_user']
    user.groups = list(custom_user_groups)
    datastore.user.save('user', user)

    # Send a message with a garbled sha, this should be dropped
    in_queue.push(make_message(
        files={'sha256': '1'*10}
    ))

    # Process garbled message
    ingester.handle_ingest()
    ingester.counter.increment.assert_called_with('error')

    # Send a message that is fine, but has an illegal metadata field
    in_queue.push(make_message(dict(
        metadata={
            'tobig': 'a' * (ingester.config.submission.max_metadata_length + 2),
            'small': '100'
        }
    ), params={'submitter': 'user', 'groups': custom_user_groups}))

    # Process those ok message
    ingester.running.counter = 1
    ingester.handle_ingest()

    # The only task that makes it through though fit these parameters
    task = ingester.unique_queue.pop()
    assert task
    task = IngestTask(task)
    assert task.submission.files[0].sha256 == '0' * 64  # Only the valid sha passed through
    assert 'tobig' not in task.submission.metadata  # The bad metadata was stripped
    assert task.submission.metadata['small'] == '100'  # The valid metadata is unchanged
    assert task.submission.params.submitter == 'user'
    assert task.submission.params.groups == custom_user_groups

    # None of the other tasks should reach the end
    assert ingester.unique_queue.length() == 0
    assert ingester.ingest_queue.length() == 0


def test_ingest_stale_score_exists(ingest_harness):
    datastore, ingester, in_queue = ingest_harness
    get_if_exists = datastore.filescore.get_if_exists
    try:
        # Add a stale file score to the database for every file always
        from assemblyline.odm.models.filescore import FileScore
        datastore.filescore.get_if_exists = mock.MagicMock(
            return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=0))
        )

        # Process a message that hits the stale score
        in_queue.push(make_message())
        ingester.handle_ingest()

        # The stale filescore was retrieved
        datastore.filescore.get_if_exists.assert_called_once()

        # but message was ingested as a cache miss
        task = ingester.unique_queue.pop()
        assert task
        task = IngestTask(task)
        assert task.submission.files[0].sha256 == '0' * 64

        assert ingester.unique_queue.length() == 0
        assert ingester.ingest_queue.length() == 0
    finally:
        datastore.filescore.get_if_exists = get_if_exists


def test_ingest_score_exists(ingest_harness):
    datastore, ingester, in_queue = ingest_harness
    get_if_exists = datastore.filescore.get_if_exists
    try:
        # Add a valid file score for all files
        from assemblyline.odm.models.filescore import FileScore
        datastore.filescore.get_if_exists = mock.MagicMock(
            return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=time.time()))
        )

        # Ingest a file
        in_queue.push(make_message())
        ingester.handle_ingest()

        # No file has made it into the internal buffer => cache hit and drop
        datastore.filescore.get_if_exists.assert_called_once()
        ingester.counter.increment.assert_any_call('cache_hit')
        ingester.counter.increment.assert_any_call('duplicates')
        assert ingester.unique_queue.length() == 0
        assert ingester.ingest_queue.length() == 0
    finally:
        datastore.filescore.get_if_exists = get_if_exists


def test_ingest_groups_custom(ingest_harness):
    datastore, ingester, in_queue = ingest_harness

    user = random_minimal_obj(User)
    user.name = 'user'
    custom_user_groups = ['users', 'the_user']
    user.groups = list(custom_user_groups)
    datastore.user.save('user', user)

    in_queue.push(make_message(params={'submitter': 'user', 'groups': ['group_b']}))
    ingester.handle_ingest()

    task = ingester.unique_queue.pop()
    assert task
    task = IngestTask(task)
    assert task.submission.params.submitter == 'user'
    assert task.submission.params.groups == ['group_b']


def test_ingest_size_error(ingest_harness):
    datastore, ingester, in_queue = ingest_harness

    # Send a rather big file
    submission = make_message(
        files={
            'size': ingester.config.submission.max_file_size + 1,
            # 'ascii': 'abc'
        },
        params={
            'ignore_size': False,
            'never_drop': False
        }
    )
    fo = random_minimal_obj(File)
    fo.sha256 = submission['files'][0]['sha256']
    datastore.file.save(submission['files'][0]['sha256'], fo)
    submission['notification'] = {'queue': 'drop_test'}
    in_queue.push(submission)
    ingester.handle_ingest()

    # No files in the internal buffer
    assert ingester.unique_queue.length() == 0
    assert ingester.ingest_queue.length() == 0

    # A file was dropped
    queue_name = _notification_queue_prefix + submission['notification']['queue']
    queue = ingester.notification_queues[queue_name]
    message = queue.pop()
    assert message is not None

def test_ingest_always_create_submission(ingest_harness):
    datastore, ingester, in_queue = ingest_harness

    # Simulate configuration where we'll always create a submission
    ingester.config.core.ingester.always_create_submission = True
    get_if_exists = datastore.filescore.get_if_exists
    try:
        # Add a valid file score for all files
        from assemblyline.odm.models.filescore import FileScore
        from assemblyline.odm.models.submission import Submission
        datastore.filescore.get_if_exists = mock.MagicMock(
            return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='001', time=time.time()))
        )
        # Create a submission for cache hit
        old_sub = random_minimal_obj(Submission)
        old_sub.sid = '001'
        old_sub.params.psid = '000'
        old_sub = old_sub.as_primitives()
        datastore.submission.save('001', old_sub)

        # Ingest a file
        submission_msg = make_message(message={'sid': '002', 'metadata': {'blah': 'blah'}})
        submission_msg['sid'] = '002'
        in_queue.push(submission_msg)
        ingester.handle_ingest()

        # No file has made it into the internal buffer => cache hit and drop
        datastore.filescore.get_if_exists.assert_called_once()
        ingester.counter.increment.assert_any_call('cache_hit')
        ingester.counter.increment.assert_any_call('duplicates')
        assert ingester.unique_queue.length() == 0
        assert ingester.ingest_queue.length() == 0

        # Check to see if new submission was created
        new_sub = datastore.submission.get_if_exists('002', as_obj=False)
        assert new_sub and new_sub['params']['psid'] == old_sub['sid']

        # Check to see if certain properties are same (anything relating to analysis)
        assert all([old_sub.get(attr) == new_sub.get(attr) \
                    for attr in ['error_count', 'errors', 'file_count', 'files', 'max_score', 'results', 'state', 'verdict']])

        # Check to see if certain properties are different
        # (anything that isn't related to analysis but can be set at submission time)
        assert all([old_sub.get(attr) != new_sub.get(attr) \
                    for attr in ['expiry_ts', 'metadata', 'params', 'times']])

        # Check to see if certain properties have been nullified
        # (properties that are set outside of submission)
        assert not all([new_sub.get(attr) \
                        for attr in ['archived', 'archive_ts', 'to_be_deleted', 'from_archive']])
    finally:
        datastore.filescore.get_if_exists = get_if_exists
