
import json
import pytest
from unittest import mock
import time


from al_core.ingester.run_ingest import IngesterInput
from al_core.ingester.ingester import IngestTask
from .client import IngesterClient
from al_core.submission_client import SubmissionClient

from al_core.mocking.datastore import MockDatastore
from assemblyline.datastore.helper import AssemblylineDatastore

from al_core.mocking import clean_redis

class TrueCountTimes:
    """A helper object that replaces a boolean, switching from true to false after its value is read."""
    def __init__(self, count):
        self.counter = count

    def __bool__(self):
        self.counter -= 1
        return self.counter >= 0


def make_message(**message):
    """A helper function to fill in some fields that are largely invariant across tests."""
    send = dict(
        # describe the file being ingested
        sha256='0'*64,
        file_size=100,
        classification='U',
        metadata={},

        # Information about who wants this file ingested
        params={
            'submitter': 'user',
            'groups': ['users'],
        }
    )
    send.update(**message)
    return send


@pytest.fixture
@mock.patch('al_core.ingester.ingester.SubmissionClient', new=mock.MagicMock(spec=SubmissionClient))
def ingest_harness(clean_redis):
    """"Setup a test environment.

    By using a fake redis and datastore, we:
        a) ensure that this test runs regardless of any code errors in replaced modules
        b) ensure that the datastore and redis is EMPTY every time the test runs
           isolating this test from any other test run at the same time
    """
    datastore = AssemblylineDatastore(MockDatastore())
    ingester = IngesterInput(datastore=datastore, redis=clean_redis, persistent_redis=clean_redis)
    client = IngesterClient(redis=clean_redis, persistent_redis=clean_redis)
    ingester.running = TrueCountTimes(1)
    return datastore, ingester, client

def test_ingest_simple(ingest_harness):
    datastore, ingester, client = ingest_harness
    # Let the ingest loop run an extra time because we send two messages
    ingester.running.counter += 1

    # Send a message with a garbled sha, this should be dropped
    client.ingest(**make_message(sha256='1'*10))

    # Send a message that is fine, but has an illegal metadata field
    client.ingest(**make_message(
        metadata={
            'tobig': 'a' * (client.config.submission.max_metadata_length + 2),
            'small': '100'
        }
    ))

    # Process those messages
    ingester.try_run()

    mm = ingester.ingester
    # The only task that makes it through though fit these parameters
    task = mm.unique_queue.pop()
    assert task
    task = IngestTask(json.loads(task))
    assert task.sha256 == '0' * 64  # Only the valid sha passed through
    assert 'tobig' not in task.metadata  # The bad metadata was stripped
    assert task.metadata['small'] == '100'  # The valid metadata is unchanged

    # None of the other tasks should reach the end
    assert mm.unique_queue.length() == 0
    assert mm.ingest_queue.length() == 0


def test_ingest_stale_score_exists(ingest_harness):
    datastore, ingester, client = ingest_harness

    # Add a stale file score to the database for every file always
    from assemblyline.odm.models.filescore import FileScore
    datastore.filescore.get = mock.MagicMock(return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=0)))

    # Process a message that hits the stale score
    client.ingest(**make_message())
    ingester.try_run()

    # The stale filescore was retrieved
    datastore.filescore.get.assert_called_once()

    # but message was ingested as a cache miss
    mm = ingester.ingester
    task = mm.unique_queue.pop()
    assert task
    task = IngestTask(json.loads(task))
    assert task.sha256 == '0' * 64

    assert mm.unique_queue.length() == 0
    assert mm.ingest_queue.length() == 0


def test_ingest_score_exists(ingest_harness):
    datastore, ingester, client = ingest_harness

    # Add a valid file score for all files
    from assemblyline.odm.models.filescore import FileScore
    datastore.filescore.get = mock.MagicMock(return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=time.time())))

    # Ingest a file
    client.ingest(**make_message())
    ingester.try_run()

    # No file has made it into the internal buffer => cache hit and drop
    datastore.filescore.get.assert_called_once()
    assert ingester.ingester.unique_queue.length() == 0
    assert ingester.ingester.ingest_queue.length() == 0


def test_ingest_groups_error(ingest_harness):
    datastore, ingester, client = ingest_harness

    # Send a message with invalid group parameter, and user data missing
    client.ingest(**make_message(params={'groups': []}))
    ingester.try_run()

    # dropped file with no known user
    assert ingester.ingester.unique_queue.length() == 0
    assert ingester.ingester.ingest_queue.length() == 0


def test_ingest_size_error(ingest_harness):
    datastore, ingester, client = ingest_harness

    # Send a rather big file
    client.ingest(**make_message(file_size=10**10))
    ingester.try_run()

    # No files in the internal buffer
    mm = ingester.ingester
    assert mm.unique_queue.length() == 0
    assert mm.ingest_queue.length() == 0

    # A file was dropped
    assert mm.drop_queue.pop(blocking=False) is not None
    assert mm.drop_queue.length() == 0
