import logging
import json
from unittest import mock
import time

from assemblyline.common import log
from .run_ingest import ingester
from al_core.middleman.middleman import Middleman, IngestTask
from .client import MiddlemanClient


from al_core.mocking.datastore import MockDatastore
from assemblyline.datastore.helper import AssemblylineDatastore


class TrueCountTimes:
    def __init__(self, count):
        self.counter = count

    def __bool__(self):
        self.counter -= 1
        return self.counter >= 0


def rgetattr(obj, path):
    if isinstance(path, str):
        path = path.split('.')
    if len(path) == 1:
        return getattr(obj, path[0])
    return rgetattr(getattr(obj, path[0]), path[1:])


class MakeMiddleman:
    def __init__(self):
        self.instance = None
        self.calls = []
        self.client_calls = []
        self.assignments = []
        self.client = MiddlemanClient()

    def __call__(self, *args, **kwargs):
        if not self.instance:
            self.instance = Middleman(*args, **kwargs)

            for call in self.calls:
                method = rgetattr(self.instance, call[0])
                method(*call[1], **call[2])

            for call in self.assignments:
                setattr(self.instance, call[0], call[1])

            for method, args, kwargs in self.client_calls:
                method = rgetattr(self.client, method)
                print(len(args))
                print(len(kwargs))
                method(*args, **kwargs)

        return self.instance

    def call(self, method, *args, **kwargs):
        self.calls.append((method, args, kwargs))

    def client_call(self, method, *args, **kwargs):
        self.client_calls.append((method, args, kwargs))

    def assign(self, attribute_name, value):
        self.assignments.append((attribute_name, value))


def make_message(**message):
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


@mock.patch('al_core.middleman.middleman.SubmissionTool', new=mock.MagicMock())
def test_ingest_simple():
    ds = AssemblylineDatastore(MockDatastore())
    client = MiddlemanClient()
    middleman_factory = MakeMiddleman()
    middleman_factory.assign("running", TrueCountTimes(2))
    middleman_factory.client_call('ingest', **make_message(sha256='1'*10))
    middleman_factory.client_call('ingest', **make_message(
        metadata={
            'tobig': 'a' * (client.config.submission.max_metadata_length + 2),
            'small': '100'
        }
    ))

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
        ingester(logger=logger, datastore=ds)

    mm = middleman_factory.instance
    # The only task that makes it through though fit these parameters
    task = mm.unique_queue.pop()
    assert task
    task = IngestTask(json.loads(task))
    assert task.sha256 == '0' * 64
    assert 'tobig' not in task.metadata
    assert task.metadata['small'] == '100'

    # None of the other tasks should reach the end
    assert mm.unique_queue.length() == 0
    assert mm.ingest_queue.length() == 0


@mock.patch('al_core.middleman.middleman.SubmissionTool', new=mock.MagicMock())
def test_ingest_stale_score_exists():
    from assemblyline.odm.models.filescore import FileScore
    ds = AssemblylineDatastore(MockDatastore())
    ds.filescore.get = mock.MagicMock(return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=0)))
    middleman_factory = MakeMiddleman()
    middleman_factory.assign("running", TrueCountTimes(1))
    middleman_factory.client_call('ingest', **make_message())

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
        ingester(logger=logger, datastore=ds)

    mm = middleman_factory.instance

    task = mm.unique_queue.pop()
    assert task
    task = IngestTask(json.loads(task))
    assert task.sha256 == '0' * 64

    assert mm.unique_queue.length() == 0
    assert mm.ingest_queue.length() == 0


@mock.patch('al_core.middleman.middleman.SubmissionTool', new=mock.MagicMock())
def test_ingest_score_exists():
    from assemblyline.odm.models.filescore import FileScore
    ds = AssemblylineDatastore(MockDatastore())
    ds.filescore.get = mock.MagicMock(return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=time.time())))
    middleman_factory = MakeMiddleman()
    middleman_factory.assign("running", TrueCountTimes(1))
    middleman_factory.client_call('ingest', **make_message())

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
        ingester(logger=logger, datastore=ds)

    mm = middleman_factory.instance
    assert mm.unique_queue.length() == 0
    assert mm.ingest_queue.length() == 0


@mock.patch('al_core.middleman.middleman.SubmissionTool', new=mock.MagicMock())
def test_ingest_groups_error():
    ds = AssemblylineDatastore(MockDatastore())
    middleman_factory = MakeMiddleman()
    middleman_factory.assign("running", TrueCountTimes(1))
    middleman_factory.client_call('ingest', **make_message(
        params={'groups': []},
    ))

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
        ingester(logger=logger, datastore=ds)

    mm = middleman_factory.instance
    assert mm.unique_queue.length() == 0
    assert mm.ingest_queue.length() == 0


@mock.patch('al_core.middleman.middleman.SubmissionTool', new=mock.MagicMock())
def test_ingest_size_error():
    ds = AssemblylineDatastore(MockDatastore())
    middleman_factory = MakeMiddleman()
    middleman_factory.assign("running", TrueCountTimes(1))
    middleman_factory.client_call('ingest', **make_message(
        file_size=10**10,
    ))

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
        ingester(logger=logger, datastore=ds)

    mm = middleman_factory.instance
    assert mm.unique_queue.length() == 0
    assert mm.ingest_queue.length() == 0
    assert mm.drop_queue.pop() is not None
    assert mm.drop_queue.length() == 0
