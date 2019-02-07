import logging
import mock
import fakeredis
import pytest
import time


from assemblyline.common import log
from assemblyline.odm.models.submission import SubmissionParams
from assemblyline.odm.models.filescore import FileScore

from .test_worker_ingest import AssemblylineDatastore, MockDatastore, MakeMiddleman, TrueCountTimes
import al_core.middleman.run_submit
from al_core.middleman.middleman import IngestTask, _dup_prefix


@pytest.fixture
def clean_redis():
    return fakeredis.FakeStrictRedis()


def test_submit_simple(clean_redis):
    ds = AssemblylineDatastore(MockDatastore())
    mf = MakeMiddleman()
    mf.assign('running', TrueCountTimes(1))
    mf.assign('client', mock.MagicMock())
    mf.call('unique_queue.push', 0, IngestTask({
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

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('al_core.middleman.run_submit.Middleman', mf):
        al_core.middleman.run_submit.submitter(logger=logger, datastore=ds, volatile=True,
                                               redis=clean_redis, persistent_redis=clean_redis)

    # No tasks should be left in the queue
    mm = mf.instance
    assert mm.unique_queue.pop() is None
    mm.client.submit.assert_called()


def test_submit_duplicate(clean_redis):
    ds = AssemblylineDatastore(MockDatastore())
    mf = MakeMiddleman()
    mf.assign('running', TrueCountTimes(1))
    mf.assign('client', mock.MagicMock())
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
    task.scan_key = task.params.create_filescore_key(task.sha256, [])
    mf.call('scanning.add', task.scan_key, task.as_primitives())
    mf.call('unique_queue.push', 0, task.json())

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('al_core.middleman.run_submit.Middleman', mf):
        al_core.middleman.run_submit.submitter(logger=logger, datastore=ds, volatile=True,
                                               redis=clean_redis, persistent_redis=clean_redis)

    # No tasks should be left in the queue
    mm = mf.instance
    # No tasks should be left in the queue
    assert mm.unique_queue.pop() is None
    # The task should have  been pushed to the duplicates queue
    assert mm.duplicate_queue.length(_dup_prefix + task.scan_key) == 1


def test_existing_score(clean_redis):
    ds = AssemblylineDatastore(MockDatastore())
    ds.filescore.get = mock.MagicMock(return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=time.time())))
    mf = MakeMiddleman()
    mf.assign('running', TrueCountTimes(1))
    mf.assign('client', mock.MagicMock())
    mf.call('unique_queue.push', 0, IngestTask({
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

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('al_core.middleman.run_submit.Middleman', mf):
        al_core.middleman.run_submit.submitter(logger=logger, datastore=ds, volatile=True,
                                               redis=clean_redis, persistent_redis=clean_redis)

    mm = mf.instance
    # No tasks should be left in the queue
    assert mm.unique_queue.pop() is None
    # We should have received a notification about our task
    assert mm.notification_queues['our_queue'].length() == 1

