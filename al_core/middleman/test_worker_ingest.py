import time
import logging
import json
import mock
import concurrent.futures

from assemblyline.common import log
from .run_ingest import ingester
from .middleman import Middleman, IngestTask
from .client import MiddlemanClient


from mocking.datastore import MockDatastore
from assemblyline.datastore.helper import AssemblylineDatastore



class TrueCountTimes:
    def __init__(self, count):
        self.counter = count

    def __bool__(self):
        self.counter -= 1
        return self.counter >= 0


class MakeMiddleman:
    def __init__(self):
        self.instance = None
        self.calls = []
        self.assignments = []

    def __call__(self, *args, **kwargs):
        if not self.instance:
            self.instance = Middleman(*args, **kwargs)

            for call in self.calls:
                method = getattr(self.instance, call[0])
                method(*call[1], **call[2])

            for call in self.assignments:
                setattr(self.instance, call[0], call[1])

        return self.instance

    def call(self, method, *args, **kwargs):
        self.calls.append((method, args, kwargs))

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


# def send_messages(middleman_factory, ds, messages):
#     while middleman_factory.instance is None:
#         time.sleep(0.1)
#     mm = middleman_factory.instance
#     client = MiddlemanClient()
#
#     try:
#         for message in messages:
#             send = dict(
#                 # describe the file being ingested
#                 sha256='0'*64,
#                 file_size=100,
#                 classification='U',
#                 metadata={},
#
#                 # Information about who wants this file ingested
#                 params={
#                     'submitter': 'user',
#                     'groups': ['users'],
#                 }
#             )
#             send.update(**message)
#
#             client.ingest(**send)
#         time.sleep(0.1)
#
#     finally:
#         mm.running = False


def test_ingest_simple():
    ds = AssemblylineDatastore(MockDatastore())
    client = MiddlemanClient()
    middleman_factory = MakeMiddleman()
    middleman_factory.call('client.ingest', make_message(sha256='1'*10))
    middleman_factory.call('client.ingest', make_message(
        metadata={
            'tobig': 'a' * (client.config.submission.max_metadata_length + 2),
            'small': '100'
        }
    ))

    # pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    # future = pool.submit(send_messages, middleman_factory, ds, [
    #     # This submission should be dropped
    #     dict(
    #         sha256='1'*10,
    #     ),
    #     # This submission should be processed
    #     dict(
    #         metadata={
    #             'tobig': 'a' * (client.config.submission.max_metadata_length + 2),
    #             'small': '100'
    #         }
    #     )
    # ])

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
        ingester(logger=logger, datastore=ds)
    future.result()

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


# def test_ingest_stale_score_exists():
#     from assemblyline.odm.models.filescore import FileScore
#     ds = AssemblylineDatastore(MockDatastore())
#     ds.filescore.get = mock.MagicMock(return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=0)))
#     middleman_factory = MakeMiddleman()
#     pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
#     future = pool.submit(send_messages, middleman_factory, ds, [{}])
#
#     log.init_logging("middleman")
#     logger = logging.getLogger('assemblyline.middleman.ingester')
#
#     with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
#         ingester(logger=logger, datastore=ds)
#     future.result()
#
#     mm = middleman_factory.instance
#
#     task = mm.unique_queue.pop()
#     assert task
#     task = IngestTask(json.loads(task))
#     assert task.sha256 == '0' * 64
#
#     assert mm.unique_queue.length() == 0
#     assert mm.ingest_queue.length() == 0
#
#
# def test_ingest_score_exists():
#     from assemblyline.odm.models.filescore import FileScore
#     ds = AssemblylineDatastore(MockDatastore())
#     ds.filescore.get = mock.MagicMock(return_value=FileScore(dict(psid='000', expiry_ts=0, errors=0, score=10, sid='000', time=time.time())))
#     middleman_factory = MakeMiddleman()
#     pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
#     future = pool.submit(send_messages, middleman_factory, ds, [{}])
#
#     log.init_logging("middleman")
#     logger = logging.getLogger('assemblyline.middleman.ingester')
#
#     with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
#         ingester(logger=logger, datastore=ds)
#     future.result()
#
#     mm = middleman_factory.instance
#     assert mm.unique_queue.length() == 0
#     assert mm.ingest_queue.length() == 0
#
#
# def test_ingest_groups_error():
#     ds = AssemblylineDatastore(MockDatastore())
#     middleman_factory = MakeMiddleman()
#     pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
#     future = pool.submit(send_messages, middleman_factory, ds, [{
#         'params': {'groups': []},
#     }])
#
#     log.init_logging("middleman")
#     logger = logging.getLogger('assemblyline.middleman.ingester')
#
#     with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
#         ingester(logger=logger, datastore=ds)
#     future.result()
#
#     mm = middleman_factory.instance
#     assert mm.unique_queue.length() == 0
#     assert mm.ingest_queue.length() == 0
#
#
# def test_ingest_size_error():
#     ds = AssemblylineDatastore(MockDatastore())
#     middleman_factory = MakeMiddleman()
#     pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
#     future = pool.submit(send_messages, middleman_factory, ds, [{
#         'file_size': 10**10,
#     }])
#
#     log.init_logging("middleman")
#     logger = logging.getLogger('assemblyline.middleman.ingester')
#
#     with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
#         ingester(logger=logger, datastore=ds)
#     future.result()
#
#     mm = middleman_factory.instance
#     assert mm.unique_queue.length() == 0
#     assert mm.ingest_queue.length() == 0
#     assert mm.drop_queue.pop() is not None
#     assert mm.drop_queue.length() == 0
