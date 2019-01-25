import time
import logging
import json
import mock
import concurrent.futures

from assemblyline.common import forge
from assemblyline.common import log
from .ingest_worker import ingester
from .middleman import Middleman, IngestTask
from .client import MiddlemanClient


from mocking.datastore import MockDatastore
from assemblyline.datastore.helper import AssemblylineDatastore

class make_middleman:
    def __init__(self):
        self.instance = None

    def __call__(self, *args, **kwargs):
        if not self.instance:
            self.instance = Middleman(*args, **kwargs)
        return self.instance


def send_ingest_messages(middleman_factory, ds, messages):
    while middleman_factory.instance is None:
        time.sleep(0.1)
    mm = middleman_factory.instance
    client = MiddlemanClient()

    try:
        for message in messages:
            send = dict(
                # describe the file being ingested
                sha256='0'*64,
                file_size=100,
                classification='U',
                metadata={},

                # Information about who wants this file ingested
                submitter='user',
                groups=['users'],

                # What services should this be submitted to
                selected_services=[],
                resubmit_services=[],
            )
            send.update(**message)

            client.ingest(**send)
        time.sleep(1)

    finally:
        mm.running = False


def test_ingest_simple():
    ds = AssemblylineDatastore(MockDatastore())
    client = MiddlemanClient()
    middleman_factory = make_middleman()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = pool.submit(send_ingest_messages, middleman_factory, ds, [
        # This submission should be dropped
        dict(
            sha256='1'*10,
        ),
        # This submission should be processed
        dict(
            metadata={
                'tobig': 'a' * (client.config.submission.max_metadata_length + 2),
                'small': '100'
            }
        )
    ])

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('middleman.ingest_worker.Middleman', middleman_factory):
        ingester(logger=logger, datastore=ds)

    mm = middleman_factory.instance
    # The only task that makes it through though fit these parameters
    task = IngestTask(json.loads(mm.unique_queue.pop()))
    assert task.sha256 == '0' * 64
    assert 'tobig' not in task.metadata
    assert task.metadata['small'] == '100'
    print(task.json())

    # None of the other tasks should reach the end
    assert mm.unique_queue.pop() is None

    future.result()


def test_ingest_groups_error():
    ds = AssemblylineDatastore(MockDatastore())
    middleman_factory = make_middleman()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = pool.submit(send_ingest_messages, middleman_factory, ds, [dict(
        groups=[],
    )])

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('middleman.ingest_worker.Middleman', middleman_factory):
        ingester(logger=logger, datastore=ds)

    mm = middleman_factory.instance
    assert mm.unique_queue.pop() is None

    future.result()
