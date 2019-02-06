import concurrent.futures
import logging
import mock
import json

from assemblyline.common import log

from .test_worker_ingest import AssemblylineDatastore, MockDatastore, MiddlemanClient, make_middleman, send_messages, IngestTask
from middleman.run_ingest import ingester
from middleman.run_submit import submitter


def test_submit_simple():
    ds = AssemblylineDatastore(MockDatastore())
    client = MiddlemanClient()
    middleman_factory = make_middleman()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = pool.submit(send_messages, middleman_factory, ds, [
        # This submission should be processed
        # dict(
        #     metadata={
        #         'tobig': 'a' * (client.config.submission.max_metadata_length + 2),
        #         'small': '100'
        #     }
        # )
    ])

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
        ingester(logger=logger, datastore=ds)
    future.result()

    mm = middleman_factory.instance
    mm.running = TrueCountTimes(1)

    with mock.patch('middleman.run_ingest.Middleman', middleman_factory):
        submitter(logger=logger, datastore=ds, volatile=True)

    # No tasks should be left in the queue
    assert mm.unique_queue.pop() is None
    mm.client.submit.assert_called()
