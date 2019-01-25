import time
import logging
import json
import mock
import concurrent.futures

from assemblyline.common import forge
from assemblyline.common import log
from .submit_worker import submitter
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


def send_inner_message(middleman_factory, ds, messages):
    while middleman_factory.instance is None:
        time.sleep(0.1)
    mm = middleman_factory.instance
    # client = MiddlemanClient()

    try:
        for message in messages:
            mm.unique_queue.push(100, message)
        time.sleep(1)

    finally:
        mm.running = False


def test_ingest_simple():
    ds = AssemblylineDatastore(MockDatastore())
    client = MiddlemanClient()
    middleman_factory = make_middleman()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = pool.submit(send_inner_message, middleman_factory, ds, [
        # Internal message taken from the output of the ingestor test
        {"ignore_size": False, "never_drop": False, "ignore_cache": False, "deep_scan": False, "ignore_dynamic_recursion_prevention": False, "ignore_filtering": False, "max_extracted": 100, "max_supplementary": 100, "completed_queue": "m-complete", "notification_queue": "", "ingest_time": "2019-01-25T20:46:12.022572Z", "scan_key": "5e4d36261946db3fe0bd24177e9608f8v0", "priority": 20.0, "sha256": "0000000000000000000000000000000000000000000000000000000000000000", "file_size": 100, "classification": "U", "metadata": {"small": "100"}, "description": "Bulk: 0000000000000000000000000000000000000000000000000000000000000000", "submitter": "user", "groups": ["users"], "selected_services": [], "resubmit_services": [], "params": {}}
    ])

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.submitter')

    with mock.patch('middleman.submit_worker.Middleman', middleman_factory):
        submitter(logger=logger, datastore=ds)

    mm = middleman_factory.instance
    assert mm.unique_queue.pop() is None

    future.result()
