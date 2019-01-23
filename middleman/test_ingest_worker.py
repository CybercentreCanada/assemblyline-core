import time
import logging
import mock
import concurrent.futures

from assemblyline.common import forge
from assemblyline.common import log
from .ingest_worker import ingester
from .middleman import Middleman
from .client import MiddlemanClient


class make_middleman:
    def __init__(self):
        self.instance = None

    def __call__(self, *args, **kwargs):
        if not self.instance:
            self.instance = Middleman(*args, **kwargs)
        return self.instance


def send_messages(middleman_factory):
    while middleman_factory.instance is None:
        time.sleep(0.1)
    mm = middleman_factory.instance
    client = MiddlemanClient()

    try:
        client.ingest(
            # parameters that control middleman's handling of the task
            # ignore_size = odm.Boolean(default=False)
            # never_drop = odm.Boolean(default=False)
            # ignore_cache = odm.Boolean(default=False)

            # describe the file being ingested
            sha256='0'*64,
            file_size=100,
            classification='U',
            metadata={},

            # Information about who wants this file ingested
            submitter='user',
            groups=['abc', 'users'],

            # What services should this be submitted to
            selected_services=[],
            resubmit_services=[],
        )

        time.sleep(1)
        print(mm.unique_queue.pop())


    finally:
        mm.running = False


def test_ingest_worker():
    middleman_factory = make_middleman()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = pool.submit(send_messages, middleman_factory)

    log.init_logging("middleman")
    logger = logging.getLogger('assemblyline.middleman.ingester')

    with mock.patch('middleman.ingest_worker.Middleman', middleman_factory):
        ingester(logger)

    future.result()
