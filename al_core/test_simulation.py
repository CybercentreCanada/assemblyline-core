import random
import json
import pytest
from typing import List

from al_core.dispatching.client import DispatchClient
from al_core.dispatching.dispatcher import service_queue_name, ServiceTask
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.datastore.stores.es_store import ESStore

from al_core.middleman.run_ingest import MiddlemanIngester
from al_core.middleman.run_internal import MiddlemanInternals
from al_core.middleman.run_submit import MiddlemanSubmitter

from al_core.dispatching.run_files import FileDispatchServer
from al_core.dispatching.run_submissions import SubmissionDispatchServer

from al_core.server_base import ServerBase

from al_core.mocking import clean_redis
from assemblyline.odm.models.service import Service
from assemblyline.remote.datatypes.queues.named import NamedQueue


@pytest.fixture(scope='module')
def es_connection():
    document_store = ESStore(['127.0.0.1'])
    if not document_store.ping():
        return pytest.skip("Connection to the Elasticsearch server failed. This test cannot be performed...")
    return AssemblylineDatastore(document_store)


class MockService(ServerBase):
    """Replaces everything past the dispatcher.

    Including service API, in the future probably include that in this test.
    """
    def __init__(self, name, datastore, redis, filestore):
        super().__init__('service.'+name)
        self.service_name = name
        self.queue = NamedQueue(service_queue_name(name), redis)
        self.dispatch_client = DispatchClient(datastore, redis)
        self.filestore = filestore

    def try_run(self):
        while True:
            task = ServiceTask(self.queue.pop())
            file = self.filestore.get(task.file_hash)

            instructions = json.loads(file)
            instructions.get(self.service_name, {})

            if instructions.get('failure', False):
                self.dispatch_client.service_failed(task)

            self.dispatcher.service_finished(task, result)


def test_simulate_core(es_connection, clean_redis):
    from assemblyline.common import forge
    config = forge.get_config()

    threads: List[ServerBase] = [
        # Start the middleman components
        MiddlemanIngester(datastore=es_connection, redis=clean_redis, persistent_redis=clean_redis),
        MiddlemanSubmitter(datastore=es_connection, redis=clean_redis, persistent_redis=clean_redis),
        MiddlemanInternals(datastore=es_connection, redis=clean_redis, persistent_redis=clean_redis),

        # Start the dispatcher
        FileDispatchServer(datastore=es_connection, redis=clean_redis, redis_persist=clean_redis),
        SubmissionDispatchServer(datastore=es_connection, redis=clean_redis, redis_persist=clean_redis),
    ]

    for stage in config.services.stages:
        for num in range(3):
            service = Service({
                'name': f'{stage}-{num}',
                'stage': stage,
                'version': '0',
                'category': random.choice(config.services.categories),
                'accepts': '',
                'rejects': '',
                'realm': 'abc',
                'repo': '/dev/null',
                'enabled': True
            })
            es_connection.service.save(service.name, service)

    [t.start() for t in threads]




    [t.stop() for t in threads]


    [t.raising_join() for t in threads]

    assert False

