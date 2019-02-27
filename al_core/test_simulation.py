import random
import json
import hashlib
import pytest
from unittest import mock
from typing import List


from assemblyline.common import forge

from al_core.dispatching.client import DispatchClient
from al_core.dispatching.dispatcher import service_queue_name, ServiceTask
from al_core.middleman.client import MiddlemanClient
from al_core.submission_client import SubmissionClient
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.datastore.stores.es_store import ESStore

from al_core.middleman.run_ingest import MiddlemanIngester
from al_core.middleman.run_internal import MiddlemanInternals
from al_core.middleman.run_submit import MiddlemanSubmitter

from al_core.dispatching.run_files import FileDispatchServer
from al_core.dispatching.run_submissions import SubmissionDispatchServer

from al_core.server_base import ServerBase

from al_core.mocking import clean_redis
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.service import Service
from assemblyline.remote.datatypes.queues.named import NamedQueue


from al_core.dispatching.test_scheduler import dummy_service


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
        while self.running:

            message = self.queue.pop(timeout=1)
            if not message:
                continue
            print(self.service_name, 'has recieved a job', message)

            task = ServiceTask(message)

            file = self.filestore.get(task.fileinfo.sha256)

            instructions = json.loads(file)
            instructions.get(self.service_name, {})

            if instructions.get('failure', False):
                error = None
                if 'error' in instructions:
                    error = Error(instructions['error'])
                self.dispatch_client.service_failed(task, error=error)

            result_data = {
                'classification': 'U',
                'response': {
                    'service_version': '0',
                    'service_name': self.service_name,
                },
                'result': {
                },
                'sha256': task.fileinfo.sha256
            }

            result_data.update(instructions.get('result', {}))

            result = Result(result_data)
            self.dispatch_client.service_finished(task, result)


def test_simulate_core(es_connection, clean_redis):
    threads = []
    filestore = forge.get_filestore()
    try:
        threads: List[ServerBase] = [
            # Start the middleman components
            MiddlemanIngester(datastore=es_connection, redis=clean_redis, persistent_redis=clean_redis),
            MiddlemanSubmitter(datastore=es_connection, redis=clean_redis, persistent_redis=clean_redis),
            MiddlemanInternals(datastore=es_connection, redis=clean_redis, persistent_redis=clean_redis),

            # Start the dispatcher
            FileDispatchServer(datastore=es_connection, redis=clean_redis, redis_persist=clean_redis),
            SubmissionDispatchServer(datastore=es_connection, redis=clean_redis, redis_persist=clean_redis),
        ]

        es_connection.service.delete_matching('*')
        es_connection.service.save('pre', dummy_service('pre', 'extract'))
        threads.append(MockService('pre', es_connection, clean_redis, filestore))
        es_connection.service.save('core-a', dummy_service('core-a', 'core'))
        threads.append(MockService('core-a', es_connection, clean_redis, filestore))
        es_connection.service.save('core-b', dummy_service('core-b', 'core'))
        threads.append(MockService('core-b', es_connection, clean_redis, filestore))
        es_connection.service.save('post', dummy_service('finish', 'post'))
        threads.append(MockService('post', es_connection, clean_redis, filestore))

        for t in threads:
            t.daemon = True
            t.start()

        notification_queue_name = 'test-feedback-queue'
        notification_queue = NamedQueue(notification_queue_name, clean_redis)

        client = MiddlemanClient(clean_redis, clean_redis)

        # =========================================================================

        body = {

        }
        body = json.dumps(body).encode()
        sha256 = hashlib.sha256()
        sha256.update(body)
        filestore.save(sha256.hexdigest(), body)

        client.ingest(
            sha256=sha256.hexdigest(),
            file_size=len(body),
            classification='U',
            metadata={},
            params=dict(
                groups=['user'],
            ),
        )

        result = notification_queue.pop(timeout=5)
        assert result

    finally:
        [t.stop() for t in threads]
        [t.raising_join() for t in threads]

    assert False

