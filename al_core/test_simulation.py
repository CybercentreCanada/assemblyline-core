import uuid
import random
import json
import hashlib
import pytest
from unittest import mock
from typing import List

from assemblyline.common import forge
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.datastore.stores.es_store import ESStore
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.service import Service
from assemblyline.odm.models.submission import Submission
from assemblyline.remote.datatypes.counters import MetricCounter
from assemblyline.remote.datatypes.queues.named import NamedQueue

from al_core.dispatching.client import DispatchClient
from al_core.dispatching.dispatcher import service_queue_name, ServiceTask
from al_core.ingester.client import IngesterClient
from al_core.submission_client import SubmissionClient
from al_core.ingester.ingester import IngestTask
from al_core.watcher import WatcherServer

from al_core.ingester.run_ingest import IngesterInput
from al_core.ingester.run_internal import IngesterInternals
from al_core.ingester.run_submit import IngesterSubmitter

from al_core.dispatching.run_files import FileDispatchServer
from al_core.dispatching.run_submissions import SubmissionDispatchServer

from al_core.server_base import ServerBase

from al_core.mocking import clean_redis


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
        self.datastore = datastore
        self.filestore = filestore
        self.hits = dict()
        self.drops = dict()

    def try_run(self):
        while self.running:

            message = self.queue.pop(timeout=1)
            if not message:
                continue
            print(self.service_name, 'has received a job', message['sid'])

            task = ServiceTask(message)

            file = self.filestore.get(task.fileinfo.sha256)

            instructions = json.loads(file)
            instructions = instructions.get(self.service_name, {})
            print(self.service_name, 'following instruction:', instructions)
            hits = self.hits[task.fileinfo.sha256] = self.hits.get(task.fileinfo.sha256, 0) + 1

            if 'drop' in instructions:
                if instructions['drop'] >= hits:
                    self.drops[task.fileinfo.sha256] = self.drops.get(task.fileinfo.sha256, 0) + 1
                    continue

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
            result_key = instructions.get('result_key', uuid.uuid4().hex)
            self.datastore.result.save(result_key, result)
            self.dispatch_client.service_finished(task, result, result_key)


@pytest.fixture
def replace_config(request):
    old_get_config = forge._get_config

    def replace():
        forge._get_config = old_get_config
    request.addfinalizer(replace)

    def new_config(*_, **__):
        config = old_get_config()
        config.core.dispatcher.timeout = 3
        return config
    forge._get_config = new_config



@mock.patch('al_core.ingester.ingester.MetricCounter', new=mock.MagicMock(spec=MetricCounter))
@mock.patch('al_core.dispatching.dispatcher.MetricCounter', new=mock.MagicMock(spec=MetricCounter))
def test_simulate_core(es_connection, clean_redis, replace_config):
    ds = es_connection
    threads = []
    filestore = forge.get_filestore()
    try:
        threads: List[ServerBase] = [
            # Start the ingester components
            IngesterInput(datastore=ds, redis=clean_redis, persistent_redis=clean_redis),
            IngesterSubmitter(datastore=ds, redis=clean_redis, persistent_redis=clean_redis),
            IngesterInternals(datastore=ds, redis=clean_redis, persistent_redis=clean_redis),

            # Start the dispatcher
            FileDispatchServer(datastore=ds, redis=clean_redis, redis_persist=clean_redis),
            SubmissionDispatchServer(datastore=ds, redis=clean_redis, redis_persist=clean_redis),
            WatcherServer(redis=clean_redis),
        ]

        ds.service.delete_matching('*')
        ds.service.save('pre', dummy_service('pre', 'extract'))
        threads.append(MockService('pre', ds, clean_redis, filestore))
        pre_service = threads[-1]
        ds.service.save('core-a', dummy_service('core-a', 'core'))
        threads.append(MockService('core-a', ds, clean_redis, filestore))
        ds.service.save('core-b', dummy_service('core-b', 'core'))
        threads.append(MockService('core-b', ds, clean_redis, filestore))
        ds.service.save('post', dummy_service('finish', 'post'))
        threads.append(MockService('post', ds, clean_redis, filestore))

        for t in threads:
            t.daemon = True
            t.start()

        client = IngesterClient(clean_redis, clean_redis)

        # =========================================================================

        body = {
            'salt': uuid.uuid4().hex
        }
        body = json.dumps(body).encode()
        sha256 = hashlib.sha256()
        sha256.update(body)
        filestore.save(sha256.hexdigest(), body)

        # Submit two identical jobs, check that they get deduped by ingester
        for _ in range(2):
            client.ingest(
                sha256=sha256.hexdigest(),
                file_size=len(body),
                classification='U',
                metadata={},
                params=dict(
                    groups=['user'],
                ),
                notification_queue='1',
                notification_threshold=0
            )

        notification_queue = NamedQueue('1', clean_redis)
        first_task = notification_queue.pop(timeout=5)
        second_task = notification_queue.pop(timeout=5)

        # Submit the same body, but change a parameter so the cache key misses,
        client.ingest(
            sha256=sha256.hexdigest(),
            file_size=len(body),
            classification='U',
            metadata={},
            params=dict(
                groups=['user'],
                max_extracted=10000
            ),
            notification_queue='2',
            notification_threshold=0
        )

        notification_queue = NamedQueue('2', clean_redis)
        third_task = notification_queue.pop(timeout=5)

        body = {
            'salt': uuid.uuid4().hex,
            'pre': {'drop': 1}
        }
        body = json.dumps(body).encode()
        sha256 = hashlib.sha256()
        sha256.update(body)
        filestore.save(sha256.hexdigest(), body)

        # This time have a service drop, this is comparable to a crash in the service server
        client.ingest(
            sha256=sha256.hexdigest(),
            file_size=len(body),
            classification='U',
            metadata={},
            params=dict(
                groups=['user'],
                max_extracted=10000
            ),
            notification_queue='drop',
            notification_threshold=0
        )

        notification_queue = NamedQueue('drop', clean_redis)
        dropped_task = notification_queue.pop(timeout=10)
        assert dropped_task and IngestTask(dropped_task)
        assert pre_service.drops[sha256.hexdigest()] == 1
        assert pre_service.hits[sha256.hexdigest()] == 2

    finally:
        [t.stop() for t in threads]
        [t.raising_join() for t in threads]

    # One of the submission will get processed fully
    first_task = IngestTask(first_task)
    first_submission: Submission = ds.submission.get(first_task.sid)
    assert first_submission.state == 'completed'
    assert len(first_submission.files) == 1
    assert len(first_submission.results) == 4

    # The other will get processed as a duplicate
    # (Which one is the 'real' one and which is the duplicate isn't important for our purposes)
    second_task = IngestTask(second_task)
    assert second_task.sid == first_task.sid

    # The third task should not be deduplicated by ingester, so will have a different submission
    third_task = IngestTask(third_task)
    third_submission: Submission = ds.submission.get(third_task.sid)
    assert third_submission.state == 'completed'
    assert first_submission.sid != third_submission.sid
    assert len(third_submission.files) == 1
    assert len(third_submission.results) == 4



