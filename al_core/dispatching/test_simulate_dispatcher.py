import logging
import pytest
from unittest import mock
import time
import random
import threading

from retrying import retry

from assemblyline.remote.datatypes.queues.named import NamedQueue
from redis.exceptions import ConnectionError

from al_core.dispatching.run_submissions import SubmissionDispatchServer
from al_core.dispatching.run_files import FileDispatchServer
from assemblyline.datastore.stores.es_store import ESStore
import assemblyline.odm as odm
import assemblyline.odm.models as models
import assemblyline.odm.models.submission
import assemblyline.odm.models.file
import assemblyline.odm.models.error
import assemblyline.odm.models.config

from assemblyline.odm.randomizer import random_model_obj

from assemblyline.odm.models.result import Result
# from assemblyline.odm.models.error import Error
from al_core.dispatching.dispatcher import service_queue_name, ServiceTask
# from configuration import Service, Scheduler
# from configuration import ConfigManager, Service


class SetupException(Exception):
    pass


@pytest.fixture(scope='session')
def redis_connection():
    from assemblyline.remote.datatypes import get_client
    c = get_client(None, None, None, False)
    try:
        ret_val = c.ping()
        if ret_val:
            return c
    except ConnectionError:
        pass

    return pytest.skip("Connection to the Redis server failed. This test cannot be performed...")


@retry(stop_max_attempt_number=10, wait_random_min=100, wait_random_max=500)
def setup_store(docstore, request):
    try:
        ret_val = docstore.ping()
        if ret_val:
            docstore.register('submissions', models.submission.Submission)
            docstore.register('results', models.result.Result)
            docstore.register('files', models.file.File)
            docstore.register('errors', models.error.Error)

            request.addfinalizer(docstore.submissions.wipe)
            request.addfinalizer(docstore.results.wipe)
            request.addfinalizer(docstore.files.wipe)
            request.addfinalizer(docstore.errors.wipe)

            return docstore
    except ConnectionError:
        pass
    raise SetupException("Could not setup Datastore: %s" % docstore.__class__.__name__)


@pytest.fixture(scope='module')
def es_connection(request):
    try:
        document_store = setup_store(ESStore(['127.0.0.1']), request)
    except SetupException:
        document_store = None

    if document_store:
        return document_store

    return pytest.skip("Connection to the Elasticsearch server failed. This test cannot be performed...")


class MockService:
    def __init__(self, name, dispatcher):
        self.name = name
        self.queue = NamedQueue(service_queue_name(name))
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.dispatcher = dispatcher
        self.thread.start()

    def run(self):
        while True:
            task = ServiceTask(self.queue.pop())
            time.sleep(random.random())

            if random.random() < 0.001:
                continue

            if random.random() < 0.01:
                self.dispatcher.service_failed(task)
                continue

            result = random_model_obj(Result)
            self.dispatcher.service_finished(task, result)


def test_simulate_dispatcher(redis_connection, es_connection):


    from assemblyline.common import log
    log.init_logging()

    # Create a configuration with a set of services
    class MockScheduler(Scheduler):
        def __init__(self, *args, **kwargs):
            pass

        def services(self):
            return {
                'extract': Service(dict(
                    name='extract',
                    category='static',
                    stage='pre',
                ))
            }

        # def build_schedule(self, *args):
        #     return [
        #         ['extract', 'wrench'],
        #         ['av-a', 'av-b', 'frankenstrings'],
        #         ['xerox']
        #     ]
        #
        # def build_service_config(self, *args):
        #     return {}
        #
        # def service_failure_limit(self, *args):
        #     return 5

    with mock.patch('dispatcher.Scheduler', MockScheduler):
        # Start the dispatch servers
        submission_server = SubmissionDispatchServer(redis_connection, es_connection)
        file_server = FileDispatchServer(redis_connection, es_connection)
        submission_server.start()
        file_server.start()

        # Create a set of daemons that act like those services exist
        sched = MockScheduler(es_connection)
        for name in sched.services():
            print(f'Creating mock service {name}')
            MockService(name, submission_server.dispatcher)

        # Start sending randomly generated jobs
        submissions = []
        for _ in range(10):
            sub = random_model_obj(models.submission.Submission)
            sub.status = 'incomplete'
            es_connection.submissions.save(sub.sid, sub)
            submission_server.dispatcher.submission_queue.push(sub.json())
            submissions.append(sub.sid)

        # Wait for all of the jobs to finish
        while len(submission_server.dispatcher.submission_queue) > 0 or len(submission_server.dispatcher.file_queue) > 0:
            print(len(submission_server.dispatcher.submission_queue), len(submission_server.dispatcher.file_queue))
            time.sleep(1)

        submission_server.stop()
        file_server.stop()

        submission_server.join()
        file_server.join()

        # Verify that all of the jobs have reasonable results
        for sid in submissions:
            sub = es_connection.submissions.get(sid)
            assert sub.status == 'complete'
            # TODO check that results exist
