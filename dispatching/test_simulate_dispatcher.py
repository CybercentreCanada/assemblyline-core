import logging
import pytest

from retrying import retry

from redis.exceptions import ConnectionError

from submission_server import SubmissionDispatchServer
from file_server import FileDispatchServer
from assemblyline.datastore.stores.es_store import ESStore

from dispatcher import Submission

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
            docstore.register('submissions', Submission)

            request.addfinalizer(docstore.submissions.wipe)

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


@pytest.fixture(scope='module')
def solr_connection(request):
    from assemblyline.datastore.stores.solr_store import SolrStore

    try:
        collection = setup_store(SolrStore(['127.0.0.1']), request)
    except SetupException:
        collection = None

    if collection:
        return collection

    return pytest.skip("Connection to the SOLR server failed. This test cannot be performed...")


def test_simulate_dispatcher(redis_connection, solr_connection):

    class Config(ConfigManager):
        pass

    

    # Create a configuration with a set of services

    # Create a set of daemons that act like those services exist
    pass

    # Start the dispatch servers
    submission_server = SubmissionDispatchServer(logging, redis_connection, es_connection)
    file_server = FileDispatchServer(logging, redis_connection, es_connection)
    submission_server.start()
    file_server.start()

    # Start sending randomly generated jobs
    pass

    # Wait for all of the jobs to finish
    pass

    # Verify that all of the jobs have reasonable results
    pass
