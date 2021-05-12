"""
Pytest configuration file, setup global pytest fixtures and functions here.
"""
import os

from assemblyline.common import forge
original_classification = forge.get_classification


def test_classification(yml_config=None):
    path = os.path.join(os.path.dirname(__file__), 'classification.yml')
    return original_classification(path)


forge.get_classification = test_classification


from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.datastore.stores.es_store import ESStore
from redis.exceptions import ConnectionError

import pytest
original_skip = pytest.skip

# Check if we are in an unattended build environment where skips won't be noticed
IN_CI_ENVIRONMENT = any(indicator in os.environ for indicator in
                        ['CI', 'BITBUCKET_BUILD_NUMBER', 'AGENT_JOBSTATUS'])


def skip_or_fail(message):
    """Skip or fail the current test, based on the environment"""
    if IN_CI_ENVIRONMENT:
        pytest.fail(message)
    else:
        original_skip(message)


# Replace the built in skip function with our own
pytest.skip = skip_or_fail


@pytest.fixture(scope='session')
def config():
    config = forge.get_config()
    config.logging.log_level = 'INFO'
    config.logging.log_as_json = False
    config.core.metrics.apm_server.server_url = None
    config.core.metrics.export_interval = 1
    return config


@pytest.fixture(scope='module')
def datastore_connection(config):
    store = ESStore(config.datastore.hosts)
    ret_val = store.ping()
    if not ret_val:
        pytest.skip("Could not connect to datastore")

    return AssemblylineDatastore(store)


@pytest.fixture(scope='module')
def archive_connection(config):
    store = ESStore(config.datastore.hosts, archive_access=True)
    ret_val = store.ping()
    if not ret_val:
        pytest.skip("Could not connect to datastore")

    return AssemblylineDatastore(store)


@pytest.fixture(scope='session')
def redis_connection():
    from assemblyline.remote.datatypes import get_client
    c = get_client(None, None, False)
    try:
        ret_val = c.ping()
        if ret_val:
            return c
    except ConnectionError:
        pass

    return pytest.skip("Connection to the Redis server failed. This test cannot be performed...")


@pytest.fixture
def clean_redis(redis_connection):
    try:
        redis_connection.flushdb()
        yield redis_connection
    finally:
        redis_connection.flushdb()


@pytest.fixture(scope='module')
def filestore(config):
    try:
        return forge.get_filestore(config, connection_attempts=1)
    except ConnectionError as err:
        pytest.skip(str(err))
