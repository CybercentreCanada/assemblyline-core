import uuid
import time

import pytest
from redis.exceptions import ConnectionError

import dispatch_hash


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


def test_single(redis_connection):
    disp = dispatch_hash.DispatchHash('test-disptach-hash', redis_connection)
    try:
        file_hash = uuid.uuid4().hex
        service = 'service_name'
        result_key = 'some-result'

        # An empty dispatch hash isn't finished
        assert not disp.all_finished()

        # If we call dispatch, the time should be set
        now = time.time()
        disp.dispatch(file_hash, service)
        assert abs(now - disp.dispatch_time(file_hash, service)) < 1
        assert not disp.finished(file_hash, service)
        assert disp.dispatch_count() == 1
        assert not disp.all_finished()

        # After failing, the time should be reset
        disp.fail_dispatch(file_hash, service)
        assert disp.dispatch_time(file_hash, service) == 0
        assert disp.dispatch_count() == 0
        assert not disp.finished(file_hash, service)
        assert not disp.all_finished()

        # Try dispatching again
        now = time.time()
        disp.dispatch(file_hash, service)
        assert abs(now - disp.dispatch_time(file_hash, service)) < 1
        assert not disp.finished(file_hash, service)
        assert disp.dispatch_count() == 1
        assert not disp.all_finished()

        # Success rather than failure
        disp.finish(file_hash, service, result_key)
        assert disp.dispatch_time(file_hash, service) == 0
        assert disp.dispatch_count() == 0
        assert disp.finished_count() == 1
        assert disp.all_finished()
        assert disp.finished(file_hash, service) == result_key
        assert disp.all_finished()

    finally:
        disp.delete()
