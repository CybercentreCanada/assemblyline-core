
import time

from assemblyline.common.uid import get_random_id
from assemblyline_core.dispatching import dispatch_hash

# noinspection PyUnresolvedReferences
from .mocking import clean_redis


def test_single(clean_redis):
    disp = dispatch_hash.DispatchHash('test-disptach-hash', clean_redis)
    try:
        file_hash = get_random_id()
        service = 'service_name'
        result_key = 'some-result'

        # An empty dispatch hash isn't finished
        assert not disp.all_finished()

        # If we call dispatch, the key should be set
        disp.dispatch(file_hash, service)
        assert disp.dispatch_key(file_hash, service) is not None
        disp.set_dispatch_key(file_hash, service, b'abc123')
        assert disp.dispatch_key(file_hash, service) == b'abc123'
        assert not disp.finished(file_hash, service)
        assert disp.dispatch_count() == 1
        assert not disp.all_finished()

        # After failing, the time should be reset
        disp.fail_recoverable(file_hash, service)
        assert disp.dispatch_key(file_hash, service) is None
        assert disp.dispatch_count() == 0
        assert not disp.finished(file_hash, service)
        assert not disp.all_finished()

        # Try dispatching again
        disp.dispatch(file_hash, service)
        assert disp.dispatch_key(file_hash, service) is not None
        disp.set_dispatch_key(file_hash, service, b'abc123')
        assert disp.dispatch_key(file_hash, service) == b'abc123'
        assert not disp.finished(file_hash, service)
        assert disp.dispatch_count() == 1
        assert not disp.all_finished()

        # Success rather than failure
        disp.finish(file_hash, service, result_key, 0, "U")
        assert disp.dispatch_key(file_hash, service) is None
        assert disp.dispatch_count() == 0
        assert disp.finished_count() == 1
        assert disp.all_finished()
        assert disp.finished(file_hash, service) == dispatch_hash.DispatchRow('result', result_key, 0, False, 'U')
        assert disp.all_finished()

    finally:
        disp.delete()
