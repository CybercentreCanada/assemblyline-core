
import pytest
import redis.exceptions

from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.common.uid import get_random_id

from assemblyline_core.watcher.client import WatcherClient
from assemblyline_core.watcher.run_watcher import WatcherServer

from .mocking import ToggleTrue, RedisTime


@pytest.fixture(scope='module')
def redis(redis_connection):
    redis_connection.flushdb()
    yield redis_connection
    redis_connection.flushdb()


def test_watcher(redis_connection):
    redis_connection.time = RedisTime()
    rds = redis_connection
    queue_name = get_random_id()
    out_queue = NamedQueue(queue_name, rds)
    try:
        # Create a server and hijack its running flag and the current time in 'redis'
        client = WatcherClient(rds)
        server = WatcherServer(rds, rds)
        server.running = ToggleTrue()
        rds.time.current = 0
        assert out_queue.length() == 0

        # Send a simple event to occur soon
        client.touch(10, 'one-second', queue_name, {'first': 'one'})
        server.try_run()
        assert out_queue.length() == 0  # Nothing yet
        rds.time.current = 12  # Jump forward 12 seconds
        server.try_run()
        assert out_queue.length() == 1
        assert out_queue.pop() == {'first': 'one'}

        # Send a simple event to occur soon, then change our mind
        client.touch(10, 'one-second', queue_name, {'first': 'one'})
        client.touch(20, 'one-second', queue_name, {'first': 'one'})
        server.try_run()
        assert out_queue.length() == 0  # Nothing yet

        # Set events to occur, in inverse order, reuse a key, overwrite content and timeout
        client.touch(200, 'one-second', queue_name, {'first': 'last'})
        client.touch(100, '100-second', queue_name, {'first': '100'})
        client.touch(50, '50-second', queue_name, {'first': '50'})
        server.try_run()
        assert out_queue.length() == 0  # Nothing yet

        for _ in range(15):
            rds.time.current += 20
            server.try_run()

        assert out_queue.length() == 3
        assert out_queue.pop() == {'first': '50'}
        assert out_queue.pop() == {'first': '100'}
        assert out_queue.pop() == {'first': 'last'}

        # Send a simple event to occur soon, then stop it
        rds.time.current = 0
        client.touch(10, 'one-second', queue_name, {'first': 'one'})
        server.try_run()
        assert out_queue.length() == 0  # Nothing yet
        client.clear('one-second')
        rds.time.current = 12  # Jump forward 12 seconds
        server.try_run()
        assert out_queue.length() == 0  # still nothing because it was cleared

    finally:
        out_queue.delete()


