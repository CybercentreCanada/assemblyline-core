
import baseconv
import uuid
import pytest

from assemblyline.remote.datatypes.queues.named import NamedQueue

from al_core.mocking import ToggleTrue, RedisTime

from al_core.watcher import WatcherClient
from al_core.watcher.run_watcher import WatcherServer


@pytest.fixture
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


def test_watcher(redis_connection):
    redis_connection.time = RedisTime()
    redis = redis_connection
    queue_name = baseconv.base62.encode(uuid.uuid4().int)
    out_queue = NamedQueue(queue_name, redis)
    try:
        # Create a server and hijack its running flag and the current time in 'redis'
        client = WatcherClient(redis)
        server = WatcherServer(redis)
        server.running = ToggleTrue()
        redis.time.current = 0
        assert out_queue.length() == 0

        # Send a simple event to occur soon
        client.touch(10, 'one-second', queue_name, {'first': 'one'})
        server.try_run()
        assert out_queue.length() == 0  # Nothing yet
        redis.time.current = 12  # Jump forward 12 seconds
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
            redis.time.current += 20
            server.try_run()

        assert out_queue.length() == 3
        assert out_queue.pop() == {'first': '50'}
        assert out_queue.pop() == {'first': '100'}
        assert out_queue.pop() == {'first': 'last'}

        # Send a simple event to occur soon, then stop it
        redis.time.current = 0
        client.touch(10, 'one-second', queue_name, {'first': 'one'})
        server.try_run()
        assert out_queue.length() == 0  # Nothing yet
        client.clear('one-second')
        redis.time.current = 12  # Jump forward 12 seconds
        server.try_run()
        assert out_queue.length() == 0  # still nothing because it was cleared

    finally:
        out_queue.delete()