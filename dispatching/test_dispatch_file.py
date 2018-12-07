import time
import mock
import json

from models import Submission
import dispatch_file
from dispatch_file import service_queue_name


class MockFactory:
    def __init__(self, mock_type):
        self.type = mock_type
        self.mocks = {}

    def __call__(self, name, *args):
        if name not in self.mocks:
            self.mocks[name] = self.type(name, *args)
        return self.mocks[name]

    def __getitem__(self, name):
        return self.mocks[name]

    def __len__(self):
        return len(self.mocks)

    def flush(self):
        self.mocks.clear()


class MockDispatchHash:
    def __init__(self, *args):
        self._dispatched = {}
        self._finished = {}

    @staticmethod
    def _key(file_hash, service):
        return f"{file_hash}_{service}"

    def finished(self, file_hash, service):
        return self._key(file_hash, service) in self._finished

    def dispatch_time(self, file_hash, service):
        return self._dispatched.get(self._key(file_hash, service), 0)

    def dispatch(self, file_hash, service):
        self._dispatched[self._key(file_hash, service)] = time.time()

    def finish(self, file_hash, service, result_key):
        self._key(file_hash, service)
        self._finished[]
        self._dispatched[] = time.time()

    def fail_dispatch(self, file_hash, service):
        self._dispatched[self._key(file_hash, service)] = 0


class FakeDatastore:
    def __init__(self):
        self.submissions = self
        self.results = self

    def get(self, key):
        return {
            'first-submission': Submission({
                'files': ['totally-a-legit-hash']
            })
        }[key]

    def exists(self, key):
        return False


class MockQueue:
    def __init__(self, *args, **kwargs):
        self.queue = []

    def push(self, obj):
        self.queue.append(obj)

    def length(self):
        return len(self.queue)

    def __len__(self):
        return len(self.queue)


class FakeConfig:
    def __init__(self, *args, **kwargs):
        pass

    def build_schedule(self, *args):
        return [
            ['extract', 'wrench'],
            ['av-a', 'av-b', 'frankenstrings'],
            ['xerox']
        ]

    def build_service_config(self, service, submission):
        return {}

    def service_timeout(self, service):
        return 60*10


def test_dispatcher():
    with mock.patch('dispatch_file.NamedQueue', MockFactory(MockQueue)) as mq:
        with mock.patch('dispatch_file.DispatchHash', MockFactory(MockDispatchHash)) as dh:
            with mock.patch('dispatch_file.ConfigManager', FakeConfig):
                dispatcher = dispatch_file.FileDispatcher(FakeDatastore(), tuple())
                print('==== first dispatch')
                # Submit a problem, and check that it gets added to the dispatch hash
                # and the right service queues
                dispatcher.handle(json.dumps({
                    'sid': 'first-submission',
                    'file_hash': 'totally-a-legit-hash',
                    'file_type': 'unknown',
                    'depth': 0
                }))

                assert dh['first-submission'].dispatch_time('totally-a-legit-hash', 'extract') > 0
                assert len(mq[service_queue_name('extract')]) == 1
                assert len(mq[service_queue_name('wrench')]) == 1
                assert len(mq) == 3

                # Making the same call again should have no effect
                print('==== second dispatch')
                dispatcher.handle(json.dumps({
                    'sid': 'first-submission',
                    'file_hash': 'totally-a-legit-hash',
                    'file_type': 'unknown',
                    'depth': 0
                }))

                assert dh['first-submission'].dispatch_time('totally-a-legit-hash', 'extract') > 0
                assert len(mq[service_queue_name('extract')]) == 1
                assert len(mq[service_queue_name('wrench')]) == 1
                print(mq.mocks)
                assert len(mq) == 3

                # Push back the timestamp in the dispatch hash to simulate a timeout,
                # make sure it gets pushed into that service queue again
                print('==== third dispatch')
                mq.flush()
                dh['first-submission'].fail_dispatch('totally-a-legit-hash', 'extract')

                dispatcher.handle(json.dumps({
                    'sid': 'first-submission',
                    'file_hash': 'totally-a-legit-hash',
                    'file_type': 'unknown',
                    'depth': 0
                }))

                assert dh['first-submission'].dispatch_time('totally-a-legit-hash', 'extract') > 0
                assert len(mq[service_queue_name('extract')]) == 1
                assert len(mq) == 1

                # Mark extract as finished in the dispatch table, add a result object
                # for the wrench service, it should move to the second batch of services
                print('==== fourth dispatch')
                mq.flush()
                dh['first-submission'].finish('totally-a-legit-hash', 'extract')

                dispatcher.handle(json.dumps({
                    'sid': 'first-submission',
                    'file_hash': 'totally-a-legit-hash',
                    'file_type': 'unknown',
                    'depth': 0
                }))

                assert dh['first-submission'].dispatch_time('totally-a-legit-hash', 'extract') > 0
                assert len(mq[service_queue_name('extract')]) == 1
                assert len(mq) == 1
