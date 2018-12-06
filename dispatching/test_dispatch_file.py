import dispatch_file
import mock
import json
from models import Submission


class FakeDatastore:
    def __init__(self):
        self.submissions = self

    def get(self, key):
        return {
            'first-submission': Submission({
                'files': ['totally-a-legit-hash']
            })
        }[key]


class FakeDispatchHash:
    def __init__(self, *args):
        pass

    def finished(self, file_hash, service):
        return False


class FakeQueue:
    def __init__(self, *args, **kwargs):
        pass


class FakeConfig:
    def __init__(self, *args, **kwargs):
        pass

    def build_schedule(self, *args):
        return [
            ['extract', 'wrench'],
            ['av-a', 'av-b', 'frankenstrings'],
            ['xerox']
        ]


@mock.patch('dispatch_file.NamedQueue', FakeQueue)
@mock.patch('dispatch_file.DispatchHash', FakeDispatchHash)
@mock.patch('dispatch_file.ConfigManager', FakeConfig)
def test_dispatcher():
    dispatcher = dispatch_file.FileDispatcher(FakeDatastore(), tuple())

    dispatcher.handle(json.dumps({
        'sid': 'first-submission',
        'file_hash': 'totally-a-legit-hash',
        'file_type': 'unknown',
        'depth': 0
    }))
