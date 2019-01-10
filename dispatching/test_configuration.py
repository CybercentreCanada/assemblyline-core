from configuration import Scheduler, Service
from easydict import EasyDict
from assemblyline.odm.models.submission import Submission
from assemblyline.odm.randomizer import random_model_obj


class FakeDatastore:
    def __init__(self):
        self.services = self

    def register(*args):
        pass

    def search(self, *args, **kwargs):
        return {'items': [Service({
                    'name': 'extract',
                    'stage': 'pre',
                    'category': 'static',
                    'accepts': 'archive/.*',
                    'rejects': None,
                }),
                Service({
                    'name': 'AnAV',
                    'stage': 'core',
                    'category': 'av',
                    'accepts': '.*',
                    'rejects': '',
                }),
                Service({
                    'name': 'cuckoo',
                    'stage': 'core',
                    'category': 'dynamic',
                    'accepts': 'document/.*|executable/.*',
                    'rejects': None,
                }),
                Service({
                    'name': 'polish',
                    'stage': 'post',
                    'category': 'static',
                    'accepts': '.*',
                    'rejects': None
                })]}


class FakeConfig:
    def __init__(self):
        self.core = EasyDict({'dispatcher': {'stages': ['pre', 'core', 'post']}})


def submission(selected, excluded):
    sub = random_model_obj(Submission)
    sub.params.services.selected = selected
    sub.params.services.excluded = excluded
    return sub


def test_schedule():
    manager = Scheduler(FakeDatastore(), FakeConfig())
    schedule = manager.build_schedule(submission(['static', 'av'], ['dynamic']), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [[], ['AnAV'], ['polish']]))
    schedule = manager.build_schedule(submission(['static', 'av', 'dynamic'], []), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [[], ['AnAV', 'cuckoo'], ['polish']]))
    schedule = manager.build_schedule(submission([], []), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [[], ['AnAV', 'cuckoo'], ['polish']]))
    schedule = manager.build_schedule(submission([], []), 'archive/zip')
    assert all(set(a) == set(b) for a, b in zip(schedule, [['extract'], ['AnAV'], ['polish']]))
