import pytest

from assemblyline.odm.models.submission import Submission
from assemblyline.odm.models.config import Config, DEFAULT_CONFIG
from assemblyline.odm.models.service import Service
from assemblyline.odm.randomizer import random_model_obj

from al_core.dispatching.scheduler import Scheduler


def dummy_service(name, stage, accepts='', rejects=''):
    return Service({
        'name': name,
        'stage': stage,
        'category': 'static',
        'accepts': accepts,
        'rejects': rejects,
        'realm': 'abc',
        'repo': '/dev/null',
        'version': '0',
        'enabled': True
    })


class FakeDatastore:
    def __init__(self):
        self.service = self

    def search(self, *args, **kwargs):
        return {'items': [Service({
                    'name': 'extract',
                    'stage': 'pre',
                    'category': 'static',
                    'accepts': 'archive/.*',
                    'rejects': '',
                    'realm': 'abc',
                    'repo': '/dev/null',
                    'version': '0',
                }),
                Service({
                    'name': 'AnAV',
                    'stage': 'core',
                    'category': 'av',
                    'accepts': '.*',
                    'rejects': '',
                    'realm': 'abc',
                    'repo': '/dev/null',
                    'version': '0',
                }),
                Service({
                    'name': 'cuckoo',
                    'stage': 'core',
                    'category': 'dynamic',
                    'accepts': 'document/.*|executable/.*',
                    'rejects': '',
                    'realm': 'abc',
                    'repo': '/dev/null',
                    'version': '0',
                }),
                Service({
                    'name': 'polish',
                    'stage': 'post',
                    'category': 'static',
                    'accepts': '.*',
                    'rejects': '',
                    'realm': 'abc',
                    'repo': '/dev/null',
                    'version': '0',
                }),
                Service({
                    'name': 'not_documents',
                    'stage': 'post',
                    'category': 'static',
                    'accepts': '.*',
                    'rejects': 'document/*',
                    'realm': 'abc',
                    'repo': '/dev/null',
                    'version': '0',
                })
            ]}


def submission(selected, excluded):
    sub = random_model_obj(Submission)
    sub.params.services.selected = selected
    sub.params.services.excluded = excluded
    return sub


@pytest.fixture
def scheduler():
    config = Config(DEFAULT_CONFIG)
    config.core.dispatcher.stages = ['pre', 'core', 'post']
    return Scheduler(FakeDatastore(), config)


def test_schedule_simple(scheduler):
    schedule = scheduler.build_schedule(submission(['static', 'av'], ['dynamic']), 'document/word')
    for a, b in zip(schedule, [[], ['AnAV'], ['polish']]):
        assert set(a) == set(b)

def test_schedule_no_excludes(scheduler):
    schedule = scheduler.build_schedule(submission(['static', 'av', 'dynamic'], []), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [[], ['AnAV', 'cuckoo'], ['polish']]))

def test_schedule_all_defaults_word(scheduler):
    schedule = scheduler.build_schedule(submission([], []), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [[], ['AnAV', 'cuckoo'], ['polish']]))

def test_schedule_all_defaults_zip(scheduler):
    schedule = scheduler.build_schedule(submission([], []), 'archive/zip')
    assert all(set(a) == set(b) for a, b in zip(schedule, [['extract'], ['AnAV'], ['polish', 'not_documents']]))
