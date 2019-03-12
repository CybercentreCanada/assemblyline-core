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
        'version': '0',
        'enabled': True,
        'timeout': 2
    })


class FakeDatastore:
    def __init__(self):
        self.service = self

    def stream_search(self, *args, **kwargs):
        return []

    def multiget(self, *args, **kwargs):
        return {
            'extract': Service({
                'name': 'extract',
                'stage': 'pre',
                'category': 'static',
                'accepts': 'archive/.*',
                'rejects': '',
                'version': '0',
            }),
            'AnAV': Service({
                'name': 'AnAV',
                'stage': 'core',
                'category': 'av',
                'accepts': '.*',
                'rejects': '',
                'version': '0',
            }),
            'cuckoo': Service({
                'name': 'cuckoo',
                'stage': 'core',
                'category': 'dynamic',
                'accepts': 'document/.*|executable/.*',
                'rejects': '',
                'version': '0',
            }),
            'polish': Service({
                'name': 'polish',
                'stage': 'post',
                'category': 'static',
                'accepts': '.*',
                'rejects': '',
                'version': '0',
            }),
            'not_documents': Service({
                'name': 'not_documents',
                'stage': 'post',
                'category': 'static',
                'accepts': '.*',
                'rejects': 'document/*',
                'version': '0',
            })
        }


def submission(selected, excluded):
    sub = random_model_obj(Submission)
    sub.params.services.selected = selected
    sub.params.services.excluded = excluded
    return sub


@pytest.fixture
def scheduler():
    config = Config(DEFAULT_CONFIG)
    config.services.stages = ['pre', 'core', 'post']
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
