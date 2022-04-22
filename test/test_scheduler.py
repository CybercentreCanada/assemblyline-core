import pytest

from assemblyline.odm.models.submission import Submission
from assemblyline.odm.models.config import Config, DEFAULT_CONFIG
from assemblyline.odm.models.service import Service
from assemblyline.odm.randomizer import random_model_obj

from assemblyline_core.dispatching.dispatcher import Scheduler
from assemblyline_core.server_base import get_service_stage_hash, ServiceStage


@pytest.fixture(scope='module')
def redis(redis_connection):
    redis_connection.flushdb()
    yield redis_connection
    redis_connection.flushdb()


def dummy_service(name, stage, category='static', accepts='', rejects=None, docid=None, extra_data=False):
    return Service({
        'name': name,
        'stage': stage,
        'category': category,
        'accepts': accepts,
        'uses_temp_submission_data': extra_data,
        'uses_tags': extra_data,
        'rejects': rejects,
        'version': '0',
        'enabled': True,
        'timeout': 2,
        'docker_config': {
            'image': 'somefakedockerimage:latest'
        }
    }, docid=docid)


# noinspection PyUnusedLocal,PyMethodMayBeStatic
class FakeDatastore:
    def __init__(self):
        self.service = self

    def stream_search(self, *args, **kwargs):
        return []

    def list_all_services(self, full=True):
        return {
            'extract': dummy_service(
                name='extract',
                stage='pre',
                accepts='archive/.*',
            ),
            'AnAV': dummy_service(
                name='AnAV',
                stage='core',
                category='av',
                accepts='.*',
            ),
            'cuckoo': dummy_service(
                name='cuckoo',
                stage='core',
                category='dynamic',
                accepts='document/.*|executable/.*',
            ),
            'polish': dummy_service(
                name='polish',
                stage='post',
                category='static',
                accepts='.*',
            ),
            'not_documents': dummy_service(
                name='not_documents',
                stage='post',
                category='static',
                accepts='.*',
                rejects='document/*',
            ),
            'Safelist': dummy_service(
                name='Safelist',
                stage='pre',
                category='static',
                accepts='.*',
            )
        }.values()


def submission(selected, excluded):
    sub = random_model_obj(Submission)
    sub.params.services.selected = selected
    sub.params.services.excluded = excluded
    return sub


@pytest.fixture
def scheduler(redis):
    config = Config(DEFAULT_CONFIG)
    config.services.stages = ['pre', 'core', 'post']
    stages = get_service_stage_hash(redis)
    ds = FakeDatastore()
    for service in ds.list_all_services():
        stages.set(service.name, ServiceStage.Running)
    return Scheduler(ds, config, redis)


def test_schedule_simple(scheduler):
    schedule = scheduler.build_schedule(submission(['static', 'av'], ['dynamic']), 'document/word')
    for a, b in zip(schedule, [['Safelist'], ['AnAV'], ['polish']]):
        assert set(a) == set(b)


def test_schedule_no_excludes(scheduler):
    schedule = scheduler.build_schedule(submission(['static', 'av', 'dynamic'], []), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [['Safelist'], ['AnAV', 'cuckoo'], ['polish']]))


def test_schedule_all_defaults_word(scheduler):
    schedule = scheduler.build_schedule(submission([], []), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [['Safelist'], ['AnAV', 'cuckoo'], ['polish']]))


def test_schedule_all_defaults_zip(scheduler):
    schedule = scheduler.build_schedule(submission([], []), 'archive/zip')
    assert all(set(a) == set(b)
               for a, b in zip(schedule, [['extract', 'Safelist'], ['AnAV'], ['polish', 'not_documents']]))


def test_schedule_service_safelist(scheduler):
    # Safelist service should still be scheduled
    schedule = scheduler.build_schedule(submission(["Safelist"], []), 'document/word', file_depth=0)
    for a, b in zip(schedule, [["Safelist"], [], []]):
        assert set(a) == set(b)

    # Safelist service should NOT still be scheduled because we're not enforcing Safelist service by default
    # and deep_scan and ignore_filtering is OFF for this submission
    sub = submission(["Safelist"], [])
    sub.params.deep_scan = False
    sub.params.ignore_filtering = False
    schedule = scheduler.build_schedule(sub, 'document/word', file_depth=1)
    for a, b in zip(schedule, [[], [], []]):
        assert set(a) == set(b)

    # Safelist service should be scheduled because we're enabling deep_scan
    sub.params.deep_scan = True
    sub.params.ignore_filtering = False
    schedule = scheduler.build_schedule(sub, 'document/word', file_depth=1)
    for a, b in zip(schedule, [["Safelist"], [], []]):
        assert set(a) == set(b)

    # Safelist service should be scheduled because we're enabling ignore_filtering
    sub.params.deep_scan = False
    sub.params.ignore_filtering = True
    schedule = scheduler.build_schedule(sub, 'document/word', file_depth=1)
    for a, b in zip(schedule, [["Safelist"], [], []]):
        assert set(a) == set(b)
