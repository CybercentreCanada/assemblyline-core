from configuration import Scheduler, Service
from easydict import EasyDict

#
# class FakeServiceDatastore:
#     def __init__(self):
#         self.blobs = self
#
#     def get(self, key):
#         assert key == 'seed'
#         return {
#             'services': {
#                 'categories': ['dynamic', 'static', 'av'],
#                 'stages':
#                 'master_list': {

#                 }
#             }
#         }


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


class FakeSubmission:
    def __init__(self, selected, excluded):
        self.selected_categories = selected
        self.excluded_categories = excluded


def test_schedule():
    manager = Scheduler(FakeDatastore(), FakeConfig())
    schedule = manager.build_schedule(FakeSubmission(['static', 'av'], ['dynamic']), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [[], ['AnAV'], ['polish']]))
    schedule = manager.build_schedule(FakeSubmission(['static', 'av', 'dynamic'], []), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [[], ['AnAV', 'cuckoo'], ['polish']]))
    schedule = manager.build_schedule(FakeSubmission([], []), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [[], ['AnAV', 'cuckoo'], ['polish']]))
    schedule = manager.build_schedule(FakeSubmission([], []), 'archive/zip')
    assert all(set(a) == set(b) for a, b in zip(schedule, [['extract'], ['AnAV'], ['polish']]))
