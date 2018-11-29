from configuration import ConfigManager


class FakeServiceDatastore:
    def __init__(self):
        self.blobs = self

    def get(self, key):
        assert key == 'seed'
        return {
            'services': {
                'categories': ['dynamic', 'static', 'av'],
                'stages': ['pre', 'core', 'post'],
                'master_list': {
                    'extract': {
                        'stage': 'pre',
                        'category': 'static',
                        'accepts': 'archive/.*',
                        'rejects': None,
                    },
                    'AnAV': {
                        'stage': 'core',
                        'category': 'av',
                        'accepts': '.*',
                        'rejects': '',
                    },
                    'cuckoo': {
                        'stage': 'core',
                        'category': 'dynamic',
                        'accepts': 'document/.*|executable/.*',
                        'rejects': None,
                    },
                    'polish': {
                        'stage': 'post',
                        'category': 'static',
                        'accepts': '.*',
                        'rejects': None
                    }
                }
            }
        }


class FakeSubmission:
    def __init__(self, selected, excluded):
        self.selected_categories = selected
        self.excluded_categories = excluded


def test_schedule():
    manager = ConfigManager(FakeServiceDatastore())
    schedule = manager.build_schedule(FakeSubmission(['static', 'av'], ['dynamic']), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [[], ['AnAV'], ['polish']]))
    schedule = manager.build_schedule(FakeSubmission(['static', 'av', 'dynamic'], []), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [[], ['AnAV', 'cuckoo'], ['polish']]))
    schedule = manager.build_schedule(FakeSubmission([], []), 'document/word')
    assert all(set(a) == set(b) for a, b in zip(schedule, [[], ['AnAV', 'cuckoo'], ['polish']]))
    schedule = manager.build_schedule(FakeSubmission([], []), 'archive/zip')
    assert all(set(a) == set(b) for a, b in zip(schedule, [['extract'], ['AnAV'], ['polish']]))
