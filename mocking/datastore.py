
class MockCollection:
    def __init__(self, schema=None):
        self._docs = {}
        self.next_searches = []
        self.schema = schema

    def get(self, key):
        if key not in self._docs:
            return None
        return self._docs[key]

    def exists(self, key):
        print('exists', key, self._docs, key in self._docs)
        return key in self._docs

    def save(self, key, doc):
        self._docs[key] = doc

    def search(self, query, fl=None, rows=None):
        if self.next_searches:
            return self.next_searches.pop(0)
        return {
            'items': [],
            'total': 0,
            'offset': 0,
            'rows': 0
        }

    def delete(self, key):
        self._docs.pop(key, None)


class MockDatastore:
    def __init__(self):
        self._collections = {}

    def register(self, name, schema=None):
        assert isinstance(name, str)
        self._collections[name] = MockCollection(schema)

    def __getattr__(self, name):
        if name not in self._collections:
            self._collections[name] = MockCollection()
        return self._collections[name]
