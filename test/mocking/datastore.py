
class MockCollection:
    def __init__(self, schema=None):
        self._docs = {}
        self.next_searches = []
        self.schema = schema

    # noinspection PyUnusedLocal
    def get(self, key, as_obj=True, force_archive_access=False):
        if key not in self._docs:
            return None
        if not as_obj:
            return self._docs[key].as_primitives()
        return self._docs[key]

    def multiget(self, key_list, as_obj=True, as_dictionary=True):
        if as_dictionary:
            return {key: self.get(key, as_obj=as_obj) for key in key_list}
        return [self.get(key, as_obj=as_obj) for key in key_list]

    def exists(self, key):
        print('exists', key, self._docs, key in self._docs)
        return key in self._docs

    # noinspection PyUnusedLocal
    def save(self, key, doc, force_archive_access=False):
        if not self.schema or isinstance(doc, self.schema):
            self._docs[key] = doc
            return
        self._docs[key] = self.schema(doc)

    def search(self, query, fl=None, rows=None):
        if self.next_searches:
            return self.next_searches.pop(0)
        return {
            'items': [],
            'total': 0,
            'offset': 0,
            'rows': 0
        }

    def stream_search(self, *_, **__):
        for key, doc in self._docs.items():
            data = doc.as_primitives()
            data['id'] = key
            yield data

    def delete(self, key):
        self._docs.pop(key, None)

    def commit(self):
        pass

    def __len__(self):
        return len(self._docs)


class MockDatastore:
    def __init__(self, collections=None):
        self.__collection_names = collections
        self._collections = {}

    def register(self, name, schema=None):
        assert isinstance(name, str)
        if self.__collection_names:
            assert name in self.__collection_names
        self._collections[name] = MockCollection(schema)

    def __getattr__(self, name):
        if self.__collection_names:
            assert name in self.__collection_names
        if name not in self._collections:
            self._collections[name] = MockCollection()
        return self._collections[name]
