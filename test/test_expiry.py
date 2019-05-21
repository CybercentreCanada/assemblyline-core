
import pytest
import random

from al_core.expiry.run_expiry import ExpiryManager
from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso
from assemblyline.odm.randomizer import random_model_obj

MAX_OBJECTS = 10
MIN_OBJECTS = 2
datastore = forge.get_datastore()
collections_len = {}


def purge_data():
    for name, definition in datastore.ds.get_models().items():
        if hasattr(definition, 'expiry_ts'):
            getattr(datastore, name).wipe()


@pytest.fixture(scope="module")
def ds(request):
    for name, definition in datastore.ds.get_models().items():
        if hasattr(definition, 'expiry_ts'):
            collection = getattr(datastore, name)
            expiry_len = random.randint(MIN_OBJECTS, MAX_OBJECTS)
            for x in range(expiry_len):
                obj = random_model_obj(collection.model_class)
                obj.expiry_ts = now_as_iso(-10000)
                collection.save(x, obj)

            collection.commit()
            collections_len[name] = expiry_len

    request.addfinalizer(purge_data)
    return datastore


class FakeCounter(object):
    def __init__(self):
        self.counts = {}

    def increment(self, name, increment_by=1):
        if name not in self.counts:
            self.counts[name] = 0

        self.counts[name] += increment_by

    def get(self, name):
        return self.counts.get(name, 0)


def test_expire_all(ds):
    expiry = ExpiryManager()
    expiry.counter = FakeCounter()
    expiry.run_once()

    for k, v in collections_len.items():
        assert v == expiry.counter.get(k)
        collection = getattr(ds, k)
        collection.commit()
        assert collection.search("id:*")['total'] == 0
