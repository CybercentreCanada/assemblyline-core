
import pytest
import random
import concurrent.futures

from assemblyline_core.expiry.run_expiry import ExpiryManager
from assemblyline.common.isotime import now_as_iso
from assemblyline.odm.randomizer import random_model_obj

MAX_OBJECTS = 10
MIN_OBJECTS = 2
expiry_collections_len = {}
archive_collections_len = {}


@pytest.fixture(scope='module')
def datastore(archive_connection):
    return archive_connection


def purge_data(datastore):
    for name, definition in datastore.ds.get_models().items():
        if hasattr(definition, 'expiry_ts'):
            getattr(datastore, name).wipe()


@pytest.fixture(scope="function")
def ds_expiry(request, datastore):
    for name, definition in datastore.ds.get_models().items():
        if hasattr(definition, 'expiry_ts'):
            collection = getattr(datastore, name)
            collection.wipe()
            expiry_len = random.randint(MIN_OBJECTS, MAX_OBJECTS)
            for x in range(expiry_len):
                obj = random_model_obj(collection.model_class)
                obj.expiry_ts = now_as_iso(-10000)
                collection.save('longer_name'+str(x), obj)

            expiry_collections_len[name] = expiry_len
            collection.commit()

    request.addfinalizer(lambda: purge_data(datastore))
    return datastore


@pytest.fixture(scope="function")
def ds_archive(request, datastore):
    for name, definition in datastore.ds.get_models().items():
        if hasattr(definition, 'archive_ts'):
            collection = getattr(datastore, name)
            collection.wipe()
            expiry_len = random.randint(MIN_OBJECTS, MAX_OBJECTS)
            for x in range(expiry_len):
                obj = random_model_obj(collection.model_class)
                obj.archive_ts = now_as_iso(-10000)
                collection.save('longer_name'+str(x), obj)

            archive_collections_len[name] = expiry_len
            collection.commit()

    request.addfinalizer(lambda: purge_data(datastore))
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


def test_expire_all(ds_expiry):
    expiry = ExpiryManager()
    expiry.running = True
    expiry.counter = FakeCounter()
    expiry.counter_archive = FakeCounter()
    with concurrent.futures.ThreadPoolExecutor(5) as pool:
        expiry.run_expiry_once(pool)

    for k, v in expiry_collections_len.items():
        assert v == expiry.counter.get(k)
        collection = getattr(ds_expiry, k)
        collection.commit()
        assert collection.search("id:*")['total'] == 0


def test_archive_all(ds_archive):
    expiry = ExpiryManager(force_ilm=True)
    expiry.counter = FakeCounter()
    expiry.counter_archive = FakeCounter()
    with concurrent.futures.ThreadPoolExecutor(5) as pool:
        expiry.run_archive_once(pool)

    for k, v in archive_collections_len.items():
        assert v == expiry.counter_archive.get(k)
        collection = getattr(ds_archive, k)
        collection.commit()
        assert collection.search("id:*")['total'] == 0
        assert collection.search("id:*", use_archive=True)['total'] == v
