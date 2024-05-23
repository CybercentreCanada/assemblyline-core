
import pytest
import random
import concurrent.futures

from assemblyline.common.isotime import now_as_iso
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.randomizer import random_model_obj

from assemblyline_core.expiry.run_expiry import ExpiryManager

MAX_OBJECTS = 10
MIN_OBJECTS = 2
expiry_collections_len = {}
archive_collections_len = {}


def purge_data(datastore_connection: AssemblylineDatastore):
    for name, definition in datastore_connection.ds.get_models().items():
        if hasattr(definition, 'expiry_ts'):
            getattr(datastore_connection, name).wipe()


@pytest.fixture(scope="function")
def ds_expiry(request, datastore_connection):
    for name, definition in datastore_connection.ds.get_models().items():
        if hasattr(definition, 'expiry_ts'):
            collection = getattr(datastore_connection, name)
            collection.wipe()
            expiry_len = random.randint(MIN_OBJECTS, MAX_OBJECTS)
            for x in range(expiry_len):
                obj = random_model_obj(collection.model_class)
                if hasattr(definition, 'from_archive'):
                    obj.from_archive = False
                obj.expiry_ts = now_as_iso(-10000)
                collection.save('longer_name'+str(x), obj)

            expiry_collections_len[name] = expiry_len
            collection.commit()

    request.addfinalizer(lambda: purge_data(datastore_connection))
    return datastore_connection


class FakeCounter(object):
    def __init__(self):
        self.counts = {}

    def increment(self, name, increment_by=1):
        if name not in self.counts:
            self.counts[name] = 0

        self.counts[name] += increment_by

    def get(self, name):
        return self.counts.get(name, 0)


def test_expire_all(config, ds_expiry, filestore):
    expiry = ExpiryManager(config=config, datastore=ds_expiry, filestore=filestore)
    expiry.running = True
    expiry.counter = FakeCounter()
    with concurrent.futures.ThreadPoolExecutor(5) as pool:
        for collection in expiry.expirable_collections:
            expiry.feed_expiry_jobs(collection=collection, pool=pool, start='*', jobs=[])

    for k, v in expiry_collections_len.items():
        assert v == expiry.counter.get(k)
        collection = getattr(ds_expiry, k)
        collection.commit()
        assert collection.search("id:*")['total'] == 0
