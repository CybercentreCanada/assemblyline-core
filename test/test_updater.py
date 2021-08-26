from typing import Union

import redis
import tempfile
import pytest

from assemblyline_core.server_base import ServiceStage
from assemblyline_core.updater import run_updater
from assemblyline.common.isotime import now_as_iso
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.models.service import UpdateConfig
from assemblyline.odm.random_data import create_services
from assemblyline.odm.randomizer import random_model_obj

from mocking import MockDatastore


@pytest.fixture(scope='session')
def updater_directory():
    with tempfile.TemporaryDirectory() as tmpdir:
        run_updater.FILE_UPDATE_DIRECTORY = tmpdir
        yield tmpdir


@pytest.fixture
def ds():
    return AssemblylineDatastore(MockDatastore())


@pytest.fixture
def updater(clean_redis: redis.Redis, ds, updater_directory):
    return run_updater.ServiceUpdater(redis_persist=clean_redis, redis=clean_redis, datastore=ds)


def test_service_changes(updater: run_updater.ServiceUpdater):
    ds: MockDatastore = updater.datastore.ds
    # Base conditions, nothing anywhere
    assert updater.services.length() == 0
    assert len(updater.datastore.list_all_services()) == 0

    # Nothing does nothing
    updater.sync_services()
    assert updater.services.length() == 0
    assert len(updater.datastore.list_all_services()) == 0

    # Any non-disabled services should be picked up by the updater
    create_services(updater.datastore, limit=1)
    for data in ds._collections['service']._docs.values():
        data.enabled = True
        updater._service_stage_hash.set(data.name, ServiceStage.Update)
        data.update_config = random_model_obj(UpdateConfig)
    assert len(updater.datastore.list_all_services(full=True)) == 1
    updater.sync_services()
    assert updater.services.length() == 1
    assert len(updater.datastore.list_all_services(full=True)) == 1

    # It should be scheduled to update ASAP
    for data in updater.services.items().values():
        assert data['next_update'] <= now_as_iso()

    # Disable the service and it will disappear from redis
    for data in ds._collections['service']._docs.values():
        data.enabled = False
    updater.sync_services()
    assert updater.services.length() == 0
    assert len(updater.datastore.list_all_services(full=True)) == 1
