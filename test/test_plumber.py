from unittest import mock
from redis import Redis

from assemblyline.odm.messages.task import Task

from assemblyline.odm.random_data import random_model_obj
from assemblyline.odm.models.service import Service

from assemblyline_core.plumber.run_plumber import Plumber
from assemblyline_core.server_base import ServiceStage

from mocking import TrueCountTimes


def test_expire_missing_service():
    redis = mock.MagicMock(spec=Redis)
    redis.keys.return_value = [b'service-queue-not-service-a']
    redis.zcard.return_value = 0
    redis_persist = mock.MagicMock(spec=Redis)
    datastore = mock.MagicMock()

    service_a = random_model_obj(Service)
    service_a.name = 'a'
    service_a.enabled = True

    datastore.list_all_services.return_value = [service_a]
    plumber = Plumber(redis=redis, redis_persist=redis_persist, datastore=datastore, delay=1)
    plumber.get_service_stage = mock.MagicMock(return_value=ServiceStage.Running)
    plumber.dispatch_client = mock.MagicMock()

    task = random_model_obj(Task)
    plumber.dispatch_client.request_work.side_effect = [task, None, None]

    plumber.running = TrueCountTimes(count=1)
    plumber.try_run()

    assert plumber.dispatch_client.service_failed.call_count == 1
    args = plumber.dispatch_client.service_failed.call_args
    assert args[0][0] == task.sid


def test_flush_paused_queues():
    redis = mock.MagicMock(spec=Redis)
    redis.keys.return_value = [b'service-queue-a']
    redis.zcard.return_value = 0
    redis_persist = mock.MagicMock(spec=Redis)
    datastore = mock.MagicMock()

    service_a = random_model_obj(Service)
    service_a.name = 'a'
    service_a.enabled = True

    datastore.list_all_services.return_value = [service_a]

    plumber = Plumber(redis=redis, redis_persist=redis_persist, datastore=datastore, delay=1)
    plumber.get_service_stage = mock.MagicMock(return_value=ServiceStage.Running)
    plumber.dispatch_client = mock.MagicMock()

    task = random_model_obj(Task)
    plumber.dispatch_client.request_work.side_effect = [task, None, None]

    plumber.running = TrueCountTimes(count=1)
    plumber.try_run()

    assert plumber.dispatch_client.service_failed.call_count == 0

    plumber.get_service_stage = mock.MagicMock(return_value=ServiceStage.Paused)

    plumber.running = TrueCountTimes(count=1)
    plumber.try_run()

    assert plumber.dispatch_client.service_failed.call_count == 1
    args = plumber.dispatch_client.service_failed.call_args
    assert args[0][0] == task.sid
