from unittest import mock

from assemblyline.odm.messages.task import Task

from assemblyline.odm.random_data import random_model_obj
from assemblyline.odm.models.service import Service

from assemblyline_core.plumber.run_plumber import Plumber

from .mocking import TrueCountTimes


def test_expire_missing_service():
    redis = mock.MagicMock()
    redis.keys.return_value = [b'service-queue-XXX']
    redis_persist = mock.MagicMock()
    datastore = mock.MagicMock()

    service_a = random_model_obj(Service)
    service_a.name = 'a'

    datastore.list_all_services.return_value = [service_a]

    with mock.patch('assemblyline_core.plumber.run_plumber.time'):
        plumber = Plumber(redis=redis, redis_persist=redis_persist, datastore=datastore)
        plumber.dispatch_client = mock.MagicMock()
        plumber.dispatch_client.request_work.return_value = None

        task = random_model_obj(Task)
        plumber.dispatch_client.request_work.side_effect = [task, None, None]

        plumber.running = TrueCountTimes(count=1)
        plumber.try_run()

        assert plumber.dispatch_client.service_failed.call_count == 1
        args = plumber.dispatch_client.service_failed.call_args
        assert args[0][0] == task.sid