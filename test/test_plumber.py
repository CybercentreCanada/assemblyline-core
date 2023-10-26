from time import sleep
from unittest import mock

from assemblyline.odm.messages.task import Task
from assemblyline.odm.models.service import Service
from assemblyline.odm.random_data import random_model_obj
from assemblyline_core.plumber.run_plumber import Plumber
from assemblyline_core.server_base import ServiceStage
from mocking import TrueCountTimes
from redis import Redis


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
    datastore.ds.ca_certs = None
    datastore.ds.get_hosts.return_value = ["http://localhost:9200"]

    plumber = Plumber(redis=redis, redis_persist=redis_persist, datastore=datastore, delay=1)
    plumber.get_service_stage = mock.MagicMock(return_value=ServiceStage.Running)
    plumber.dispatch_client = mock.MagicMock()

    task = random_model_obj(Task)
    plumber.dispatch_client.request_work.side_effect = [task, None, None]

    plumber.running = TrueCountTimes(count=1)
    plumber.service_queue_plumbing()

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
    datastore.ds.ca_certs = None
    datastore.ds.get_hosts.return_value = ["http://localhost:9200"]

    plumber = Plumber(redis=redis, redis_persist=redis_persist, datastore=datastore, delay=1)
    plumber.get_service_stage = mock.MagicMock(return_value=ServiceStage.Running)
    plumber.dispatch_client = mock.MagicMock()

    task = random_model_obj(Task)
    plumber.dispatch_client.request_work.side_effect = [task, None, None]

    plumber.running = TrueCountTimes(count=1)
    plumber.service_queue_plumbing()

    assert plumber.dispatch_client.service_failed.call_count == 0

    plumber.get_service_stage = mock.MagicMock(return_value=ServiceStage.Paused)

    plumber.running = TrueCountTimes(count=1)
    plumber.service_queue_plumbing()

    assert plumber.dispatch_client.service_failed.call_count == 1
    args = plumber.dispatch_client.service_failed.call_args
    assert args[0][0] == task.sid


def test_cleanup_old_tasks(datastore_connection):
    # Create a bunch of random "old" tasks and clean them up
    redis = mock.MagicMock(spec=Redis)
    redis_persist = mock.MagicMock(spec=Redis)
    plumber = Plumber(redis=redis, redis_persist=redis_persist, datastore=datastore_connection, delay=1)

    # Generate new documents in .tasks index
    num_old_tasks = 10
    [plumber.datastore.ds.client.index(index=".tasks", document={
        "completed": True,
        "task": {
            "start_time_in_millis": 0
        }
    }) for _ in range(num_old_tasks)]
    sleep(1)

    # Assert that these have been indeed committed to the tasks index
    assert plumber.datastore.ds.client.search(index='.tasks',
                                              q="task.start_time_in_millis:0",
                                              track_total_hits=True,
                                              size=0)['hits']['total']['value'] == num_old_tasks

    # Run task cleanup, we should return to no more "old" completed tasks
    plumber.running = TrueCountTimes(count=1)
    plumber.cleanup_old_tasks()
    sleep(1)
    assert plumber.datastore.ds.client.search(index='.tasks',
                                              q="task.start_time_in_millis:0",
                                              track_total_hits=True,
                                              size=0)['hits']['total']['value'] == 0
