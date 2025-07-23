from time import sleep
from unittest import mock

from assemblyline_core.plumber.run_plumber import Plumber
from assemblyline_core.server_base import ServiceStage
from mocking import TrueCountTimes
from redis import Redis

from assemblyline.odm.messages.task import Task
from assemblyline.odm.models.service import Service
from assemblyline.odm.models.user import ApiKey, User
from assemblyline.odm.random_data import random_model_obj


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


def test_user_setting_migrations(datastore_connection):
    from assemblyline.odm.models.config import SubmissionProfileParams

    SubmissionProfileParams.fields().keys()
    # Create a bunch of random "old" tasks and clean them up
    redis = mock.MagicMock(spec=Redis)
    redis_persist = mock.MagicMock(spec=Redis)
    plumber = Plumber(redis=redis, redis_persist=redis_persist, datastore=datastore_connection, delay=1)

    # Create a user with old settings (format prior to 4.6)
    settings = {'classification': 'TLP:CLEAR', 'deep_scan': False, 'description': '', 'download_encoding': 'cart', 'default_external_sources': ['Malware Bazaar', 'VirusTotal'], 'default_zip_password': 'zippy', 'executive_summary': False, 'expand_min_score': 500, 'generate_alert': False, 'ignore_cache': False, 'ignore_dynamic_recursion_prevention': False, 'ignore_recursion_prevention': False, 'ignore_filtering': False, 'malicious': False, 'priority': 369, 'profile': False, 'service_spec': {'AVClass': {'include_malpedia_dataset': False}}, 'services': {'selected': ['Extraction', 'ConfigExtractor', 'YARA'], 'excluded': [], 'rescan': [], 'resubmit': [], 'runtime_excluded': []}, 'submission_view': 'report', 'ttl': 0}

    user_account = random_model_obj(User, as_json=True)
    user_account['uname'] = "admin"
    user_account['apikeys'] = {'test': random_model_obj(ApiKey, as_json=True)}

    # Disable the dynamic mapping prevention to allow an old document to be inserted
    datastore_connection.ds.client.indices.put_mapping(index='user_settings', dynamic='false')
    datastore_connection.ds.client.index(index="user_settings", id="admin", document=settings)
    datastore_connection.ds.client.index(index="user", id="admin", document=user_account)

    datastore_connection.user_settings.commit()
    datastore_connection.user.commit()

    # Initiate the migration
    plumber.user_apikey_cleanup()
    plumber.migrate_user_settings()

    # Check that the settings have been migrated
    migrated_settings = datastore_connection.user_settings.get("admin", as_obj=False)

    # Check to see if API keys for the user were transferred to the new index
    assert datastore_connection.apikey.search('uname:admin', rows=0)['total'] > 0

    # Deprecated settings should be removed
    assert "ignore_dynamic_recursion_prevention" not in migrated_settings

    # All former submission settings at the root-level should be moved to submission profiles
    assert all([key not in migrated_settings for key in SubmissionProfileParams.fields().keys()] )

    for settings in migrated_settings['submission_profiles'].values():
        assert settings['classification'] == 'TLP:C'
        assert settings['deep_scan'] is False
        assert settings['generate_alert'] is False
        assert settings['ignore_cache'] is False
        assert settings['priority'] == 369
        # Full service spec should be preserved in default profile (along with others by default if there's no restricted parameters)
        assert settings['service_spec'] == {'AVClass': {'include_malpedia_dataset': False}}
        assert settings['ttl'] == 0
