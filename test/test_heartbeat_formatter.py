from unittest import mock

from assemblyline.common import forge
from assemblyline.remote.datatypes.hash import ExpiringHash

from assemblyline_core.metrics import heartbeat_formatter
from assemblyline_core.metrics.heartbeat_formatter import HEARTBEAT_HASH, HeartbeatFormatter


ALERTER_METRICS = {"created": 1, "error": 0, "received": 3, "updated": 2, "wait": 0}


def make_formatter():
    formatter = HeartbeatFormatter.__new__(HeartbeatFormatter)
    formatter.sender = "test_sender"
    formatter.log = mock.Mock()
    formatter.status_queue = mock.Mock()
    formatter.heartbeat_cache = mock.Mock()
    formatter.alert_queue = mock.Mock(**{"length.return_value": 4})
    formatter.alert_retry_queue = mock.Mock(**{"length.return_value": 1})
    return formatter


def test_heartbeat_is_cached_under_type_and_name():
    formatter = make_formatter()

    with mock.patch.object(heartbeat_formatter.time, "time", return_value=1700000000.0):
        formatter.send_heartbeat("alerter", "alerter", ALERTER_METRICS, 2)

    formatter.status_queue.publish.assert_called_once()
    published = formatter.status_queue.publish.call_args.args[0]
    assert published["msg_type"] == "AlerterHeartbeat"
    assert published["msg"]["queues"] == {"alert": 4, "alert_retry": 1}

    formatter.heartbeat_cache.set.assert_called_once_with(
        "alerter.alerter", {**published, "timestamp": 1700000000.0})


def test_cache_failure_does_not_stop_publish():
    formatter = make_formatter()
    formatter.heartbeat_cache.set.side_effect = ConnectionError("redis down")

    formatter.send_heartbeat("alerter", "alerter", ALERTER_METRICS, 2)

    formatter.status_queue.publish.assert_called_once()
    formatter.log.exception.assert_called_once()
    assert "cache" in formatter.log.exception.call_args.args[0]


def test_cache_is_expiring_hash_with_two_export_intervals():
    config = forge.get_config()
    with mock.patch.object(heartbeat_formatter.forge, "get_datastore"), \
            mock.patch.object(heartbeat_formatter, "get_client", return_value=mock.MagicMock()), \
            mock.patch.object(heartbeat_formatter, "BackgroundScheduler"):
        formatter = HeartbeatFormatter("test_sender", mock.Mock(), config=config, redis=mock.MagicMock())

    assert isinstance(formatter.heartbeat_cache, ExpiringHash)
    assert formatter.heartbeat_cache.name == HEARTBEAT_HASH
    assert formatter.heartbeat_cache.ttl == config.core.metrics.export_interval * 2
