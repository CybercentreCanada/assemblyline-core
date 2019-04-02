from apscheduler.schedulers.background import BackgroundScheduler

from al_core.alerter.run_alerter import ALERT_QUEUE_NAME
from al_core.dispatching.dispatcher import DISPATCH_TASK_HASH, SUBMISSION_QUEUE
from al_core.ingester.ingester import INGEST_QUEUE_NAME
from al_core.dispatching.dispatcher import service_queue_name
from al_core.ingester.ingester import drop_chance
from assemblyline.common import forge, metrics
from assemblyline.datastore import SearchException
from assemblyline.odm.messages.alerter_heartbeat import AlerterMessage
from assemblyline.odm.messages.dispatcher_heartbeat import DispatcherMessage
from assemblyline.odm.messages.expiry_heartbeat import ExpiryMessage
from assemblyline.odm.messages.ingest_heartbeat import IngestMessage
from assemblyline.odm.messages.service_heartbeat import ServiceMessage
from assemblyline.odm.messages.service_timing_heartbeat import ServiceTimingMessage
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.hash import Hash
from assemblyline.remote.datatypes.queues.comms import CommsQueue
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.queues.priority import PriorityQueue

STATUS_QUEUE = "status"

# noinspection PyBroadException
class HeartbeatManager(object):
    def __init__(self, sender, log, config=None, redis=None):
        self.sender = sender
        self.log = log

        self.config = config or forge.get_config()
        self.datastore = forge.get_datastore(self.config)


        self.redis = redis or get_client(
            db=self.config.core.redis.nonpersistent.db,
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )
        self.redis_persist = get_client(
            db=self.config.core.redis.persistent.db,
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )
        self.status_queue = CommsQueue(STATUS_QUEUE, self.redis)
        self.dispatch_active_hash = Hash(DISPATCH_TASK_HASH, self.redis_persist)
        self.dispatcher_submission_queue = NamedQueue(SUBMISSION_QUEUE, self.redis)
        self.ingest_scanning = Hash('m-scanning-table', self.redis_persist)
        self.ingest_unique_queue = PriorityQueue('m-unique', self.redis_persist)
        self.ingest_queue = NamedQueue(INGEST_QUEUE_NAME, self.redis_persist)
        self.alert_queue = NamedQueue(ALERT_QUEUE_NAME, self.redis_persist)

        constants = forge.get_constants(self.config)
        self.c_rng = constants.PRIORITY_RANGES['critical']
        self.h_rng = constants.PRIORITY_RANGES['high']
        self.m_rng = constants.PRIORITY_RANGES['medium']
        self.l_rng = constants.PRIORITY_RANGES['low']
        self.c_s_at = self.config.core.ingester.sampling_at['critical']
        self.h_s_at = self.config.core.ingester.sampling_at['high']
        self.m_s_at = self.config.core.ingester.sampling_at['medium']
        self.l_s_at = self.config.core.ingester.sampling_at['low']

        self.to_expire = {k: 0 for k in metrics.EXPIRY_METRICS}
        if self.config.core.expiry.batch_delete:
            self.delete_query = f"expiry_ts:[* TO {self.datastore.ds.now}-{self.config.core.expiry.delay}" \
                f"{self.datastore.ds.hour}/DAY]"
        else:
            self.delete_query = f"expiry_ts:[* TO {self.datastore.ds.now}-{self.config.core.expiry.delay}" \
                f"{self.datastore.ds.hour}]"

        self.scheduler = BackgroundScheduler(daemon=True)
        self.scheduler.add_job(self._reload_expiry_queues, 'interval',
                               seconds=self.config.core.metrics.export_interval * 4)
        self.scheduler.start()

    def _reload_expiry_queues(self):
        try:
            self.log.info("Refreshing expiry queues...")
            for collection_name in metrics.EXPIRY_METRICS:
                try:
                    collection = getattr(self.datastore, collection_name)
                    self.to_expire[collection_name] = collection.search(self.delete_query, rows=0, fl='id')['total']
                except SearchException:
                    self.to_expire[collection_name] = 0
        except Exception:
            self.log.exception("Unknown exception occurred while reloading expiry queues:")

    def send_heartbeat(self, m_type, m_name, m_data, instances):
        if m_type == "dispatcher":
            try:
                msg = {
                    "sender": self.sender,
                    "msg": {
                        "inflight": {
                            "max": self.config.core.dispatcher.max_inflight,
                            "outstanding": self.dispatch_active_hash.length()
                        },
                        "instances": instances,
                        "metrics": m_data,
                        "queues": {
                            "ingest": self.dispatcher_submission_queue.length()
                        }
                    }
                }
                self.status_queue.publish(DispatcherMessage(msg).as_primitives())
                self.log.info(f"Sent dispatcher heartbeat: {msg['msg']}")
            except Exception:
                self.log.exception("An exception occurred while generating DispatcherMessage")

        elif m_type == "ingester":
            try:
                c_q_len = self.ingest_unique_queue.count(*self.c_rng)
                h_q_len = self.ingest_unique_queue.count(*self.h_rng)
                m_q_len = self.ingest_unique_queue.count(*self.m_rng)
                l_q_len = self.ingest_unique_queue.count(*self.l_rng)

                msg = {
                    "sender": self.sender,
                    "msg": {
                        "instances": instances,
                        "metrics": m_data,
                        "processing": {
                            "inflight": self.ingest_scanning.length()
                        },
                        "processing_chance": {
                            "critical": 1 - drop_chance(c_q_len, self.c_s_at),
                            "high": 1 - drop_chance(h_q_len, self.h_s_at),
                            "low": 1 - drop_chance(l_q_len, self.l_s_at),
                            "medium": 1 - drop_chance(m_q_len, self.m_s_at)
                        },
                        "queues": {
                            "critical": c_q_len,
                            "high": h_q_len,
                            "ingest": self.ingest_queue.length(),
                            "low": l_q_len,
                            "medium": m_q_len
                        }
                    }
                }
                self.status_queue.publish(IngestMessage(msg).as_primitives())
                self.log.info(f"Sent ingester heartbeat: {msg['msg']}")
            except Exception:
                self.log.exception("An exception occurred while generating IngestMessage")

        elif m_type == "alerter":
            try:
                msg = {
                    "sender": self.sender,
                    "msg": {
                        "instances": instances,
                        "metrics": m_data,
                        "queues": {
                            "alert": self.alert_queue.length()
                        }
                    }
                }
                self.status_queue.publish(AlerterMessage(msg).as_primitives())
                self.log.info(f"Sent alerter heartbeat: {msg['msg']}")
            except Exception:
                self.log.exception("An exception occurred while generating AlerterMessage")

        elif m_type == "expiry":
            try:
                msg = {
                    "sender": self.sender,
                    "msg": {
                        "instances": instances,
                        "metrics": m_data,
                        "queues": self.to_expire
                    }
                }
                self.status_queue.publish(ExpiryMessage(msg).as_primitives())
                self.log.info(f"Sent expiry heartbeat: {msg['msg']}")
            except Exception:
                self.log.exception("An exception occurred while generating ExpiryMessage")

        elif m_type == "service":
            try:
                msg = {
                    "sender": self.sender,
                    "msg": {
                        "instances": instances,
                        "metrics": m_data,
                        "queue": NamedQueue(service_queue_name(m_name), host=self.redis).length(),
                        "service_name": m_name
                    }
                }
                self.status_queue.publish(ServiceMessage(msg).as_primitives())
                self.log.info(f"Sent service heartbeat: {msg['msg']}")
            except Exception:
                self.log.exception("An exception occurred while generating ServiceMessage")

        elif m_type == "service_timing":
            try:
                msg = {
                    "sender": self.sender,
                    "msg": {
                        "instances": instances,
                        "metrics": m_data,
                        "service_name": m_name
                    }
                }
                self.status_queue.publish(ServiceTimingMessage(msg).as_primitives())
                self.log.info(f"Sent service timing heartbeat: {msg['msg']}")
            except Exception:
                self.log.exception("An exception occurred while generating ServiceTimingMessage")

        else:
            self.log.warning(f"Skipping unknown counter: {m_name} [{m_type}] ==> {m_data}")
