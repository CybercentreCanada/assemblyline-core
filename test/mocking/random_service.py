import random
import time

from assemblyline.common.forge import CachedObject

from assemblyline.common.constants import SERVICE_STATE_HASH, ServiceStatus
from assemblyline.common import forge
from assemblyline.common.uid import get_random_id
from assemblyline.remote.datatypes.hash import ExpiringHash
from assemblyline_core.dispatching.client import DispatchClient
from assemblyline_core.server_base import ServerBase
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.metrics import MetricsFactory
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.odm.messages.service_heartbeat import Metrics
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.file import File
from assemblyline.odm.models.result import Result
from assemblyline.odm.randomizer import random_model_obj, random_minimal_obj
from assemblyline.remote.datatypes.queues.priority import select


class RandomService(ServerBase):
    """Replaces everything past the dispatcher.

    Including service API, in the future probably include that in this test.
    """
    def __init__(self, datastore=None, filestore=None):
        super().__init__('assemblyline.randomservice')
        self.config = forge.get_config()
        self.datastore = datastore or forge.get_datastore()
        self.filestore = filestore or forge.get_filestore()
        self.client_id = get_random_id()
        self.service_state_hash = ExpiringHash(SERVICE_STATE_HASH, ttl=30 * 60)

        self.counters = {n: MetricsFactory('service', Metrics, name=n, config=self.config)
                         for n in self.datastore.service_delta.keys()}
        self.queues = [forge.get_service_queue(name) for name in self.datastore.service_delta.keys()]
        self.dispatch_client = DispatchClient(self.datastore)
        self.service_info = CachedObject(self.datastore.list_all_services, kwargs={'as_obj': False})

    def run(self):
        self.log.info("Random service result generator ready!")
        self.log.info("Monitoring queues:")
        for q in self.queues:
            self.log.info(f"\t{q.name}")

        self.log.info("Waiting for messages...")
        while self.running:
            # Reset Idle flags
            for s in self.service_info:
                if s['enabled']:
                    self.service_state_hash.set(f"{self.client_id}_{s['name']}",
                                                (s['name'], ServiceStatus.Idle, time.time() + 30 + 5))

            message = select(*self.queues, timeout=1)
            if not message:
                continue

            archive_ts = now_as_iso(self.config.datastore.ilm.days_until_archive * 24 * 60 * 60)
            if self.config.submission.dtl:
                expiry_ts = now_as_iso(self.config.submission.dtl * 24 * 60 * 60)
            else:
                expiry_ts = None
            queue, msg = message
            task = ServiceTask(msg)

            if not self.dispatch_client.running_tasks.add(task.key(), task.as_primitives()):
                continue

            # Set service busy flag
            self.service_state_hash.set(f"{self.client_id}_{task.service_name}",
                                        (task.service_name, ServiceStatus.Running, time.time() + 30 + 5))

            # METRICS
            self.counters[task.service_name].increment('execute')
            # METRICS (not caching here so always miss)
            self.counters[task.service_name].increment('cache_miss')

            self.log.info(f"\tQueue {queue} received a new task for sid {task.sid}.")
            action = random.randint(1, 10)
            if action >= 2:
                if action > 8:
                    result = random_minimal_obj(Result)
                else:
                    result = random_model_obj(Result)
                result.sha256 = task.fileinfo.sha256
                result.response.service_name = task.service_name
                result.archive_ts = archive_ts
                result.expiry_ts = expiry_ts
                result.response.extracted = result.response.extracted[task.depth+2:]
                result.response.supplementary = result.response.supplementary[task.depth+2:]
                result_key = Result.help_build_key(sha256=task.fileinfo.sha256,
                                                   service_name=task.service_name,
                                                   service_version='0',
                                                   is_empty=result.is_empty())

                self.log.info(f"\t\tA result was generated for this task: {result_key}")

                new_files = result.response.extracted + result.response.supplementary
                for f in new_files:
                    if not self.datastore.file.get(f.sha256):
                        random_file = random_model_obj(File)
                        random_file.archive_ts = archive_ts
                        random_file.expiry_ts = expiry_ts
                        random_file.sha256 = f.sha256
                        self.datastore.file.save(f.sha256, random_file)
                    if not self.filestore.exists(f.sha256):
                        self.filestore.put(f.sha256, f.sha256)

                time.sleep(random.randint(0, 2))

                self.dispatch_client.service_finished(task.sid, result_key, result)

                # METRICS
                if result.result.score > 0:
                    self.counters[task.service_name].increment('scored')
                else:
                    self.counters[task.service_name].increment('not_scored')

            else:
                error = random_model_obj(Error)
                error.archive_ts = archive_ts
                error.expiry_ts = expiry_ts
                error.sha256 = task.fileinfo.sha256
                error.response.service_name = task.service_name
                error.type = random.choice(["EXCEPTION", "SERVICE DOWN", "SERVICE BUSY"])

                error_key = error.build_key('0')

                self.log.info(f"\t\tA {error.response.status}:{error.type} "
                              f"error was generated for this task: {error_key}")

                self.dispatch_client.service_failed(task.sid, error_key, error)

                # METRICS
                if error.response.status == "FAIL_RECOVERABLE":
                    self.counters[task.service_name].increment('fail_recoverable')
                else:
                    self.counters[task.service_name].increment('fail_nonrecoverable')


if __name__ == "__main__":
    RandomService().serve_forever()
