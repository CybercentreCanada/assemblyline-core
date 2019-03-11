import hashlib
import logging
import random
import time

from al_core.dispatching.client import DispatchClient
from al_core.dispatching.dispatcher import service_queue_name
from al_core.server_base import ServerBase
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.file import File
from assemblyline.odm.models.result import Result
from assemblyline.odm.randomizer import random_model_obj, random_minimal_obj
from assemblyline.remote.datatypes.queues.named import NamedQueue, select


class RandomService(ServerBase):
    """Replaces everything past the dispatcher.

    Including service API, in the future probably include that in this test.
    """
    def __init__(self, datastore, filestore):
        log_level = logging.DEBUG if forge.get_config().core.dispatcher.debug_logging else logging.INFO
        super().__init__("assemblyline.randomservice", log_level=log_level)
        self.datastore = datastore
        self.filestore = filestore

        self.queues = [NamedQueue(service_queue_name(name)) for name in datastore.service.keys()]
        self.dispatch_client = DispatchClient(datastore)

    def run(self):
        self.log.info("Random service result generator ready!")
        self.log.info("Monitoring queues:")
        for q in self.queues:
            self.log.info(f"\t{q.name}")

        self.log.info("Waiting for messages...")
        while self.running:
            message = select(*self.queues, timeout=1)
            if not message:
                continue

            queue, msg = message
            task = ServiceTask(msg)
            self.log.info(f"\tQueue {queue} received a new task for sid {task.sid}.")
            action = random.randint(1, 10)
            if action >= 2:
                if action > 8:
                    result = random_minimal_obj(Result)
                else:
                    result = random_model_obj(Result)
                result.sha256 = task.fileinfo.sha256
                result.response.service_name = task.service_name
                result_key = result.build_key(hashlib.md5(task.service_config.encode("utf-8")).hexdigest())

                result.response.extracted = result.response.extracted[task.depth+2:]
                result.response.supplementary = result.response.supplementary[task.depth+2:]

                self.log.info(f"\t\tA result was generated for this task: {result_key}")

                new_files = result.response.extracted + result.response.supplementary
                for f in new_files:
                    if not self.datastore.file.get(f.sha256):
                        random_file = random_model_obj(File)
                        random_file.sha256 = f.sha256
                        self.datastore.file.save(f.sha256, random_file)
                    if not self.filestore.exists(f.sha256):
                        self.filestore.save(f.sha256, f.sha256)

                time.sleep(random.randint(0, 4))

                self.datastore.result.save(result_key, result)
                self.dispatch_client.service_finished(task, result, result_key)
            else:
                error = random_model_obj(Error)
                error.sha256 = task.fileinfo.sha256
                error.response.service_name = task.service_name
                error.type = random.choice(["EXCEPTION", "SERVICE DOWN", "SERVICE BUSY"])
                error_key = error.build_key(hashlib.md5(task.service_config.encode("utf-8")).hexdigest())
                self.log.info(f"\t\tA {error.response.status}:{error.type} error was generated for this task: {error_key}")

                self.dispatch_client.service_failed(task, error, error_key)


if __name__ == "__main__":
    from assemblyline.common import forge
    RandomService(forge.get_datastore(), forge.get_filestore()).serve_forever()
