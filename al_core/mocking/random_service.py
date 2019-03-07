import random
import signal

from al_core.dispatching.client import DispatchClient
from al_core.dispatching.dispatcher import service_queue_name
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.file import File
from assemblyline.odm.models.result import Result
from assemblyline.odm.randomizer import random_model_obj, random_minimal_obj
from assemblyline.remote.datatypes.queues.named import NamedQueue, select


class RandomService(object):
    """Replaces everything past the dispatcher.

    Including service API, in the future probably include that in this test.
    """
    def __init__(self, datastore, filestore):
        self.datastore = datastore
        self.filestore = filestore
        self.running = False

        self.queues = [NamedQueue(service_queue_name(name)) for name in datastore.service.keys()]
        self.dispatch_client = DispatchClient(datastore)

        signal.signal(signal.SIGINT, self.interrupt_handler)
        signal.signal(signal.SIGTERM, self.interrupt_handler)

    def interrupt_handler(self, _signum, _stack_frame):
        print("Random service result generator caught signal. Coming down...")
        self.running = False

    def run(self):
        self.running = True
        print("Random service result generator ready!")
        print("Monitoring queues:")
        for q in self.queues:
            print(f"\t{q.name}")

        print("Waiting for messages...")
        while self.running:
            message = select(*self.queues, timeout=1)
            if not message:
                continue

            queue, msg = message
            task = ServiceTask(msg)
            print(f"\tQueue {queue} received a new task for sid {task.sid}.")
            action = random.randint(1, 10)
            if action >= 2:
                if action > 8:
                    result = random_minimal_obj(Result)
                else:
                    result = random_model_obj(Result)
                result.sha256 = task.fileinfo.sha256
                result.response.service_name = task.service_name
                result_key = result.build_key(task.service_config)

                print(f"\t\tA result was generated for this task: {result_key}")

                new_files = result.response.extracted + result.response.supplementary
                for f in new_files:
                    if not self.datastore.file.get(f.sha256):
                        random_file = random_model_obj(File)
                        random_file.sha256 = f.sha256
                        self.datastore.file.save(f.sha256, random_file)
                    if not self.filestore.exists(f.sha256):
                        self.filestore.save(f.sha256, f.sha256)


                self.datastore.result.save(result_key, result)
                self.dispatch_client.service_finished(task, result, result_key)
            else:
                error = random_model_obj(Error)
                error.sha256 = task.fileinfo.sha256
                error.response.service_name = task.service_name
                print(f"\t\tA {error.response.status} error was generated for this task: {error.response.message}")

                self.dispatch_client.service_failed(task, error)


if __name__ == "__main__":
    from assemblyline.common import forge

    rs = RandomService(forge.get_datastore(), forge.get_filestore())
    rs.run()
