import elasticapm
import time

from assemblyline.common.forge import get_service_queue
from assemblyline.remote.datatypes.exporting_counter import export_metrics_once

from assemblyline.common.constants import DISPATCH_RUNNING_TASK_HASH, SCALER_TIMEOUT_QUEUE

from assemblyline.odm.messages.task import Task

from assemblyline_core.dispatching.dispatch_hash import DispatchHash

from assemblyline.common.metrics import MetricsFactory
from assemblyline.odm.messages.watcher_heartbeat import Metrics
from assemblyline.odm.messages.service_heartbeat import Metrics as ServiceMetrics
from assemblyline.remote.datatypes import retry_call
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.queues.priority import UniquePriorityQueue
from assemblyline.remote.datatypes.hash import ExpiringHash

from assemblyline_core.server_base import CoreBase
from assemblyline_core.watcher.client import WATCHER_HASH, WATCHER_QUEUE, MAX_TIMEOUT, WatcherAction


class WatcherServer(CoreBase):
    def __init__(self, redis=None, redis_persist=None):
        super().__init__('assemblyline.watcher', redis=redis, redis_persist=redis_persist)

        # Watcher structures
        self.hash = ExpiringHash(name=WATCHER_HASH, ttl=MAX_TIMEOUT, host=self.redis_persist)
        self.queue = UniquePriorityQueue(WATCHER_QUEUE, self.redis_persist)

        # Task management structures
        self.running_tasks = ExpiringHash(DISPATCH_RUNNING_TASK_HASH, host=self.redis)  # TODO, move to persistant?
        self.scaler_timeout_queue = NamedQueue(SCALER_TIMEOUT_QUEUE, host=self.redis_persist)

        # Metrics tracking
        self.counter = MetricsFactory(metrics_type='watcher', schema=Metrics, name='watcher',
                                      redis=self.redis, config=self.config)

        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="watcher")
        else:
            self.apm_client = None

    def try_run(self):
        counter = self.counter
        apm_client = self.apm_client

        while self.running:
            self.heartbeat()

            # Download all messages from the queue that have expired
            seconds, _ = retry_call(self.redis.time)
            messages = self.queue.dequeue_range(0, seconds)

            cpu_mark = time.process_time()
            time_mark = time.time()

            # Try to pass on all the messages to their intended recipient, try not to let
            # the failure of one message from preventing the others from going through
            for key in messages:
                # Start of transaction
                if apm_client:
                    apm_client.begin_transaction('process_messages')

                message = self.hash.pop(key)
                if message:
                    try:
                        if message['action'] == WatcherAction.TimeoutTask:
                            self.cancel_service_task(message['task_key'], message['worker'])
                        else:
                            queue = NamedQueue(message['queue'], self.redis)
                            queue.push(message['message'])

                        self.counter.increment('expired')
                        # End of transaction (success)
                        if apm_client:
                            apm_client.end_transaction('watch_message', 'success')
                    except Exception as error:
                        # End of transaction (exception)
                        if apm_client:
                            apm_client.end_transaction('watch_message', 'error')

                        self.log.exception(error)
                else:
                    # End of transaction (duplicate)
                    if apm_client:
                        apm_client.end_transaction('watch_message', 'duplicate')

                    self.log.warning(f'Handled watch twice: {key} {len(key)} {type(key)}')

            counter.increment_execution_time('cpu_seconds', time.process_time() - cpu_mark)
            counter.increment_execution_time('busy_seconds', time.time() - time_mark)

            if not messages:
                time.sleep(0.1)

    def cancel_service_task(self, task_key, worker):
        # We believe a service task has timed out, try and read it from running tasks
        # If we can't find the task in running tasks, it finished JUST before timing out, let it go
        task = self.running_tasks.pop(task_key)
        if not task:
            return

        # We can confirm that the task is ours now, even if the worker finished, the result will be ignored
        task = Task(task)
        self.log.info(f"[{task.sid}] Service {task.service_name} timed out on {task.fileinfo.sha256}.")

        # Mark the previous attempt as invalid and redispatch it
        dispatch_table = DispatchHash(task.sid, self.redis)
        dispatch_table.fail_recoverable(task.fileinfo.sha256, task.service_name)
        dispatch_table.dispatch(task.fileinfo.sha256, task.service_name)
        key = get_service_queue(task.service_name, self.redis).push(task.priority, task.as_primitives())
        dispatch_table.set_dispatch_key(task.fileinfo.sha256, task.service_name, key)

        # We push the task of killing the container off on the scaler, which already has root access
        # the scaler can also double check that the service name and container id match, to be sure
        # we aren't accidentally killing the wrong container
        self.scaler_timeout_queue.push({
            'service': task.service_name,
            'container': worker
        })

        # Report to the metrics system that a recoverable error has occurred for that service
        export_metrics_once(task.service_name, ServiceMetrics, dict(fail_recoverable=1),
                            host=worker, counter_type='service')
