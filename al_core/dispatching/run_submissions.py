"""
A dispatcher server that ensures all of the files in a submission are complete.
"""
import elasticapm
import logging

from assemblyline.common import forge

from al_core.dispatching.dispatcher import Dispatcher, SubmissionTask
from al_core.server_base import ServerBase


class SubmissionDispatchServer(ServerBase):
    def __init__(self, datastore=None, redis=None, redis_persist=None, logger=None):
        log_level = logging.DEBUG if forge.get_config().core.dispatcher.debug_logging else logging.INFO
        super().__init__('assemblyline.dispatcher.submissions', logger, log_level=log_level)

        config = forge.get_config()
        datastore = datastore or forge.get_datastore(config)
        self.dispatcher = Dispatcher(logger=self.log, redis=redis, redis_persist=redis_persist, datastore=datastore)

        if config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=config.core.metrics.apm_server.server_url,
                                                service_name="dispatcher")
        else:
            self.apm_client = None

    def close(self):
        if self.apm_client:
            elasticapm.uninstrument()

    def try_run(self):
        queue = self.dispatcher.submission_queue

        while self.running:
            try:
                message = queue.pop(timeout=1)
                if not message:
                    continue

                # Start of process dispatcher transaction
                if self.apm_client:
                    self.apm_client.begin_transaction('Process dispatcher message')

                # This is probably a complete task
                if 'submission' in message:
                    task = SubmissionTask(message)
                    if self.apm_client:
                        elasticapm.tag(sid=task.submission.sid)
                        
                # This is just as sid nudge, this submission should already be running
                elif 'sid' in message:
                    active_task = self.dispatcher.active_tasks.get(message['sid'])
                    if self.apm_client:
                        elasticapm.tag(sid=message['sid'])
                    if active_task is None:
                        self.log.warning(f"[{message['sid']}] Dispatcher was nudged for inactive submission.")
                        # End of process dispatcher transaction (success)
                        if self.apm_client:
                            self.apm_client.end_transaction('submission_message', 'inactive')
                        continue

                    task = SubmissionTask(active_task)

                else:
                    self.log.error(f'Corrupted submission message in dispatcher {message}')
                    # End of process dispatcher transaction (success)
                    if self.apm_client:
                        self.apm_client.end_transaction('submission_message', 'corrupted')
                    continue

                self.dispatcher.dispatch_submission(task)

                # End of process dispatcher transaction (success)
                if self.apm_client:
                    self.apm_client.end_transaction('submission_message', 'success')

            except Exception as error:
                self.log.exception(error)
                # End of process dispatcher transaction (success)
                if self.apm_client:
                    self.apm_client.end_transaction('submission_message', 'exception')

    def stop(self):
        self.dispatcher.submission_queue.push(None)
        super().stop()


if __name__ == '__main__':
    SubmissionDispatchServer().serve_forever()