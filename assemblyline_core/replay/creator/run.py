import os

from assemblyline_core.replay.client import APIClient, DirectClient
from assemblyline_core.replay.replay import ReplayBase


class ReplayCreator(ReplayBase):
    def __init__(self):
        super().__init__("assemblyline.replay_creator")

        if not self.replay_config.creator.alert_input.enabled and \
                not self.replay_config.creator.submission_input.enabled:
            return

        # Create cache directory
        os.makedirs(self.replay_config.creator.working_directory, exist_ok=True)

        # Load client
        client_config = dict(lookback_time=self.replay_config.creator.lookback_time,
                             alert_fqs=self.replay_config.creator.alert_input.filter_queries,
                             badlist_fqs=self.replay_config.creator.badlist_input.filter_queries,
                             safelist_fqs=self.replay_config.creator.safelist_input.filter_queries,
                             submission_fqs=self.replay_config.creator.submission_input.filter_queries,
                             workflow_fqs=self.replay_config.creator.workflow_input.filter_queries)

        if self.replay_config.creator.client.type == 'direct':
            self.log.info("Using direct database access client")
            self.client = DirectClient(self.log, **client_config)
        elif self.replay_config.creator.client.type == 'api':
            self.log.info(f"Using API access client to ({self.replay_config.creator.client.options.host})")
            client_config.update(self.replay_config.creator.client.options.as_primitives())
            self.client = APIClient(self.log, **client_config)
        else:
            raise ValueError(f'Invalid client type ({self.replay_config.creator.client.type}). '
                             'Must be either \'api\' or \'direct\'.')

    def try_run(self):
        threads = {}
        if self.replay_config.creator.alert_input.enabled:
            threads['Load Alerts'] = self.client.setup_alert_input_queue

        if self.replay_config.creator.badlist_input.enabled:
            threads['Load Badlist Items'] = self.client.setup_badlist_input_queue

        if self.replay_config.creator.safelist_input.enabled:
            threads['Load Safelist Items'] = self.client.setup_safelist_input_queue

        if self.replay_config.creator.submission_input.enabled:
            threads['Load Submissions'] = self.client.setup_submission_input_queue

        if self.replay_config.creator.workflow_input.enabled:
            threads['Load Workflows'] = self.client.setup_workflow_input_queue

        if threads:
            self.maintain_threads(threads)
        else:
            self.log.warning("There are no configured input, terminating")
            self.main_loop_exit.set()
            self.stop()


if __name__ == '__main__':
    with ReplayCreator() as replay:
        replay.serve_forever()
