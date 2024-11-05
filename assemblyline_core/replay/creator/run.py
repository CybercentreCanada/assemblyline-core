import os

from assemblyline_core.replay.client import APIClient, DirectClient
from assemblyline_core.replay.replay import ReplayBase, INPUT_TYPES

class ReplayCreator(ReplayBase):
    def __init__(self):
        super().__init__("assemblyline.replay_creator")

        if not self.replay_config.creator.alert_input.enabled and \
                not self.replay_config.creator.submission_input.enabled:
            return

        # Create cache directory
        os.makedirs(self.replay_config.creator.working_directory, exist_ok=True)

        # Load client
        client_config = {f'{input_type}_fqs': getattr(self.replay_config.creator, f'{input_type}_input').filter_queries
                         for input_type in INPUT_TYPES}
        client_config['lookback_time'] = self.replay_config.creator.lookback_time

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
        for input_type in INPUT_TYPES:
            if getattr(self.replay_config.creator, f'{input_type}_input').enabled:
                threads[f'Load {input_type.capitalize()}s'] = getattr(self.client, f'setup_{input_type}_input_queue')

        if threads:
            self.maintain_threads(threads)
        else:
            self.log.warning("There are no configured input, terminating")
            self.main_loop_exit.set()
            self.stop()


if __name__ == '__main__':
    with ReplayCreator() as replay:
        replay.serve_forever()
