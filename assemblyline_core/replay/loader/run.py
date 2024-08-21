import shelve
import os

from datetime import datetime, timedelta

from assemblyline_core.replay.client import APIClient, DirectClient
from assemblyline_core.replay.replay import ReplayBase


class ReplayLoader(ReplayBase):
    def __init__(self):
        super().__init__("assemblyline.replay_loader")

        # Make sure all directories exist
        os.makedirs(self.replay_config.loader.working_directory, exist_ok=True)
        os.makedirs(self.replay_config.loader.input_directory, exist_ok=True)
        os.makedirs(self.replay_config.loader.failed_directory, exist_ok=True)

        # Create/Load the cache
        self.cache = shelve.open(os.path.join(self.replay_config.loader.working_directory, 'loader_cache.db'))
        self.last_sync_check = datetime.now()
        if 'files' not in self.cache:
            self.cache['files'] = set()

        # Load client
        if self.replay_config.loader.client.type == 'direct':
            self.log.info("Using direct database access client")
            self.client = DirectClient(self.log)
        elif self.replay_config.loader.client.type == 'api':
            self.log.info(f"Using API access client to ({self.replay_config.loader.client.options.host})")
            self.client = APIClient(self.log, **self.replay_config.loader.client.options.as_primitives())
        else:
            raise ValueError(f'Invalid client type ({self.replay_config.loader.client.type}). '
                             'Must be either \'api\' or \'direct\'.')

    def load_files(self, once=False):
        while self.running:
            new_files = False
            new_cache = set()

            # Check the datastore periodically for the last Replay bundle that was imported
            if datetime.now() > self.last_sync_check + timedelta(seconds=self.replay_config.loader.sync_check_interval):

                if not self.client.query_alerts(
                        query=f"metadata.bundle.loaded:[now-{self.replay_config.loader.sync_check_interval}s TO now]",
                        track_total_hits=True):
                    self.log.warning("Haven't received a new bundle since the last check!")
                self.last_sync_check = datetime.now()

            for root, _, files in os.walk(self.replay_config.loader.input_directory, topdown=False):
                for name in files:
                    # Unexpected files that could be the result of external transfer mechanisms
                    if name.startswith('.') or not (name.endswith('.al_bundle') or \
                                                    name.endswith('.al_json') or \
                                                    name.endswith('.al_json.cart')):
                        continue

                    file_path = os.path.join(root, name)

                    # Cache file
                    new_cache.add(file_path)

                    if file_path not in self.cache['files']:
                        self.log.info(f'Queueing file: {file_path}')
                        self.client.put_file(file_path)
                        new_files = True

                    # Cache file
                    self.cache['files'].add(file_path)

            # Cleanup cache
            self.cache['files'] = new_cache

            if once:
                break

            if not new_files:
                self.sleep(5)

    def try_run(self):
        threads = {
            # Pull in completed submissions
            'File loader': self.load_files
        }

        self.maintain_threads(threads)

    def stop(self):
        super().stop()
        self.cache.close()


if __name__ == '__main__':
    with ReplayLoader() as replay:
        replay.serve_forever()
