import shelve
import shutil
import os

from queue import Empty, Queue

from assemblyline_core.replay.client import APIClient, DirectClient
from assemblyline_core.replay.replay import ReplayBase


class ReplayLoader(ReplayBase):
    def __init__(self):
        super().__init__("assemblyline.replay_loader")
        self.file_queue = Queue()

        # Create cache directory
        os.makedirs(self.replay_config.loader.working_directory, exist_ok=True)

        # Create/Load the cache
        self.cache = shelve.open(os.path.join(self.replay_config.loader.working_directory, 'loader_cache.db'))
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

    def process_file(self):
        while self.running:
            try:
                file_path = self.file_queue.get(block=True, timeout=3)
                self.log.info(f"Processing file: {file_path}")
                try:
                    self.client.load_bundle(file_path,
                                            min_classification=self.replay_config.loader.min_classification,
                                            rescan_services=self.replay_config.loader.rescan)
                    if os.path.exists(file_path):
                        os.unlink(file_path)
                except Exception:
                    # Make sure failed directory exists
                    os.makedirs(self.replay_config.loader.failed_directory, exist_ok=True)

                    self.log.error(f"Failed to load the bundle file {file_path}, moving it to the failed directory.")
                    failed_path = os.path.join(self.replay_config.loader.failed_directory, os.path.basename(file_path))
                    shutil.move(file_path, failed_path)
            except Empty:
                pass

    def load_files(self):
        while self.running:
            new_files = False
            new_cache = set()
            for root, _, files in os.walk(self.replay_config.loader.input_directory, topdown=False):
                for name in files:
                    file_path = os.path.join(root, name)

                    # Cache file
                    new_cache.add(file_path)

                    if file_path not in self.cache['files']:
                        self.file_queue.put(file_path)
                        new_files = True

                    # Cache file
                    self.cache['files'].add(file_path)

            # Cleanup cache
            self.cache['files'] = new_cache

            if not new_files:
                self.sleep(5)

    def try_run(self):
        threads = {
            # Pull in completed submissions
            'File loader': self.load_files
        }

        for ii in range(self.replay_config.loader.input_threads):
            threads[f'File processor #{ii}'] = self.process_file

        self.maintain_threads(threads)

    def stop(self):
        self.cache.close()
        return super().stop()


if __name__ == '__main__':
    with ReplayLoader() as replay:
        replay.serve_forever()
