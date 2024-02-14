import shutil
import os

from assemblyline_core.replay.client import APIClient, DirectClient
from assemblyline_core.replay.replay import ReplayBase


class ReplayLoaderWorker(ReplayBase):
    def __init__(self):
        super().__init__("assemblyline.replay_loader.worker")

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

    def process_file(self, once=False):
        while self.running:
            file_path = self.client.get_next_file()

            if file_path:
                self.log.info(f"Processing file: {file_path}")
                try:
                    if file_path.endswith(".al_bundle"):
                        self.client.load_bundle(file_path,
                                                min_classification=self.replay_config.loader.min_classification,
                                                rescan_services=self.replay_config.loader.rescan)
                    elif file_path.endswith(".al_json"):
                        self.client.load_json(file_path)

                    if os.path.exists(file_path):
                        os.unlink(file_path)
                except OSError as e:
                    # Critical exception occurred
                    if 'Stale file handle' in str(e):
                        # Terminate on stale file handle from NFS mount
                        self.log.warning("Stale file handle detected. Terminating..")
                        self.stop()
                    elif 'Invalid cross-device link' in str(e):
                        # Terminate on NFS-related error
                        self.log.warning("'Invalid cross-device link' exception detected. Terminating..")
                        self.stop()
                except Exception:
                    # Make sure failed directory exists
                    os.makedirs(self.replay_config.loader.failed_directory, exist_ok=True)

                    self.log.error(f"Failed to load the bundle file {file_path}, moving it to the failed directory.")
                    failed_path = os.path.join(self.replay_config.loader.failed_directory, os.path.basename(file_path))
                    shutil.move(file_path, failed_path)

            if once:
                break

    def try_run(self):
        threads = {}

        for ii in range(self.replay_config.loader.input_threads):
            threads[f'File processor #{ii}'] = self.process_file

        self.maintain_threads(threads)

    def stop(self):
        return super().stop()


if __name__ == '__main__':
    with ReplayLoaderWorker() as replay:
        replay.serve_forever()
