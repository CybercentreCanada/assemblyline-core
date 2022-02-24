import os

from pprint import pformat
from queue import Empty, Queue

from assemblyline_core.replay.replay import ReplayBase


class ReplayLoader(ReplayBase):
    def __init__(self):
        super().__init__("assemblyline.replay_loader")
        self.replay_config = self.replay_config['creator']
        self.log.debug(pformat(self.replay_config))
        self.file_queue = Queue()
        self.file_processor_count = int(os.environ.get('FILE_THREADS', '6'))

    def process_file(self):
        while self.running:
            try:
                file_path = self.file_queue.get(block=True, timeout=3)
                self.log.info(f"Processing file: {file_path}")
                self.sleep(1)
            except Empty:
                pass

    def load_files(self):
        while self.running:
            # TODO: loop through files in directory and queue them for processing
            pass

    def try_run(self):
        threads = {
            # Pull in completed submissions
            'File loader': self.load_files
        }

        for ii in range(self.file_processor_count):
            # Finilize submissions that are done
            threads[f'File processor #{ii}'] = self.process_file

        self.maintain_threads(threads)


if __name__ == '__main__':
    with ReplayLoader() as replay:
        replay.serve_forever()