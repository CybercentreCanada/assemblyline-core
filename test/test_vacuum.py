import tempfile
import os
import os.path
import pathlib
import uuid
import threading

from assemblyline.odm.models.config import Config
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline_core.vacuum import crawler


def test_crawler(config: Config, redis_connection):
    try:
        with tempfile.TemporaryDirectory() as workdir:
            # Configure an environment
            data_dir = os.path.join(workdir, 'meta')
            os.mkdir(data_dir)

            config.core.vacuum.data_directories = [data_dir]
            config.core.vacuum.list_cache_directory = workdir
            config.core.vacuum.worker_cache_directory = workdir

            # Put some files named the right thing
            file_names = [uuid.uuid4().hex + '.meta' for _ in range(100)]
            for name in file_names:
                pathlib.Path(data_dir, name).touch()

            # Kick off a crawler thread
            threading.Thread(target=crawler.run, args=[config, redis_connection], daemon=True).start()

            # Check that they have been picked up
            queue = NamedQueue(crawler.VACUUM_BUFFER_NAME, redis_connection)
            while file_names:
                path = queue.pop(timeout=1)
                if path is None:
                    assert False
                file_names = [f for f in file_names if not path.endswith(f)]
    finally:
        # shut down the crawler
        crawler.stop_event.set()
