import tempfile
import os
import os.path
import pathlib
import uuid
import json
import hashlib
import time
import random
import threading

from assemblyline.odm.models.config import Config
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline_core.vacuum import crawler, worker


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


def test_worker(config: Config, redis_connection):
    try:
        with tempfile.TemporaryDirectory() as workdir:
            # Configure an environment
            data_dir = os.path.join(workdir, 'meta')
            file_dir = os.path.join(workdir, 'files')
            os.mkdir(data_dir)
            os.mkdir(file_dir)

            config.core.vacuum.data_directories = [data_dir]
            config.core.vacuum.file_directories = [file_dir]
            config.core.vacuum.list_cache_directory = workdir
            config.core.vacuum.worker_cache_directory = workdir
            config.core.vacuum.assemblyline_user = 'service-account'
            config.core.vacuum.safelist = [{
                'name': 'good_streams',
                'conditions': {
                    'stream': '10+'
                }
            }]
            config.core.vacuum.worker_threads = 1

            # Place a file that will be safe
            first_file = os.path.join(data_dir, uuid.uuid4().hex + '.meta')
            with open(first_file, 'w') as temp:
                temp.write(json.dumps({
                    'sha256': '0'*64,
                    'metadata': {'stream': '100'}
                }))

            # Place a file that will be ingested
            test_file = random.randbytes(100)
            sha256 = hashlib.sha256(test_file).hexdigest()
            name = os.path.join(file_dir, sha256[0], sha256[1], sha256[2], sha256[3])
            os.makedirs(name)
            with open(os.path.join(name, sha256), 'bw') as temp:
                temp.write(test_file)

            second_file = os.path.join(data_dir, uuid.uuid4().hex + '.meta')
            with open(second_file, 'w') as temp:
                temp.write(json.dumps({
                    'sha256': sha256,
                    'metadata': {'stream': '99'}
                }))

            # Start the worker
            threading.Thread(target=worker.run, args=[config, redis_connection, redis_connection], daemon=True).start()

            # Tell the worker about the files
            queue = NamedQueue(crawler.VACUUM_BUFFER_NAME, redis_connection)
            queue.push(first_file)
            queue.push(second_file)

            # Get a message from for the ingested file
            ingest_queue = NamedQueue("m-ingest", redis_connection)
            ingested = ingest_queue.pop(timeout=20)
            assert ingested is not None
            assert ingested['files'][0]['sha256'] == sha256
            assert ingest_queue.length() == 0

            # Make sure all the meta files have been consumed
            rounds = 0
            while os.path.exists(first_file) or os.path.exists(second_file) and rounds < 10:
                time.sleep(0.1)
                rounds += 1
            assert not os.path.exists(first_file)
            assert not os.path.exists(second_file)

    finally:
        worker.stop_event.set()
