from __future__ import annotations
import concurrent.futures
import json
import logging
import os
import io
import signal
from time import sleep, time
from typing import TYPE_CHECKING

from assemblyline.common.log import init_logging
from assemblyline.common.forge import get_config
from assemblyline.odm.models.config import Config
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes import get_client as get_redis_client

from multiprocessing import Event

if TYPE_CHECKING:
    from redis import Redis


DEL_TIME: int = 60 * 60  # An hour
MAX_QUEUE_LENGTH = 100000
VACUUM_BUFFER_NAME = 'vacuum-file-buffer'

stop_event = Event()


# noinspection PyUnusedLocal
def sigterm_handler(_signum=0, _frame=None):
    stop_event.set()


_last_heartbeat: float = 0


def heartbeat(config: Config):
    global _last_heartbeat
    if _last_heartbeat + 3 < time():
        with io.open(config.logging.heartbeat_file, 'ab'):
            os.utime(config.logging.heartbeat_file)
        _last_heartbeat = time()


logger = logging.getLogger('assemblyline.vacuum')


def main():
    config = get_config()
    signal.signal(signal.SIGTERM, sigterm_handler)

    # Initialize logging
    init_logging('assemblyline.vacuum')
    logger.info('Vacuum starting up...')

    # Initialize cache
    logger.info("Connect to redis...")
    redis = get_redis_client(config.core.redis.nonpersistent.host, config.core.redis.nonpersistent.port, False)
    run(config, redis)


def run(config: Config, redis: Redis):
    vacuum_config = config.core.vacuum

    # connect to workers
    logger.info("Connect to work queue...")
    queue = NamedQueue(VACUUM_BUFFER_NAME, redis)

    logger.info("Load cache...")
    files_list_cache = os.path.join(vacuum_config.list_cache_directory, 'visited.json')
    try:
        with open(files_list_cache, 'r') as handle:
            previous_iteration_files: set[str] = set(json.load(handle))
    except (OSError, json.JSONDecodeError):
        previous_iteration_files = set()

    # Make sure we can access the cache file
    with open(files_list_cache, 'w'):
        pass

    this_iteration_files: list[str] = []
    length = queue.length()

    # Make sure some input is configured
    if not vacuum_config.data_directories:
        logger.error("No input directory configured.")
        return

    logger.info("Starting main loop...")
    while not stop_event.is_set():
        heartbeat(config)
        remove_dir_list = []
        futures: list[concurrent.futures.Future] = []
        with concurrent.futures.ThreadPoolExecutor(20) as pool:
            for data_directory in vacuum_config.data_directories:
                for root, dirs, files in os.walk(data_directory):
                    heartbeat(config)
                    while len(futures) > 50:
                        futures = [f for f in futures if not f.done()]
                        heartbeat(config)
                        sleep(0.1)

                    if length > MAX_QUEUE_LENGTH:
                        while len(futures) > 0:
                            futures = [f for f in futures if not f.done()]
                            heartbeat(config)
                            sleep(0.1)
                        length = queue.length()

                    while length > MAX_QUEUE_LENGTH:
                        logger.warning("Backlog full")
                        length = queue.length()
                        for _ in range(120):
                            heartbeat(config)
                            sleep(1)
                            if stop_event.is_set():
                                break
                        if stop_event.is_set():
                            break

                    if stop_event.is_set():
                        break

                    if not dirs and not files:
                        if root == data_directory:
                            continue

                        cur_time = time()
                        dir_time = os.lstat(root).st_mtime
                        if (cur_time - dir_time) > DEL_TIME:
                            logger.debug('Directory %s marked for removal.' % root)
                            remove_dir_list.append(root)
                        else:
                            logger.debug(f'Directory {root} empty but not old enough. '
                                         f'[{int(cur_time - dir_time)}/{DEL_TIME}]')
                        continue

                    if files:
                        new_file_list = [os.path.join(root, f) for f in files
                                         if not f.startswith(".") and not f.endswith('.bad')]
                        new_files = set(new_file_list) - previous_iteration_files

                        if new_files:
                            futures.append(pool.submit(queue.push, *new_files))
                            # queue.push(*new_files)
                            length += len(new_files)
                            this_iteration_files.extend(new_files)

        with open(files_list_cache, 'w') as handle:
            json.dump(this_iteration_files, handle)

        previous_iteration_files = set(this_iteration_files)
        this_iteration_files = []

        for d in remove_dir_list:
            logger.debug("Removing empty directory: %s" % d)
            # noinspection PyBroadException
            try:
                os.rmdir(d)
            except Exception:
                pass

        if not stop_event.is_set():
            sleep(5)

    logger.info('Good bye!')


if __name__ == '__main__':
    main()
