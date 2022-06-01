import concurrent.futures
import json
import sys
import logging
import os
import signal
from argparse import ArgumentParser
from time import sleep, time

import yaml

from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes import get_client as get_redis_client

from multiprocessing import Event

from .worker import main as worker_main

DEL_TIME: int = 60 * 60  # An hour
MAX_QUEUE_LENGTH = 1000000

stop_event = Event()


# noinspection PyUnusedLocal
def sigterm_handler(_signum=0, _frame=None):
    stop_event.set()


def main():
    parser = ArgumentParser(description="Bulk import tool for assemblyline.")
    parser.add_argument('-d', '--dry_run', dest='dry_run',  action='store_true',
                        help="Process files but does not send to processing", default=False)
    # parser.add_argument("-p", "--path", dest="path", help="Path to source directory.", default=None)
    parser.add_argument("-v", "--verbose", action='store_true', dest="verbose",
                        help="Turn on verbose mode", default=False)
    parser.add_argument("-c", "--config", dest="config_path",
                        help="Path to configuration file.", required=True)
    parser.add_argument("-w", "--workers", dest="workers", type=int,
                        help="Launch workers rather than collecting files.", default=0)
    parser.add_argument("-r", "--reconnect", dest="reconnect", type=int,
                        help="Reset the connection to the api servers.", default=0)
    options = parser.parse_args()

    try:
        with open(options.config_path, 'r') as _cf:
            config = Config(**yaml.safe_load(_cf))
    except (OSError, ValueError):
        parser.print_help()
        exit(1)

    config.dry_run = options.dry_run
    config.reconnect = options.reconnect
    signal.signal(signal.SIGTERM, sigterm_handler)

    # Initialize logging
    # init_logging('assemblyline.vacuum')
    logger = logging.getLogger('assemblyline.vacuum')
    logger.addHandler(logging.StreamHandler(sys.stdout))
    if options.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    logger.info('Vacuum starting up...')

    if options.workers > 0:
        return worker_main(options, config, stop_event)

    # Initialize cache
    logger.info("Connect to redis...")
    redis = get_redis_client('vacuum-redis', 6379, False)

    # connect to workers
    logger.info("Connect to work queue...")
    queue = NamedQueue('work', redis)

    logger.info("Load cache...")
    files_list_cache = os.path.join(config.list_cache_directory, 'visited.json')
    try:
        with open(files_list_cache, 'r') as handle:
            previous_iteration_files = set(json.load(handle))
    except (OSError, json.JSONDecodeError):
        previous_iteration_files = set()

    # Make sure we can access the cache file
    with open(files_list_cache, 'w'):
        pass

    this_iteration_files = []
    length = queue.length()

    logger.info("Starting main loop...")
    while not stop_event.is_set():
        remove_dir_list = []
        futures = []
        with concurrent.futures.ThreadPoolExecutor(20) as pool:
            for data_directory in config.data_directories:
                for root, dirs, files in os.walk(data_directory):
                    while len(futures) > 50:
                        futures = [f for f in futures if not f.done()]
                        sleep(0.1)

                    if length > MAX_QUEUE_LENGTH:
                        while len(futures) > 0:
                            futures = [f for f in futures if not f.done()]
                            sleep(0.1)
                        length = queue.length()

                    while length > MAX_QUEUE_LENGTH:
                        logger.warning("Backlog full")
                        length = queue.length()
                        for _ in range(120):
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
                        new_files = [os.path.join(root, f) for f in files if not f.startswith(".")]
                        new_files = set(new_files) - previous_iteration_files

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

