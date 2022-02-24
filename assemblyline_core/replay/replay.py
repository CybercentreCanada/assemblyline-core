import functools
import os
import time
import threading
import yaml

from queue import Empty, Queue
from typing import Callable

from assemblyline.common import forge
from assemblyline.common.dict_utils import recursive_update
from assemblyline.common.isotime import now_as_iso
from assemblyline_core.server_base import ServerBase
from assemblyline_client import get_client

CONFIG_PATH = '/etc/assemblyline/replay.yml'
EMPTY_WAIT_TIME = 30


class ClientBase(object):
    def __init__(self, log):
        # Set logger
        self.log = log

        # Setup input queues
        self.alert_input_queue = Queue()
        self.submission_input_queue = Queue()

        # Setup timming
        self.last_alert_time = now_as_iso(-1 * 60 * 60 * 24)  # Last 24h
        self.last_alert_id = None
        self.last_submission_time = now_as_iso(-1 * 60 * 60 * 24)  # Last 24h
        self.last_submission_id = None

        # Set running flag
        self.running = True

    def _get_next_alert_ids(self, _):
        raise NotImplementedError()

    def _get_next_submission_ids(self, _):
        raise NotImplementedError()

    def stop(self):
        self.running = False

    def setup_alert_input_queue(self):
        while self.running:
            alert_input_query = f"reporting_ts:[{self.last_alert_time} TO now] AND NOT id:{self.last_alert_id}"
            alerts = self._get_next_alert_ids(alert_input_query)

            for a in alerts['items']:
                self.alert_input_queue.put(a)
                self.last_alert_id = a['id']
                self.last_alert_time = a['reporting_ts']
            if alerts['total'] == 0:
                for _ in range(EMPTY_WAIT_TIME):
                    if not self.running:
                        break
                    time.sleep(1)

    def setup_submission_input_queue(self):
        while self.running:
            sub_query = f"times.completed:[{self.last_submission_time} TO now] AND NOT id:{self.last_submission_id}"
            submissions = self._get_next_submission_ids(sub_query)

            for sub in submissions['items']:
                self.submission_input_queue.put(sub)
                self.last_submission_id = sub['id']
                self.last_submission_time = sub['times']['completed']
            if submissions['total'] == 0:
                for _ in range(EMPTY_WAIT_TIME):
                    if not self.running:
                        break
                    time.sleep(1)

    def get_next_alert(self):
        try:
            return self.alert_input_queue.get(block=True, timeout=3)
        except Empty:
            return None

    def get_next_submission(self):
        try:
            return self.submission_input_queue.get(block=True, timeout=3)
        except Empty:
            return None


class APIClient(ClientBase):
    def __init__(self, log, host, user, apikey, verify):
        # Setup AL client
        self.al_client = get_client(host, apikey=(user, apikey), verify=verify)

        super().__init__(log)

    def _get_next_alert_ids(self, query):
        return self.al_client.search.alert(query, fl="id,*", sort="reporting_ts asc", rows=100)

    def _get_next_submission_ids(self, query):
        return self.al_client.search.submission(query, fl="id,*", sort="times.completed asc", rows=100)


class DirectClient(ClientBase):
    def __init__(self, log):
        # Setup datastore
        self.datastore = forge.get_datastore()

        super().__init__(log)

    def _get_next_alert_ids(self, query):
        return self.datastore.alert.search(query, fl="id,*", sort="reporting_ts asc", rows=100, as_obj=False)

    def _get_next_submission_ids(self, query):
        return self.datastore.submission.search(query, fl="id,*", sort="times.completed asc", rows=100, as_obj=False)


class ReplayBase(ServerBase):
    def __init__(self, component_name):
        super().__init__(component_name)

        # Load default config
        with open(os.path.join(__file__.rsplit(os.path.sep, 1)[0], 'replay.yml'), 'rb') as fh:
            self.replay_config = yaml.safe_load(fh)

        # Load updated values
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, 'rb') as fh:
                self.replay_config = recursive_update(self.replay_config, yaml.safe_load(fh))

        # Thread events related to exiting
        self.main_loop_exit = threading.Event()

    def stop(self):
        super().stop()
        self.main_loop_exit.wait(30)

    def sleep(self, timeout: float):
        self.stopping.wait(timeout)
        return self.running

    def log_crashes(self, fn):
        @functools.wraps(fn)
        def with_logs(*args, **kwargs):
            # noinspection PyBroadException
            try:
                fn(*args, **kwargs)
            except Exception:
                self.log.exception(f'Crash in dispatcher: {fn.__name__}')
        return with_logs

    def maintain_threads(self, expected_threads: dict[str, Callable[..., None]]):
        expected_threads = {name: self.log_crashes(start) for name, start in expected_threads.items()}
        threads: dict[str, threading.Thread] = {}

        # Run as long as we need to
        while self.running:
            # Check for any crashed threads
            for name, thread in list(threads.items()):
                if not thread.is_alive():
                    self.log.warning(f'Restarting thread: {name}')
                    threads.pop(name)

            # Start any missing threads
            for name, function in expected_threads.items():
                if name not in threads:
                    self.log.info(f'Starting thread: {name}')
                    threads[name] = thread = threading.Thread(target=function, name=name)
                    thread.start()

            # Take a break before doing it again
            super().heartbeat()
            self.sleep(2)

        for _t in threads.values():
            _t.join()

        self.main_loop_exit.set()
