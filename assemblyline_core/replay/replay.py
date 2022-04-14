import functools
import os
import threading
import yaml

from typing import Callable

from assemblyline.common.dict_utils import recursive_update
from assemblyline.common.forge import env_substitute
from assemblyline.odm.models.replay import ReplayConfig
from assemblyline_core.server_base import ServerBase

CONFIG_PATH = os.environ.get('REPLAY_CONFIG_PATH', '/etc/assemblyline/replay.yml')


class ReplayBase(ServerBase):
    def __init__(self, component_name):
        super().__init__(component_name)

        # Load updated values
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH) as yml_fh:
                self.replay_config = ReplayConfig(recursive_update(ReplayConfig().as_primitives(),
                                                                   yaml.safe_load(env_substitute(yml_fh.read()))))
        else:
            self.replay_config = ReplayConfig()

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
