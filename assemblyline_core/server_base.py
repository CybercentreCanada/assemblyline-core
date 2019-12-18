"""
A base classes and utilities to provide a common set of behaviours for
the assemblyline core server nodes.
"""
import enum
import time
import threading
import logging
import signal
import sys
from typing import cast, Dict

from assemblyline.remote.datatypes.hash import Hash

from assemblyline.odm.models.service import Service

from assemblyline.remote.datatypes import get_client

from assemblyline.common import forge, log as al_log


SHUTDOWN_SECONDS_LIMIT = 10


class ServerBase(threading.Thread):
    """Utility class for Assmblyline server processes.

    Inheriting from thread so that the main work is done off the main thread.
    This lets the main thread handle interrupts properly, even when the workload
    makes a blocking call that would normally stop this.
    """
    def __init__(self, component_name: str, logger: logging.Logger = None,
                 shutdown_timeout: float = SHUTDOWN_SECONDS_LIMIT):
        super().__init__(name=component_name)
        al_log.init_logging(component_name)

        self.running = None
        self.log = logger or logging.getLogger(component_name)
        self._exception = None
        self._traceback = None
        self._shutdown_timeout = shutdown_timeout if shutdown_timeout is not None else SHUTDOWN_SECONDS_LIMIT
        self._old_sigint = None
        self._old_sigterm = None
        self._stopped = False

    def __enter__(self):
        self.log.info(f"Initialized")
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.close()
        if _exc_type is not None:
            self.log.exception(f'Terminated because of an {_exc_type} exception')
        else:
            self.log.info(f'Terminated')

    def __stop(self):
        """Hard stop, can still be blocked in some cases, but we should try to avoid them."""
        time.sleep(self._shutdown_timeout)
        self.log.error(f"Server has shutdown hard after waiting {self._shutdown_timeout} seconds to stop")

        if not self._stopped:
            self._stopped = True
            exit(1)  # So any static analysis tools get the behaviour of this function 'correct'
        import ctypes
        ctypes.string_at(0)  # SEGFAULT out of here

    def close(self):
        pass

    def interrupt_handler(self, signum, stack_frame):
        self.log.info(f"Instance caught signal. Coming down...")
        self.stop()
        if signum == signal.SIGINT and self._old_sigint:
            self._old_sigint(signum, stack_frame)
        if signum == signal.SIGTERM and self._old_sigterm:
            self._old_sigterm(signum, stack_frame)

    def raising_join(self):
        self.join()
        if self._traceback and self._exception:
            raise self._exception.with_traceback(self._traceback)

    # noinspection PyBroadException
    def run(self):
        try:
            self.try_run()
        except Exception:
            _, self._exception, self._traceback = sys.exc_info()
            self.log.exception("Exiting:")

    def serve_forever(self):
        self.start()
        self.join()

    def start(self):
        """Start the server workload."""
        self.running = True
        super().start()
        self.log.info(f"Started")
        self._old_sigint = signal.signal(signal.SIGINT, self.interrupt_handler)
        self._old_sigterm = signal.signal(signal.SIGTERM, self.interrupt_handler)

    def stop(self):
        """Ask nicely for the server to stop.

        After a timeout, a hard stop will be triggered.
        """
        # The running loops should stop within a few seconds of this flag being set.
        self.running = False

        # If it doesn't stop within a few seconds, this other thread should kill the entire process
        stop_thread = threading.Thread(target=self.__stop)
        stop_thread.daemon = True
        stop_thread.start()

    def try_run(self):
        pass


# This table in redis tells us about the current stage of operation a service is in.
# This is complementary to the 'enabled' flag in the service spec.
# If the service is marked as enabled=true, each component should take steps needed to move it to the 'Running' stage.
# If the service is marked as enabled=false, each component should take steps needed to stop it.
class ServiceStage(enum.IntEnum):
    # A service is not running
    # - if enabled scaler will start dependent containers and move to next stage
    Off = 0
    # A service is not running, but dependencies have been started
    # - if enabled updater will try to
    Update = 1
    # At this stage scaler will begin
    Running = 2
    Paused = 3

    # If at any time a service is disabled, scaler will stop the dependent containers


def get_service_stage_hash(redis):
    """A hash from service name to ServiceStage enum values."""
    return Hash('service-stage', redis)


class CoreBase(ServerBase):
    """Expands the basic server setup in server base with some initialization steps most core servers take."""

    def __init__(self, component_name: str, logger: logging.Logger = None,
                 shutdown_timeout: float = None, config=None, datastore=None,
                 redis=None, redis_persist=None):
        super().__init__(component_name=component_name, logger=logger, shutdown_timeout=shutdown_timeout)

        self.config = config or forge.get_config()
        self.datastore = datastore or forge.get_datastore(self.config)

        # Connect to all of our persistent redis structures
        self.redis = redis or get_client(
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )
        self.redis_persist = redis_persist or get_client(
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )

        # Create a cached service data object, and access to the service status
        self.service_info = cast(Dict[str, Service], forge.CachedObject(self._get_services))
        self._service_stage_hash = get_service_stage_hash(self.redis)

    def _get_services(self):
        # noinspection PyUnresolvedReferences
        return {x.name: x for x in self.datastore.list_all_services(full=True)}

    def get_service_stage(self, service_name: str) -> ServiceStage:
        return ServiceStage(self._service_stage_hash.get(service_name) or ServiceStage.Off)

    def is_service_running(self, service_name: str) -> bool:
        # TODO should we add an option to just return off/running based on the service
        #      enabled/disabled flag when doing development
        return self.service_info[service_name].enabled and self.get_service_stage(service_name) == ServiceStage.Running

