"""
A base class to provide a common set of behaviours for the assemblyline core server nodes.
"""
import time
import threading
import logging
import signal
import sys

from assemblyline.common import log as al_log

SHUTDOWN_SECONDS_LIMIT = 10


class ServerBase(threading.Thread):
    """Utility class for Assmblyline server processes.

    Inheriting from thread so that the main work is done off the main thread.
    This lets the main thread handle interrupts properly, even when the workload
    makes a blocking call that would normally stop this.
    """
    def __init__(self, component_name, logger=None):
        super().__init__(name=component_name)
        al_log.init_logging(component_name)
        self.running = None
        self.log = logger or logging.getLogger(component_name)
        self._exception = None
        self._traceback = None

    def start(self):
        """Start the server workload."""
        self.running = True
        super().start()
        signal.signal(signal.SIGINT, self.interrupt_handler)
        signal.signal(signal.SIGTERM, self.interrupt_handler)

    def interrupt_handler(self, _signum, _stack_frame):
        self.log.info(f"{self.name} instance caught signal. Coming down...")
        self.stop()

    def try_run(self):
        pass

    def run(self):
        try:
            self.try_run()
        except:
            _, self._exception, self._traceback = sys.exc_info()
            self.log.exception("Exiting:")

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

    def __stop(self):
        """Hard stop"""
        time.sleep(SHUTDOWN_SECONDS_LIMIT)
        self.log.error(f"{self.name} has shutdown hard after waiting {SHUTDOWN_SECONDS_LIMIT} seconds to stop.")
        exit(1)

    def serve_forever(self):
        self.start()
        self.join()

    def raising_join(self):
        self.join()
        if self._traceback and self._exception:
            raise self._exception.with_traceback(self._traceback)
