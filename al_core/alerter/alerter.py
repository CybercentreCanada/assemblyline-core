
import logging
import signal

from assemblyline.common import forge
from assemblyline.common import log as al_log
from assemblyline.common import net
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.exporting_counter import AutoExportingCounters
from assemblyline.remote.datatypes.queues.named import NamedQueue



ALERT_QUEUE_NAME = 'm-alert'
MAX_RETRIES = 10

class Alerter(object):
    def __init__(self, logger):
        self.config = forge.get_config()
        # Publish counters to the metrics sink.
        self.counter = AutoExportingCounters(
            name='alerter',
            host=net.get_hostip(),
            export_interval_secs=5,
            channel=forge.get_metrics_sink(),
            auto_log=True,
            auto_flush=True)
        self.counter.start()
        self.datastore = forge.get_datastore(self.config)
        self.log = logger
        self.persistent_redis = get_client(
            db=self.config.core.redis.persistent.db,
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )
        self.process_alert_message = forge.get_process_alert_message()
        self.running = False

        self.alert_queue = NamedQueue(ALERT_QUEUE_NAME, self.persistent_redis)
        signal.signal(signal.SIGINT, self.interrupt)
        signal.signal(signal.SIGTERM, self.interrupt)

    def __enter__(self):
        self.log.info('Alerter ready to process alerts!')
        return self

    def __exit__(self):
        self.close()
        self.log.info('Alerter stopped.')

    # noinspection PyUnusedLocal
    def interrupt(self, unused1, unused2):
        self.log.info("Caught signal. Coming down...")
        self.running = False

    def close(self):
        if self.counter:
            self.counter.stop()

    def run(self):
        self.running = True
        while self.running:
            alert = self.alert_queue.pop(timeout=1)
            if not alert:
                continue

            self.counter.increment('alert.received')
            try:
                self.process_alert_message(self.counter, self.datastore, self.log, alert)
            except Exception as ex:  # pylint: disable=W0703
                retries = alert['alert_retries'] = alert.get('alert_retries', 0) + 1
                if retries > MAX_RETRIES:
                    self.log.exception(f'Max retries exceeded for: {alert}')
                else:
                    self.alert_queue.push(alert)
                    if 'Submission not finalized' not in str(ex):
                        self.log.exception(f'Unhandled exception processing: {alert}')


if __name__ == "__main__":
    al_log.init_logging("alerter")
    log = logging.getLogger('assemblyline.alerter')

    log.info('Starting Alerter...')
    # noinspection PyBroadException
    try:
        with Alerter(log) as alerter:
            alerter.run()
    except Exception:
        log.exception("Unhandled exception caught while processing alerts:")
