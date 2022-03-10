import os
import shelve
import threading
from assemblyline.common.isotime import epoch_to_iso
from assemblyline.filestore import FileStore

from assemblyline_core.replay.client import APIClient, DirectClient
from assemblyline_core.replay.replay import ReplayBase


class ReplayCreator(ReplayBase):
    def __init__(self):
        super().__init__("assemblyline.replay_creator")

        # Create cache directory
        os.makedirs(self.replay_config.creator.working_directory, exist_ok=True)

        # Load/create cache
        self.cache_lock = threading.Lock()
        self.cache = shelve.open(os.path.join(self.replay_config.creator.working_directory, 'creator_cache.db'))

        # Load client
        client_config = dict(last_alert_time=self.cache.get('last_alert_time', None),
                             last_submission_time=self.cache.get('last_submission_time', None),
                             last_alert_id=self.cache.get('last_alert_id', None),
                             last_submission_id=self.cache.get('last_submission_id', None),
                             alert_fqs=self.replay_config.creator.alert_input.filter_queries,
                             submission_fqs=self.replay_config.creator.submission_input.filter_queries)

        if self.replay_config.creator.client.type == 'direct':
            self.log.info("Using direct database access client")
            self.client = DirectClient(self.log, **client_config)
        elif self.replay_config.creator.client.type == 'api':
            self.log.info(f"Using API access client to ({self.replay_config.creator.client.options.host})")
            client_config.update(self.replay_config.creator.client.options.as_primitives())
            self.client = APIClient(self.log, **client_config)
        else:
            raise ValueError(f'Invalid client type ({self.replay_config.creator.client.type}). '
                             'Must be either \'api\' or \'direct\'.')

    def process_alerts(self):
        while self.running:
            filestore = FileStore(self.replay_config.creator.output_filestore)
            # Process alerts found
            alert = self.client.get_next_alert()
            if alert:
                self.log.info(f"Processing alert: {alert['alert_id']}")

                # Make sure directories exists
                os.makedirs(self.replay_config.creator.working_directory, exist_ok=True)

                # Create the bundle
                bundle_path = os.path.join(self.replay_config.creator.working_directory,
                                           f"alert_{alert['alert_id']}.al_bundle")
                self.client.create_alert_bundle(alert['alert_id'], bundle_path)

                # Move the bundle
                filestore.upload(bundle_path, f"alert_{alert['alert_id']}.al_bundle")

                # Save ID to cache
                with self.cache_lock:
                    if alert['reporting_ts'] > self.cache.get('last_alert_time', epoch_to_iso(0)):
                        self.cache['last_alert_id'] = alert['alert_id']
                        self.cache['last_alert_time'] = alert['reporting_ts']

    def process_submissions(self):
        while self.running:
            filestore = FileStore(self.replay_config.creator.output_filestore)
            # Process submissions found
            submission = self.client.get_next_submission()
            if submission:
                self.log.info(f"Processing submission: {submission['sid']}")

                # Make sure directories exists
                os.makedirs(self.replay_config.creator.working_directory, exist_ok=True)

                # Create the bundle
                bundle_path = os.path.join(self.replay_config.creator.working_directory,
                                           f"submission_{submission['sid']}.al_bundle")
                self.client.create_submission_bundle(submission['sid'], bundle_path)

                # Move the bundle
                filestore.upload(bundle_path, f"submission_{submission['sid']}.al_bundle")

                # Save ID to cache
                with self.cache_lock:
                    if submission['times']['completed'] > self.cache.get('last_submission_time', epoch_to_iso(0)):
                        self.cache['last_submission_id'] = submission['sid']
                        self.cache['last_submission_time'] = submission['times']['completed']

    def try_run(self):
        threads = {}
        if self.replay_config.creator.alert_input.enabled:
            threads['Load Alerts'] = self.client.setup_alert_input_queue
            for ii in range(self.replay_config.creator.alert_input.threads):
                threads[f'Alert process thread #{ii}'] = self.process_alerts

        if self.replay_config.creator.submission_input.enabled:
            threads['Load Submissions'] = self.client.setup_submission_input_queue
            for ii in range(self.replay_config.creator.submission_input.threads):
                threads[f'Submission process thread #{ii}'] = self.process_submissions

        if threads:
            self.maintain_threads(threads)
        else:
            self.log.warning("There are no configured input, terminating")

    def stop(self):
        self.cache.close()
        return super().stop()


if __name__ == '__main__':
    with ReplayCreator() as replay:
        replay.serve_forever()
