import json
import os

from assemblyline_core.replay.client import APIClient, DirectClient
from assemblyline_core.replay.replay import ReplayBase


class ReplayCreator(ReplayBase):
    def __init__(self):
        super().__init__("assemblyline.replay_creator")

        # Load/create cache
        self.cache = {}
        self.cache_file = os.path.join(self.replay_config.creator.working_directory, 'cache.json')
        if os.path.exists(self.cache_file):
            with open(self.cache_file) as c_fp:
                try:
                    self.cache.update(json.load(c_fp))
                except json.JSONDecodeError:
                    os.unlink(self.cache_file)

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
            # Process alerts found
            alert = self.client.get_next_alert()
            if alert:
                self.log.info(f"Processing alert: {alert['alert_id']}")
                # TODO: Bundle using alert_id

                # Save ID to cache
                self.cache['last_alert_id'] = alert['alert_id']
                self.cache['last_alert_time'] = alert['reporting_ts']

    def process_submissions(self):
        while self.running:
            # Process submissions found
            submission = self.client.get_next_submission()
            if submission:
                self.log.info(f"Processing submission: {submission['sid']}")
                # TODO: Bundle using sid

                # Save ID to cache
                self.cache['last_submission_id'] = submission['sid']
                self.cache['last_submission_time'] = submission['times']['completed']

    def _save_cache(self):
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w') as c_fp:
            json.dump(self.cache, c_fp)

    def save_cache(self):
        while self.running:
            self._save_cache()
            for _ in range(self.replay_config.creator.cache_save_interval):
                if not self.running:
                    break
                self.sleep(1)

        # Save on exit
        self._save_cache()

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
            threads['Save cache'] = self.save_cache
            self.maintain_threads(threads)
        else:
            self.log.warning("There are no configured input, terminating")


if __name__ == '__main__':
    with ReplayCreator() as replay:
        replay.serve_forever()
