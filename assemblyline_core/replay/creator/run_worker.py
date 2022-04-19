import os

from assemblyline.filestore import FileStore
from assemblyline_core.replay.client import APIClient, DirectClient
from assemblyline_core.replay.replay import ReplayBase


class ReplayCreatorWorker(ReplayBase):
    def __init__(self):
        super().__init__("assemblyline.replay_creator.worker")

        if not self.replay_config.creator.alert_input.enabled and \
                not self.replay_config.creator.submission_input.enabled:
            return

        # Initialize filestore object
        self.filestore = FileStore(self.replay_config.creator.output_filestore)

        # Create cache directory
        os.makedirs(self.replay_config.creator.working_directory, exist_ok=True)

        # Load client
        client_config = dict(lookback_time=self.replay_config.creator.lookback_time,
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

    def process_alerts(self, once=False):
        while self.running:
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
                self.filestore.upload(bundle_path, f"alert_{alert['alert_id']}.al_bundle")

                # Remove temp file
                if os.path.exists(bundle_path):
                    os.unlink(bundle_path)

                # Set alert state done
                self.client.set_single_alert_complete(alert['alert_id'])

            if once:
                break

    def process_submissions(self, once=False):
        while self.running:
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
                self.filestore.upload(bundle_path, f"submission_{submission['sid']}.al_bundle")

                # Remove temp file
                if os.path.exists(bundle_path):
                    os.unlink(bundle_path)

                # Set submission state done
                self.client.set_single_submission_complete(submission['sid'])

            if once:
                break

    def try_run(self):
        threads = {}
        if self.replay_config.creator.alert_input.enabled:
            for ii in range(self.replay_config.creator.alert_input.threads):
                threads[f'Alert process thread #{ii}'] = self.process_alerts

        if self.replay_config.creator.submission_input.enabled:
            for ii in range(self.replay_config.creator.submission_input.threads):
                threads[f'Submission process thread #{ii}'] = self.process_submissions

        if threads:
            self.maintain_threads(threads)
        else:
            self.log.warning("There are no configured input, terminating")
            self.main_loop_exit.set()
            self.stop()


if __name__ == '__main__':
    with ReplayCreatorWorker() as replay:
        replay.serve_forever()
