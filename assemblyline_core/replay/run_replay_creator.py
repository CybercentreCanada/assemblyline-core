
from pprint import pformat, pprint

from assemblyline_core.replay.replay import APIClient, DirectClient, ReplayBase


class ReplayCreator(ReplayBase):
    def __init__(self):
        super().__init__("assemblyline.replay_creator")
        self.replay_config = self.replay_config['creator']
        self.log.debug(pformat(self.replay_config))
        if self.replay_config['source']['direct']:
            self.log.info("Using direct database access client")
            self.client = DirectClient(self.log)
        else:
            self.log.info(f"Using API access client to ({self.replay_config['source']['client']['host']})")
            self.client = APIClient(self.log, **self.replay_config['source']['client'])

    def pull_alerts(self):
        while self.running:
            # Pull alerts from AL instance matching the patterns
            self.sleep(1)

    def pull_submissions(self):
        while self.running:
            # Pull submissions from AL instance matching the patterns
            msg = self.client.get_next_submission()
            if msg:
                pprint(msg)

    def try_run(self):
        threads = {}
        if self.replay_config['input']['alert']['enabled']:
            threads['Pull Alerts'] = self.pull_alerts
            threads['Alert Input Queue'] = self.client.setup_alert_input_queue

        if self.replay_config['input']['submission']['enabled']:
            threads['Pull Submissions'] = self.pull_submissions
            threads['Submission Input Queue'] = self.client.setup_submission_input_queue

        if threads:
            self.maintain_threads(threads)
        else:
            self.log.warning("There are no configured input, terminating")


if __name__ == '__main__':
    with ReplayCreator() as replay:
        replay.serve_forever()