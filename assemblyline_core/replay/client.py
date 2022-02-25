import os
import time

from queue import Empty, Queue

from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso
from assemblyline_client import get_client

EMPTY_WAIT_TIME = int(os.environ.get('EMPTY_WAIT_TIME', '30'))


class ClientBase(object):
    def __init__(self, log, alert_fqs=None, submission_fqs=None):
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

        # Setup filter queries
        self.alert_fqs = alert_fqs
        self.submission_fqs = submission_fqs

        # Set running flag
        self.running = True

    def _get_next_alert_ids(self, *_):
        raise NotImplementedError()

    def _get_next_submission_ids(self, *_):
        raise NotImplementedError()

    def stop(self):
        self.running = False

    def setup_alert_input_queue(self):
        while self.running:
            alert_input_query = f"reporting_ts:[{self.last_alert_time} TO now]"
            if self.last_alert_id is not None:
                alert_input_query = f"{alert_input_query} AND NOT id:{self.last_alert_id}"
            alerts = self._get_next_alert_ids(alert_input_query, self.alert_fqs)

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
            if self.last_submission_id is not None:
                sub_query = f"{sub_query} AND NOT id:{self.last_submission_id}"
            submissions = self._get_next_submission_ids(sub_query, self.submission_fqs)

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
    def __init__(self, log, host, user, apikey, verify, alert_fqs=None, submission_fqs=None):
        # Setup AL client
        self.al_client = get_client(host, apikey=(user, apikey), verify=verify)

        super().__init__(log, alert_fqs=alert_fqs, submission_fqs=submission_fqs)

    def _get_next_alert_ids(self, query, filter_queries):
        return self.al_client.search.alert(query, fl="id,*", sort="reporting_ts asc", rows=100, filters=filter_queries)

    def _get_next_submission_ids(self, query, filter_queries):
        return self.al_client.search.submission(
            query, fl="id,*", sort="times.completed asc", rows=100, filters=filter_queries)


class DirectClient(ClientBase):
    def __init__(self, log, alert_fqs=None, submission_fqs=None):
        # Setup datastore
        self.datastore = forge.get_datastore()

        super().__init__(log, alert_fqs=alert_fqs, submission_fqs=submission_fqs)

    def _get_next_alert_ids(self, query, filter_queries):
        return self.datastore.alert.search(
            query, fl="id,*", sort="reporting_ts asc", rows=100, as_obj=False, filters=filter_queries)

    def _get_next_submission_ids(self, query, filter_queries):
        return self.datastore.submission.search(
            query, fl="id,*", sort="times.completed asc", rows=100, as_obj=False, filters=filter_queries)
