import os
import time

from queue import Empty, Queue

from assemblyline.common import forge
from assemblyline.common.bundling import create_bundle, import_bundle
from assemblyline.common.isotime import now_as_iso
from assemblyline_client import get_client

EMPTY_WAIT_TIME = int(os.environ.get('EMPTY_WAIT_TIME', '30'))


class ClientBase(object):
    def __init__(self, log,
                 last_alert_time=None, last_submission_time=None,
                 last_alert_id=None, last_submission_id=None,
                 alert_fqs=None, submission_fqs=None):
        # Set logger
        self.log = log

        # Setup input queues
        self.alert_input_queue = Queue()
        self.submission_input_queue = Queue()

        # Setup timming
        self.last_alert_time = last_alert_time or now_as_iso(-1 * 60 * 60 * 24)  # Last 24h
        self.last_alert_id = last_alert_id or None
        self.last_submission_time = last_submission_time or now_as_iso(-1 * 60 * 60 * 24)  # Last 24h
        self.last_submission_id = last_submission_id or None

        # Setup filter queries
        self.alert_fqs = alert_fqs
        self.submission_fqs = submission_fqs

        # Set running flag
        self.running = True

    def _get_next_alert_ids(self, *_):
        raise NotImplementedError()

    def _get_next_submission_ids(self, *_):
        raise NotImplementedError()

    def create_alert_bundle(self, *_):
        raise NotImplementedError()

    def create_submission_bundle(self, *_):
        raise NotImplementedError()

    def load_bundle(self, *_):
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
                self.last_alert_id = a['alert_id']
                self.last_alert_time = a['reporting_ts']
            if alerts['total'] == 0:
                for _ in range(EMPTY_WAIT_TIME):
                    if not self.running:
                        break
                    time.sleep(1)

    def setup_submission_input_queue(self):
        while self.running:
            sub_query = f"times.completed:[{self.last_submission_time} TO now]"
            if self.last_submission_id is not None:
                sub_query = f"{sub_query} AND NOT id:{self.last_submission_id}"
            submissions = self._get_next_submission_ids(sub_query, self.submission_fqs)

            for sub in submissions['items']:
                self.submission_input_queue.put(sub)
                self.last_submission_id = sub['sid']
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
    def __init__(self, log, host, user, apikey, verify,
                 last_alert_time=None, last_submission_time=None,
                 last_alert_id=None, last_submission_id=None,
                 alert_fqs=None, submission_fqs=None):
        # Setup AL client
        self.al_client = get_client(host, apikey=(user, apikey), verify=verify)

        super().__init__(log,
                         last_alert_time=last_alert_time,  last_submission_time=last_submission_time,
                         last_alert_id=last_alert_id, last_submission_id=last_submission_id,
                         alert_fqs=alert_fqs, submission_fqs=submission_fqs)

    def _get_next_alert_ids(self, query, filter_queries):
        return self.al_client.search.alert(
            query, fl="alert_id,reporting_ts", sort="reporting_ts asc", rows=100, filters=filter_queries)

    def _get_next_submission_ids(self, query, filter_queries):
        return self.al_client.search.submission(
            query, fl="sid,times.completed", sort="times.completed asc", rows=100, filters=filter_queries)

    def create_alert_bundle(self, alert_id, bundle_path):
        self.al_client.bundle.create(alert_id, output=bundle_path, use_alert=True)

    def create_submission_bundle(self, sid, bundle_path):
        self.al_client.bundle.create(sid, output=bundle_path)

    def load_bundle(self, bundle_path, min_classification, rescan_services, exist_ok=True):
        self.al_client.bundle.import_bundle(bundle_path,
                                            min_classification=min_classification,
                                            rescan_services=rescan_services,
                                            exist_ok=exist_ok)


class DirectClient(ClientBase):
    def __init__(self, log,
                 last_alert_time=None, last_submission_time=None,
                 last_alert_id=None, last_submission_id=None,
                 alert_fqs=None, submission_fqs=None):
        # Setup datastore
        self.datastore = forge.get_datastore()

        super().__init__(log,
                         last_alert_time=last_alert_time, last_submission_time=last_submission_time,
                         last_alert_id=last_alert_id, last_submission_id=last_submission_id,
                         alert_fqs=alert_fqs, submission_fqs=submission_fqs)

    def _get_next_alert_ids(self, query, filter_queries):
        return self.datastore.alert.search(
            query, fl="alert_id,reporting_ts", sort="reporting_ts asc", rows=100, as_obj=False, filters=filter_queries)

    def _get_next_submission_ids(self, query, filter_queries):
        return self.datastore.submission.search(
            query, fl="sid,times.completed", sort="times.completed asc", rows=100, as_obj=False, filters=filter_queries)

    def create_alert_bundle(self, alert_id, bundle_path):
        temp_bundle_file = create_bundle(alert_id, working_dir=os.path.dirname(bundle_path), use_alert=True)
        os.rename(temp_bundle_file, bundle_path)

    def create_submission_bundle(self, sid, bundle_path):
        temp_bundle_file = create_bundle(sid, working_dir=os.path.dirname(bundle_path))
        os.rename(temp_bundle_file, bundle_path)

    def load_bundle(self, bundle_path, min_classification, rescan_services, exist_ok=True):
        import_bundle(bundle_path,
                      min_classification=min_classification,
                      rescan_services=rescan_services,
                      exist_ok=exist_ok)
