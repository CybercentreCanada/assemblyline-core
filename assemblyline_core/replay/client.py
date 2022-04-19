import os
import time

from assemblyline.common import forge
from assemblyline.common.bundling import create_bundle, import_bundle
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.queues.named import NamedQueue


EMPTY_WAIT_TIME = int(os.environ.get('EMPTY_WAIT_TIME', '30'))
REPLAY_REQUESTED = 'requested'
REPLAY_PENDING = 'pending'
REPLAY_DONE = 'done'


class ClientBase(object):
    def __init__(self, log, alert_fqs=None, submission_fqs=None, lookback_time='*'):
        # Set logger
        self.log = log

        # Setup timming
        self.last_alert_time = self.last_submission_time = self.lookback_time = lookback_time

        # Setup filter queries
        self.pending_fq = f'NOT metadata.replay:{REPLAY_PENDING}'
        self.done_fq = f'NOT metadata.replay:{REPLAY_DONE}'
        self.alert_fqs = alert_fqs or []
        self.submission_fqs = submission_fqs or []

        # Set running flag
        self.running = True

    def _get_next_alert_ids(self, *_):
        raise NotImplementedError()

    def _get_next_submission_ids(self, *_):
        raise NotImplementedError()

    def _set_bulk_alert_pending(self, *_):
        raise NotImplementedError()

    def _set_bulk_submission_pending(self, *_):
        raise NotImplementedError()

    def _stream_alert_ids(self, *_):
        raise NotImplementedError()

    def _stream_submission_ids(self, *_):
        raise NotImplementedError()

    def create_alert_bundle(self, *_):
        raise NotImplementedError()

    def create_submission_bundle(self, *_):
        raise NotImplementedError()

    def load_bundle(self, *_):
        raise NotImplementedError()

    def stop(self):
        self.running = False

    def set_single_alert_complete(self, *_):
        raise NotImplementedError()

    def set_single_submission_complete(self, *_):
        raise NotImplementedError()

    def setup_alert_input_queue(self, once=False):
        # Bootstrap recovery of pending replayed alerts
        for a in self._stream_alert_ids(f"metadata.replay:{REPLAY_PENDING}"):
            self.log.info(f"Replaying alert: {a['alert_id']}")
            self.put_alert(a)

        # Create the list of filter queries
        processing_fqs = self.alert_fqs + [self.pending_fq, self.done_fq]

        # Run
        while self.running:
            # Find alerts
            alert_input_query = f"reporting_ts:{{{self.last_alert_time} TO now]"
            alerts = self._get_next_alert_ids(alert_input_query, processing_fqs)

            # Set their pending state
            if alerts['items']:
                last_time = alerts['items'][-1]['reporting_ts']
                bulk_query = f"reporting_ts:{{{self.last_alert_time} TO {last_time}]"
                count = len(alerts['items'])
                self._set_bulk_alert_pending(bulk_query, processing_fqs, count)
                self.last_alert_time = last_time

            # Queue them
            for a in alerts['items']:
                self.log.info(f"Replaying alert: {a['alert_id']}")
                self.put_alert(a)

            # Wait if nothing found
            if alerts['total'] == 0:
                self.last_alert_time = self.lookback_time
                for _ in range(EMPTY_WAIT_TIME):
                    if not self.running:
                        break
                    time.sleep(1)

            if once:
                break

    def setup_submission_input_queue(self, once=False):
        # Bootstrap recovery of pending replayed submission
        for sub in self._stream_submission_ids(f"metadata.replay:{REPLAY_PENDING}"):
            self.log.info(f"Replaying submission: {sub['sid']}")
            self.put_submission(sub)

        # Create the list of filter queries
        processing_fqs = self.submission_fqs + [self.pending_fq, self.done_fq]

        # Run
        while self.running:
            # Find submissions
            sub_query = f"times.completed:{{{self.last_submission_time} TO now]"
            submissions = self._get_next_submission_ids(sub_query, processing_fqs)

            # Set their pending state
            if submissions['items']:
                last_time = submissions['items'][-1]['times']['completed']
                bulk_query = f"times.completed:{{{self.last_submission_time} TO {last_time}]"
                count = len(submissions['items'])
                self._set_bulk_submission_pending(bulk_query, processing_fqs, count)
                self.last_submission_time = last_time

            # Queue them
            for sub in submissions['items']:
                self.log.info(f"Replaying submission: {sub['sid']}")
                self.put_submission(sub)

            # Wait if nothing found
            if submissions['total'] == 0:
                self.last_submission_time = self.lookback_time
                for _ in range(EMPTY_WAIT_TIME):
                    if not self.running:
                        break
                    time.sleep(1)

            if once:
                break

    def get_next_alert(self):
        raise NotImplementedError()

    def get_next_submission(self):
        raise NotImplementedError()

    def put_alert(self, *_):
        raise NotImplementedError()

    def put_submission(self, *_):
        raise NotImplementedError()

    def get_next_file(self):
        raise NotImplementedError()

    def put_file(self, *_):
        raise NotImplementedError()


class APIClient(ClientBase):
    def __init__(self, log, host, user, apikey, verify, alert_fqs=None, submission_fqs=None, lookback_time='*'):
        # Import assemblyline client
        from assemblyline_client import get_client

        # Setup AL client
        self.al_client = get_client(host, apikey=(user, apikey), verify=verify)

        super().__init__(log, alert_fqs=alert_fqs, submission_fqs=submission_fqs, lookback_time=lookback_time)

    def _get_next_alert_ids(self, query, filter_queries):
        return self.al_client.search.alert(
            query, fl="alert_id,reporting_ts", sort="reporting_ts asc", rows=100, filters=filter_queries)

    def _get_next_submission_ids(self, query, filter_queries):
        return self.al_client.search.submission(
            query, fl="sid,times.completed", sort="times.completed asc", rows=100, filters=filter_queries)

    def _set_bulk_alert_pending(self, query, filter_queries, max_docs):
        self.al_client.replay.set_bulk_pending('alert', query, filter_queries, max_docs)

    def _set_bulk_submission_pending(self, query, filter_queries, max_docs):
        self.al_client.replay.set_bulk_pending('submission', query, filter_queries, max_docs)

    def _stream_alert_ids(self, query):
        return self.al_client.search.stream.alert(query, fl="alert_id,reporting_ts")

    def _stream_submission_ids(self, query):
        return self.al_client.search.stream.submission(query, fl="sid,times.completed")

    def create_alert_bundle(self, alert_id, bundle_path):
        self.al_client.bundle.create(alert_id, output=bundle_path, use_alert=True)

    def create_submission_bundle(self, sid, bundle_path):
        self.al_client.bundle.create(sid, output=bundle_path)

    def load_bundle(self, bundle_path, min_classification, rescan_services, exist_ok=True):
        self.al_client.bundle.import_bundle(bundle_path,
                                            min_classification=min_classification,
                                            rescan_services=rescan_services,
                                            exist_ok=exist_ok)

    def set_single_alert_complete(self, alert_id):
        self.al_client.replay.set_complete('alert', alert_id)

    def set_single_submission_complete(self, sid):
        self.al_client.replay.set_complete('submission', sid)

    def get_next_alert(self):
        return self.al_client.replay.get_message('alert')

    def get_next_file(self):
        return self.al_client.replay.get_message('file')

    def get_next_submission(self):
        return self.al_client.replay.get_message('submission')

    def put_alert(self, alert):
        self.al_client.replay.put_message('alert', alert)

    def put_file(self, path):
        self.al_client.replay.put_message('file', path)

    def put_submission(self, submission):
        self.al_client.replay.put_message('submission', submission)


class DirectClient(ClientBase):
    def __init__(self, log, alert_fqs=None, submission_fqs=None, lookback_time='*'):
        # Setup datastore
        config = forge.get_config()
        redis = get_client(config.core.redis.nonpersistent.host, config.core.redis.nonpersistent.port, False)

        self.datastore = forge.get_datastore(config=config)
        self.alert_queue = NamedQueue("replay_alert", host=redis)
        self.file_queue = NamedQueue("replay_file", host=redis)
        self.submission_queue = NamedQueue("replay_submission", host=redis)

        super().__init__(log, alert_fqs=alert_fqs, submission_fqs=submission_fqs, lookback_time=lookback_time)

    def _get_next_alert_ids(self, query, filter_queries):
        return self.datastore.alert.search(
            query, fl="alert_id,reporting_ts", sort="reporting_ts asc", rows=100, as_obj=False, filters=filter_queries)

    def _get_next_submission_ids(self, query, filter_queries):
        return self.datastore.submission.search(
            query, fl="sid,times.completed", sort="times.completed asc", rows=100, as_obj=False, filters=filter_queries)

    def _set_bulk_alert_pending(self, query, filter_queries, max_docs):
        operations = [(self.datastore.alert.UPDATE_SET, 'metadata.replay', REPLAY_PENDING)]
        self.datastore.alert.update_by_query(query, operations, filters=filter_queries, max_docs=max_docs)

    def _set_bulk_submission_pending(self, query, filter_queries, max_docs):
        operations = [(self.datastore.submission.UPDATE_SET, 'metadata.replay', REPLAY_PENDING)]
        self.datastore.submission.update_by_query(query, operations, filters=filter_queries, max_docs=max_docs)

    def _stream_alert_ids(self, query):
        return self.datastore.alert.stream_search(query, fl="alert_id,reporting_ts", as_obj=False)

    def _stream_submission_ids(self, query):
        return self.datastore.submission.stream_search(query, fl="sid,times.completed", as_obj=False)

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

    def set_single_alert_complete(self, alert_id):
        operations = [(self.datastore.alert.UPDATE_SET, 'metadata.replay', REPLAY_DONE)]
        self.datastore.alert.update(alert_id, operations)

    def set_single_submission_complete(self, sid):
        operations = [(self.datastore.submission.UPDATE_SET, 'metadata.replay', REPLAY_DONE)]
        self.datastore.submission.update(sid, operations)

    def get_next_alert(self):
        return self.alert_queue.pop(blocking=True, timeout=30)

    def get_next_file(self):
        return self.file_queue.pop(blocking=True, timeout=30)

    def get_next_submission(self):
        return self.submission_queue.pop(blocking=True, timeout=30)

    def put_alert(self, alert):
        self.alert_queue.push(alert)

    def put_file(self, path):
        self.file_queue.push(path)

    def put_submission(self, submission):
        self.submission_queue.push(submission)
