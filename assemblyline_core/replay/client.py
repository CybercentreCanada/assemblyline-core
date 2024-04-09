import json
import os
import time

from assemblyline.common import forge
from assemblyline.common.bundling import create_bundle, import_bundle
from assemblyline.odm import Model
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.hash import Hash
from assemblyline_core.badlist_client import BadlistClient
from assemblyline_core.safelist_client import SafelistClient

EMPTY_WAIT_TIME = int(os.environ.get('EMPTY_WAIT_TIME', '30'))
REPLAY_REQUESTED = 'requested'
REPLAY_PENDING = 'pending'
REPLAY_DONE = 'done'


class ClientBase(object):
    def __init__(self, log, lookback_time='*',
                 alert_fqs=None, badlist_fqs=None, safelist_fqs=None, submission_fqs=None, workflow_fqs=None):
        # Set logger
        self.log = log

        # Setup timming
        self.last_alert_time = self.last_submission_time = self.lookback_time = lookback_time

        # Setup filter queries
        self.pending_fq = f'NOT metadata.replay:{REPLAY_PENDING}'
        self.done_fq = f'NOT metadata.replay:{REPLAY_DONE}'
        self.alert_fqs = alert_fqs or []
        self.badlist_fqs = badlist_fqs or []
        self.safelist_fqs = safelist_fqs or []
        self.submission_fqs = submission_fqs or []
        self.workflow_fqs = workflow_fqs or []

        # Set running flag
        self.running = True

    def _put_checkpoint(self, *_):
        raise NotImplementedError()

    def _get_checkpoint(self, *_):
        raise NotImplementedError()

    def _get_next_object_ids(self, collection, query, filter_queries, fl, sort):
        raise NotImplementedError()

    def _get_next_alert_ids(self, query, filter_queries):
        return self._get_next_object_ids("alert", query, filter_queries, "alert_id,reporting_ts", "reporting_ts asc")

    def _get_next_submission_ids(self, query, filter_queries):
        return self._get_next_object_ids("submission", query, filter_queries, "sid,times.completed",
                                         "times.completed asc")

    def _set_bulk_object_pending(self, collection, query, filter_queries, max_docs):
        raise NotImplementedError()

    def _set_bulk_alert_pending(self, query, filter_queries, max_docs):
        self._set_bulk_object_pending("alert", query, filter_queries, max_docs)

    def _set_bulk_submission_pending(self, query, filter_queries, max_docs):
        self._set_bulk_object_pending("submission", query, filter_queries, max_docs)

    def _stream_objects(self, collection, query, fl="*", filter_queries=[]):
        raise NotImplementedError()

    def _stream_alert_ids(self, query):
        return self._stream_objects("alert", query, "alert_id,reporting_ts")

    def _stream_submission_ids(self, query):
        return self._stream_objects("submission", query, "sid,times.completed")

    def create_al_bundle(self, id, bundle_path, use_alert=False):
        raise NotImplementedError()

    def create_alert_bundle(self, alert_id, bundle_path):
        self.create_al_bundle(alert_id, bundle_path, use_alert=True)

    def create_submission_bundle(self, sid, bundle_path):
        self.create_al_bundle(sid, bundle_path)

    def load_bundle(self, *_):
        raise NotImplementedError()

    def load_json(self, *_):
        raise NotImplementedError()

    def stop(self):
        self.running = False

    def set_single_object_complete(self, collection, id):
        raise NotImplementedError()

    def set_single_alert_complete(self, alert_id):
        self.set_single_object_complete("alert", alert_id)

    def set_single_submission_complete(self, sid):
        self.set_single_object_complete("submission", sid)

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
            sub_query = f"times.completed:[{self.last_submission_time} TO now]"
            submissions = self._get_next_submission_ids(sub_query, processing_fqs)

            # Set their pending state
            if submissions['items']:
                last_time = submissions['items'][-1]['times']['completed']
                bulk_query = f"times.completed:[{self.last_submission_time} TO {last_time}]"
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

    def _setup_checkpoint_based_input_queue(self, collection: str, id_field: str, date_field: str, once=False):
        # At bootstrap, get the last checkpoint
        checkpoint = self._get_checkpoint(collection)
        fqs = getattr(self, f"{collection}_fqs")

        # Run
        while self.running:
            # Find objects of the collection that haven't been replayed
            for obj in self._stream_objects(
                    collection, f"{date_field}:[{checkpoint} TO now]", fl="*,id", filter_queries=fqs):
                self.log.info(f"Replaying {collection}: {obj[id_field]}")
                # Submit name queue to be tasked to worker(s) for replay
                self.put_message(collection, obj)
                # Update checkpoint
                checkpoint = obj[date_field]

            # Wait if there are no more items to queue at this time
            if self._query(collection, f"{date_field}:[{checkpoint} TO now]", fqs, rows=0)['total'] == 0:
                for _ in range(EMPTY_WAIT_TIME):
                    if not self.running:
                        break
                    time.sleep(1)

            if once:
                break

    def setup_workflow_input_queue(self, once=False):
        self._setup_checkpoint_based_input_queue("workflow", "workflow_id", "last_edit", once)

    def setup_badlist_input_queue(self, once=False):
        self._setup_checkpoint_based_input_queue("badlist", "id", "updated", once)

    def setup_safelist_input_queue(self, once=False):
        self._setup_checkpoint_based_input_queue("safelist", "id", "updated", once)

    def _query(self, collection, query, filter_queries=[], rows=None, track_total_hits=False):
        raise NotImplementedError()

    def query_alerts(self, query="*", track_total_hits=False):
        self._query("alert", query=query, track_total_hits=track_total_hits)

    def get_next_message(self, message_type):
        raise NotImplementedError()

    def get_next_alert(self):
        return self.get_next_message("alert")

    def get_next_badlist(self):
        return self.get_next_message("badlist")

    def get_next_file(self):
        return self.get_next_message("file")

    def get_next_safelist(self):
        return self.get_next_message("safelist")

    def get_next_submission(self):
        return self.get_next_message("submission")

    def get_next_workflow(self):
        return self.get_next_message("workflow")

    def put_message(self, message_type, message):
        raise NotImplementedError()

    def put_alert(self, alert):
        self.put_message("alert", alert)

    def put_badlist(self, badlist):
        self.put_message("badlist", badlist)

    def put_file(self, path):
        self.put_message("file", path)

    def put_safelist(self, safelist):
        self.put_message("safelist", safelist)

    def put_submission(self, submission):
        self.put_message("submission", submission)

    def put_workflow(self, workflow):
        self.put_message("workflow", workflow)


class APIClient(ClientBase):
    def __init__(self, log, host, user, apikey, verify, **kwargs):
        from assemblyline_client import get_client

        # Setup AL client
        self.al_client = get_client(host, apikey=(user, apikey), verify=verify)

        super().__init__(log, **kwargs)

    def _put_checkpoint(self, collection, checkpoint):
        return self.al_client.replay.put_checkpoint(collection, checkpoint)

    def _get_checkpoint(self, collection):
        return self.al_client.replay.get_checkpoint(collection)

    def _get_next_object_ids(self, collection, query, filter_queries, fl, sort):
        return getattr(self.al_client.search, collection)(query, fl=fl, sort=sort, rows=100, filters=filter_queries)

    def _set_bulk_object_pending(self, collection, query, filter_queries, max_docs):
        self.al_client.replay.set_bulk_pending(collection, query, filter_queries, max_docs)

    def _stream_objects(self, collection, query, fl="*", filter_queries=[]):
        return getattr(self.al_client.search.stream, collection)(query, fl=fl, filters=filter_queries, as_obj=False)

    def create_al_bundle(self, id, bundle_path, use_alert=False):
        self.al_client.bundle.create(id, output=bundle_path, use_alert=use_alert)

    def load_bundle(self, bundle_path, min_classification, rescan_services, exist_ok=True):
        self.al_client.bundle.import_bundle(bundle_path,
                                            min_classification=min_classification,
                                            rescan_services=rescan_services,
                                            exist_ok=exist_ok)

    def load_json(self, file_path):
        from assemblyline_client import ClientError

        # We're assuming all JSON that loaded has an "enabled" field
        collection = os.path.basename(file_path).split('_', 1)[0]
        with open(file_path) as fp:
            data_blob = json.load(fp)

        if isinstance(data_blob, list):
            for data in data_blob:
                id = data.pop("id")
                try:
                    # Let's see if there's an existing document with the same ID in the collection
                    obj = getattr(self.al_client, collection)(id)

                    if collection == "workflow":
                        # If there has been any edits by another user, then preserve the enabled state
                        # Otherwise, the workflow will be synchronized with the origin system
                        if obj['edited_by'] != data['edited_by']:
                            data['enabled'] = obj["enabled"]

                        self.al_client.workflow.update(id, data)
                    elif collection == "badlist":
                        data['enabled'] = obj["enabled"]
                        self.al_client.badlist.add_update(data)
                    elif collection == "safelist":
                        data['enabled'] = obj["enabled"]
                        self.al_client.safelist.add_update(data)
                except ClientError as e:
                    if e.status_code == 404:
                        # The document doesn't exist in the system, therefore create it
                        if collection == "workflow":
                            self.al_client.workflow.add(data)
                        elif collection == "badlist":
                            self.al_client.badlist.add_update(data)
                        elif collection == "safelist":
                            self.al_client.safelist.add_update(data)
                        return
                    raise

    def set_single_object_complete(self, collection, id):
        self.al_client.replay.set_complete(collection, id)

    def _query(self, collection, query, filter_queries=[], rows=None, track_total_hits=False):
        return getattr(self.al_client.search, collection)(
            query=query, filters=filter_queries, rows=rows, track_total_hits=track_total_hits
        )

    def get_next_message(self, message_type):
        return self.al_client.replay.get_message(message_type)

    def put_message(self, message_type, message):
        if isinstance(message, Model):
            message = message.as_primitives()
        self.al_client.replay.put_message(message_type, message)


class DirectClient(ClientBase):
    def __init__(self, log, **kwargs):
        from assemblyline.remote.datatypes import get_client

        # Setup datastore
        config = forge.get_config()
        redis = get_client(config.core.redis.nonpersistent.host, config.core.redis.nonpersistent.port, False)
        # Initialize connection to redis-persistent for checkpointing
        redis_persist = get_client(config.core.redis.persistent.host,
                                   config.core.redis.persistent.port, False)
        self.datastore = forge.get_datastore(config=config)
        self.queues = {
            queue_type: NamedQueue(f"replay_{queue_type}", host=redis)
            for queue_type in ['alert', 'file', 'submission', 'safelist', 'badlist', 'workflow']
        }
        self.checkpoint_hash = Hash('replay_checkpoints', redis_persist)

        super().__init__(log, **kwargs)

    def _query(self, collection, query, filter_queries=[], rows=None, track_total_hits=False):
        return getattr(self.datastore, collection).search(
            query, filters=filter_queries, rows=rows, track_total_hits=track_total_hits, as_obj=False
        )

    def _put_checkpoint(self, collection, checkpoint):
        self.checkpoint_hash.set(collection, checkpoint)

    def _get_checkpoint(self, collection) -> str:
        return self.checkpoint_hash.get(collection) or "*"

    def _get_next_object_ids(self, collection, query, filter_queries, fl, sort):
        return getattr(self.datastore, collection).search(query, fl=fl, sort=sort, rows=100, filters=filter_queries, as_obj=False)

    def _set_bulk_object_pending(self, collection, query, filter_queries, max_docs):
        ds_collection = getattr(self.datastore, collection)
        operations = [(ds_collection.UPDATE_SET, 'metadata.replay', REPLAY_PENDING)]
        ds_collection.update_by_query(query, operations, filters=filter_queries, max_docs=max_docs)

    def _stream_objects(self, collection, query, fl="*", filter_queries=[]):
        return getattr(self.datastore, collection).stream_search(query, fl=fl, filters=filter_queries, as_obj=False)

    def create_al_bundle(self, id, bundle_path, use_alert=False):
        temp_bundle_file = create_bundle(id, working_dir=os.path.dirname(bundle_path), use_alert=use_alert)
        os.rename(temp_bundle_file, bundle_path)

    def load_bundle(self, bundle_path, min_classification, rescan_services, exist_ok=True):
        import_bundle(bundle_path,
                      min_classification=min_classification,
                      rescan_services=rescan_services,
                      exist_ok=exist_ok)

    def load_json(self, file_path):
        # We're assuming all JSON that loaded has an "enabled" field
        collection = os.path.basename(file_path).split('_', 1)[0]
        with open(file_path) as fp:
            data_blob = json.load(fp)

        if isinstance(data_blob, list):
            es_collection = getattr(self.datastore, collection)
            for data in data_blob:
                id = data.pop("id")

                # Let's see if there's an existing document with the same ID in the collection
                obj = es_collection.get_if_exists(id, as_obj=False)

                if collection == "workflow":
                    # If there has been any edits by another user, then preserve the enabled state
                    # Otherwise, the workflow will be synchronized with the origin system
                    if obj and obj['edited_by'] != data['edited_by']:
                        data['enabled'] = obj["enabled"]
                    es_collection.save(id, data)
                elif collection == "badlist":
                    if obj:
                        # Preserve the system's enabled state of the item
                        data['enabled'] = obj["enabled"]
                    es_collection.save(id, BadlistClient._merge_hashes(data, obj))
                elif collection == "safelist":
                    if obj:
                        # Preserve the system's enabled state of the item
                        data['enabled'] = obj["enabled"]
                    es_collection.save(id, SafelistClient._merge_hashes(data, obj))
            es_collection.commit()

    def set_single_object_complete(self, collection, id):
        ds_collection = getattr(self.datastore, collection)
        operations = [(ds_collection.UPDATE_SET, 'metadata.replay', REPLAY_DONE)]
        ds_collection.update(id, operations)

    def get_next_message(self, message_type):
        return self.queues[message_type].pop(blocking=True, timeout=30)

    def put_message(self, message_type, message):
        if isinstance(message, Model):
            message = message.as_primitives()
        self.queues[message_type].push(message)
