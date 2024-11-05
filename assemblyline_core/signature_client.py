import logging

from assemblyline.common import forge
from assemblyline.common.isotime import iso_to_epoch, now_as_iso
from assemblyline.common.memory_zip import InMemoryZip
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.messages.changes import Operation
from assemblyline.odm.models.service import SIGNATURE_DELIMITERS
from assemblyline.odm.models.signature import DEPLOYED_STATUSES, STALE_STATUSES, DRAFT_STATUSES


DEFAULT_DELIMITER = "\n\n"
CLASSIFICATION = forge.get_classification()


# Signature class
class SignatureClient:
    """A helper class to simplify signature management for privileged services and service-server."""

    def __init__(self, datastore: AssemblylineDatastore = None, config=None, classification_replace_map={}):
        self.log = logging.getLogger('assemblyline.signature_client')
        self.config = config or forge.CachedObject(forge.get_config)
        self.datastore = datastore or forge.get_datastore(self.config)
        self.service_list = forge.CachedObject(self.datastore.list_all_services, kwargs=dict(as_obj=False, full=True))
        self.delimiters = forge.CachedObject(self._get_signature_delimiters)
        self.classification_replace_map = classification_replace_map

    def _get_signature_delimiters(self):
        signature_delimiters = {}
        for service in self.service_list:
            if service.get("update_config", {}).get("generates_signatures", False):
                signature_delimiters[service['name'].lower()] = self._get_signature_delimiter(service['update_config'])
        return signature_delimiters

    def _get_signature_delimiter(self, update_config):
        delimiter_type = update_config['signature_delimiter']
        if delimiter_type == 'custom':
            delimiter = update_config['custom_delimiter'].encode().decode('unicode-escape')
        else:
            delimiter = SIGNATURE_DELIMITERS.get(delimiter_type, '\n\n')
        return {'type': delimiter_type, 'delimiter': delimiter}

    def _update_classification(self, signature):
        classification = signature['classification']
        # Update classification of signatures based on rewrite definition
        for term, replacement in self.classification_replace_map.items():
            if replacement.startswith('_'):
                # Replace with known field in Signature model
                # Otherwise replace with literal
                if signature.get(replacement[1:]):
                    replacement = signature[replacement[1:]]

            classification = classification.replace(term, replacement)

        # Save the (possibly) updated classfication
        signature['classification'] = classification


    def add_update(self, data, dedup_name=True):
        if data.get('type', None) is None or data['name'] is None or data['data'] is None:
            raise ValueError("Signature id, name, type and data are mandatory fields.")

        # Compute signature ID if missing
        data['signature_id'] = data.get('signature_id', data['name'])

        key = f"{data['type']}_{data['source']}_{data['signature_id']}"

        # Test signature name
        if dedup_name:
            check_name_query = f"name:\"{data['name']}\" " \
                f"AND type:\"{data['type']}\" " \
                f"AND source:\"{data['source']}\" " \
                f"AND NOT id:\"{key}\""
            other = self.datastore.signature.search(check_name_query, fl='id', rows='0')
            if other['total'] > 0:
                raise ValueError("A signature with that name already exists")

        old = self.datastore.signature.get(key, as_obj=False)
        op = Operation.Modified if old else Operation.Added
        if old:
            if old['data'] == data['data']:
                return True, key, None

            # Ensure that the last state change, if any, was made by a user and not a system account.
            user_modified_last_state = old['state_change_user'] not in ['update_service_account', None]

            # If rule state is moving to an active state but was disabled by a user before:
            # Keep original inactive state, a user changed the state for a reason
            if user_modified_last_state and data['status'] == 'DEPLOYED' and data['status'] != old['status']:
                data['status'] = old['status']

            # Preserve last state change
            data['state_change_date'] = old['state_change_date']
            data['state_change_user'] = old['state_change_user']

            # Preserve signature stats
            data['stats'] = old['stats']

        self._update_classification(data)

        # Save the signature
        success = self.datastore.signature.save(key, data)
        return success, key, op

    def add_update_many(self, source, sig_type, data, dedup_name=True):
        if source is None or sig_type is None or not isinstance(data, list):
            raise ValueError("Source, source type and data are mandatory fields.")

        # Test signature names
        names_map = {x['name']: f"{x['type']}_{x['source']}_{x.get('signature_id', x['name'])}" for x in data}

        skip_list = []
        if dedup_name:
            for item in self.datastore.signature.stream_search(f"type: \"{sig_type}\" AND source:\"{source}\"",
                                                               fl="id,name", as_obj=False, item_buffer_size=1000):
                lookup_id = names_map.get(item['name'], None)
                if lookup_id and lookup_id != item['id']:
                    skip_list.append(lookup_id)

            if skip_list:
                data = [
                    x for x in data
                    if f"{x['type']}_{x['source']}_{x.get('signature_id', x['name'])}" not in skip_list]

        old_data = self.datastore.signature.multiget(list(names_map.values()), as_dictionary=True, as_obj=False,
                                                     error_on_missing=False)

        plan = self.datastore.signature.get_bulk_plan()
        for rule in data:
            key = f"{rule['type']}_{rule['source']}_{rule.get('signature_id', rule['name'])}"
            if key in old_data:
                # Ensure that the last state change, if any, was made by a user and not a system account.
                user_modified_last_state = old_data[key]['state_change_user'] not in ['update_service_account', None]

                # If rule state is moving to an active state but was disabled by a user before:
                # Keep original inactive state, a user changed the state for a reason
                if user_modified_last_state and rule['status'] == 'DEPLOYED' and rule['status'] != old_data[key][
                        'status']:
                    rule['status'] = old_data[key]['status']

                # Preserve last state change
                rule['state_change_date'] = old_data[key]['state_change_date']
                rule['state_change_user'] = old_data[key]['state_change_user']

                # Preserve signature stats
                rule['stats'] = old_data[key]['stats']

            self._update_classification(rule)

            plan.add_upsert_operation(key, rule)

        if not plan.empty:
            res = self.datastore.signature.bulk(plan)
            return {"success": len(res['items']), "errors": res['errors'], "skipped": skip_list}

        return {"success": 0, "errors": [], "skipped": skip_list}

    def change_status(self, signature_id, status, user={}):
        possible_statuses = DEPLOYED_STATUSES + DRAFT_STATUSES
        if status not in possible_statuses:
            raise ValueError(f"You cannot apply the status {status} on yara rules.")

        data = self.datastore.signature.get(signature_id, as_obj=False)
        if data:
            if user and not CLASSIFICATION.is_accessible(user['classification'],
                                                         data.get('classification', CLASSIFICATION.UNRESTRICTED)):
                raise PermissionError("You are not allowed change status on this signature")

            if data['status'] in STALE_STATUSES and status not in DRAFT_STATUSES:
                raise ValueError(f"Only action available while signature in {data['status']} "
                                 f"status is to change signature to a DRAFT status. ({', '.join(DRAFT_STATUSES)})")

            if data['status'] in DEPLOYED_STATUSES and status in DRAFT_STATUSES:
                raise ValueError(f"You cannot change the status of signature {signature_id} from "
                                 f"{data['status']} to {status}.")

            today = now_as_iso()
            uname = user.get('uname')

            if status not in ['DISABLED', 'INVALID', 'TESTING']:
                query = f"status:{status} AND signature_id:{data['signature_id']} AND NOT id:{signature_id}"
                others_operations = [
                    ('SET', 'last_modified', today),
                    ('SET', 'state_change_date', today),
                    ('SET', 'state_change_user', uname),
                    ('SET', 'status', 'DISABLED')
                ]
                self.datastore.signature.update_by_query(query, others_operations)

            operations = [
                ('SET', 'last_modified', today),
                ('SET', 'state_change_date', today),
                ('SET', 'state_change_user', uname),
                ('SET', 'status', status)
            ]

            return self.datastore.signature.update(signature_id, operations), data
        raise FileNotFoundError(f"Signature not found. ({signature_id})")

    def download(self, query=None, access=None) -> bytes:
        if not query:
            query = "*"

        output_files = {}

        signature_list = sorted(
            self.datastore.signature.stream_search(
                query, fl="signature_id,type,source,data,order", access_control=access, as_obj=False,
                item_buffer_size=1000),
            key=lambda x: x['order'])

        for sig in signature_list:
            out_fname = f"{sig['type']}/{sig['source']}"
            if self.delimiters.get(sig['type'], {}).get('type', None) == 'file':
                out_fname = f"{out_fname}/{sig['signature_id']}"
            output_files.setdefault(out_fname, [])
            output_files[out_fname].append(sig['data'])

        output_zip = InMemoryZip()
        for fname, data in output_files.items():
            separator = self.delimiters.get(fname.split('/')[0], {}).get('delimiter', DEFAULT_DELIMITER)
            output_zip.append(fname, separator.join(data))

        return output_zip.read()

    def update_available(self, since='', sig_type='*'):
        since = since or '1970-01-01T00:00:00.000000Z'
        last_update = iso_to_epoch(since)
        last_modified = iso_to_epoch(self.datastore.get_signature_last_modified(sig_type))

        return last_modified > last_update
