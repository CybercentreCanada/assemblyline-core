import hashlib
import logging
import yaml

from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.models.user import ROLES
from assemblyline.remote.datatypes.lock import Lock

CLASSIFICATION = forge.get_classification()


class InvalidSafehash(Exception):
    pass


# Safelist class
class SafelistClient:
    """A helper class to simplify safelisting for privileged services and service-server."""

    def __init__(self, datastore: AssemblylineDatastore = None, config=None):
        self.log = logging.getLogger('assemblyline.safelist_client')
        self.config = config or forge.CachedObject(forge.get_config)
        self.datastore = datastore or forge.get_datastore(self.config)

    def _preprocess_object(self, data: dict):
        # Set defaults
        data.setdefault('classification', CLASSIFICATION.UNRESTRICTED)
        data.setdefault('hashes', {})
        data.setdefault('expiry_ts', None)
        if data['type'] == 'tag':
            # Remove file related fields
            data.pop('file', None)
            data.pop('hashes', None)
            data.pop('signature', None)

            tag_data = data.get('tag', None)
            if tag_data is None or 'type' not in tag_data or 'value' not in tag_data:
                raise ValueError("Tag data not found")

            hashed_value = f"{tag_data['type']}: {tag_data['value']}".encode('utf8')
            data['hashes'] = {
                'md5': hashlib.md5(hashed_value).hexdigest(),
                'sha1': hashlib.sha1(hashed_value).hexdigest(),
                'sha256': hashlib.sha256(hashed_value).hexdigest()
            }

        elif data['type'] == 'signature':
            # Remove file related fields
            data.pop('file', None)
            data.pop('hashes', None)
            data.pop('tag', None)

            sig_data = data.get('signature', None)
            if sig_data is None or 'name' not in sig_data:
                raise ValueError("Signature data not found")

            hashed_value = f"signature: {sig_data['name']}".encode('utf8')
            data['hashes'] = {
                'md5': hashlib.md5(hashed_value).hexdigest(),
                'sha1': hashlib.sha1(hashed_value).hexdigest(),
                'sha256': hashlib.sha256(hashed_value).hexdigest()
            }

        elif data['type'] == 'file':
            data.pop('signature', None)
            data.pop('tag', None)
            data.setdefault('file', {})

        # Ensure expiry_ts is set on tag-related items
        dtl = data.pop('dtl', None) or self.config.core.expiry.safelisted_tag_dtl
        if dtl:
            data['expiry_ts'] = now_as_iso(dtl * 24 * 3600)

        # Set last updated
        data['added'] = data['updated'] = now_as_iso()

        # Find the best hash to use for the key
        for hash_key in ['sha256', 'sha1', 'md5']:
            qhash = data['hashes'].get(hash_key, None)
            if qhash:
                break

        # Validate hash length
        if not qhash:
            raise ValueError("No valid hash found")

        return qhash

    def add_update(self, safelist_object: dict, user: dict = None):
        qhash = self._preprocess_object(safelist_object)

        # Validate sources
        src_map = {}
        for src in safelist_object['sources']:
            if user:
                if src['type'] == 'user':
                    if src['name'] != user['uname']:
                        raise ValueError(f"You cannot add a source for another user. {src['name']} != {user['uname']}")
                else:
                    if ROLES.signature_import not in user['roles']:
                        raise PermissionError("You do not have sufficient priviledges to add an external source.")

            # Find the highest classification of all sources
            safelist_object['classification'] = CLASSIFICATION.max_classification(
                safelist_object['classification'], src.get('classification', None))

            src_map[src['name']] = src

        with Lock(f'add_or_update-safelist-{qhash}', 30):
            old = self.datastore.safelist.get_if_exists(qhash, as_obj=False)
            if old:
                # Save data to the DB
                self.datastore.safelist.save(qhash, SafelistClient._merge_hashes(safelist_object, old))
                return qhash, "update"
            else:
                try:
                    safelist_object['sources'] = list(src_map.values())
                    self.datastore.safelist.save(qhash, safelist_object)
                    return qhash, "add"
                except Exception as e:
                    return ValueError(f"Invalid data provided: {str(e)}")

    def add_update_many(self, list_of_safelist_objects: list):
        if not isinstance(list_of_safelist_objects, list):
            raise ValueError("Could not get the list of hashes")

        new_data = {}
        for safelist_object in list_of_safelist_objects:
            qhash = self._preprocess_object(safelist_object)
            new_data[qhash] = safelist_object

        # Get already existing hashes
        old_data = self.datastore.safelist.multiget(list(new_data.keys()), as_dictionary=True, as_obj=False,
                                                    error_on_missing=False)

        # Test signature names
        plan = self.datastore.safelist.get_bulk_plan()
        for key, val in new_data.items():
            # Use maximum classification
            old_val = old_data.get(key, {'classification': CLASSIFICATION.UNRESTRICTED,
                                         'hashes': {}, 'sources': [], 'type': val['type']})

            # Add upsert operation
            plan.add_upsert_operation(key, SafelistClient._merge_hashes(val, old_val))

        if not plan.empty:
            # Execute plan
            res = self.datastore.safelist.bulk(plan)
            return {"success": len(res['items']), "errors": res['errors']}

        return {"success": 0, "errors": []}

    def exists(self, qhash):
        return self.datastore.safelist.get_if_exists(qhash, as_obj=False)

    def get_safelisted_tags(self, tag_types):
        if isinstance(tag_types, str):
            tag_types = tag_types.split(',')

        with forge.get_cachestore('system', config=self.config, datastore=self.datastore) as cache:
            tag_safelist_yml = cache.get('tag_safelist_yml')
            if tag_safelist_yml:
                tag_safelist_data = yaml.safe_load(tag_safelist_yml)
            else:
                tag_safelist_data = forge.get_tag_safelist_data()

        if tag_types:
            output = {
                'match': {k: v for k, v in tag_safelist_data.get('match', {}).items()
                          if k in tag_types or tag_types == []},
                'regex': {k: v for k, v in tag_safelist_data.get('regex', {}).items()
                          if k in tag_types or tag_types == []},
            }
            for tag in tag_types:
                for sl in self.datastore.safelist.stream_search(
                        f"type:tag AND enabled:true AND tag.type:{tag}", as_obj=False):
                    output['match'].setdefault(sl['tag']['type'], [])
                    output['match'][sl['tag']['type']].append(sl['tag']['value'])

        else:
            output = tag_safelist_data
            for sl in self.datastore.safelist.stream_search("type:tag AND enabled:true", as_obj=False):
                output['match'].setdefault(sl['tag']['type'], [])
                output['match'][sl['tag']['type']].append(sl['tag']['value'])

        return output

    def get_safelisted_signatures(self, **_):
        output = [
            item['signature']['name']
            for item in self.datastore.safelist.stream_search(
                "type:signature AND enabled:true", fl="signature.name", as_obj=False)]

        return output

    @staticmethod
    def _merge_hashes(new, old):
        # Account for the possibility of merging with null types
        if not (new or old):
            # Both are null
            raise ValueError("New and old are both null")
        elif not (new and old):
            # Only one is null, in which case return the other
            return new or old

        try:
            # Check if hash types match
            if new['type'] != old['type']:
                raise InvalidSafehash(f"Safe hash type mismatch: {new['type']} != {old['type']}")

            # Use the new classification but we will recompute it later anyway
            old['classification'] = new['classification']

            # Update updated time
            old['updated'] = new.get('updated', now_as_iso())

            # Update hashes
            old['hashes'].update({k: v for k, v in new['hashes'].items() if v})

            # Update type specific info
            if old['type'] == 'file':
                old.setdefault('file', {})
                new_names = new.get('file', {}).pop('name', [])
                if 'name' in old['file']:
                    for name in new_names:
                        if name not in old['file']['name']:
                            old['file']['name'].append(name)
                elif new_names:
                    old['file']['name'] = new_names
                old['file'].update({k: v for k, v in new.get('file', {}).items() if v})
            elif old['type'] == 'tag':
                old['tag'] = new['tag']

            # Merge sources
            src_map = {x['name']: x for x in new['sources']}
            if not src_map:
                raise InvalidSafehash("No valid source found")

            old_src_map = {x['name']: x for x in old['sources']}
            for name, src in src_map.items():
                if name not in old_src_map:
                    old_src_map[name] = src
                else:
                    old_src = old_src_map[name]
                    if old_src['type'] != src['type']:
                        raise InvalidSafehash(f"Source {name} has a type conflict: {old_src['type']} != {src['type']}")

                    for reason in src['reason']:
                        if reason not in old_src['reason']:
                            old_src['reason'].append(reason)
                    old_src['classification'] = src.get('classification', old_src['classification'])
            old['sources'] = list(old_src_map.values())

            # Calculate the new classification
            for src in old['sources']:
                old['classification'] = CLASSIFICATION.max_classification(
                    old['classification'], src.get('classification', None))

            # Set the expiry
            old['expiry_ts'] = new.get('expiry_ts', None)
            return old
        except Exception as e:
            raise InvalidSafehash(f"Invalid data provided: {str(e)}")
