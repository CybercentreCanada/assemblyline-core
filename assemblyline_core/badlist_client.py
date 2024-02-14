import hashlib
import logging

from assemblyline.common import forge
from assemblyline.common.chunk import chunk
from assemblyline.common.isotime import now_as_iso
from assemblyline.datastore.helper import AssemblylineDatastore

CHUNK_SIZE = 1000
CLASSIFICATION = forge.get_classification()


class InvalidBadhash(Exception):
    pass


class BadlistClient:
    """A helper class to simplify badlisting for privileged services and service-server."""

    def __init__(self, datastore: AssemblylineDatastore = None, config=None):
        self.log = logging.getLogger('assemblyline.badlist_client')
        self.config = config or forge.CachedObject(forge.get_config)
        self.datastore = datastore or forge.get_datastore(self.config)

    # Badlist
    def exists(self, qhash):
        return self.datastore.badlist.get_if_exists(qhash, as_obj=False)

    def exists_tags(self, tag_map):
        lookup_keys = []
        for tag_type, tag_values in tag_map.items():
            for tag_value in tag_values:
                lookup_keys.append(hashlib.sha256(f"{tag_type}: {tag_value}".encode('utf8')).hexdigest())

        # Elasticsearch's result window can't be more than 10000 rows
        # we will query for matches in chunks
        results = []
        for key_chunk in chunk(lookup_keys, CHUNK_SIZE):
            results += self.datastore.badlist.search("*", fl="*", rows=CHUNK_SIZE,
                                                     as_obj=False, key_space=key_chunk)['items']

        return results

    def find_similar_tlsh(self, tlsh):
        return self.datastore.badlist.search(f"hashes.tlsh:{tlsh}", fl="*", as_obj=False)['items']

    def find_similar_ssdeep(self, ssdeep):
        try:
            _, long, _ = ssdeep.replace('/', '\\/').split(":")
            return self.datastore.badlist.search(f"hashes.ssdeep:{long}~", fl="*", as_obj=False)['items']
        except ValueError:
            self.log.warning(f'This is not a valid SSDeep hash: {ssdeep}')
            return []

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
                raise InvalidBadhash(f"Bad hash type mismatch: {new['type']} != {old['type']}")

            # Use the new classification but we will recompute it later anyway
            old['classification'] = new['classification']

            # Update updated time
            old['updated'] = new.get('updated', now_as_iso())

            # Update hashes
            old['hashes'].update({k: v for k, v in new['hashes'].items() if v})

            # Merge attributions
            if not old['attribution']:
                old['attribution'] = new.get('attribution', None)
            elif new.get('attribution', None):
                for key in ["actor", 'campaign', 'category', 'exploit', 'implant', 'family', 'network']:
                    old_value = old['attribution'].get(key, []) or []
                    new_value = new['attribution'].get(key, []) or []
                    old['attribution'][key] = list(set(old_value + new_value)) or None

            if old['attribution'] is not None:
                old['attribution'] = {key: value for key, value in old['attribution'].items() if value}

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
                raise InvalidBadhash("No valid source found")

            old_src_map = {x['name']: x for x in old['sources']}
            for name, src in src_map.items():
                if name not in old_src_map:
                    old_src_map[name] = src
                else:
                    old_src = old_src_map[name]
                    if old_src['type'] != src['type']:
                        raise InvalidBadhash(f"Source {name} has a type conflict: {old_src['type']} != {src['type']}")

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
            raise InvalidBadhash(f"Invalid data provided: {str(e)}")
