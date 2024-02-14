import logging

import yaml

from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso
from assemblyline.datastore.helper import AssemblylineDatastore

CLASSIFICATION = forge.get_classification()


class InvalidSafehash(Exception):
    pass


# Tasking class
class SafelistClient:
    """A helper class to simplify safelisting for privileged services and service-server."""

    def __init__(self, datastore: AssemblylineDatastore = None, config=None):
        self.log = logging.getLogger('assemblyline.safelist_client')
        self.config = config or forge.CachedObject(forge.get_config)
        self.datastore = datastore or forge.get_datastore(self.config)

    # Safelist
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
