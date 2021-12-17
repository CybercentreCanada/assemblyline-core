import logging

import yaml

from assemblyline.common import forge
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.filestore import FileStore


# Tasking class
class SafelistClient:
    """A helper class to simplify safelisting for privileged services and service-server."""

    def __init__(self, datastore: AssemblylineDatastore = None, filestore: FileStore = None,
                 config=None, redis=None):
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
