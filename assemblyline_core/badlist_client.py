import logging

from assemblyline.common import forge
from assemblyline.datastore.helper import AssemblylineDatastore


# Tasking class
class BadlistClient:
    """A helper class to simplify badlisting for privileged services and service-server."""

    def __init__(self, datastore: AssemblylineDatastore = None, config=None):
        self.log = logging.getLogger('assemblyline.badlist_client')
        self.config = config or forge.CachedObject(forge.get_config)
        self.datastore = datastore or forge.get_datastore(self.config)

    # Badlist
    def exists(self, qhash):
        return self.datastore.badlist.get_if_exists(qhash, as_obj=False)

    def get_badlisted_tags(self, tag_types):
        output = {}
        if isinstance(tag_types, str):
            tag_types = tag_types.split(',')

        if tag_types:
            for tag in tag_types:
                for sl in self.datastore.badlist.stream_search(
                        f"type:tag AND enabled:true AND tag.type:{tag}", as_obj=False):
                    output.setdefault(sl['tag']['type'], [])
                    output[sl['tag']['type']].append(sl['tag']['value'])

        else:
            for sl in self.datastore.badlist.stream_search("type:tag AND enabled:true", as_obj=False):
                output.setdefault(sl['tag']['type'], [])
                output[sl['tag']['type']].append(sl['tag']['value'])

        return output
