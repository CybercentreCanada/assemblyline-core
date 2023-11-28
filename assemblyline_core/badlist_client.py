import hashlib
import logging

from assemblyline.common import forge
from assemblyline.common.chunk import chunk
from assemblyline.datastore.helper import AssemblylineDatastore

CHUNK_SIZE = 1000


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
