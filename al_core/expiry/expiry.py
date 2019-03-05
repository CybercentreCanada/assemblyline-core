
import logging
import time

from assemblyline.common import forge, log as al_log

SLEEP_TIME = 5


class ExpiryManager(object):
    def __init__(self, log):
        self.log = log
        self.datastore = forge.get_datastore()
        self.filestore = forge.get_filestore()
        self.expirable_collections = []

        for name, definition in self.datastore.ds.get_models().items():
            if hasattr(definition, 'expiry_ts'):
                self.expirable_collections.append(getattr(self.datastore, name))

    def run(self):
        while True:
            for collection in self.expirable_collections:
                delete_query = f"expiry_ts:[* TO {self.datastore.ds.now}]"
                number_to_delete = collection.search(delete_query, rows=0, as_obj=False)['total']

                self.log.info(f"Processing collection: {collection.name}")
                if number_to_delete != 0:
                    self.log.info(f"\tStarting to delete {number_to_delete} items...")

                    # Proceed with deletion
                    collection.delete_matching(delete_query)
                    self.log.info("\tDone!")
                else:
                    self.log.info("\tNothing to delete in this collection.")

            time.sleep(SLEEP_TIME)


if __name__ == "__main__":
    al_log.init_logging("expiry")
    em = ExpiryManager(logging.getLogger('assemblyline.expiry'))
    em.run()
