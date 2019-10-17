import hashlib
import logging
import os
import shutil
import time
from urllib.parse import urlparse

import requests
import yaml

from assemblyline.common import log as al_log
from assemblyline.common.digests import get_sha256_for_file
from assemblyline.common.isotime import now_as_iso

al_log.init_logging('service_updater')

LOGGER = logging.getLogger('assemblyline.service_updater')


UPDATE_CONFIGURATION_PATH = os.environ.get('UPDATE_CONFIGURATION_PATH', None)
UPDATE_OUTPUT_PATH = os.environ.get('UPDATE_OUTPUT_PATH', None)


def url_update() -> None:
    """
    Using an update configuration file as an input, which contains a list of sources, download all the file(s) which
    have been modified since the last update.
    """
    if os.path.exists(UPDATE_CONFIGURATION_PATH):
        with open(UPDATE_CONFIGURATION_PATH, 'r') as yml_fh:
            update_config = yaml.safe_load(yml_fh)

    sources = update_config.get('sources', None)

    # Exit if no update sources given
    if not sources:
        exit()

    # Create a requests session
    session = requests.Session()

    files_sha256 = []

    # Go through each source and download file
    for source in sources:
        uri = source['uri']

        username = source.get('username', None)
        password = source.get('password', None)
        auth = (username, password) if username and password else None

        headers = source.get('headers', None)

        try:
            # Check the response header for the last modified date
            response = session.head(uri, auth=auth, headers=headers)
            last_modified = response.headers.get('Last-Modified', None)
            if last_modified:
                # Convert the last modified time to epoch
                last_modified = time.mktime(time.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z"))

                # Compare the last modified time with the last updated time
                if update_config.get('previous_update', None) and last_modified <= update_config['previous_update']:
                    # File has not been modified since last update, do nothing
                    continue

            if update_config.get('previous_update', None):
                previous_update = time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.gmtime(update_config['previous_update']))
                if headers:
                    headers['If-Modified-Since'] = previous_update
                else:
                    headers = {
                        'If-Modified-Since': previous_update,
                    }

            response = session.get(uri, auth=auth, headers=headers)

            # Check the response code
            if response.status_code == requests.codes['not_modified']:
                # File has not been modified since last update, do nothing
                continue
            elif response.ok:
                file_name = os.path.basename(urlparse(uri).path)
                file_path = os.path.join(UPDATE_OUTPUT_PATH, file_name)
                with open(file_path, 'wb') as f:
                    f.write(response.content)

                # Append the SHA256 of the file to a list of downloaded files
                files_sha256.append(get_sha256_for_file(file_path))
        except requests.Timeout:
            # TODO: should we retry?
            pass
        except Exception as e:
            # Catch all other types of exceptions such as ConnectionError, ProxyError, etc.
            LOGGER.info(str(e))
            exit()  # TODO: Should we exit even if one file fails to download? Or should we continue downloading other files?

    if files_sha256:
        new_hash = hashlib.md5(' '.join(sorted(files_sha256)).encode('utf-8')).hexdigest()

        # Check if the new update hash matches the previous update hash
        if new_hash == update_config.get('previous_hash', None):
            # Update file(s) not changed, delete the downloaded files and exit
            shutil.rmtree(UPDATE_OUTPUT_PATH, ignore_errors=True)
            exit()

        # Create the response yaml
        with open(os.path.join(UPDATE_OUTPUT_PATH, 'response.yaml'), 'w') as yml_fh:
            yaml.safe_dump(dict(
                previous_update=now_as_iso(),
                previous_hash=new_hash,
            ), yml_fh)

        LOGGER.info("Service update file(s) successfully downloaded")

    # Close the requests session
    session.close()


if __name__ == '__main__':
    url_update()
