import json
import logging
import os
import shutil
import time

from copy import deepcopy

import certifi
import requests
import yaml

from assemblyline.common import log as al_log
from assemblyline.common.digests import get_sha256_for_file
from assemblyline.common.isotime import iso_to_epoch

al_log.init_logging('service_updater')

LOGGER = logging.getLogger('assemblyline.updater.service')


UPDATE_CONFIGURATION_PATH = os.environ.get('UPDATE_CONFIGURATION_PATH', None)
UPDATE_OUTPUT_PATH = os.environ.get('UPDATE_OUTPUT_PATH', "/tmp/updater_output")


def test_file(_):
    return True


def url_update(test_func=test_file) -> None:
    """
    Using an update configuration file as an input, which contains a list of sources, download all the file(s) which
    have been modified since the last update.
    """
    update_config = {}
    # Load configuration
    if UPDATE_CONFIGURATION_PATH and os.path.exists(UPDATE_CONFIGURATION_PATH):
        with open(UPDATE_CONFIGURATION_PATH, 'r') as yml_fh:
            update_config = yaml.safe_load(yml_fh)
    else:
        LOGGER.warning("Could not find update configuration file.")
        exit(1)

    # Cleanup output path
    if os.path.exists(UPDATE_OUTPUT_PATH):
        if os.path.isdir(UPDATE_OUTPUT_PATH):
            shutil.rmtree(UPDATE_OUTPUT_PATH)
        else:
            os.unlink(UPDATE_OUTPUT_PATH)
    os.makedirs(UPDATE_OUTPUT_PATH)

    # Get sources
    sources = update_config.get('sources', None)
    # Exit if no update sources given
    if not sources:
        exit()

    # Parse updater configuration
    previous_update = update_config.get('previous_update', None)
    previous_hash = update_config.get('previous_hash', None) or {}
    if previous_hash:
        previous_hash = json.loads(previous_hash)
    if isinstance(previous_update, str):
        previous_update = iso_to_epoch(previous_update)

    # Create a requests session
    session = requests.Session()

    files_sha256 = {}

    # Go through each source and download file
    for source in sources:
        uri = source['uri']
        name = source['name']

        if not uri or not name:
            LOGGER.warning(f"Invalid source: {source}")
            continue

        LOGGER.info(f"Downloading file '{name}' from uri '{uri}' ...")

        username = source.get('username', None)
        password = source.get('password', None)
        auth = (username, password) if username and password else None
        ca_cert = source.get('ca_cert', None)
        ignore_ssl_errors = source.get('ssl_ignore_errors', False)

        headers = source.get('headers', None)

        if ca_cert:
            # Add certificate to requests
            cafile = certifi.where()
            with open(cafile, 'a') as ca_editor:
                ca_editor.write(f"\n{ca_cert}")

        session.verify = not ignore_ssl_errors

        try:
            # Check the response header for the last modified date
            response = session.head(uri, auth=auth, headers=headers)
            last_modified = response.headers.get('Last-Modified', None)
            if last_modified:
                # Convert the last modified time to epoch
                last_modified = time.mktime(time.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z"))

                # Compare the last modified time with the last updated time
                if update_config.get('previous_update', None) and last_modified <= previous_update:
                    # File has not been modified since last update, do nothing
                    LOGGER.info("File has not changed since last time, Skipping...")
                    continue

            if update_config.get('previous_update', None):
                previous_update = time.strftime("%a, %d %b %Y %H:%M:%S %Z", time.gmtime(previous_update))
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
                LOGGER.info("File has not changed since last time, Skipping...")
                continue
            elif response.ok:
                file_path = os.path.join(UPDATE_OUTPUT_PATH, name)
                with open(file_path, 'wb') as f:
                    f.write(response.content)

                if not test_func(file_path):
                    os.unlink(file_path)
                    LOGGER.warning(f"The downloaded file was invalid. It will not be part of this update...")
                    continue

                # Append the SHA256 of the file to a list of downloaded files
                sha256 = get_sha256_for_file(file_path)
                if previous_hash.get(name, None) != sha256:
                    files_sha256[name] = sha256
                else:
                    LOGGER.info("File as the same hash as last time. Skipping...")

                LOGGER.info("File successfully downloaded!")
        except requests.Timeout:
            LOGGER.warning(f"Cannot find the file for source {name} with url {uri} - (Timeout)")
            continue
        except Exception as e:
            # Catch all other types of exceptions such as ConnectionError, ProxyError, etc.
            LOGGER.warning(f"Source {name} failed with error: {str(e)}")

    if files_sha256:
        new_hash = deepcopy(previous_hash)
        new_hash.update(files_sha256)

        # Check if the new update hash matches the previous update hash
        if new_hash == previous_hash:
            # Update file(s) not changed, delete the downloaded files and exit
            shutil.rmtree(UPDATE_OUTPUT_PATH, ignore_errors=True)
            exit()

        # Create the response yaml
        with open(os.path.join(UPDATE_OUTPUT_PATH, 'response.yaml'), 'w') as yml_fh:
            yaml.safe_dump(dict(
                hash=json.dumps(new_hash),
            ), yml_fh)

        LOGGER.info("Service update file(s) successfully downloaded")

    # Close the requests session
    session.close()


if __name__ == '__main__':
    url_update()
