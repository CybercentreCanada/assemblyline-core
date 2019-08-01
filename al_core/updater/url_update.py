import shutil
import tempfile
import yaml
import os
import time

import requests

from assemblyline.common.digests import get_sha256_for_file
from assemblyline.common.isotime import now_as_iso

UPDATE_CONFIGURATION_PATH = os.environ.get('UPDATE_CONFIGURATION_PATH', None)
OUTPUT_DIRECTORY = os.environ.get('OUTPUT_DIRECTORY', None)


def url_update() -> None:
    """Download a file as an update to a service.

    TODO Future enhancement: if we can keep the time when the file was most
         recently updated, we should be able to use HTTP HEAD and cache headers
         to skip repeatedly downloading the same file.

    :param url: The url from which we want to download the new file.
    :param last_sha256: hash of the previous update
    :param output_directory: Directory to write all of our update data.
    :return: True if the contents of output_directory should be taken as a new update.
             On a non-true return or exception raise, output_directory is destroyed.
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

    # Go through each source and download file
    for source in sources:
        uri = source['uri']

        username = source.get('username', None)
        password = source.get('password', None)
        auth = None
        if username and password:
            auth = (username, password)

        headers = source.get('headers', None)

        try:
            # Check the response header for the last modified date
            response = session.head(uri, auth=auth, headers=headers)
            last_modified = response.headers.get('Last-Modified', None)
            if last_modified:
                # Convert the last modified time to epoch
                last_modified = time.mktime(time.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z"))

                # Compare the last modified time with the last updated time
                if last_modified <= update_config.get('last_updated', 0):
                    # File has not been modified since last update, so skip it
                    continue

            last_updated = time.strftime("%a, %d %b %Y %H:%M:%S %Z",
                                         time.gmtime(update_config.get('last_updated', now_as_iso())))
            if headers:
                headers['If-Modified-Since'] = last_updated
            else:
                headers = {
                    'If-Modified-Since': last_updated,
                }

            response = session.get(uri, auth=auth, headers=headers)

            # Check the response code
            if response.status_code == requests.codes['not_modified']:
                # File has not been modified since last update, do nothing
                pass
            elif response.ok:
                # TODO: save the file
                temp_file = tempfile.NamedTemporaryFile(suffix=now_as_iso(), dir=OUTPUT_DIRECTORY)
                # with open(temp_file, 'wb') as out_file:
                #     shutil.copyfileobj(r, out_file)
        except requests.Timeout:
            # TODO
            pass
        except Exception as e:
            # Except all other types of exceptions such as ConnectionError, ProxyError, etc.
            # TODO
            pass

    # TODO: Compare the hash of the new update file(s) and previous update file hash
    # new_sha256 = get_sha256_for_file(temp_file)
    # if last_sha256 != new_sha256:
    #     temp_file.close()
    #     return new_sha256
    # else:
    #     temp_file.close()

    # TODO: If the hash is different, then keep the new files, otherwise delete them and exit

    # Close the requests session
    session.close()


if __name__ == '__main__':
    """
    Take a config.yaml which contains a list of sources. Download all the file(s) which have been modified
    since the last update.
    """

    url_update()
