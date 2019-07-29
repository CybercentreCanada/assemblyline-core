import os
import shutil
import tempfile

import urllib3

from assemblyline.common.digests import get_sha256_for_file
from assemblyline.common.isotime import now_as_iso

http = urllib3.PoolManager()


def url_update(url: str, sha256: str, output_directory: str) -> True:
    """Download a file as an update to a service.

    TODO Future enhancement: if we can keep the time when the file was most
         recently updated, we should be able to use HTTP HEAD and cache headers
         to skip repeatedly downloading the same file.

    :param url: The url from which we want to download the new file.
    :param sha256: hash of the previous update
    :param output_directory: Directory to write all of our update data.
    :return: True if the contents of output_directory should be taken as a new update.
             On a non-true return or exception raise, output_directory is destroyed.
    """
    temp_file = tempfile.NamedTemporaryFile(suffix=now_as_iso(), dir=output_directory)

    # Download the file from URL
    with http.request('GET', url, preload_content=False) as r, open(temp_file, 'wb') as out_file:
        shutil.copyfileobj(r, out_file)

    # Compare the SHA256 of the new update file and previous update file
    if sha256 != get_sha256_for_file(temp_file):
        os.rename(temp_file.name, dest)

    temp_file.close()
