import os
import shutil
import tempfile

import urllib3

from assemblyline.common.digests import get_sha256_for_file
from assemblyline.common.isotime import now_as_iso

http = urllib3.PoolManager()


def url_update(url: str, sha256: str):
    temp_file = tempfile.NamedTemporaryFile(suffix=now_as_iso())
    dest = '/opt/alv4/'  # TODO: path to shared volume

    # Download the file from URL
    with http.request('GET', url, preload_content=False) as r, open(temp_file, 'wb') as out_file:
        shutil.copyfileobj(r, out_file)

    # Compare the SHA256 of the new update file and previous update file
    if sha256 != get_sha256_for_file(temp_file):
        os.rename(temp_file.name, dest)

    temp_file.close()
