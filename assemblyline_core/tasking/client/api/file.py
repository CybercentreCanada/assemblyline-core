import os
import shutil
import tempfile

from assemblyline.common import identify
from assemblyline.common.isotime import now_as_iso
from assemblyline_core.tasking.config import FILESTORE, LOGGER, STORAGE, config
from assemblyline_core.tasking.helper.response import stream_file_response
from assemblyline.filestore import FileStoreException


def download_file(sha256, **_) -> tuple:
    with tempfile.NamedTemporaryFile() as temp_file:
        try:
            FILESTORE.download(sha256, temp_file.name)
            f_size = os.path.getsize(temp_file.name)
            return stream_file_response(open(temp_file.name, 'rb'), sha256, f_size)
        except FileStoreException as e:
            LOGGER.error(e)
            raise e


def upload_files(client_info, request, **_):
    sha256 = request.headers['sha256']
    classification = request.headers['classification']
    ttl = int(request.headers['ttl'])
    is_section_image = request.headers.get('is_section_image', 'false').lower() == 'true'

    with tempfile.NamedTemporaryFile(mode='bw') as temp_file:
        # Try reading multipart data from 'files' or a single file post from stream
        if request.content_type.startswith('multipart'):
            file = request.files['file']
            file.save(temp_file.name)
        elif request.stream.is_exhausted:
            if request.stream.limit == len(request.data):
                temp_file.write(request.data)
            else:
                raise ValueError("Cannot find the uploaded file...")
        else:
            shutil.copyfileobj(request.stream, temp_file)

        # Identify the file info of the uploaded file
        file_info = identify.fileinfo(temp_file.name)

        # Validate SHA256 of the uploaded file
        if sha256 == file_info['sha256']:
            file_info['archive_ts'] = now_as_iso(config.datastore.ilm.days_until_archive * 24 * 60 * 60)
            file_info['classification'] = classification
            if ttl:
                file_info['expiry_ts'] = now_as_iso(ttl * 24 * 60 * 60)
            else:
                file_info['expiry_ts'] = None

            # Update the datastore with the uploaded file
            STORAGE.save_or_freshen_file(file_info['sha256'], file_info, file_info['expiry_ts'],
                                         file_info['classification'], is_section_image=is_section_image)

            # Upload file to the filestore (upload already checks if the file exists)
            FILESTORE.upload(temp_file.name, file_info['sha256'])
        else:
            LOGGER.warning(f"{client_info['client_id']} - {client_info['service_name']} "
                           f"uploaded file (SHA256: {file_info['sha256']}) doesn't match "
                           f"expected file (SHA256: {sha256})")
            return False, f"Uploaded file does not match expected file hash. [{file_info['sha256']} != {sha256}]"

    LOGGER.info(f"{client_info['client_id']} - {client_info['service_name']} "
                f"successfully uploaded file (SHA256: {file_info['sha256']})")

    return True
