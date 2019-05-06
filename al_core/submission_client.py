""" File Submission Interfaces.

TODO update this

The Submission service encapsulates the core functionality of accepting,
triaging and forwarding a submission to the dispatcher.

There are three primary modes of submission:

  two-phase (presubmit + submit)
  inline  (submit file)
  existing (submit a file that is already in the file cache/SAN)

In two-phase mode, submission is a presubmit followed by a submit.
A 'presubmit' is sent to the submission service first. If the server already
has a copy of the sample it indicates as such to the client which saves the
client from copying the file again. Once the client has copied the file
(if required) it then issues a final 'submit'.

"""

import logging
import tempfile
import os
from typing import Tuple, List
from datetime import datetime, timedelta

from assemblyline.common import forge
from assemblyline.common import identify
from assemblyline.common.importing import load_module_by_path
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.str_utils import safe_str
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.messages.submission import Submission as SubmissionObject
from assemblyline.odm.models.submission import Submission, File
from assemblyline.filestore import CorruptedFileStoreException, FileStore

from al_core.dispatching.client import DispatchClient


def assert_valid_sha256(sha256):
    if len(sha256) != 64:
        raise ValueError('Invalid SHA256: %s' % sha256)


class SubmissionException(Exception):
    pass


class SubmissionClient:
    """A helper class to simplify submitting files from internal or external sources.

    This tool helps take care of interactions between the filestore,
    datastore, dispatcher, and any sources of files to be processed.
    """

    def __init__(self, datastore: AssemblylineDatastore=None, filestore: FileStore=None,
                 config=None, redis=None):
        self.log = logging.getLogger('assemblyline.submission_client')
        self.config = config or forge.CachedObject(forge.get_config)
        self.datastore = datastore or forge.get_datastore(self.config)
        self.filestore = filestore or forge.get_filestore(self.config)
        self.redis = redis

        # A client for interacting with the dispatcher
        self.dispatcher = DispatchClient(datastore, redis)

    def submit(self, submission_obj: SubmissionObject, local_files: List = None, cleanup=True, completed_queue=None):
        """Submit several files in a single submission.

        After this method runs, there should be no local copies of the file left.
        """
        if local_files is None:
            local_files = []

        try:
            classification = str(submission_obj.params.classification)
            expiry = now_as_iso(submission_obj.params.ttl * 60 * 60 * 24)
            max_size = self.config.submission.max_file_size

            if len(submission_obj.files) == 0:
                if len(local_files) == 0:
                    raise SubmissionException("No files found to submit...")

                for local_file in local_files:
                    # Upload/download, extract, analyze files
                    file_hash, size, new_metadata = self._ready_file(local_file, expiry, classification, cleanup)
                    self.filestore.upload(local_file, file_hash)
                    submission_obj.metadata.update(**new_metadata)

                    # Check that after we have resolved exactly what to pass on, that it
                    # remains a valid target for scanning
                    if size > max_size and not submission_obj.params.ignore_size:
                        msg = "File too large (%d > %d). Submission failed" % (size, max_size)
                        raise SubmissionException(msg)
                    elif size == 0:
                        msg = "File empty. Submission failed"
                        raise SubmissionException(msg)

                    submission_obj.files.append(File({
                        'name': safe_str(os.path.basename(local_file)),
                        'size': size,
                        'sha256': file_hash,
                    }))
            else:
                for f in submission_obj.files:
                    temporary_path = None
                    try:
                        fd, temporary_path = tempfile.mkstemp(prefix="submission.submit")
                        os.close(fd)  # We don't need the file descriptor open
                        self.filestore.download(f.sha256, temporary_path)
                        file_hash, size, new_metadata = self._ready_file(temporary_path, expiry,
                                                                         classification, cleanup, sha256=f.sha256)
                        submission_obj.metadata.update(**new_metadata)

                        # Check that after we have resolved exactly what to pass on, that it
                        # remains a valid target for scanning
                        if size > max_size and not submission_obj.params.ignore_size:
                            msg = "File too large (%d > %d). Submission failed" % (size, max_size)
                            raise SubmissionException(msg)
                        elif size == 0:
                            msg = "File empty. Submission failed"
                            raise SubmissionException(msg)

                        if f.size is None:
                            f.size = size

                    finally:
                        if temporary_path:
                            if os.path.exists(temporary_path):
                                os.unlink(temporary_path)

            # We should now have all the information we need to construct a submission object
            sub = Submission(dict(
                classification=classification,
                error_count=0,
                errors=[],
                expiry_ts=datetime.utcnow() + timedelta(days=submission_obj.params.ttl),
                file_count=len(submission_obj.files),
                files=submission_obj.files,
                max_score=0,
                metadata=submission_obj.metadata,
                params=submission_obj.params,
                results=[],
                sid=submission_obj.sid,
                state='submitted'
            ))
            self.datastore.submission.save(sub.sid, sub)

            self.log.debug("Submission complete. Dispatching: %s", sub.sid)
            self.dispatcher.dispatch_submission(sub, completed_queue=completed_queue)

            return sub
        finally:
            # Just in case this method fails clean up local files
            if cleanup:
                for path in local_files:
                    if path and os.path.exists(path):
                        # noinspection PyBroadException
                        try:
                            os.unlink(path)
                        except Exception:
                            self.log.error("Couldn't delete dangling file %s", path)

    def _ready_file(self, local_path: str, expiry, classification, cleanup, sha256=None) -> Tuple[str, int, dict]:
        """Take a file from local storage and prepare it for submission.

        After this method finished the file will ONLY exist on the filestore, not locally.
        """
        massaged_path = None
        try:
            # Analyze the file and make sure the file table is up to date
            fileinfo = identify.fileinfo(local_path)

            if sha256 is not None and fileinfo['sha256'] != sha256:
                raise CorruptedFileStoreException('SHA256 mismatch between received '
                                                  'and calculated sha256. %s != %s' % (sha256, fileinfo['sha256']))
            self.datastore.save_or_freshen_file(fileinfo['sha256'], fileinfo, expiry, classification, redis=self.redis)

            # Check if there is an integrated decode process for this file
            # eg. files that are packaged, and the contained file (not the package
            # that local_path points to) should be passed into the system.
            decode_file = load_module_by_path(self.config.submission.decode_file)
            massaged_path, _, fileinfo, al_meta = decode_file(local_path, fileinfo)

            if massaged_path:
                local_path = massaged_path
                sha256 = fileinfo['sha256']
                self.filestore.upload(local_path, sha256)
                self.datastore.save_or_freshen_file(sha256, fileinfo, expiry, classification, redis=self.redis)

            return fileinfo['sha256'], fileinfo['size'], al_meta

        finally:
            # If we extracted anything delete it
            if massaged_path:
                if os.path.exists(massaged_path):
                    os.unlink(massaged_path)

            # If we DIDN'T download anything, still delete it
            if local_path and cleanup:
                if os.path.exists(local_path):
                    os.unlink(local_path)

    # TODO this would be nice to have
    # def submit_extracted(self, task: Task, result: Result, local_files: List[str], cleanup=True):
    #     """Submit several files in a single submission.
    #
    #     After this method runs, there should be no local copies of the file left.
    #     """
    #     try:
    #         max_size = self.config.submission.max_file_size
    #         expiry = now_as_iso(submission_obj.params.ttl * 60 * 60 * 24)
    #         extracted_ok = []
    #
    #         for local_file, extraction_info in local_files:
    #             # Upload/download, extract, analyze files
    #             classification = extraction_info.classification
    #             file_hash, size, new_metadata = self._ready_file(local_file, expiry, classification, cleanup)
    #             self.filestore.upload(local_file, file_hash)
    #
    #             # Check that after we have resolved exactly what to pass on, that it
    #             # remains a valid target for scanning
    #             if size > max_size and not submission_obj.params.ignore_size:
    #                 self.log.error(f"{task.service_name} has extracted oversize file {size} > {max_size}")
    #                 continue
    #             elif size == 0:
    #                 self.log.warning(f"{task.service_name} has extracted an empty file.")
    #                 continue
    #
    #             if new_metadata:
    #                 self.log.warning(f"{task.service_name} has extracted a file with "
    #                                  f"new metadata, which is being ignored.")
    #
    #             extracted_ok
    #
    #     finally:
    #         # Just in case this method fails clean up local files
    #         if cleanup:
    #             for path in local_files:
    #                 if path and os.path.exists(path):
    #                     try:
    #                         os.unlink(path)
    #                     except:
    #                         self.log.error("Couldn't delete dangling file %s", path)
