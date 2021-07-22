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
import os
from typing import List, Tuple

from assemblyline.common import forge, identify
from assemblyline.common.codec import decode_file
from assemblyline.common.dict_utils import flatten
from assemblyline.common.isotime import now_as_iso, epoch_to_iso
from assemblyline.common.str_utils import safe_str
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.filestore import FileStore
from assemblyline.odm.messages.submission import Submission as SubmissionObject
from assemblyline.odm.models.submission import File, Submission
from assemblyline_core.dispatching.client import DispatchClient

Classification = forge.get_classification()


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

    def __init__(self, datastore: AssemblylineDatastore = None, filestore: FileStore = None,
                 config=None, redis=None):
        self.log = logging.getLogger('assemblyline.submission_client')
        self.config = config or forge.CachedObject(forge.get_config)
        self.datastore = datastore or forge.get_datastore(self.config)
        self.filestore = filestore or forge.get_filestore(self.config)
        self.redis = redis

        # A client for interacting with the dispatcher
        self.dispatcher = DispatchClient(datastore, redis)

    def submit(self, submission_obj: SubmissionObject, local_files: List = None, completed_queue=None):
        """Submit several files in a single submission.

        After this method runs, there should be no local copies of the file left.
        """
        if local_files is None:
            local_files = []

        if len(submission_obj.files) == 0 and len(local_files) == 0:
            raise SubmissionException("No files found to submit...")

        if submission_obj.params.ttl:
            expiry = epoch_to_iso(submission_obj.time.timestamp() + submission_obj.params.ttl * 24 * 60 * 60)
        else:
            expiry = None
        max_size = self.config.submission.max_file_size

        for local_file in local_files:
            # Upload/download, extract, analyze files
            file_hash, size, new_metadata = self._ready_file(local_file, expiry,
                                                             str(submission_obj.params.classification))
            new_name = new_metadata.pop('name', safe_str(os.path.basename(local_file)))
            submission_obj.params.classification = new_metadata.pop('classification',
                                                                    submission_obj.params.classification)
            submission_obj.metadata.update(**flatten(new_metadata))

            # Check that after we have resolved exactly what to pass on, that it
            # remains a valid target for scanning
            if size > max_size and not submission_obj.params.ignore_size:
                msg = "File too large (%d > %d). Submission failed" % (size, max_size)
                raise SubmissionException(msg)
            elif size == 0:
                msg = "File empty. Submission failed"
                raise SubmissionException(msg)

            submission_obj.files.append(File({
                'name': new_name,
                'size': size,
                'sha256': file_hash,
            }))

        # Clearing runtime_excluded on initial submit or resubmit
        submission_obj.params.services.runtime_excluded = []

        # We should now have all the information we need to construct a submission object
        sub = Submission(dict(
            archive_ts=now_as_iso(self.config.datastore.ilm.days_until_archive * 24 * 60 * 60),
            classification=submission_obj.params.classification,
            error_count=0,
            errors=[],
            expiry_ts=expiry,
            file_count=len(submission_obj.files),
            files=submission_obj.files,
            max_score=0,
            metadata=submission_obj.metadata,
            params=submission_obj.params,
            results=[],
            sid=submission_obj.sid,
            state='submitted',
            scan_key=submission_obj.scan_key,
        ))

        if self.config.ui.allow_malicious_hinting and submission_obj.params.malicious:
            sub.verdict = {"malicious": [submission_obj.params.submitter]}

        self.datastore.submission.save(sub.sid, sub)

        self.log.debug("Submission complete. Dispatching: %s", sub.sid)
        self.dispatcher.dispatch_submission(sub, completed_queue=completed_queue)

        return sub

    def _ready_file(self, local_path: str, expiry, classification) -> Tuple[str, int, dict]:
        """Take a file from local storage and prepare it for submission.

        After this method finished the file will ONLY exist on the filestore, not locally.
        """
        extracted_path = None
        try:
            # Analyze the file and make sure the file table is up to date
            fileinfo = identify.fileinfo(local_path)

            if fileinfo['size'] == 0:
                raise SubmissionException("File empty. Submission failed")

            # Check if there is an integrated decode process for this file
            # eg. files that are packaged, and the contained file (not the package
            # that local_path points to) should be passed into the system.
            extracted_path, fileinfo, al_meta = decode_file(local_path, fileinfo)
            al_meta['classification'] = al_meta.get('classification', classification)

            if extracted_path:
                local_path = extracted_path

            self.filestore.upload(local_path, fileinfo['sha256'])
            self.datastore.save_or_freshen_file(fileinfo['sha256'], fileinfo, expiry,
                                                al_meta['classification'], redis=self.redis)
            return fileinfo['sha256'], fileinfo['size'], al_meta

        finally:
            # If we extracted anything delete it
            if extracted_path:
                if os.path.exists(extracted_path):
                    os.unlink(extracted_path)
