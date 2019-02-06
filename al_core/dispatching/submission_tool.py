""" File Submission Interfaces.

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
import uuid
# import pprint
# import time
from datetime import datetime, timedelta

from assemblyline.common import forge
from assemblyline.common import identify
from assemblyline.common.importing import load_module_by_path
from assemblyline.common.str_utils import safe_str
from assemblyline.odm.models.submission import Submission, SubmissionParams
from assemblyline.filestore import CorruptedFileStoreException
from assemblyline.remote.datatypes.lock import Lock

from .client import DispatchClient


def assert_valid_sha256(sha256):
    if len(sha256) != 64:
        raise ValueError('Invalid SHA256: %s' % sha256)


class SubmissionException(Exception):
    pass


class SubmissionTool:
    """A helper class to simplify submitting files from internal or external sources.

    This tool helps take care of tests and interactions between the filestore,
    datastore, dispatcher, and any sources of files to be processed.
    """

    def __init__(self, datastore, transport, redis):
        self.log = logging.getLogger('assemblyline.submission')
        self.config = forge.CachedObject(forge.get_config)
        self.datastore = datastore
        self.transport = transport or forge.get_filestore(self.config)
        self.classification_engine = forge.get_classification()

        # A client for interacting with the dispatcher
        self.dispatcher = DispatchClient(datastore, redis)

    def presubmit(self):
        raise NotImplementedError()

    def submit(self, sha256: str, path: str, metadata: dict, params: SubmissionParams):
        """Execute a submit on a single file we already have in storage."""
        assert_valid_sha256(sha256)

        classification = params.classification
        expiry = datetime.utcnow() + timedelta(days=params.ttl)

        # By the time submit is called, either the file was in our cache
        # and we freshed its ttl or the client has successfully transfered
        # the file to us.
        local_path = self.transport.local_path(sha256)

        if not self.transport.exists(sha256):
            raise SubmissionException(f'File specified is not on server: {sha256} {self.transport}.')

        temporary_path = massaged_path = None
        try:
            # If we don't have the file locally, download it
            if not local_path:
                fd, temporary_path = tempfile.mkstemp(prefix="submission.submit")
                os.close(fd)  # We don't need the file descriptor open
                self.transport.download(sha256, temporary_path)
                local_path = temporary_path

            # Analize the file and make sure the datastore is up to date
            fileinfo = identify.fileinfo(local_path)
            if fileinfo['sha256'] != sha256:
                raise CorruptedFileStoreException('SHA256 mismatch between received '
                                                  'and calculated sha256. %s != %s' % (sha256, fileinfo['sha256']))
            self.save_or_freshen_file(sha256, fileinfo, expiry, classification)

            # Check if there is an integrated decode process for this file
            # eg. files that are packaged, and the contained file (not the package
            # that local_path points to) should be passed into the system.
            decode_file = load_module_by_path(self.config.submission.decode_file)
            massaged_path, _, fileinfo, al_meta = decode_file(local_path, fileinfo)
            metadata.update(al_meta)

            if massaged_path:
                local_path = massaged_path
                sha256 = fileinfo['sha256']
                self.transport.put(local_path, sha256)
                self.save_or_freshen_file(sha256, fileinfo, expiry, classification)

            # Check that after we have resolved exactly what to pass on, that it
            # remains a valid target for scanning
            max_size = self.config.submission.max_file_size
            if fileinfo['size'] > max_size and not params.ignore_size:
                msg = "File too large (%d > %d). Submission failed" % (fileinfo['size'], max_size)
                raise SubmissionException(msg)
            elif fileinfo['size'] == 0:
                msg = "File empty. Submission failed"
                raise SubmissionException(msg)

            # We should now have all the information we need to construct a submission object
            sub = Submission(dict(
                classification=classification,
                error_count=0,
                errors=[],
                expiry_ts=datetime.utcnow() + timedelta(days=params.ttl),
                file_count=1,
                files=[dict(
                    name=safe_str(path),
                    sha256=sha256
                )],
                max_score=0,
                metadata=metadata,
                params=params.as_primitives(),
                results=[],
                sid=str(uuid.uuid4()),
                state='submitted',
                times={'submitted': datetime.utcnow()}
            ))

            if sub.is_initial():
                self.datastore.save(sub.sid, sub)

            self.log.debug("Submission complete. Dispatching: %s", sub.json())

            self.dispatcher.dispatch_submission(sub)
            return sub
        finally:
            if massaged_path:
                if os.path.exists(massaged_path):
                    os.unlink(massaged_path)

            if temporary_path:
                if os.path.exists(temporary_path):
                    os.unlink(temporary_path)

    def save_or_freshen_file(self, sha256, fileinfo, expiry, classification):

        with Lock(f'save-or-freshen-file-{sha256}', 5):
            current_fileinfo = self.datastore.file.get(sha256).as_primitives() or {}

            # Remove control fields from file info and update current file info
            for x in ['classification', 'expiry', 'seen']:
                fileinfo.pop(x, None)
            current_fileinfo.update(fileinfo)

            # Update expiry time
            current_fileinfo['expiry'] = max(current_fileinfo.get('expiry', expiry), expiry)

            # Update seen counters
            now = datetime.utcnow()
            current_fileinfo['seen'] = seen = current_fileinfo.get('seen', {})
            seen['count'] = seen.get('count', 0) + 1
            seen['last'] = now
            seen['first'] = seen.get('first', now)

            # Update Classification
            classification = self.classification_engine.min_classification(
                current_fileinfo.get('classification', classification),
                classification
            )
            current_fileinfo['classification'] = classification
            parts = self.classification_engine.get_access_control_parts(classification)
            current_fileinfo.update(parts)

            self.datastore.files.save(sha256, current_fileinfo)

    def submit_existing(self):
        raise NotImplementedError()

#
# from assemblyline.al.common import forge
# from assemblyline.al.common.task import Task
# from assemblyline.al.common.remote_datatypes import ExpiringHash
# from assemblyline.al.core.filestore import CorruptedFileStoreException
# from assemblyline.common.charset import safe_str
# from assemblyline.common.isotime import now_as_iso
#
#
# SUBMISSION_AUTH = (safe_str(config.submissions.user), safe_str(config.submissions.password))
# SHARDS = config.core.dispatcher.shards
# VERIFY = config.submissions.get('verify', False)
#
#
#
# def assert_valid_file(path):
#     if not os.path.exists(path):
#         raise Exception('File does not exist: %s' % path)
#     if os.path.isdir(path):
#         raise Exception('Expected file. Found directory: %s' % path)
#
#
#
#
# def effective_ttl(settings):
#     return settings.get('ttl', config.submissions.ttl)
#
#
# def max_extracted(settings):
#     return settings.get('max_extracted', config.services.limits.max_extracted)
#
#
# def max_supplementary(settings):
#     return settings.get('max_supplementary', config.services.limits.max_supplementary)
#
#
# def ttl_to_expiry(ttl):
#     return now_as_iso(int(ttl) * 24 * 60 * 60)
#
#
# class SubmissionWrapper(object):
#
#     @classmethod
#     def check_exists(cls, transport, sha256_list):
#         log.debug("CHECK EXISTS (): %s", sha256_list)
#         existing = []
#         missing = []
#         for sha256 in sha256_list:
#             if not transport.exists(sha256):
#                 missing.append(sha256)
#             else:
#                 existing.append(sha256)
#         return {'existing': existing, 'missing': missing}
#
#     # noinspection PyBroadException
#     @classmethod
#     def identify(cls, transport, storage, sha256, **kw):
#         """ Identify a file. """
#         assert_valid_sha256(sha256)
#
#         classification = kw['classification']
#
#         kw['ttl'] = ttl = effective_ttl(kw)
#         kw['__expiry_ts__'] = expiry = ttl_to_expiry(ttl)
#
#         # By the time identify is called, either the file was in our cache
#         # and we freshed its ttl or the client has successfully transfered
#         # the file to us.
#         local_path = transport.local_path(sha256)
#         if not local_path:
#             path = kw.get("path", None)
#             if path and os.path.exists(path):
#                 local_path = path
#
#         if not transport.exists(sha256):
#             log.warning('File specified is not on server: %s %s.',
#                         sha256, str(transport))
#             return None
#
#         temporary_path = None
#         try:
#             if not local_path:
#                 fd, temporary_path = tempfile.mkstemp(prefix="submission.identify")
#                 os.close(fd)  # We don't need the file descriptor open
#                 transport.download(sha256, temporary_path)
#                 local_path = temporary_path
#
#             fileinfo = identify.fileinfo(local_path)
#
#             storage.save_or_freshen_file(sha256, fileinfo, expiry, classification)
#         finally:
#             if temporary_path:
#                 if os.path.exists(temporary_path):
#                     os.unlink(temporary_path)
#
#         return fileinfo
#
#     @classmethod
#     def presubmit(cls, transport, sha256, **kw):
#         """ Execute a presubmit.
#
#             Checks if this file is already cached.
#             If not, it returns a location for the client to copy the file.
#
#
#             result dictionary example:
#             { 'exists': False,
#               'sha256': u'012345678....9876543210',
#               'upload_path': u'/home/aluser/012345678....9876543210'
#             }
#
#             """
#         log.debug("PRESUBMIT: %s", sha256)
#         assert_valid_sha256(sha256)
#
#         if transport.exists(sha256):
#             return SubmissionWrapper.result_dict(transport, sha256, True, None, kw)
#
#         # We don't have this file. Tell the client as much and tell it where
#         # to transfer the file before issuing the final submit.
#         log.debug('Cache miss. Client should transfer to %s', sha256)
#         return SubmissionWrapper.result_dict(transport, sha256, False, sha256, kw)
#
#     @classmethod
#     def submit_inline(cls, storage, transport, file_paths, **kw):
#         """ Submit local samples to the submission service.
#
#             submit_inline can be used when the sample to submit is already
#             local to the submission service. It does the presumit, filestore
#             upload and submit.
#
#             Any kw are passed to the Task created to dispatch this submission.
#         """
#         classification = kw['classification']
#
#         kw['max_extracted'] = max_extracted(kw)
#         kw['max_supplementary'] = max_supplementary(kw)
#         kw['ttl'] = ttl = effective_ttl(kw)
#         kw['__expiry_ts__'] = expiry = ttl_to_expiry(ttl)
#
#         submissions = []
#         file_tuples = []
#         dispatch_request = None
#         # Generate static fileinfo data for each file.
#         for file_path in file_paths:
#
#             file_name = os.path.basename(file_path)
#             fileinfo = identify.fileinfo(file_path)
#
#             ignore_size = kw.get('ignore_size', False)
#             max_size = config.submissions.max.size
#             if fileinfo['size'] > max_size and not ignore_size:
#                 msg = "File too large (%d > %d). Submission Failed" % \
#                       (fileinfo['size'], max_size)
#                 raise SubmissionException(msg)
#             elif fileinfo['size'] == 0:
#                 msg = "File empty. Submission Failed"
#                 raise SubmissionException(msg)
#
#             decode_file = forge.get_decode_file()
#             temp_path, original_name, fileinfo, al_meta = \
#                 decode_file(file_path, fileinfo)
#
#             if temp_path:
#                 file_path = temp_path
#                 if not original_name:
#                     original_name = os.path.splitext(file_name)[0]
#                 file_name = original_name
#
#             sha256 = fileinfo['sha256']
#
#             storage.save_or_freshen_file(sha256, fileinfo, expiry, classification)
#
#             file_tuples.append((file_name, sha256))
#
#             if not transport.exists(sha256):
#                 log.debug('File not on remote filestore. Uploading %s', sha256)
#                 transport.put(file_path, sha256, location='near')
#
#             if temp_path:
#                 os.remove(temp_path)
#
#             # We'll just merge the mandatory arguments, fileinfo, and any
#             # optional kw and pass those all on to the dispatch callback.
#             task_args = fileinfo
#             task_args['priority'] = 0  # Just a default.
#             task_args.update(kw)
#             task_args['srl'] = sha256
#             task_args['original_filename'] = file_name
#             task_args['path'] = file_name
#
#             if 'metadata' in task_args:
#                 task_args['metadata'].update(al_meta)
#             else:
#                 task_args['metadata'] = al_meta
#
#             dispatch_request = Task.create(**task_args)
#             submissions.append(dispatch_request)
#
#         storage.create_submission(
#             dispatch_request.sid,
#             dispatch_request.as_submission_record(),
#             file_tuples)
#
#         dispatch_queue = forge.get_dispatch_queue()
#         for submission in submissions:
#             dispatch_queue.submit(submission)
#
#         log.debug("Submission complete. Dispatched: %s", dispatch_request)
#
#         # Ugly - fighting with task to give UI something that makes sense.
#         file_result_tuples = \
#             zip(file_paths, [dispatch_request.raw for dispatch_request in submissions])
#         result = submissions[0].raw.copy()
#         fileinfos = []
#         for filename, result in file_result_tuples:
#             finfo = result['fileinfo']
#             finfo['original_filename'] = os.path.basename(filename)
#             finfo['path'] = finfo['original_filename']
#             fileinfos.append(finfo)
#         result['fileinfo'] = fileinfos
#         return result
#
#     # noinspection PyBroadException
#     @classmethod
#     def submit_multi(cls, storage, transport, files, **kw):
#         """ Submit all files into one submission
#
#             submit_multi can be used when all the files are already present in the
#             file storage.
#
#             files is an array of (name, sha256) tuples
#
#             Any kw are passed to the Task created to dispatch this submission.
#         """
#         sid = str(uuid.uuid4())
#         classification = kw['classification']
#
#         kw['max_extracted'] = max_extracted(kw)
#         kw['max_supplementary'] = max_supplementary(kw)
#         kw['ttl'] = ttl = effective_ttl(kw)
#         kw['__expiry_ts__'] = expiry = ttl_to_expiry(ttl)
#
#         submissions = []
#         temporary_path = None
#         dispatch_request = None
#         # Generate static fileinfo data for each file.
#         for name, sha256 in files:
#             local_path = transport.local_path(sha256)
#
#             if not transport.exists(sha256):
#                 raise SubmissionException('File specified is not on server: %s %s.' % (sha256, str(transport)))
#
#             try:
#                 if not local_path:
#                     fd, temporary_path = tempfile.mkstemp(prefix="submission.submit_multi")
#                     os.close(fd)  # We don't need the file descriptor open
#                     transport.download(sha256, temporary_path)
#                     local_path = temporary_path
#
#                 fileinfo = identify.fileinfo(local_path)
#                 storage.save_or_freshen_file(sha256, fileinfo, expiry, classification)
#
#                 decode_file = forge.get_decode_file()
#                 massaged_path, new_name, fileinfo, al_meta = \
#                     decode_file(local_path, fileinfo)
#
#                 if massaged_path:
#                     name = new_name
#                     local_path = massaged_path
#                     sha256 = fileinfo['sha256']
#
#                     if not transport.exists(sha256):
#                         transport.put(local_path, sha256)
#                     storage.save_or_freshen_file(sha256, fileinfo, expiry, classification)
#
#                 ignore_size = kw.get('ignore_size', False)
#                 max_size = config.submissions.max.size
#                 if fileinfo['size'] > max_size and not ignore_size:
#                     msg = "File too large (%d > %d). Submission failed" % (fileinfo['size'], max_size)
#                     raise SubmissionException(msg)
#                 elif fileinfo['size'] == 0:
#                     msg = "File empty. Submission failed"
#                     raise SubmissionException(msg)
#
#                 # We'll just merge the mandatory arguments, fileinfo, and any
#                 # optional kw and pass those all on to the dispatch callback.
#                 task_args = fileinfo
#                 task_args['priority'] = 0  # Just a default.
#                 task_args.update(kw)
#                 task_args['srl'] = sha256
#                 task_args['original_filename'] = name
#                 task_args['sid'] = sid
#                 task_args['path'] = name
#
#                 if 'metadata' in task_args:
#                     task_args['metadata'].update(al_meta)
#                 else:
#                     task_args['metadata'] = al_meta
#
#                 dispatch_request = Task.create(**task_args)
#                 submissions.append(dispatch_request)
#             finally:
#                 if temporary_path:
#                     if os.path.exists(temporary_path):
#                         os.unlink(temporary_path)
#
#         storage.create_submission(
#             dispatch_request.sid,
#             dispatch_request.as_submission_record(),
#             files)
#
#         dispatch_queue = forge.get_dispatch_queue()
#         for submission in submissions:
#             dispatch_queue.submit(submission)
#
#         log.debug("Submission complete. Dispatched: %s", dispatch_request)
#         return submissions[0].raw.copy()
#
#     @classmethod
#     def watch(cls, sid, watch_queue):
#         t = Task.watch(**{
#             'priority': config.submissions.max.priority,
#             'sid': sid,
#             'watch_queue': watch_queue,
#         })
#         n = forge.determine_dispatcher(sid)
#         forge.get_control_queue('control-queue-' + str(n)).push(t.raw)
#
#     @classmethod
#     def result_dict(cls, transport, sha256, exists, upload_path, kw):
#         return {
#             'exists': exists,
#             'upload_path': upload_path,
#             'filestore': str(transport),
#             'sha256': sha256,
#             'kwargs': kw,
#         }
#
#
# class SubmissionService(object):
#     def __init__(self):
#         self.storage = forge.get_datastore()
#         self.transport = forge.get_filestore()
#
#         log.info("Submission service instantiated. Transport::{0}".format(
#             self.transport))
#
#     def check_exists(self, sha256_list):
#         return SubmissionWrapper.check_exists(self.transport, sha256_list)
#
#     def identify(self, sha256, **kw):
#         return SubmissionWrapper.identify(self.transport, self.storage, sha256, **kw)
#
#     def presubmit(self, sha256, **kw):
#         return SubmissionWrapper.presubmit(self.transport, sha256, **kw)
#
#     def submit(self, sha256, path, priority, submitter, **kw):
#         return SubmissionWrapper.submit(self.transport, self.storage, sha256, path, priority, submitter, **kw)
#
#     def submit_inline(self, file_paths, **kw):
#         return SubmissionWrapper.submit_inline(self.storage, self.transport, file_paths, **kw)
#
#     def submit_multi(self, files, **kw):
#         return SubmissionWrapper.submit_multi(self.storage, self.transport, files, **kw)
#
#     @classmethod
#     def watch(cls, sid, watch_queue):
#         return SubmissionWrapper.watch(sid, watch_queue)
#
#     def result_dict(self, sha256, exists, upload_path, kw):
#         # noinspection PyProtectedMember
#         return SubmissionWrapper.result_dict(self.transport, sha256, exists, upload_path, kw)
#
#
# class SubmissionClient(object):
#     # noinspection PyArgumentList
#     def __init__(self, server_url=None, datastore=None):
#         if not server_url:
#             server_url = config.submissions.url
#
#         self.server_url = server_url
#
#         self.datastore = datastore
#         self.is_unix = os.name == "posix"
#         if not self.is_unix:
#             from assemblyline_client import Client
#             try:
#                 # AL_Client 3.4+
#                 self.client = Client(self.server_url, auth=SUBMISSION_AUTH, verify=VERIFY)
#             except TypeError:
#                 # AL_Client 3.3-
#                 self.client = Client(self.server_url, auth=SUBMISSION_AUTH)
#         elif self.datastore is None:
#             self.datastore = forge.get_datastore()
#
#     def check_srls(self, srl_list):
#         if self.is_unix:
#             return self._check_srls_unix(srl_list)
#         else:
#             return self._check_srls_windows(srl_list)
#
#     def _check_srls_unix(self, srl_list):
#         if not srl_list:
#             return True
#         with forge.get_filestore() as f_transport:
#             result = SubmissionWrapper.check_exists(f_transport, srl_list)
#         return len(result.get('existing', [])) == len(srl_list)
#
#     def _check_srls_windows(self, srl_list):
#         if not srl_list:
#             return True
#         result = self.client.submit.checkexists(*srl_list)
#         return len(result.get('existing', [])) == len(srl_list)
#
#     def identify_supplementary(self, rd, **kw):
#         # Pass along all parameters as query arguments.
#         submits = {k: dict(kw.items() + v.items()) for k, v in rd.iteritems()}
#         if self.is_unix:
#             return self._identify_supplementary_unix(submits)
#         else:
#             return self._identify_supplementary_windows(submits)
#
#     def _identify_supplementary_unix(self, submits):
#         submit_results = {}
#         with forge.get_filestore() as f_transport:
#             for key, submit in submits.iteritems():
#                 file_info = SubmissionWrapper.identify(f_transport, self.datastore, **submit)
#                 if file_info:
#                     submit_result = {"status": "succeeded", "fileinfo": file_info}
#                 else:
#                     submit_result = {"status": "failed", "fileinfo": {}}
#                 submit_results[key] = submit_result
#         return submit_results
#
#     def _identify_supplementary_windows(self, submits):
#         return self.client.submit.identify(submits)
#
#     def presubmit_local_files(self, file_paths, **kw):
#         default_error = {'succeeded': False, 'error': 'Unknown Error'}
#         presubmit_requests = {}
#         presubmit_results = {}
#
#         ignore_size = kw.get('ignore_size', False)
#         max_size = config.submissions.max.size
#
#         # Prepare the batch presubmit.
#         rid_map = {}
#         for rid, local_path in enumerate(file_paths):
#             rid = str(rid)
#             rid_map[rid] = local_path
#             try:
#                 assert_valid_file(local_path)
#                 d = digests.get_digests_for_file(local_path,
#                                                  calculate_entropy=False)
#                 if d['size'] > max_size and not ignore_size:
#                     presubmit_results[rid] = {
#                         'succeeded': False,
#                         'error': 'file too large (%d > %d). Skipping' % (d['size'], max_size),
#                     }
#                     continue
#                 elif d['size'] == 0:
#                     presubmit_results[rid] = {
#                         'succeeded': False,
#                         'error': 'file is empty. Skipping',
#                     }
#                     continue
#                 presubmit_requests[rid] = d
#                 # Set a default error. Overwritten on success.
#                 presubmit_results[rid] = default_error.copy()
#             except Exception as ex:  # pylint: disable=W0703
#                 log.error("Exception processing local file: %s. Skipping", ex)
#                 presubmit_results[rid] = {
#                     'succeeded': False,
#                     'error': 'local failure before presubmit: {0}'.format(ex),
#                 }
#                 continue
#
#         if self.is_unix:
#             presubmit_results = self._presubmit_local_files_unix(presubmit_requests, presubmit_results)
#         else:
#             presubmit_results = self._presubmit_local_files_windows(presubmit_requests, presubmit_results)
#
#         if len(presubmit_results) != len(file_paths):
#             log.error('Problem submitting %s: %s',
#                       pprint.pformat(file_paths),
#                       pprint.pformat(presubmit_results))
#
#         # noinspection PyUnresolvedReferences
#         for rid, result in presubmit_results.iteritems():
#             result['path'] = rid_map[rid]
#
#         return presubmit_results
#
#     def _presubmit_local_files_unix(self, presubmit_requests, presubmit_results):
#         with forge.get_filestore() as f_transport:
#             for key, presubmit in presubmit_requests.iteritems():
#                 succeeded = True
#                 presubmit_result = {}
#                 try:
#                     presubmit_result = SubmissionWrapper.presubmit(f_transport, **presubmit)
#                 except Exception as e:  # pylint: disable=W0703
#                     succeeded = False
#                     msg = 'Failed to presubmit for {0}:{1}'.format(key, e)
#                     presubmit_result['error'] = msg
#                 presubmit_result['succeeded'] = succeeded
#                 presubmit_results[key] = presubmit_result
#         return presubmit_results
#
#     def _presubmit_local_files_windows(self, presubmit_requests, presubmit_results):
#         presubmit_results.update(self.client.submit.presubmit(presubmit_requests))
#
#         return presubmit_results
#
#     def submit_local_files(self, file_requests, **kw):
#         results = {}
#
#         file_paths = [
#             file_requests[k]['path'] for k in sorted(file_requests.keys(), key=int)
#         ]
#
#         successful, errors = \
#             self.transfer_local_files(file_paths, location='near', **kw)
#
#         for k in successful.keys():
#             req = file_requests.get(k, {})
#             display_name = req.pop('display_name')
#             req['path'] = display_name
#             ret = successful[k]
#             ret.update(req)
#
#             # This prevents a badly written service to resubmit the file originally submitted
#             if successful[k].get('sha256', None) == kw.get('psrl', None):
#                 path = successful[k]['path']
#                 errors[k] = {
#                     'succeeded': False,
#                     'path': path,
#                     'error': "File submission was aborted for file '%s' because it the same as its parent." % path
#                 }
#                 log.warning("Service is trying to submit the parent file as an extracted file.")
#                 del successful[k]
#             elif req.get('submission_tag') is not None:
#                 # Save off any submission tags
#                 st_name = "st/%s/%s" % (kw.get('psrl', None), successful[k].get('sha256', None))
#                 eh = ExpiringHash(st_name, ttl=7200)
#                 for st_name, st_val in req['submission_tag'].iteritems():
#                     eh.add(st_name, st_val)
#
#         # Send the submit requests.
#         if successful:
#             results = self.submit_requests(successful, **kw)
#         else:
#             log.warn('Nothing to submit after presubmission processing.')
#
#         results.update(errors)
#
#         return results
#
#     def submit_requests(self, rd, **kw):
#         # Pass along all parameters as query arguments.
#         submits = {k: dict(kw.items() + v.items()) for k, v in rd.iteritems()}
#         if self.is_unix:
#             return self._submit_requests_unix(submits)
#         else:
#             return self._submit_requests_windows(submits)
#
#     def _submit_requests_unix(self, submits):
#         submit_results = {}
#         with forge.get_filestore() as f_transport:
#             for key, submit in submits.iteritems():
#                 path = submit.get('path', './path/missing')
#                 if 'description' not in submit:
#                     submit['description'] = "Inspection of file: %s" % path
#                 submit_result = SubmissionWrapper.submit(f_transport, self.datastore, **submit)
#                 submit_results[key] = submit_result
#         return submit_results
#
#     def _submit_requests_windows(self, submits):
#         return self.client.submit.start(submits)
#
#     def submit_supplementary_files(self, file_requests, location='far', **kw):
#         results = {}
#
#         file_paths = [
#             file_requests[k]['path'] for k in sorted(file_requests.keys(), key=int)
#         ]
#
#         successful, errors = \
#             self.transfer_local_files(file_paths, location=location, **kw)
#
#         for k in successful.keys():
#             req = file_requests.get(k, {})
#             ret = successful[k]
#             ret.update(req)
#
#             # This prevents a badly written service to resubmit the file originally submitted
#             if successful[k].get('sha256', None) == kw.get('psrl', None):
#                 path = successful[k]['path']
#                 errors[k] = {
#                     'succeeded': False,
#                     'path': path,
#                     'error': "File submission was aborted for file '%s' because it the same as its parent." % path
#                 }
#                 log.warning("Service is trying to submit the parent file as a supplementary file.")
#                 del successful[k]
#
#         # Send the submit requests.
#         if successful:
#             results = self.identify_supplementary(successful, **kw)
#         else:
#             log.warn('Nothing to submit after presubmission processing.')
#
#         results.update(errors)
#
#         return results
#
#     def transfer_local_files(self, file_paths, location='all', **kw):
#         errors = {}
#         successful = {}
#
#         transfer_requests = self.presubmit_local_files(file_paths, **kw)
#
#         delete = []
#         # noinspection PyUnresolvedReferences
#         for rid, result in transfer_requests.iteritems():
#             key = result['path']
#             if key not in file_paths:
#                 log.error("Unexpected presubmit result for %s.", key)
#                 delete.append(key)
#                 continue
#
#             if not result['succeeded']:
#                 error = result.get('error', 'Unknown Error')
#                 if "too large" in error or "empty" in error:
#                     log.info('skipping failed presubmit for %s - %s', key, result)
#                 else:
#                     log.warn('skipping failed presubmit for %s - %s', key, result)
#                 errors[rid] = {
#                     'succeeded': False,
#                     'path': safe_str(key),
#                     'error': 'Presubmit failed: {0}'.format(error),
#                 }
#                 continue
#
#         for rid in delete:
#             # noinspection PyUnresolvedReferences
#             del transfer_requests[rid]
#
#         # Process presubmit results. Start building the submit requests. Keep
#         # note of all files we need to transfer to server.
#         files_to_transfer = []
#         # noinspection PyUnresolvedReferences
#         with forge.get_filestore() as f_transport:
#             for rid, result in transfer_requests.iteritems():
#                 key = result['path']
#                 # If the file doesn't exist in filestore, let the client know they
#                 # need to submit
#                 if not result.get('succeeded', True):
#                     continue
#                 elif not result.get('exists'):
#                     upload_path = result.get('upload_path')
#                     log.debug('File not on server. Should copy %s -> %s using %s',
#                               key, upload_path, str(f_transport))
#                     files_to_transfer.append((key, upload_path))
#                 else:
#                     log.debug('File is already on server.')
#
#                 # First apply the defaults
#                 successful[rid] = {'path': safe_str(key), 'sha256': result['sha256']}
#
#             # Transfer any files which the server has indicated it doesn't have.
#             if files_to_transfer:
#                 start = time.time()
#                 log.debug("Transfering files %s", str(files_to_transfer))
#                 failed_transfers = \
#                     f_transport.put_batch(files_to_transfer, location=location)
#                 if failed_transfers:
#                     log.error("The following files failed to transfer: %s",
#                               failed_transfers)
#                 end = time.time()
#                 log.debug("Transfered %s in %s.", len(files_to_transfer),
#                           (end - start))
#             else:
#                 log.debug("NO FILES TO TRANSFER.")
#
#         return successful, errors
