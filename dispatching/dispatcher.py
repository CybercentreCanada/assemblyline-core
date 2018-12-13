import time
import json
import uuid
from functools import reduce

from assemblyline.remote.datatypes.queues.named import NamedQueue
from dispatch_hash import DispatchHash
from configuration import ConfigManager, config_hash
from assemblyline import odm
from assemblyline.odm.models.submission import Submission
import watcher


def service_queue_name(service):
    return 'service-queue-'+service


@odm.model()
class FileTask(odm.Model):
    sid = odm.Keyword()
    file_hash = odm.Keyword()
    file_type = odm.Keyword()
    depth = odm.Integer()


@odm.model()
class ServiceTask(odm.Model):
    sid = odm.Keyword()
    file_hash = odm.Keyword()
    file_type = odm.Keyword()
    depth = odm.Integer()
    service_name = odm.Keyword()
    service_config = odm.Keyword()


DISPATCH_QUEUE = 'dispatch-file'
SUBMISSION_QUEUE = 'submission'


class Dispatcher:

    def __init__(self, datastore, redis):
        # Load the datastore collections that we are going to be using
        self.ds = datastore
        self.submissions = datastore.submissions
        self.results = datastore.results
        self.errors = datastore.errors
        self.files = datastore.files

        # Create a config cache that will refresh config values periodically
        self.config = ConfigManager(datastore)

        # Connect to all of our persistant redis structures
        self.redis = redis
        self.submission_queue = NamedQueue(SUBMISSION_QUEUE, *redis)
        self.file_queue = NamedQueue(SUBMISSION_QUEUE, *redis)

    def dispatch_submission(self, submission: Submission):
        """
        Find any files associated with a submission and dispatch them if they are
        not marked as in progress. If all files are finished, finalize the submission.

        Preconditions:
            - File exists in the filestore and file collection in the datastore
            - Submission is stored in the datastore
        """
        # Refresh the watch
        watcher.touch(self.redis, key=submission.sid, timeout=self.config.dispatch_timeout,
                      queue=SUBMISSION_QUEUE, message={'sid': submission.sid})

        # Open up the file/service table for this submission
        process_table = DispatchHash(submission.sid, *self.redis)
        depth_limit = self.config.extraction_depth_limit

        # Try to find all files, and extracted files
        unchecked_files = []
        for file_hash in submission.files:
            file_type = self.files.get(file_hash.sha256).type
            print(dict(
                sid=submission.sid,
                file_hash=file_hash,
                file_type=file_type,
                depth=0
            ))
            unchecked_files.append(FileTask(dict(
                sid=submission.sid,
                file_hash=file_hash,
                file_type=file_type,
                depth=0
            )))
        encountered_files = {file.sha256 for file in submission.files}
        pending_files = {}

        # For each file, we will look through all its results, any exctracted files
        # found
        while unchecked_files:
            task = unchecked_files.pop()
            sha = task.file_hash
            schedule = self.config.build_schedule(submission, file_type)

            for service_name in reduce(lambda a, b: a + b, schedule):
                # If the service is still marked as 'in progress'
                runtime = time.time() - process_table.dispatch_time(sha, service_name)
                if runtime < self.config.service_timeout(service_name):
                    pending_files[sha] = task
                    continue

                # It hasn't started, has timed out, or is finished, see if we have a result
                result_key = process_table.finished(sha, service_name)

                # No result found, mark the file as incomplete
                if not result_key:
                    pending_files[sha] = task
                    continue

                # The process table is marked that a service has been abandoned due to errors
                if result_key == 'errors':
                    continue

                # If we have hit the depth limit, ignore children
                if task.depth >= depth_limit:
                    continue

                # The result should exist then, get all the sub-files
                result = self.results.get(result_key)
                for sub_file in result.extracted_files:
                    if sub_file not in encountered_files:
                        encountered_files.add(sub_file)

                        file_type = self.files.get(file_hash).type
                        unchecked_files.append(FileTask(dict(
                            sid=submission.sid,
                            file_hash=sub_file,
                            file_type=file_type,
                            depth=task.depth + 1
                        )))

        # If there are pending files, then at least one service, on at least one
        # file isn't done yet, poke those files
        if pending_files:
            for task in pending_files.values():
                self.file_queue.push(task)
        else:
            finalize_submission()

    def dispatch_file(self, task: FileTask):
        """ Handle a message describing a file to be processed.

        This file may be:
            - A new submission or extracted file.
            - A file that has just completed a stage of processing.
            - A file that has not completed a a stage of processing, but this
              call has been triggered by a timeout or similar.

        If the file is totally new, we will setup a dispatch table, and fill it in.

        Once we make/load a dispatch table, we will dispatch whichever group the table
        shows us hasn't been completed yet.

        When we dispatch to a service, we check if the task is already in the dispatch
        queue. If it isn't proceed normally. If it is, check that the service is still online.
        """
        # Read the message content
        file_hash = task.file_hash
        submission = self.submissions.get(task.sid)

        # Refresh the watch on the submission, we are still working on it
        watcher.touch(self.redis, key=task.sid, timeout=self.config.dispatch_timeout,
                      queue=SUBMISSION_QUEUE, message={'sid': task.sid})

        # Open up the file/service table for this submission
        process_table = DispatchHash(task.sid, *self.redis)

        # Calculate the schedule for the file
        schedule = self.config.build_schedule(submission, task.file_type)

        # Go through each round of the schedule removing complete/failed services
        # Break when we find a stage that still needs processing
        outstanding = {}
        tasks_remaining = 0
        while schedule and not outstanding:
            stage = schedule.pop(0)

            for service in stage:
                # If the result is in the process table we are fine
                if process_table.finished(file_hash, service):
                    continue

                # Check if something, an error/a result already exists, to resolve this service
                config = self.config.build_service_config(service, submission)
                access_key = self._find_results(submission, file_hash, service, config)
                if access_key:
                    tasks_remaining = process_table.finish(file_hash, service, access_key)
                    continue

                outstanding[service] = config

        # Try to retry/dispatch any outstanding services
        if outstanding:
            for service, config in outstanding.items():
                # Check if this submission has already dispatched this service, and hasn't timed out yet
                if time.time() - process_table.dispatch_time(file_hash, service) < self.config.service_timeout(service):
                    continue

                # Build the actual service dispatch message
                service_task = ServiceTask(dict(
                    service_name=service,
                    service_config=json.dumps(config),
                    **task.as_primitives()
                ))

                queue = NamedQueue(service_queue_name(service), *self.redis)
                queue.push(service_task.as_primitives())
                process_table.dispatch(file_hash, service)

        else:
            # There are no outstanding services, this file is done

            # If there are no outstanding ANYTHING for this submission,
            # send a message to the submission dispatcher to finalize
            if process_table.all_finished() and tasks_remaining == 0:
                self.submission_queue.push({'sid': submission.sid})

    def _find_results(self, sid, file_hash, service, config):
        """
        Try to find any results or terminal errors that satisfy the
        request to run `service` on `file_hash` for the configuration in `submission`
        """
        # Look for results that match this submission/hash/service config
        key = self.config.build_result_key(file_hash, service, config_hash(config))
        if self.results.exists(key):
            # TODO Touch result expiry
            return key

        # NOTE these searches can be parallel
        # NOTE these searches need to be changed to match whatever the error log is set to
        # Search the errors for one matching this submission/hash/service
        # that also has a terminal flag
        results = self.errors.search(f"sid:{sid} AND file_hash:{file_hash} AND service:{service} "
                                     "AND catagory:'terminal'", rows=1, fl=[self.ds.ID])
        for result in results['items']:
            return result.id

        # Count the crash or timeout errors for submission/hash/service
        results = self.errors.search(f"sid:{sid} AND file_hash:{file_hash} AND service:{service} "
                                     "AND (catagory:'timeout' OR catagory:'crash')", rows=0)
        if results['total'] > self.config.service_failure_limit(service):
            return 'errors'

        # No reasons not to continue processing this file
        return False

    def service_failed(self, task: ServiceTask, error=None):
        # Add an error to the datastore
        if error:
            self.errors.save(uuid.guid4().hex, error)
        else:
            self.errors.save(uuid.guid4().hex, create_generic_error(task))

        # Mark the attempt to process the file over in the dispatch table
        process_table = DispatchHash(task.sid, *self.redis)
        process_table.fail_dispatch(task.file_hash, task.service_name)

        # Send a message to prompt the re-issue of the task if needed
        self.file_queue.push(FileTask(dict(
            sid=task.sid,
            file_hash=task.file_hash,
            file_type=task.file_type,
            depth=task.depth,
        )))
