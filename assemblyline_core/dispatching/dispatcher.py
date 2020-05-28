import logging
import re
import os
import time
from typing import Dict, List, cast

from assemblyline.common import isotime, forge
from assemblyline.common.constants import SUBMISSION_QUEUE, FILE_QUEUE, DISPATCH_TASK_HASH, \
    DISPATCH_RUNNING_TASK_HASH, get_temporary_submission_data_name, get_tag_set_name, make_watcher_list_name
from assemblyline.common.forge import CachedObject, get_service_queue
from assemblyline.common.metrics import MetricsFactory
from assemblyline.datastore import Collection
from assemblyline.datastore.exceptions import MultiKeyError
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.messages.dispatcher_heartbeat import Metrics
from assemblyline.odm.messages.dispatching import WatchQueueMessage
from assemblyline.odm.models.config import Config
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.service import Service
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.datatypes.hash import Hash, ExpiringHash
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline.remote.datatypes.set import ExpiringSet
from assemblyline_core.dispatching.dispatch_hash import DispatchHash
from assemblyline import odm
from assemblyline.odm.messages.task import FileInfo, Task as ServiceTask
from assemblyline.odm.models.submission import Submission
from assemblyline_core.server_base import get_service_stage_hash, ServiceStage
from assemblyline_core.watcher.client import WatcherClient

# If you are doing development and you want the system to route jobs ignoring the service setup/teardown
# set an environment variable SKIP_SERVICE_SETUP to true for all dispatcher containers
SKIP_SERVICE_SETUP = os.environ.get('SKIP_SERVICE_SETUP', 'false').lower() in ['true', '1']


@odm.model()
class SubmissionTask(odm.Model):
    """Dispatcher internal model for submissions"""
    submission: Submission = odm.Compound(Submission)
    completed_queue = odm.Optional(odm.Keyword())                     # Which queue to notify on completion


@odm.model()
class FileTask(odm.Model):
    """Dispatcher internal model for tracking each file in a submission."""
    sid = odm.Keyword()
    min_classification = odm.Classification()  # Minimum classification of the file being scanned
    parent_hash = odm.Optional(odm.Keyword())
    file_info: FileInfo = odm.Compound(FileInfo)
    depth = odm.Integer()
    max_files = odm.Integer()

    def get_tag_set_name(self) -> str:
        """Get the name of a redis set where the task tags are collected."""
        return get_tag_set_name(self.sid, self.file_info.sha256)

    def get_temporary_submission_data_name(self) -> str:
        """Get the name of a redis hash where tags for a submission are collected."""
        return get_temporary_submission_data_name(self.sid, self.file_info.sha256)


class Scheduler:
    """This object encapsulates building the schedule for a given file type for a submission."""

    def __init__(self, datastore: AssemblylineDatastore, config: Config, redis):
        self.datastore = datastore
        self.config = config
        self.services = cast(Dict[str, Service], CachedObject(self._get_services))
        self.service_stage = get_service_stage_hash(redis)

    def build_schedule(self, submission: Submission, file_type: str) -> List[Dict[str, Service]]:
        all_services = dict(self.services)

        # Load the selected and excluded services by category
        excluded = self.expand_categories(submission.params.services.excluded)
        runtime_excluded = self.expand_categories(submission.params.services.runtime_excluded)
        if not submission.params.services.selected:
            selected = [s for s in all_services.keys()]
        else:
            selected = self.expand_categories(submission.params.services.selected)

        # Add all selected, accepted, and not rejected services to the schedule
        schedule: List[Dict[str, Service]] = [{} for _ in self.config.services.stages]
        services = list(set(selected) - set(excluded) - set(runtime_excluded))
        selected = []
        skipped = []
        for name in services:
            service = all_services.get(name, None)

            if not service:
                skipped.append(name)
                logging.warning(f"Service configuration not found: {name}")
                continue

            accepted = not service.accepts or re.match(service.accepts, file_type)
            rejected = bool(service.rejects) and re.match(service.rejects, file_type)

            if accepted and not rejected:
                schedule[self.stage_index(service.stage)][name] = service
                selected.append(name)
            else:
                skipped.append(name)

        return schedule

    def expand_categories(self, services: List[str]) -> List[str]:
        """Expands the names of service categories found in the list of services.

        Args:
            services (list): List of service category or service names.
        """
        if services is None:
            return []

        services = list(services)
        categories = self.categories()

        found_services = []
        seen_categories = set()
        while services:
            name = services.pop()

            # If we found a new category mix in it's content
            if name in categories:
                if name not in seen_categories:
                    # Add all of the items in this group to the list of
                    # things that we need to evaluate, and mark this
                    # group as having been seen.
                    services.extend(categories[name])
                    seen_categories.update(name)
                continue

            # If it isn't a category, its a service
            found_services.append(name)

        # Use set to remove duplicates, set is more efficient in batches
        return list(set(found_services))

    def categories(self) -> Dict[str, List[str]]:
        all_categories = {}
        for service in self.services.values():
            try:
                all_categories[service.category].append(service.name)
            except KeyError:
                all_categories[service.category] = [service.name]
        return all_categories

    def stage_index(self, stage):
        return self.config.services.stages.index(stage)

    def _get_services(self):
        stages = self.service_stage.items()

        # noinspection PyUnresolvedReferences
        return {x.name: x for x in self.datastore.list_all_services(full=True)
                if x.enabled and (stages.get(x.name) == ServiceStage.Running or SKIP_SERVICE_SETUP)}


def depths_from_tree(file_tree: Dict[str, List[str]]) -> Dict[str, int]:
    file_children: Dict[str, List[str]] = {}
    depths: Dict[str, int] = {}
    remaining_files = set()

    for child, parents in file_tree.items():
        remaining_files.add(child)
        for parent in parents:
            if parent:
                remaining_files.add(parent)
                file_children[parent] = file_children.get(parent, []) + [child]
            else:
                depths[child] = 0

    next_round = dict(file_children)
    change = True
    while next_round and change:
        file_children = next_round
        next_round = dict()
        change = False

        for parent, children in file_children.items():
            if parent in depths:
                change = True
                for child in children:
                    depths[child] = min(depths[parent] + 1, depths.get(child, float('inf')))
            else:
                next_round[parent] = children

    return depths


class Dispatcher:

    def __init__(self, datastore, redis, redis_persist, logger, counter_name='dispatcher'):
        # Load the datastore collections that we are going to be using
        self.datastore: AssemblylineDatastore = datastore
        self.log: logging.Logger = logger
        self.submissions: Collection = datastore.submission
        self.results: Collection = datastore.result
        self.errors: Collection = datastore.error
        self.files: Collection = datastore.file

        # Create a config cache that will refresh config values periodically
        self.config: Config = forge.get_config()

        # Connect to all of our persistent redis structures
        self.redis = redis or get_client(
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )
        self.redis_persist = redis_persist or get_client(
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )

        # Build some utility classes
        self.scheduler = Scheduler(datastore, self.config, self.redis)
        self.classification_engine = forge.get_classification()
        self.timeout_watcher = WatcherClient(self.redis_persist)

        self.submission_queue = NamedQueue(SUBMISSION_QUEUE, self.redis)
        self.file_queue = NamedQueue(FILE_QUEUE, self.redis)
        self._nonper_other_queues = {}
        self.active_submissions = ExpiringHash(DISPATCH_TASK_HASH, host=self.redis_persist)
        self.running_tasks = ExpiringHash(DISPATCH_RUNNING_TASK_HASH, host=self.redis)

        # Publish counters to the metrics sink.
        self.counter = MetricsFactory(metrics_type='dispatcher', schema=Metrics, name=counter_name,
                                      redis=self.redis, config=self.config)

    def volatile_named_queue(self, name: str) -> NamedQueue:
        if name not in self._nonper_other_queues:
            self._nonper_other_queues[name] = NamedQueue(name, self.redis)
        return self._nonper_other_queues[name]

    def dispatch_submission(self, task: SubmissionTask):
        """
        Find any files associated with a submission and dispatch them if they are
        not marked as in progress. If all files are finished, finalize the submission.

        This version of dispatch submission doesn't verify each result, but assumes that
        the dispatch table has been kept up to date by other components.

        Preconditions:
            - File exists in the filestore and file collection in the datastore
            - Submission is stored in the datastore
        """
        submission = task.submission
        sid = submission.sid

        if not self.active_submissions.exists(sid):
            self.log.info(f"[{sid}] New submission received")
            self.active_submissions.add(sid, task.as_primitives())
        else:
            self.log.info(f"[{sid}] Received a pre-existing submission, check if it is complete")

        # Refresh the watch, this ensures that this function will be called again
        # if something goes wrong with one of the files, and it never gets invoked by dispatch_file.
        self.timeout_watcher.touch(key=sid, timeout=int(self.config.core.dispatcher.timeout),
                                   queue=SUBMISSION_QUEUE, message={'sid': sid})

        # Refresh the quota hold
        if submission.params.quota_item and submission.params.submitter:
            self.log.info(f"[{sid}] Submission will count towards {submission.params.submitter.upper()} quota")
            Hash('submissions-' + submission.params.submitter, self.redis_persist).add(sid, isotime.now_as_iso())

        # Open up the file/service table for this submission
        dispatch_table = DispatchHash(submission.sid, self.redis, fetch_results=True)
        file_parents = dispatch_table.file_tree()  # Load the file tree data as well

        # All the submission files, and all the file_tree files, to be sure we don't miss any incomplete children
        unchecked_hashes = [submission_file.sha256 for submission_file in submission.files]
        unchecked_hashes = list(set(unchecked_hashes) | set(file_parents.keys()))

        # Using the file tree we can recalculate the depth of any file
        depth_limit = self.config.submission.max_extraction_depth
        file_depth = depths_from_tree(file_parents)

        # Try to find all files, and extracted files, and create task objects for them
        # (we will need the file data anyway for checking the schedule later)
        max_files = len(submission.files) + submission.params.max_extracted
        unchecked_files = []  # Files that haven't been checked yet
        try:
            for sha, file_data in self.files.multiget(unchecked_hashes).items():
                unchecked_files.append(FileTask(dict(
                    sid=sid,
                    min_classification=task.submission.classification,
                    file_info=dict(
                        magic=file_data.magic,
                        md5=file_data.md5,
                        mime=file_data.mime,
                        sha1=file_data.sha1,
                        sha256=file_data.sha256,
                        size=file_data.size,
                        type=file_data.type,
                    ),
                    depth=file_depth.get(sha, 0),
                    max_files=max_files
                )))
        except MultiKeyError as missing:
            errors = []
            for file_sha in missing.keys:
                error = Error(dict(
                    archive_ts=submission.archive_ts,
                    expiry_ts=submission.expiry_ts,
                    response=dict(
                        message="Submission couldn't be completed due to missing file.",
                        service_name="dispatcher",
                        service_tool_version='4',
                        service_version='4',
                        status="FAIL_NONRECOVERABLE",
                    ),
                    sha256=file_sha,
                    type='UNKNOWN'
                ))
                error_key = error.build_key(service_tool_version=sid)
                self.datastore.error.save(error_key, error)
                errors.append(error_key)
            return self.cancel_submission(task, errors, file_parents)

        # Files that have already been encountered, but may or may not have been processed yet
        # encountered_files = {file.sha256 for file in submission.files}
        pending_files = {}  # Files that have not yet been processed

        # Track information about the results as we hit them
        file_scores: Dict[str, int] = {}

        # # Load the current state of the dispatch table in one go rather than one at a time in the loop
        prior_dispatches = dispatch_table.all_dispatches()

        # found should be added to the unchecked files if they haven't been encountered already
        for file_task in unchecked_files:
            sha = file_task.file_info.sha256
            schedule = self.build_schedule(dispatch_table, submission, sha, file_task.file_info.type)

            while schedule:
                stage = schedule.pop(0)
                for service_name in stage:
                    # Only active services should be in this dict, so if a service that was placed in the
                    # schedule is now missing it has been disabled or taken offline.
                    service = self.scheduler.services.get(service_name)
                    if not service:
                        continue

                    # If the service is still marked as 'in progress'
                    runtime = time.time() - prior_dispatches.get(sha, {}).get(service_name, 0)
                    if runtime < service.timeout:
                        pending_files[sha] = file_task
                        continue

                    # It hasn't started, has timed out, or is finished, see if we have a result
                    result_row = dispatch_table.finished(sha, service_name)

                    # No result found, mark the file as incomplete
                    if not result_row:
                        pending_files[sha] = file_task
                        continue

                    if not submission.params.ignore_filtering and result_row.drop:
                        schedule.clear()

                    # The process table is marked that a service has been abandoned due to errors
                    if result_row.is_error:
                        continue

                    # Collect information about the result
                    file_scores[sha] = file_scores.get(sha, 0) + result_row.score

        # Using the file tree find the most shallow parent of the given file
        def lowest_parent(_sha):
            # A root file won't have any parents in the dict
            if _sha not in file_parents or None in file_parents[_sha]:
                return None
            return min((file_depth.get(parent, depth_limit), parent) for parent in file_parents[_sha])[1]

        # Filter out things over the depth limit
        pending_files = {sha: ft for sha, ft in pending_files.items() if ft.depth < depth_limit}

        # Filter out files based on the extraction limits
        pending_files = {sha: ft for sha, ft in pending_files.items()
                         if dispatch_table.add_file(sha, max_files, lowest_parent(sha))}

        # If there are pending files, then at least one service, on at least one
        # file isn't done yet, and hasn't been filtered by any of the previous few steps
        # poke those files
        if pending_files:
            self.log.debug(f"[{sid}] Dispatching {len(pending_files)} files: {list(pending_files.keys())}")
            for file_task in pending_files.values():
                self.file_queue.push(file_task.as_primitives())
        else:
            self.log.debug(f"[{sid}] Finalizing submission.")
            max_score = max(file_scores.values()) if file_scores else 0  # Submissions with no results have no score
            self.finalize_submission(task, max_score, file_scores.keys())

    def _cleanup_submission(self, task: SubmissionTask, file_list: List[str]):
        """Clean up code that is the same for canceled and finished submissions"""
        submission = task.submission
        sid = submission.sid

        # Erase the temporary data which may have accumulated during processing
        for file_hash in file_list:
            hash_name = get_temporary_submission_data_name(sid, file_hash=file_hash)
            ExpiringHash(hash_name, host=self.redis).delete()

        if submission.params.quota_item and submission.params.submitter:
            self.log.info(f"[{sid}] Submission no longer counts toward {submission.params.submitter.upper()} quota")
            Hash('submissions-' + submission.params.submitter, self.redis_persist).pop(sid)

        if task.completed_queue:
            self.volatile_named_queue(task.completed_queue).push(submission.as_primitives())

        # Send complete message to any watchers.
        watcher_list = ExpiringSet(make_watcher_list_name(sid), host=self.redis)
        for w in watcher_list.members():
            NamedQueue(w).push(WatchQueueMessage({'status': 'STOP'}).as_primitives())

        # Clear the timeout watcher
        watcher_list.delete()
        self.timeout_watcher.clear(sid)
        self.active_submissions.pop(sid)

        # Count the submission as 'complete' either way
        self.counter.increment('submissions_completed')

    def cancel_submission(self, task: SubmissionTask, errors, file_list):
        """The submission is being abandoned, delete everything, write failed state."""
        submission = task.submission
        sid = submission.sid

        # Pull down the dispatch table and clear it from redis
        dispatch_table = DispatchHash(submission.sid, self.redis)
        dispatch_table.delete()

        submission.classification = submission.params.classification
        submission.error_count = len(errors)
        submission.errors = errors
        submission.state = 'failed'
        submission.times.completed = isotime.now_as_iso()
        self.submissions.save(sid, submission)

        self._cleanup_submission(task, file_list)
        self.log.error(f"[{sid}] Failed")

    def finalize_submission(self, task: SubmissionTask, max_score, file_list):
        """All of the services for all of the files in this submission have finished or failed.

        Update the records in the datastore, and flush the working data from redis.
        """
        submission = task.submission
        sid = submission.sid

        # Pull down the dispatch table and clear it from redis
        dispatch_table = DispatchHash(submission.sid, self.redis)
        all_results = dispatch_table.all_results()
        errors = dispatch_table.all_extra_errors()
        dispatch_table.delete()

        # Sort the errors out of the results
        results = []
        for row in all_results.values():
            for status in row.values():
                if status.is_error:
                    errors.append(status.key)
                elif status.bucket == 'result':
                    results.append(status.key)
                else:
                    self.log.warning(f"[{sid}] Unexpected service output bucket: {status.bucket}/{status.key}")

        submission.classification = submission.params.classification
        submission.error_count = len(errors)
        submission.errors = errors
        submission.file_count = len(file_list)
        submission.results = results
        submission.max_score = max_score
        submission.state = 'completed'
        submission.times.completed = isotime.now_as_iso()
        self.submissions.save(sid, submission)

        self._cleanup_submission(task, file_list)
        self.log.info(f"[{sid}] Completed; files: {len(file_list)} results: {len(results)} "
                      f"errors: {len(errors)} score: {max_score}")

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
        file_hash = task.file_info.sha256
        active_task = self.active_submissions.get(task.sid)

        if active_task is None:
            self.log.warning(f"[{task.sid}] Untracked submission is being processed")
            return

        submission_task = SubmissionTask(active_task)
        submission = submission_task.submission

        # Refresh the watch on the submission, we are still working on it
        self.timeout_watcher.touch(key=task.sid, timeout=int(self.config.core.dispatcher.timeout),
                                   queue=SUBMISSION_QUEUE, message={'sid': task.sid})

        # Open up the file/service table for this submission
        dispatch_table = DispatchHash(task.sid, self.redis, fetch_results=True)

        # Load things that we will need to fill out the
        file_tags = ExpiringSet(task.get_tag_set_name(), host=self.redis)
        file_tags_data = file_tags.members()
        temporary_submission_data = ExpiringHash(task.get_temporary_submission_data_name(), host=self.redis)
        temporary_data = [dict(name=row[0], value=row[1]) for row in temporary_submission_data.items()]

        # Calculate the schedule for the file
        schedule = self.build_schedule(dispatch_table, submission, file_hash, task.file_info.type)
        started_stages = []

        # Go through each round of the schedule removing complete/failed services
        # Break when we find a stage that still needs processing
        outstanding = {}
        score = 0
        errors = 0
        while schedule and not outstanding:
            stage = schedule.pop(0)
            started_stages.append(stage)

            for service_name in stage:
                service = self.scheduler.services.get(service_name)
                if not service:
                    continue

                # Load the results, if there are no results, then the service must be dispatched later
                # Don't look at if it has been dispatched, as multiple dispatches are fine,
                # but missing a dispatch isn't.
                finished = dispatch_table.finished(file_hash, service_name)
                if not finished:
                    outstanding[service_name] = service
                    continue

                # If the service terminated in an error, count the error and continue
                if finished.is_error:
                    errors += 1
                    continue

                # if the service finished, count the score, and check if the file has been dropped
                score += finished.score
                if not submission.params.ignore_filtering and finished.drop:
                    schedule.clear()
                    if schedule:  # If there are still stages in the schedule, over write them for next time
                        dispatch_table.schedules.set(file_hash, started_stages)

        # Try to retry/dispatch any outstanding services
        if outstanding:
            self.log.info(f"[{task.sid}] File {file_hash} sent to services : {', '.join(list(outstanding.keys()))}")

            for service_name, service in outstanding.items():

                # Find the actual file name from the list of files in submission
                filename = None
                for file in submission.files:
                    if task.file_info.sha256 == file.sha256:
                        filename = file.name
                        break

                # Build the actual service dispatch message
                config = self.build_service_config(service, submission)
                service_task = ServiceTask(dict(
                    sid=task.sid,
                    metadata=submission.metadata,
                    min_classification=task.min_classification,
                    service_name=service_name,
                    service_config=config,
                    fileinfo=task.file_info,
                    filename=filename or task.file_info.sha256,
                    depth=task.depth,
                    max_files=task.max_files,
                    ttl=submission.params.ttl,
                    ignore_cache=submission.params.ignore_cache,
                    ignore_dynamic_recursion_prevention=submission.params.ignore_dynamic_recursion_prevention,
                    tags=file_tags_data,
                    temporary_submission_data=temporary_data,
                    deep_scan=submission.params.deep_scan,
                    priority=submission.params.priority,
                ))
                dispatch_table.dispatch(file_hash, service_name)
                queue = get_service_queue(service_name, self.redis)
                queue.push(service_task.priority, service_task.as_primitives())

        else:
            # There are no outstanding services, this file is done
            # clean up the tags
            file_tags.delete()

            # If there are no outstanding ANYTHING for this submission,
            # send a message to the submission dispatcher to finalize
            self.counter.increment('files_completed')
            if dispatch_table.all_finished():
                self.log.info(f"[{task.sid}] Finished processing file '{file_hash}' starting submission finalization.")
                self.submission_queue.push({'sid': submission.sid})
            else:
                self.log.info(f"[{task.sid}] Finished processing file '{file_hash}'. Other files are not finished.")

    def build_schedule(self, dispatch_hash: DispatchHash, submission: Submission,
                       file_hash: str, file_type: str) -> List[List[str]]:
        """Rather than rebuilding the schedule every time we see a file, build it once and cache in redis."""
        cached_schedule = dispatch_hash.schedules.get(file_hash)
        if not cached_schedule:
            # Get the schedule for that file type based on the submission parameters
            obj_schedule = self.scheduler.build_schedule(submission, file_type)
            # The schedule built by the scheduling tool has the service objects, we just want the names for now
            cached_schedule = [list(stage.keys()) for stage in obj_schedule]
            dispatch_hash.schedules.add(file_hash, cached_schedule)
        return cached_schedule

    @classmethod
    def build_service_config(cls, service: Service, submission: Submission) -> Dict[str, str]:
        """Prepare the service config that will be used downstream.

        v3 names: get_service_params get_config_data
        """
        # Load the default service config
        params = {x.name: x.default for x in service.submission_params}

        # Over write it with values from the submission
        if service.name in submission.params.service_spec:
            params.update(submission.params.service_spec[service.name])
        return params
