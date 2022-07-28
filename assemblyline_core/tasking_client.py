import concurrent.futures
import logging
import time
from typing import Any, Dict

import elasticapm

from assemblyline.common import forge
from assemblyline.common.constants import SERVICE_STATE_HASH, ServiceStatus
from assemblyline.common.dict_utils import flatten, unflatten
from assemblyline.common.heuristics import HeuristicHandler, InvalidHeuristicException
from assemblyline.common.isotime import now_as_iso
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.filestore import FileStore
from assemblyline.odm import construct_safe
from assemblyline.odm.messages.changes import Operation
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.service import Service
from assemblyline.odm.models.tagging import Tagging
from assemblyline.remote.datatypes.events import EventSender, EventWatcher
from assemblyline.remote.datatypes.hash import ExpiringHash
from assemblyline_core.dispatching.client import DispatchClient


class TaskingClientException(Exception):
    pass


class ServiceMissingException(Exception):
    pass


# Tasking class
class TaskingClient:
    """A helper class to simplify tasking for privileged services and service-server.

    This tool helps take care of interactions between the filestore,
    datastore, dispatcher, and any sources of files to be processed.
    """

    def __init__(self, datastore: AssemblylineDatastore = None, filestore: FileStore = None,
                 config=None, redis=None, redis_persist=None, identify=None):
        self.log = logging.getLogger('assemblyline.tasking_client')
        self.config = config or forge.CachedObject(forge.get_config)
        self.datastore = datastore or forge.get_datastore(self.config)
        self.dispatch_client = DispatchClient(self.datastore, redis=redis, redis_persist=redis_persist)
        self.event_sender = EventSender('changes', redis)
        self.event_listener = EventWatcher(redis)
        self.filestore = filestore or forge.get_filestore(self.config)
        self.heuristic_handler = HeuristicHandler(self.datastore)
        self.heuristics: dict[str, Heuristic] = {}
        self.reload_heuristics({})
        self.status_table = ExpiringHash(SERVICE_STATE_HASH, ttl=60*30, host=redis)
        self.tag_safelister = forge.CachedObject(forge.get_tag_safelister, kwargs=dict(
            log=self.log, config=config, datastore=self.datastore), refresh=300)
        if identify:
            self.cleanup = False
        else:
            self.cleanup = True
        self.identify = identify or forge.get_identify(config=self.config, datastore=self.datastore, use_cache=True)
        self.event_listener.register('changes.heuristics', self.reload_heuristics)
        self.event_listener.start()

    def reload_heuristics(self, data: dict):
        service_name = data.get('service_name')
        if service_name:
            self.heuristics.update({h.heur_id: h for h in self.datastore.list_service_heuristics(service_name)})
        else:
            self.heuristics = {h.heur_id: h for h in self.datastore.list_all_heuristics()}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.stop()

    def stop(self):
        if self.cleanup:
            self.identify.stop()
        if self.event_listener:
            self.event_listener.stop()

    @elasticapm.capture_span(span_type='tasking_client')
    def upload_file(self, file_path, classification, ttl, is_section_image, expected_sha256=None):
        # Identify the file info of the uploaded file
        file_info = self.identify.fileinfo(file_path)

        # Validate SHA256 of the uploaded file
        if expected_sha256 is None or expected_sha256 == file_info['sha256']:
            file_info['archive_ts'] = now_as_iso(self.config.datastore.ilm.days_until_archive * 24 * 60 * 60)
            file_info['classification'] = classification
            if ttl:
                file_info['expiry_ts'] = now_as_iso(ttl * 24 * 60 * 60)
            else:
                file_info['expiry_ts'] = None

            # Update the datastore with the uploaded file
            self.datastore.save_or_freshen_file(file_info['sha256'], file_info, file_info['expiry_ts'],
                                                file_info['classification'], is_section_image=is_section_image)

            # Upload file to the filestore (upload already checks if the file exists)
            self.filestore.upload(file_path, file_info['sha256'])
        else:
            raise TaskingClientException("Uploaded file does not match expected file hash. "
                                         f"[{file_info['sha256']} != {expected_sha256}]")

    # Service
    @elasticapm.capture_span(span_type='tasking_client')
    def register_service(self, service_data, log_prefix=""):
        keep_alive = True

        try:
            # Get heuristics list
            heuristics = service_data.pop('heuristics', None)

            # Patch update_channel, registry_type before Service registration object creation
            service_data['update_channel'] = service_data.get(
                'update_channel', self.config.services.preferred_update_channel)
            service_data['docker_config']['registry_type'] = service_data['docker_config'] \
                .get('registry_type', self.config.services.preferred_registry_type)
            service_data['privileged'] = service_data.get('privileged', self.config.services.prefer_service_privileged)
            for dep in service_data.get('dependencies', {}).values():
                dep['container']['registry_type'] = dep.get(
                    'registry_type', self.config.services.preferred_registry_type)

            # Pop unused registration service_data
            for x in ['file_required', 'tool_version']:
                service_data.pop(x, None)

            # Create Service registration object
            service = Service(service_data)

            # Fix service version, we don't need to see the stable label
            service.version = service.version.replace('stable', '')

            # Save service if it doesn't already exist
            if not self.datastore.service.exists(f'{service.name}_{service.version}'):
                self.datastore.service.save(f'{service.name}_{service.version}', service)
                self.datastore.service.commit()
                self.log.info(f"{log_prefix}{service.name} registered")
                keep_alive = False

            # Save service delta if it doesn't already exist
            if not self.datastore.service_delta.exists(service.name):
                self.datastore.service_delta.save(service.name, {'version': service.version})
                self.datastore.service_delta.commit()
                self.log.info(f"{log_prefix}{service.name} "
                              f"version ({service.version}) registered")

            new_heuristics = []
            if heuristics:
                plan = self.datastore.heuristic.get_bulk_plan()
                for index, heuristic in enumerate(heuristics):
                    heuristic_id = f'#{index}'  # Set heuristic id to it's position in the list for logging purposes
                    try:
                        # Append service name to heuristic ID
                        heuristic['heur_id'] = f"{service.name.upper()}.{str(heuristic['heur_id'])}"

                        # Attack_id field is now a list, make it a list if we receive otherwise
                        attack_id = heuristic.get('attack_id', None)
                        if isinstance(attack_id, str):
                            heuristic['attack_id'] = [attack_id]

                        heuristic = Heuristic(heuristic)
                        heuristic_id = heuristic.heur_id
                        plan.add_upsert_operation(heuristic_id, heuristic)
                    except Exception as e:
                        msg = f"{service.name} has an invalid heuristic ({heuristic_id}): {str(e)}"
                        self.log.exception(f"{log_prefix}{msg}")
                        raise ValueError(msg)

                for item in self.datastore.heuristic.bulk(plan)['items']:
                    if item['update']['result'] != "noop":
                        new_heuristics.append(item['update']['_id'])
                        self.log.info(f"{log_prefix}{service.name} "
                                      f"heuristic {item['update']['_id']}: {item['update']['result'].upper()}")

                self.datastore.heuristic.commit()

                # Notify components watching for heuristic config changes
                self.event_sender.send('heuristics', {
                    'operation': Operation.Modified,
                    'service_name': service.name
                })

            service_config = self.datastore.get_service_with_delta(service.name, as_obj=False)

            # Notify components watching for service config changes
            self.event_sender.send('services.' + service.name, {
                'operation': Operation.Added,
                'name': service.name
            })

        except ValueError as e:  # Catch errors when building Service or Heuristic model(s)
            raise e

        return dict(keep_alive=keep_alive, new_heuristics=new_heuristics, service_config=service_config or dict())

    # Task
    @elasticapm.capture_span(span_type='tasking_client')
    def get_task(self, client_id, service_name, service_version, service_tool_version,
                 metric_factory, status_expiry=None, timeout=30):
        if status_expiry is None:
            status_expiry = time.time() + timeout

        cache_found = False

        try:
            service_data = self.dispatch_client.service_data[service_name]
        except KeyError:
            raise ServiceMissingException("The service you're asking task for does not exist, try later", 404)

        # Set the service status to Idle since we will be waiting for a task
        self.status_table.set(client_id, (service_name, ServiceStatus.Idle, status_expiry))

        # Getting a new task
        task = self.dispatch_client.request_work(client_id, service_name, service_version, timeout=timeout)

        if not task:
            # We've reached the timeout and no task found in service queue
            return None, False

        # We've got a task to process, consider us busy
        self.status_table.set(client_id, (service_name, ServiceStatus.Running, time.time() + service_data.timeout))
        metric_factory.increment('execute')

        result_key = Result.help_build_key(sha256=task.fileinfo.sha256,
                                           service_name=service_name,
                                           service_version=service_version,
                                           service_tool_version=service_tool_version,
                                           is_empty=False,
                                           task=task)

        # If we are allowed, try to see if the result has been cached
        if not task.ignore_cache and not service_data.disable_cache:
            # Checking for previous results for this key
            result = self.datastore.result.get_if_exists(result_key)
            if result:
                metric_factory.increment('cache_hit')
                if result.result.score:
                    metric_factory.increment('scored')
                else:
                    metric_factory.increment('not_scored')

                result.archive_ts = now_as_iso(self.config.datastore.ilm.days_until_archive * 24 * 60 * 60)
                if task.ttl:
                    result.expiry_ts = now_as_iso(task.ttl * 24 * 60 * 60)

                self.dispatch_client.service_finished(task.sid, result_key, result)
                cache_found = True

            if not cache_found:
                # Checking for previous empty results for this key
                result = self.datastore.emptyresult.get_if_exists(f"{result_key}.e")
                if result:
                    metric_factory.increment('cache_hit')
                    metric_factory.increment('not_scored')
                    result = self.datastore.create_empty_result_from_key(result_key)
                    self.dispatch_client.service_finished(task.sid, f"{result_key}.e", result)
                    cache_found = True

            if not cache_found:
                metric_factory.increment('cache_miss')

        else:
            metric_factory.increment('cache_skipped')

        if not cache_found:
            # No luck with the cache, lets dispatch the task to a client
            return task.as_primitives(), False

        return None, True

    @elasticapm.capture_span(span_type='tasking_client')
    def task_finished(self, service_task, client_id, service_name, metric_factory):
        exec_time = service_task.get('exec_time')

        try:
            task = ServiceTask(service_task['task'])

            if 'result' in service_task:  # Task created a result
                missing_files = self._handle_task_result(
                    exec_time, task, service_task['result'],
                    client_id, service_name, service_task['freshen'], metric_factory)
                if missing_files:
                    return dict(success=False, missing_files=missing_files)
                return dict(success=True)

            elif 'error' in service_task:  # Task created an error
                error = service_task['error']
                self._handle_task_error(exec_time, task, error, client_id, service_name, metric_factory)
                return dict(success=True)
            else:
                return None

        except ValueError as e:  # Catch errors when building Task or Result model
            raise e

    @elasticapm.capture_span(span_type='tasking_client')
    def _handle_task_result(self, exec_time: int, task: ServiceTask, result: Dict[str, Any], client_id, service_name,
                            freshen: bool, metric_factory):

        def freshen_file(file_info_list, item):
            file_info = file_info_list.get(item['sha256'], None)
            if file_info is None or not self.filestore.exists(item['sha256']):
                return True
            else:
                file_info['archive_ts'] = archive_ts
                file_info['expiry_ts'] = expiry_ts
                file_info['classification'] = item['classification']
                self.datastore.save_or_freshen_file(item['sha256'], file_info,
                                                    file_info['expiry_ts'], file_info['classification'],
                                                    is_section_image=item.get('is_section_image', False))
            return False

        archive_ts = now_as_iso(self.config.datastore.ilm.days_until_archive * 24 * 60 * 60)
        if task.ttl:
            expiry_ts = now_as_iso(task.ttl * 24 * 60 * 60)
        else:
            expiry_ts = None

        # Check if all files are in the filestore
        if freshen:
            missing_files = []
            hashes = list(set([f['sha256']
                               for f in result['response']['extracted'] + result['response']['supplementary']]))
            file_infos = self.datastore.file.multiget(hashes, as_obj=False, error_on_missing=False)

            with elasticapm.capture_span(name="handle_task_result.freshen_files",
                                         span_type="tasking_client"):
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    res = {
                        f['sha256']: executor.submit(freshen_file, file_infos, f)
                        for f in result['response']['extracted'] + result['response']['supplementary']}
                for k, v in res.items():
                    if v.result():
                        missing_files.append(k)

            if missing_files:
                return missing_files

        # Add scores to the heuristics, if any section set a heuristic
        with elasticapm.capture_span(name="handle_task_result.process_heuristics",
                                     span_type="tasking_client"):
            total_score = 0
            for section in result['result']['sections']:
                zeroize_on_sig_safe = section.pop('zeroize_on_sig_safe', True)
                section['tags'] = flatten(section['tags'])
                if section.get('heuristic'):
                    heur_id = f"{service_name.upper()}.{str(section['heuristic']['heur_id'])}"
                    section['heuristic']['heur_id'] = heur_id
                    try:
                        section['heuristic'], new_tags = self.heuristic_handler.service_heuristic_to_result_heuristic(
                            section['heuristic'], self.heuristics, zeroize_on_sig_safe)
                        for tag in new_tags:
                            section['tags'].setdefault(tag[0], [])
                            if tag[1] not in section['tags'][tag[0]]:
                                section['tags'][tag[0]].append(tag[1])
                        total_score += section['heuristic']['score']
                    except InvalidHeuristicException:
                        section['heuristic'] = None

        # Update the total score of the result
        result['result']['score'] = total_score

        # Add timestamps for creation, archive and expiry
        result['created'] = now_as_iso()
        result['archive_ts'] = archive_ts
        result['expiry_ts'] = expiry_ts

        # Pop the temporary submission data
        temp_submission_data = result.pop('temp_submission_data', None)
        if temp_submission_data:
            old_submission_data = {row.name: row.value for row in task.temporary_submission_data}
            temp_submission_data = {k: v for k, v in temp_submission_data.items()
                                    if k not in old_submission_data or v != old_submission_data[k]}
            big_temp_data = {k: len(str(v)) for k, v in temp_submission_data.items()
                             if len(str(v)) > self.config.submission.max_temp_data_length}
            if big_temp_data:
                big_data_sizes = [f"{k}={v}" for k, v in big_temp_data.items()]
                self.log.warning(f"[{task.sid}] The following temporary submission keys were ignored because they are "
                                 "bigger then the maximum data size allowed "
                                 f"[{self.config.submission.max_temp_data_length}]: {' | '.join(big_data_sizes)}")
                temp_submission_data = {k: v for k, v in temp_submission_data.items() if k not in big_temp_data}

        # Process the tag values
        with elasticapm.capture_span(name="handle_task_result.process_tags",
                                     span_type="tasking_client"):
            for section in result['result']['sections']:
                # Perform tag safelisting
                tags, safelisted_tags = self.tag_safelister.get_validated_tag_map(section['tags'])
                section['tags'] = unflatten(tags)
                section['safelisted_tags'] = safelisted_tags

                section['tags'], dropped = construct_safe(Tagging, section.get('tags', {}))

                # Set section score to zero and lower total score if service is set to zeroize score
                # and all tags were safelisted
                if section.pop('zeroize_on_tag_safe', False) and \
                        section.get('heuristic') and \
                        len(tags) == 0 and \
                        len(safelisted_tags) != 0:
                    result['result']['score'] -= section['heuristic']['score']
                    section['heuristic']['score'] = 0

                if dropped:
                    self.log.warning(f"[{task.sid}] Invalid tag data from {service_name}: {dropped}")

        result = Result(result)
        result_key = result.build_key(service_tool_version=result.response.service_tool_version, task=task)
        self.dispatch_client.service_finished(task.sid, result_key, result, temp_submission_data)

        # Metrics
        if result.result.score > 0:
            metric_factory.increment('scored')
        else:
            metric_factory.increment('not_scored')

        self.log.info(f"[{task.sid}] {client_id} - {service_name} "
                      f"successfully completed task {f' in {exec_time}ms' if exec_time else ''}")

        self.status_table.set(client_id, (service_name, ServiceStatus.Idle, time.time() + 5))

    @elasticapm.capture_span(span_type='tasking_client')
    def _handle_task_error(
            self, exec_time: int, task: ServiceTask, error: Dict[str, Any],
            client_id, service_name, metric_factory) -> None:
        self.log.info(f"[{task.sid}] {client_id} - {service_name} "
                      f"failed to complete task {f' in {exec_time}ms' if exec_time else ''}")

        # Add timestamps for creation, archive and expiry
        error['created'] = now_as_iso()
        error['archive_ts'] = now_as_iso(self.config.datastore.ilm.days_until_archive * 24 * 60 * 60)
        if task.ttl:
            error['expiry_ts'] = now_as_iso(task.ttl * 24 * 60 * 60)

        error = Error(error)
        error_key = error.build_key(service_tool_version=error.response.service_tool_version, task=task)
        self.dispatch_client.service_failed(task.sid, error_key, error)

        # Metrics
        if error.response.status == 'FAIL_RECOVERABLE':
            metric_factory.increment('fail_recoverable')
        else:
            metric_factory.increment('fail_nonrecoverable')

        self.status_table.set(client_id, (service_name, ServiceStatus.Idle, time.time() + 5))
