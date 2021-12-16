import concurrent.futures
import logging
import os
import shutil
import tempfile
import threading
import time
from typing import Any, Dict

import elasticapm
import yaml

from assemblyline.common import forge, heuristics, identify
from assemblyline.common.constants import SERVICE_STATE_HASH, ServiceStatus
from assemblyline.common.dict_utils import flatten, unflatten
from assemblyline.common.heuristics import HeuristicHandler, InvalidHeuristicException
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.metrics import MetricsFactory
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.filestore import FileStore, FileStoreException
from assemblyline.odm import construct_safe
from assemblyline.odm.messages.changes import Operation
from assemblyline.odm.messages.service_heartbeat import Metrics
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.heuristic import Heuristic
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.service import Service
from assemblyline.odm.models.tagging import Tagging
from assemblyline.remote.datatypes.events import EventSender
from assemblyline.remote.datatypes.hash import ExpiringHash
from assemblyline_core.dispatching.client import DispatchClient

METRICS_FACTORIES = {}
LOCK = threading.Lock()


# Helper functions
def get_metrics_factory(service_name):
    factory = METRICS_FACTORIES.get(service_name, None)

    if factory is None:
        with LOCK:
            factory = MetricsFactory('service', Metrics, name=service_name, export_zero=False)
            METRICS_FACTORIES[service_name] = factory

    return factory


# Tasking class
class TaskingClient:
    """A helper class to simplify tasking for privileged services and service-server.

    This tool helps take care of interactions between the filestore,
    datastore, dispatcher, and any sources of files to be processed.
    """

    def __init__(self, datastore: AssemblylineDatastore = None, filestore: FileStore = None,
                 config=None, redis=None):
        self.log = logging.getLogger('assemblyline.tasking_client')
        self.config = config or forge.CachedObject(forge.get_config)
        self.datastore = datastore or forge.get_datastore(self.config)
        self.dispatch_client = DispatchClient(self.datastore)
        self.event_sender = EventSender('changes.services',
                                        host=self.config.core.redis.nonpersistent.host,
                                        port=self.config.core.redis.nonpersistent.port)
        self.filestore = filestore or forge.get_filestore(self.config)
        self.heuristic_handler = HeuristicHandler(self.datastore)
        self.heuristics = {h.heur_id: h for h in self.datastore.list_all_heuristics()}
        self.status_table = ExpiringHash(SERVICE_STATE_HASH, ttl=60*30)
        self.tag_safelister = forge.CachedObject(forge.get_tag_safelister, kwargs=dict(
            log=self.log, config=config, datastore=self.datastore), refresh=300)

    # File
    def download_file(self, sha256, client_info, target_path=None) -> tuple:
        try:
            # Used by service task_handler
            if target_path:
                self.filestore.download(sha256, target_path)
                return

            # Used by service-server
            with tempfile.NamedTemporaryFile() as temp_file:
                self.filestore.download(sha256, temp_file.name)
                f_size = os.path.getsize(temp_file.name)
                return dict(reader=open(temp_file.name, 'rb'), name=sha256, size=f_size)

        except FileStoreException:
            self.log.exception(f"[{client_info['client_id']}] {client_info['service_name']} couldn't find file "
                               f"{sha256} requested by service ")
            raise

    def upload_files(self, client_info, request, direct_upload={}, **_):
        headers = direct_upload['headers'] if direct_upload else request.headers
        sha256 = headers['sha256']
        classification = headers['classification']
        ttl = int(headers['ttl'])
        is_section_image = headers.get('is_section_image', 'false').lower() == 'true'

        with tempfile.NamedTemporaryFile(mode='wb') as temp_file:
            if not direct_upload:
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
            else:
                file = direct_upload['file']
                temp_file.write(file.read())

            # Identify the file info of the uploaded file
            file_info = identify.fileinfo(temp_file.name)

            # Validate SHA256 of the uploaded file
            if sha256 == file_info['sha256']:
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
                self.filestore.upload(temp_file.name, file_info['sha256'])
            else:
                self.log.warning(f"{client_info['client_id']} - {client_info['service_name']} "
                                 f"uploaded file (SHA256: {file_info['sha256']}) doesn't match "
                                 f"expected file (SHA256: {sha256})")
                return False, f"Uploaded file does not match expected file hash. [{file_info['sha256']} != {sha256}]"

        self.log.info(f"{client_info['client_id']} - {client_info['service_name']} "
                      f"successfully uploaded file (SHA256: {file_info['sha256']})")

        return True, None

    # Safelist
    def exists(self, qhash, **_):
        return self.datastore.safelist.get_if_exists(qhash, as_obj=False)

    def get_safelist_for_tags(self, tag_types, **_):
        if tag_types:
            tag_types = tag_types.split(',')

        with forge.get_cachestore('system', config=self.config, datastore=self.datastore) as cache:
            tag_safelist_yml = cache.get('tag_safelist_yml')
            if tag_safelist_yml:
                tag_safelist_data = yaml.safe_load(tag_safelist_yml)
            else:
                tag_safelist_data = forge.get_tag_safelist_data()

        if tag_types:
            output = {
                'match': {k: v for k, v in tag_safelist_data.get('match', {}).items() if k in tag_types or tag_types == []},
                'regex': {k: v for k, v in tag_safelist_data.get('regex', {}).items() if k in tag_types or tag_types == []},
            }
            for tag in tag_types:
                for sl in self.datastore.safelist.stream_search(
                        f"type:tag AND enabled:true AND tag.type:{tag}", as_obj=False):
                    output['match'].setdefault(sl['tag']['type'], [])
                    output['match'][sl['tag']['type']].append(sl['tag']['value'])

        else:
            output = tag_safelist_data
            for sl in self.datastore.safelist.stream_search("type:tag AND enabled:true", as_obj=False):
                output['match'].setdefault(sl['tag']['type'], [])
                output['match'][sl['tag']['type']].append(sl['tag']['value'])

        return output

    def get_safelist_for_signatures(self, **_):
        output = [
            item['signature']['name']
            for item in self.datastore.safelist.stream_search(
                "type:signature AND enabled:true", fl="signature.name", as_obj=False)]

        return output

    # Service
    def register_service(self, client_info, json, **_):
        keep_alive = True

        try:
            # Get heuristics list
            heuristics = json.pop('heuristics', None)

            # Patch update_channel, registry_type before Service registration object creation
            json['update_channel'] = json.get('update_channel', self.config.services.preferred_update_channel)
            json['docker_config']['registry_type'] = json['docker_config'] \
                .get('registry_type', self.config.services.preferred_registry_type)
            for dep in json.get('dependencies', {}).values():
                dep['container']['registry_type'] = dep.get(
                    'registry_type', self.config.services.preferred_registry_type)

            # Pop unused registration json
            for x in ['file_required', 'tool_version']:
                json.pop(x, None)

            # Create Service registration object
            service = Service(json)

            # Fix service version, we don't need to see the stable label
            service.version = service.version.replace('stable', '')

            # Save service if it doesn't already exist
            if not self.datastore.service.exists(f'{service.name}_{service.version}'):
                self.datastore.service.save(f'{service.name}_{service.version}', service)
                self.datastore.service.commit()
                self.log.info(f"{client_info['client_id']} - {client_info['service_name']} registered")
                keep_alive = False

            # Save service delta if it doesn't already exist
            if not self.datastore.service_delta.exists(service.name):
                self.datastore.service_delta.save(service.name, {'version': service.version})
                self.datastore.service_delta.commit()
                self.log.info(f"{client_info['client_id']} - {client_info['service_name']} "
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
                        self.log.exception(f"{client_info['client_id']} - {client_info['service_name']} "
                                           f"invalid heuristic ({heuristic_id}) ignored: {str(e)}")
                        raise ValueError("Error parsing heuristics")

                for item in self.datastore.heuristic.bulk(plan)['items']:
                    if item['update']['result'] != "noop":
                        new_heuristics.append(item['update']['_id'])
                        self.log.info(f"{client_info['client_id']} - {client_info['service_name']} "
                                      f"heuristic {item['update']['_id']}: {item['update']['result'].upper()}")

                self.datastore.heuristic.commit()

            service_config = self.datastore.get_service_with_delta(service.name, as_obj=False)

            # Notify components watching for service config changes
            self.event_sender.send(service.name, {
                'operation': Operation.Added,
                'name': service.name
            })

        except ValueError as e:  # Catch errors when building Service or Heuristic model(s)
            raise e

        return dict(keep_alive=keep_alive, new_heuristics=new_heuristics, service_config=service_config or dict())

    # Task
    def get_task(self, client_info, headers, **_):
        service_name = client_info['service_name']
        service_version = client_info['service_version']
        service_tool_version = client_info['service_tool_version']
        client_id = client_info['client_id']
        remaining_time = timeout = int(float(headers.get('timeout', 30)))
        metric_factory = get_metrics_factory(service_name)

        try:
            service_data = self.dispatch_client.service_data[service_name]
        except KeyError:
            raise

        start_time = time.time()

        while remaining_time > 0:
            cache_found = False

            # Set the service status to Idle since we will be waiting for a task
            self.status_table.set(client_id, (service_name, ServiceStatus.Idle, start_time + timeout))

            # Getting a new task
            task = self.dispatch_client.request_work(client_id, service_name, service_version, timeout=remaining_time)

            if not task:
                # We've reached the timeout and no task found in service queue
                return dict(task=False)

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
                return dict(task=task.as_primitives())

            # Recalculating how much time we have left before we reach the timeout
            remaining_time = start_time + timeout - time.time()

        # We've been processing cache hit for the length of the timeout... bailing out!
        return dict(task=False)

    def task_finished(self, client_info, json, **_):
        exec_time = json.get('exec_time')

        try:
            task = ServiceTask(json['task'])

            if 'result' in json:  # Task created a result
                missing_files = self.handle_task_result(exec_time, task, json['result'], client_info, json['freshen'])
                if missing_files:
                    return dict(success=False, missing_files=missing_files)
                return dict(success=True)

            elif 'error' in json:  # Task created an error
                error = json['error']
                self.handle_task_error(exec_time, task, error, client_info)
                return dict(success=True)
            else:
                return None

        except ValueError as e:  # Catch errors when building Task or Result model
            raise e

    @elasticapm.capture_span(span_type='al_svc_server')
    def handle_task_result(self, exec_time: int, task: ServiceTask, result: Dict[str, Any], client_info: Dict[str, str],
                           freshen: bool):

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

        service_name = client_info['service_name']
        metric_factory = get_metrics_factory(service_name)

        # Add scores to the heuristics, if any section set a heuristic
        with elasticapm.capture_span(name="handle_task_result.process_heuristics",
                                     span_type="tasking_client"):
            total_score = 0
            for section in result['result']['sections']:
                zeroize_on_sig_safe = section.pop('zeroize_on_sig_safe', True)
                section['tags'] = flatten(section['tags'])
                if section.get('heuristic'):
                    heur_id = f"{client_info['service_name'].upper()}.{str(section['heuristic']['heur_id'])}"
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
                    self.log.warning(f"[{task.sid}] Invalid tag data from {client_info['service_name']}: {dropped}")

        result = Result(result)
        result_key = result.build_key(service_tool_version=result.response.service_tool_version, task=task)
        self.dispatch_client.service_finished(task.sid, result_key, result, temp_submission_data)

        # Metrics
        if result.result.score > 0:
            metric_factory.increment('scored')
        else:
            metric_factory.increment('not_scored')

        self.log.info(f"[{task.sid}] {client_info['client_id']} - {client_info['service_name']} "
                      f"successfully completed task {f' in {exec_time}ms' if exec_time else ''}")

    @elasticapm.capture_span(span_type='al_svc_server')
    def handle_task_error(self,
                          exec_time: int, task: ServiceTask, error: Dict[str, Any],
                          client_info: Dict[str, str]) -> None:
        service_name = client_info['service_name']
        metric_factory = get_metrics_factory(service_name)

        self.log.info(f"[{task.sid}] {client_info['client_id']} - {client_info['service_name']} "
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
