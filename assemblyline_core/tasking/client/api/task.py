import concurrent.futures
import os
import time
from typing import Any, Dict, cast

import elasticapm

from assemblyline.common.constants import ServiceStatus
from assemblyline.common.dict_utils import flatten, unflatten
from assemblyline.common.heuristics import InvalidHeuristicException
from assemblyline.common.isotime import now_as_iso
from assemblyline.odm import construct_safe
from assemblyline.odm.messages.service_heartbeat import Metrics
from assemblyline.odm.messages.task import Task as ServiceTask
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.tagging import Tagging
from assemblyline.remote.datatypes.exporting_counter import export_metrics_once

from assemblyline_core.tasking.config import DISPATCH_CLIENT, FILESTORE, HEURISTIC_HANDLER, HEURISTICS, LOGGER, STATUS_TABLE, STORAGE, TAG_SAFELISTER, config


SPAN_TYPE = f"al_{os.environ.get('AL_SERVICE_NAME', 'svc_server').lower()}"


def get_task(client_info, headers, **_):
    service_name = client_info['service_name']
    service_version = client_info['service_version']
    service_tool_version = client_info['service_tool_version']
    client_id = client_info['client_id']
    remaining_time = timeout = int(float(headers.get('timeout', 30)))

    try:
        service_data = DISPATCH_CLIENT.service_data[service_name]
    except KeyError:
        raise

    start_time = time.time()
    stats = {
        "execute": 0,
        "cache_miss": 0,
        "cache_hit": 0,
        "cache_skipped": 0,
        "scored": 0,
        "not_scored": 0
    }

    try:
        while remaining_time > 0:
            cache_found = False

            # Set the service status to Idle since we will be waiting for a task
            STATUS_TABLE.set(client_id, (service_name, ServiceStatus.Idle, start_time + timeout))

            # Getting a new task
            task = DISPATCH_CLIENT.request_work(client_id, service_name, service_version, timeout=remaining_time)

            if not task:
                # We've reached the timeout and no task found in service queue
                return dict(task=False)

            # We've got a task to process, consider us busy
            STATUS_TABLE.set(client_id, (service_name, ServiceStatus.Running, time.time() + service_data.timeout))
            stats['execute'] += 1

            result_key = Result.help_build_key(sha256=task.fileinfo.sha256,
                                               service_name=service_name,
                                               service_version=service_version,
                                               service_tool_version=service_tool_version,
                                               is_empty=False,
                                               task=task)

            # If we are allowed, try to see if the result has been cached
            if not task.ignore_cache and not service_data.disable_cache:
                # Checking for previous results for this key
                result = STORAGE.result.get_if_exists(result_key)
                if result:
                    stats['cache_hit'] += 1
                    if result.result.score:
                        stats['scored'] += 1
                    else:
                        stats['not_scored'] += 1

                    result.archive_ts = now_as_iso(config.datastore.ilm.days_until_archive * 24 * 60 * 60)
                    if task.ttl:
                        result.expiry_ts = now_as_iso(task.ttl * 24 * 60 * 60)

                    DISPATCH_CLIENT.service_finished(task.sid, result_key, result)
                    cache_found = True

                if not cache_found:
                    # Checking for previous empty results for this key
                    result = STORAGE.emptyresult.get_if_exists(f"{result_key}.e")
                    if result:
                        stats['cache_hit'] += 1
                        stats['not_scored'] += 1
                        result = STORAGE.create_empty_result_from_key(result_key)
                        DISPATCH_CLIENT.service_finished(task.sid, f"{result_key}.e", result)
                        cache_found = True

                if not cache_found:
                    stats['cache_miss'] += 1
            else:
                stats['cache_skipped'] += 1

            if not cache_found:
                # No luck with the cache, lets dispatch the task to a client
                return dict(task=task.as_primitives())

            # Recalculating how much time we have left before we reach the timeout
            remaining_time = start_time + timeout - time.time()

        # We've been processing cache hit for the length of the timeout... bailing out!
        return dict(task=False)
    finally:
        export_metrics_once(service_name, Metrics, stats, host=client_id, counter_type='service')


def task_finished(client_info, json, **_):
    exec_time = json.get('exec_time')

    try:
        task = ServiceTask(json['task'])

        if 'result' in json:  # Task created a result
            missing_files = handle_task_result(exec_time, task, json['result'], client_info, json['freshen'])
            if missing_files:
                return dict(success=False, missing_files=missing_files)
            return dict(success=True)

        elif 'error' in json:  # Task created an error
            error = json['error']
            handle_task_error(exec_time, task, error, client_info)
            return dict(success=True)
        else:
            return None

    except ValueError as e:  # Catch errors when building Task or Result model
        raise e


def handle_task_result(exec_time: int, task: ServiceTask, result: Dict[str, Any], client_info: Dict[str, str],
                       freshen: bool):

    def freshen_file(file_info_list, item):
        file_info = file_info_list.get(item['sha256'], None)
        if file_info is None or not FILESTORE.exists(item['sha256']):
            return True
        else:
            file_info['archive_ts'] = archive_ts
            file_info['expiry_ts'] = expiry_ts
            file_info['classification'] = item['classification']
            STORAGE.save_or_freshen_file(item['sha256'], file_info,
                                         file_info['expiry_ts'], file_info['classification'],
                                         is_section_image=item.get('is_section_image', False))
        return False

    archive_ts = now_as_iso(config.datastore.ilm.days_until_archive * 24 * 60 * 60)
    if task.ttl:
        expiry_ts = now_as_iso(task.ttl * 24 * 60 * 60)
    else:
        expiry_ts = None

    # Check if all files are in the filestore
    if freshen:
        missing_files = []
        hashes = list(set([f['sha256']
                           for f in result['response']['extracted'] + result['response']['supplementary']]))
        file_infos = STORAGE.file.multiget(hashes, as_obj=False, error_on_missing=False)

        with elasticapm.capture_span(name="handle_task_result.freshen_files",
                                     span_type=SPAN_TYPE):
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
    client_id = client_info['client_id']

    # Add scores to the heuristics, if any section set a heuristic
    with elasticapm.capture_span(name="handle_task_result.process_heuristics",
                                 span_type=SPAN_TYPE):
        total_score = 0
        for section in result['result']['sections']:
            zeroize_on_sig_safe = section.pop('zeroize_on_sig_safe', True)
            section['tags'] = flatten(section['tags'])
            if section.get('heuristic'):
                heur_id = f"{client_info['service_name'].upper()}.{str(section['heuristic']['heur_id'])}"
                section['heuristic']['heur_id'] = heur_id
                try:
                    section['heuristic'], new_tags = HEURISTIC_HANDLER.service_heuristic_to_result_heuristic(
                        section['heuristic'], HEURISTICS, zeroize_on_sig_safe)
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
                                 span_type=SPAN_TYPE):
        for section in result['result']['sections']:
            # Perform tag safelisting
            tags, safelisted_tags = TAG_SAFELISTER.get_validated_tag_map(section['tags'])
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
                LOGGER.warning(f"[{task.sid}] Invalid tag data from {client_info['service_name']}: {dropped}")

    result = Result(result)
    result_key = result.build_key(service_tool_version=result.response.service_tool_version, task=task)
    DISPATCH_CLIENT.service_finished(task.sid, result_key, result, temp_submission_data)

    # Metrics
    if result.result.score > 0:
        export_metrics_once(service_name, Metrics, dict(scored=1), host=client_id, counter_type='service')
    else:
        export_metrics_once(service_name, Metrics, dict(not_scored=1), host=client_id, counter_type='service')

    LOGGER.info(f"[{task.sid}] {client_info['client_id']} - {client_info['service_name']} "
                f"successfully completed task {f' in {exec_time}ms' if exec_time else ''}")


def handle_task_error(exec_time: int, task: ServiceTask, error: Dict[str, Any], client_info: Dict[str, str]) -> None:
    service_name = client_info['service_name']
    client_id = client_info['client_id']

    LOGGER.info(f"[{task.sid}] {client_info['client_id']} - {client_info['service_name']} "
                f"failed to complete task {f' in {exec_time}ms' if exec_time else ''}")

    # Add timestamps for creation, archive and expiry
    error['created'] = now_as_iso()
    error['archive_ts'] = now_as_iso(config.datastore.ilm.days_until_archive * 24 * 60 * 60)
    if task.ttl:
        error['expiry_ts'] = now_as_iso(task.ttl * 24 * 60 * 60)

    error = Error(error)
    error_key = error.build_key(service_tool_version=error.response.service_tool_version, task=task)
    DISPATCH_CLIENT.service_failed(task.sid, error_key, error)

    # Metrics
    if error.response.status == 'FAIL_RECOVERABLE':
        export_metrics_once(service_name, Metrics, dict(fail_recoverable=1), host=client_id, counter_type='service')
    else:
        export_metrics_once(service_name, Metrics, dict(fail_nonrecoverable=1), host=client_id, counter_type='service')
