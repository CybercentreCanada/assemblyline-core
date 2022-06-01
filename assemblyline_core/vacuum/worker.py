import contextlib
import datetime
import json
import sys
import tempfile
import logging
import os
import time
import shutil
from copy import deepcopy
from typing import Dict, Tuple, Optional
from multiprocessing import Lock, Event, Queue
from queue import Empty
from pprint import pprint
import threading

import elasticapm

from assemblyline.common.codec import decode_file
from assemblyline.common.dict_utils import flatten
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.datastore.store import ESStore
from assemblyline.common import identify, forge
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.uid import get_random_id
from assemblyline.remote.datatypes.queues.comms import CommsQueue

from assemblyline.filestore import FileStore
from assemblyline.common.str_utils import safe_str
from assemblyline.remote.datatypes import get_client as get_redis_client
from assemblyline.odm.messages.submission import Submission
from assemblyline.odm.models.submission import DEFAULT_SRV_SEL
from assemblyline.remote.datatypes.queues.named import NamedQueue

from assemblyline_ui.helper.service import ui_to_submission_params, get_default_service_spec, get_default_service_list

from .safelist import VacuumSafelist
from .department_map import DepartmentMap
from .stream_map import StreamMap, Stream

# init_logging('assemblyline.vacuum.worker')
logger = logging.getLogger('assemblyline.vacuum.worker')
engine = forge.get_classification()

if not engine.enforce:
    print("Classification must be enabled")
    time.sleep(100)
    exit(1)

APM_SPAN_TYPE = 'vacuum'


def load_user_settings(datastore, user) -> dict:
    default_settings = {}

    SERVICE_LIST = datastore.list_all_services(as_obj=False, full=True)

    settings = datastore.user_settings.get_if_exists(user['uname'], as_obj=False)
    srv_list: list[dict] = [x for x in SERVICE_LIST if x['enabled']]
    if not settings:
        def_srv_list = None
        settings = default_settings
    else:
        # Make sure all defaults are there
        for key, item in default_settings.items():
            if key not in settings:
                settings[key] = item

        # Remove all obsolete keys
        for key in list(settings.keys()):
            if key not in default_settings:
                del settings[key]

        def_srv_list = settings.get('services', {}).get('selected', None)

    settings['service_spec'] = get_default_service_spec(srv_list, settings.get('service_spec', {}))
    settings['services'] = get_default_service_list(srv_list, def_srv_list)

    return settings


class EmptyMetaException(Exception):
    pass


class NotJSONException(Exception):
    pass


class InvalidMessageException(Exception):
    pass


PROFILE_CACHE_TIME = 60 * 10


@contextlib.contextmanager
def timed(data_dict, name, client):
    start = time.time()

    if client:
        with elasticapm.capture_span(name):
            try:
                yield
            finally:
                data_dict[name] = time.time() - start
    else:
        try:
            yield
        finally:
            data_dict[name] = time.time() - start


class FileProcessor(threading.Thread):
    last_stream_id_update = datetime.datetime.now()
    stream_id_lock = Lock()
    safe_files = {}

    datastore: AssemblylineDatastore = None

    profile_lock = threading.RLock()

    profile_check = time.time()
    settings_check = time.time()
    total_rounds = threading.Semaphore(value=1000)

    user_profile = None
    user_settings = None

    def __init__(self, worker_id, stop, feedback, apm_client, config, safelist, department_map, stream_map):
        # Start these worker processes in daemon mode so the OS makes sure they exit when the vacuum process exits.
        super().__init__(daemon=True)
        # Things initialized here will be copied into the new process when it starts.
        # Anything that can't be copied easily should be initialized in 'run'.
        self.config: Config = config
        self._stop_event: Event = stop
        self._feedback: Queue = feedback
        logger.info("Connect to work queue")
        redis = get_redis_client('vacuum-redis', 6379, False)
        self.queue = NamedQueue('work', redis)
        self.worker_id = worker_id
        self.filestore: FileStore = None
        self.times = {}
        self.apm_client = apm_client
        self.safelist: VacuumSafelist = safelist
        self.department_codes = department_map
        self.stream_map = stream_map

        self.traffic_queue = CommsQueue('submissions',
                                        host=config.redis_volatile_host,
                                        port=config.redis_volatile_port)

        self.ingest_queue = NamedQueue(
            "m-ingest",
            host=config.redis_persist_host,
            port=config.redis_persist_port
        )

        with self.profile_lock:
            if self.datastore is None:
                self.datastore = AssemblylineDatastore(ESStore([self.config.datastore_url], archive_access=False))
        self.get_user()
        self.get_user_settings()

    def get_user(self) -> dict:
        if self.user_profile and time.time() - self.profile_check < PROFILE_CACHE_TIME:
            return self.user_profile

        with self.profile_lock:
            if self.user_profile and time.time() - self.profile_check < PROFILE_CACHE_TIME:
                return self.user_profile

            self.user_profile = self.datastore.user.get(self.config.assemblyline_user, as_obj=False)
            self.profile_check = time.time()
            return self.user_profile

    def get_user_settings(self) -> dict:
        if self.user_settings and time.time() - self.settings_check < PROFILE_CACHE_TIME:
            return deepcopy(self.user_settings)

        with self.profile_lock:
            if self.user_settings and time.time() - self.settings_check < PROFILE_CACHE_TIME:
                return deepcopy(self.user_settings)

            self.user_settings = load_user_settings(self.datastore, self.get_user())
            self.settings_check = time.time()
            return deepcopy(self.user_settings)

    def tt(self, name):
        return timed(self.times, name, self.apm_client)

    def get_stream(self, stream_id) -> Optional[Stream]:
        try:
            return self.stream_map[int(stream_id)]
        except (KeyError, TypeError):
            logger.warning("Invalid stream: %s" % stream_id)
        return None

    @elasticapm.capture_span(span_type=APM_SPAN_TYPE)
    def client_info(self, stream_id: str) -> Tuple[str, int, str, str, str]:
        stream = self.get_stream(stream_id)

        # stream not in cache, update cache and try again
        if not stream:
            # log and return bogus data
            logger.error('Could not find info on stream %s' % stream_id)
            return 'UNK', 10, 'UNK_STREAM', "Unknown stream ID", 'PB//CND'

        description = stream.description or "Unknown stream ID"
        name = stream.name or "UNK_STREAM"
        dept = name.split("_", 1)[0]
        zone = stream.zone_id
        classification = stream.classification or 'PB//CND'

        return dept, zone, name, description, classification

    def is_safelisted(self, msg: Dict) -> Tuple[str, str]:
        sha256 = msg['sha256']
        if sha256 in self.safe_files:
            return self.safe_files[sha256], 'cached'

        def dotdump(s):
            return ''.join(['.' if ord(x) < 32 or ord(x) > 125 else x for x in s])

        reason, hit = self.safelist.drop(msg['metadata'])
        hit = str({x: dotdump(safe_str(y)) for x, y in hit.items()})

        if reason:
            self.safe_files[sha256] = reason

        return reason, hit

    def source_file_path(self, sha256) -> Optional[str]:
        for file_directory in self.config.file_directories:
            path = os.path.join(file_directory, sha256[0], sha256[1], sha256[2], sha256[3], sha256)
            if os.path.exists(path):
                return path
        return None

    @elasticapm.capture_span(span_type=APM_SPAN_TYPE)
    def ingest(self, msg: Dict, meta_path: str, temp_file, stream_classification) -> bool:
        if 'sha256' not in msg or not msg['sha256'] or len(msg['sha256']) != 64:
            raise InvalidMessageException('SHA256 is missing form the message or is invalid.')
        sha256 = msg['sha256']

        # counts.increment('vacuum.ingested')

        # if msg.get('metadata', msg).get('protocol', None):
        #     counts.increment('vacuum.%s' % msg.get('metadata', msg)['protocol'])
        # notice = Notice(msg)
        # noinspection PyBroadException
        with self.tt('safelist'):
            try:
                reason, hit = self.is_safelisted(msg)
                if reason:
                    # return to skip this safelisted notice
                    # counts.increment('vacuum.safelist')
                    logger.info("Trusting %s due to reason: %s (%s)" % (sha256, reason, hit))
                    return True
            except Exception:
                pass

            # This is the hash of a file that is SUPPOSED to be empty, just ignore this one silently.
            if sha256 == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                return True

        with self.tt('copy'):
            # Temporarily create a local copy
            source_file = self.source_file_path(sha256)
            working_file = ''
            if source_file is not None:
                try:
                    with open(source_file, 'rb') as src:
                        shutil.copyfileobj(src, temp_file)
                    # shutil.copyfile(source_file, working_file)
                    # os.link(source_file, working_file)
                    temp_file.flush()
                    working_file = temp_file.name
                except Exception:
                    pass

            if os.path.exists(working_file):
                if os.path.getsize(working_file) == 0:
                    logger.warning(f"Skipping empty source file: {source_file} meta: {meta_path}")
                    return True

                if os.path.getsize(working_file) >= 104857600:
                    logger.warning(f"Skipping too large file: {source_file} meta: {meta_path}")
                    return True

        # os.makedirs(os.path.dirname(output_file), exist_ok=True)
        # new_file = not os.path.exists(output_file)
        # if new_file:
        #     os.link(source_file, output_file)
        extracted_path = None
        al_meta = {}

        try:
            with self.tt('user_info'):
                user = self.get_user()

                # Load default user params
                s_params: dict = ui_to_submission_params(self.get_user_settings())

                # Reset dangerous user settings to safe values
                s_params.update({
                    'deep_scan': False,
                    "priority": 150,
                    "ignore_cache": False,
                    "ignore_dynamic_recursion_prevention": False,
                    "ignore_filtering": False,
                    "type": "INGEST"
                })

                # Apply provided params
                active_classification = msg.get('overrides', {}).get('classification', 'PB//CND')
                active_classification = engine.max_classification(active_classification, 'PB//CND')
                active_classification = engine.max_classification(active_classification, stream_classification)
                s_params.update({
                    'classification': active_classification,
                    'ttl': 60,
                })
                s_params['services']['resubmit'] = ['Dynamic Analysis']

                if 'groups' not in s_params:
                    s_params['groups'] = user['groups']

                # Override final parameters
                s_params.update({
                    'generate_alert': True,
                    'max_extracted': 100,  # config.core.ingester.default_max_extracted,
                    'max_supplementary': 100,  # config.core.ingester.default_max_supplementary,
                    'priority': 100,
                    'submitter': user['uname']
                })

            # Enforce maximum DTL
            # if config.submission.max_dtl > 0:
            # s_params['ttl'] = min(
            #     int(s_params['ttl']),
            #     config.submission.max_dtl) if int(s_params['ttl']) else config.submission.max_dtl

            with self.tt('get_info'):
                fileinfo = self.datastore.file.get_if_exists(sha256, as_obj=False,
                                                             archive_access=False)
            # config.datastore.ilm.update_archive)

            # No need to re-calculate fileinfo if we have it already
            active_file = working_file
            if not fileinfo:
                if not os.path.exists(working_file):
                    logger.debug(f"Source file not found: {source_file} meta: {meta_path}")
                    return False

                # Calculate file digest
                with self.tt('identify'):
                    fileinfo = identify.fileinfo(working_file)

                    # If identified as a higher priority item (ie. documents), elevate submission priority
                    # meta_copy: dict = deepcopy(msg.get('metadata', {}))
                    # if not is_low_priority(meta_copy, self.department_codes):
                    #     s_params.update({'priority': meta_copy.get('priority', 200)})

                    if msg.get('metadata', {}).get('truncated'):
                        # File is malformed therefore don't send to certain services
                        s_params.update({
                            "services": {
                                "selected": ["AntiVirus", "Characterize", "Codevector", "FrankenStrings",
                                             "MetaPeek", "Safelist", "TagCheck", "VirusTotalCache", "YARA"],
                                "excluded": ["Cuckoo", "PE"]
                            }
                        })

                    # If still considered low priority, submit to a subset of services and
                    # resubmit to all if necessary
                    # elif s_params['priority'] == 100:
                    #     resubmit_services = list(DEFAULT_SRV_SEL) + ["Dynamic Analysis"]
                    #     s_params.update({
                    #         'services': {
                    #             'selected': ['Extract', 'MetaDefender', 'AntiVirus', 'VirusTotalCache',
                    #                          'Safelist', 'YARA'],
                    #             'resubmit': resubmit_services
                    #         },
                    #     })

                    # Validate file size
                    # if fileinfo['size'] > MAX_SIZE and not s_params.get('ignore_size', False):
                    #     msg = f"File too large ({fileinfo['size']} > {MAX_SIZE}). Ingestion failed"
                    #     return make_api_response({}, err=msg, status_code=413)
                    # elif fileinfo['size'] == 0:
                    #     return make_api_response({}, err="File empty. Ingestion failed", status_code=400)

                    # Decode cart if needed
                    extracted_path, fileinfo, al_meta = decode_file(working_file, fileinfo)
                    if extracted_path:
                        active_file = extracted_path

            # Save the file to the filestore if needs be
            # also no need to test if exist before upload because it already does that
            if os.path.exists(active_file):
                with self.tt('upload'):
                    self.filestore.upload(active_file, fileinfo['sha256'])

            # Freshen file object
            with self.tt('freshen'):
                expiry = now_as_iso(s_params['ttl'] * 24 * 60 * 60) if s_params.get('ttl', None) else None
                self.datastore.save_or_freshen_file(fileinfo['sha256'], fileinfo, expiry, s_params['classification'])

            with self.tt('create'):
                # Load metadata, setup some default values if they are missing and append the cart metadata
                ingest_id = get_random_id()
                metadata = flatten({
                    key.lower(): value
                    for key, value in msg['metadata'].items()
                    if key and value
                })
                metadata['ingest_id'] = ingest_id
                metadata['type'] = 'VACUUM'
                metadata.update(al_meta)
                if 'ts' not in metadata:
                    metadata['ts'] = now_as_iso()

                # Set description if it does not exists
                s_params['description'] = f"[{s_params['type']}] Inspection of file: {fileinfo['sha256']}"

                # Create submission object
                try:
                    submission_obj = Submission({
                        "sid": ingest_id,
                        "files": [{
                            'name': metadata.get('filename', fileinfo['sha256']),
                            'sha256': fileinfo['sha256'],
                            'size': fileinfo['size']
                        }],
                        "notification": {},
                        "metadata": metadata,
                        "params": s_params
                    })
                except (ValueError, KeyError):
                    logger.exception("Couldn't construct submission")
                    return True

            # Send submission object for processing
            with self.tt('post'):
                self.ingest_queue.push(submission_obj.as_primitives())
                # self.traffic_queue.publish(SubmissionMessage({
                #     'msg': submission_obj,
                #     'msg_type': 'SubmissionReceived',
                #     'sender': 'ui',
                # }).as_primitives())

        except Exception:
            logger.exception("Error in ingest")
            return False
        finally:
            with self.tt('cleanup'):
                # Cleanup files on disk
                try:
                    if source_file and os.path.exists(source_file):
                        os.unlink(source_file)
                except Exception:
                    pass

                try:
                    if extracted_path and os.path.exists(extracted_path):
                        os.unlink(extracted_path)
                except Exception:
                    pass

        logger.debug(f'Ingested: {source_file}')
        return True

    @elasticapm.capture_span(span_type=APM_SPAN_TYPE)
    def process_file(self, meta_path: str):
        flush_file = False
        if self.config.dry_run:
            logger.info("Processing: %s" % meta_path)

        try:
            with self.tt('load_json'):
                if not os.path.exists(meta_path):
                    logger.warning("File %s was not found..." % meta_path)
                    #     counts.increment('vacuum.skipped')
                    return

                if not os.path.getsize(meta_path):
                    raise EmptyMetaException("Metadata file '%s' is empty." % meta_path)

                with open(meta_path, 'rb') as fh:
                    try:
                        msg = json.load(fh)
                    except Exception as error:
                        logger.info('File %s failed to parsed as JSON. Will try other parsing methods...' % meta_path)
                        fh.seek(0)
                        raise NotJSONException(f"File '{meta_path}' is not a JSON Metadata "
                                               f"file because {error}: {fh.read(2000)}")

            with self.tt('client_info'):
                dept, zone, stream_name, description, stream_classification = self.client_info(msg['metadata']['stream'])
                if 'department' not in msg['metadata']:
                    msg['metadata']['department'] = dept
                if 'zone' not in msg['metadata']:
                    msg['metadata']['zone'] = zone
                if 'stream_name' not in msg['metadata']:
                    msg['metadata']['stream_name'] = stream_name
                if description and 'description' not in msg['metadata']:
                    msg['metadata']['stream_description'] = description
                if 'ip_src' in msg['metadata']:
                    dep = self.department_codes[msg['metadata']['ip_src']]
                    if dep:
                        msg['metadata']['dep_src'] = dep
                if 'ip_dst' in msg['metadata']:
                    dep = self.department_codes[msg['metadata']['ip_dst']]
                    if dep:
                        msg['metadata']['dep_dst'] = dep
                msg['metadata']['transport'] = 'tcp'

            with self.tt('ingest_outer'):
                if not self.config.dry_run:
                    with tempfile.NamedTemporaryFile(dir=self.config.worker_cache_directory) as temp_file:
                        with self.tt('ingest'):
                            self.ingest(msg, meta_path, temp_file, stream_classification)
                            flush_file = True
                        _s = time.time()
                    self.times['dir_cleanup'] = time.time() - _s
                else:
                    pprint(msg)
        except EmptyMetaException as eme:
            logger.warning(str(eme))
            flush_file = True
            # if not dry_run:
            #     counts.increment('vacuum.skipped')
        except NotJSONException as nje:
            logger.warning(str(nje))
            # if not dry_run:
            #     counts.increment('vacuum.skipped')
        except Exception as e:
            # if not dry_run:
            #     counts.increment('vacuum.error')
            logger.exception("%s: %s" % (e.__class__.__name__, str(e)))
        finally:
            if flush_file:
                with self.tt('cleanup_meta'):
                    try:
                        os.unlink(meta_path)
                    except Exception as error:
                        logger.exception(f"Exception caught deleting file: {meta_path} {error}")
                    finally:
                        if os.path.exists(meta_path):
                            logger.warning("File '%s' could not be deleted by vacuum worker" % meta_path)

    # def reconnect(self):
    #     logger.info(f'Connecting to assemblyline {self.config.assemblyline_url} as {self.config.assemblyline_user}')
    #     self.client = get_client(
    #         self.config.assemblyline_url,
    #         apikey=(self.config.assemblyline_user, self.config.assemblyline_key),
    #         verify=False
    #     )
    #
    # def soft_reconnect(self):
    #     session = requests.Session()
    #     session.verify = False
    #     session.cookies = self.client._connection.session.cookies
    #     session.headers.update(self.client._connection.session.headers)
    #     session.headers['Connection'] = 'close'
    #     session.timeout = self.client._connection.session.timeout
    #     self.client._connection.session = session

    def run(self):
        self.filestore = FileStore(self.config.filestore_destination)
        if not self.config.dry_run:
            # self.reconnect()
            # counts = counter.AutoExportingCounters(
            #     name='vacuum',
            #     host=get_hostip(),
            #     auto_flush=True,
            #     auto_log=False,
            #     export_interval_secs=al_config.system.update_interval,
            #     channel=forge.get_metrics_sink(),
            #     counter_type='vacuum')
            # counts.start()
            ...

        else:
            logger.info('DRY_MODE: Messages will not be queued, they will be displayed on screen.')

        logger.info('Waiting for files...')
        while not self._stop_event.is_set():
            if self.ingest_queue.length() > 1000000:
                time.sleep(10)
                continue

            if not FileProcessor.total_rounds.acquire(blocking=False):
                return

            try:
                _s = time.time()
                file = self.queue.pop(blocking=True, timeout=2)
                if file:
                    if self.apm_client:
                        self.apm_client.begin_transaction(APM_SPAN_TYPE)

                    self.times['popping'] = time.time() - _s
                    self.process_file(file)
                    self.times['total'] = time.time() - _s

                    if self.apm_client:
                        self.apm_client.end_transaction(APM_SPAN_TYPE, 'error')

            except EmptyMetaException as eme:
                # if not dry_run:
                #     counts.increment('vacuum.skipped')
                logger.error(str(eme))
            except Exception as e:
                # if not dry_run:
                #     counts.increment('vacuum.error')
                logger.exception("Unhandled Exception: %s" % str(e))
                if self.apm_client:
                    self.apm_client.end_transaction(APM_SPAN_TYPE, 'error')

            finally:
                self._feedback.put(self.times)
                self.times = {}
                time.sleep(1000000)


def main(options, config: Config, stop):
    logger.handlers.clear()
    logger.addHandler(logging.StreamHandler(sys.stdout))
    if options.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    apm_client = None
    if config.apm_url:
        elasticapm.instrument()
        apm_client = elasticapm.Client(server_url=config.apm_url, service_name='vacuum')

    container_id = os.environ.get("HOSTNAME", 'vacuum-worker') + '-'
    feedback = Queue()
    FileProcessor.total_rounds = threading.Semaphore(value=1000*options.workers)

    department_map, stream_map = None, None
    department_map = DepartmentMap.load(config.department_map_url)
    stream_map = StreamMap.load(config.stream_map_url)

    args = dict(
        config=config,
        department_map=department_map,
        stream_map=stream_map
    )

    workers = [FileProcessor(container_id + str(0), stop, feedback, apm_client, **args)]
    workers.extend([
        FileProcessor(container_id + str(ii), stop, feedback, None, **args)
        for ii in range(1, options.workers)
    ])
    [w.start() for w in workers]

    processed = [0]
    times = {}

    def process_update(message):
        processed[0] += 1
        for key, value in message.items():
            try:
                times[key] += value
            except KeyError:
                times[key] = value

    while not stop.is_set() and workers:
        workers = [w for w in workers if w.is_alive()]
        time.sleep(1)

        while not stop.is_set():
            try:
                line = feedback.get_nowait()
                process_update(line)
            except Empty:
                break

        if processed[0] == 0:
            continue

        _t = {name: int(value/processed[0] * 1000) for name, value in times.items()}
        values = ' '.join(sorted([f'{name}: {value}' for name, value in _t.items() if value > 1]))
        print(f'{processed[0]} [{len(workers)}] ', values, flush=True)

    while workers:
        workers = [w for w in workers if w.is_alive()]
        print(len(workers))
        time.sleep(1)
