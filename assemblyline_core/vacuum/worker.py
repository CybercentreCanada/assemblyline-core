import contextlib
import datetime
import json
import tempfile
import logging
import os
import time
import signal
import shutil
from copy import deepcopy
from typing import Dict, Tuple, Optional, Any
from multiprocessing import Lock, Event
import threading

import elasticapm

from assemblyline.common.forge import get_classification, get_config, get_datastore, get_filestore
from assemblyline.common.codec import decode_file
from assemblyline.common.dict_utils import flatten
from assemblyline.common.log import init_logging
from assemblyline.common.metrics import MetricsFactory
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.common import identify
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.uid import get_random_id
from assemblyline.odm.models.config import Config
from assemblyline.remote.datatypes.queues.comms import CommsQueue
from assemblyline.odm.messages.vacuum_heartbeat import Metrics

from assemblyline.filestore import FileStore
from assemblyline.common.str_utils import safe_str
from assemblyline.remote.datatypes import get_client as get_redis_client
from assemblyline.odm.messages.submission import Submission
from assemblyline.remote.datatypes.queues.named import NamedQueue

from assemblyline_ui.helper.service import ui_to_submission_params, get_default_service_spec, get_default_service_list

from assemblyline_core.vacuum.crawler import VACUUM_BUFFER_NAME

from .safelist import VacuumSafelist
from .department_map import DepartmentMap
from .stream_map import StreamMap, Stream


# init_logging('assemblyline.vacuum.worker')
logger = logging.getLogger('assemblyline.vacuum.worker')
stop_event = Event()


# noinspection PyUnusedLocal
def sigterm_handler(_signum=0, _frame=None):
    stop_event.set()


APM_SPAN_TYPE = 'vacuum'


def load_user_settings(datastore, user) -> dict:
    default_settings: dict[str, Any] = {}

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
def timed(name, client):
    if client:
        with elasticapm.capture_span(name):
            yield


class FileProcessor(threading.Thread):
    last_stream_id_update = datetime.datetime.now()
    stream_id_lock = Lock()
    safe_files: dict[str, str] = {}

    datastore: AssemblylineDatastore = None

    profile_lock = threading.RLock()

    profile_check = time.time()
    settings_check = time.time()
    total_rounds = threading.Semaphore(value=1000)

    user_profile: Optional[dict] = None
    user_settings: Optional[dict] = None

    def __init__(self, worker_id, counter, apm_client, redis, config, safelist, department_map, stream_map):
        # Start these worker processes in daemon mode so the OS makes sure they exit when the vacuum process exits.
        super().__init__(daemon=True)
        # Things initialized here will be copied into the new process when it starts.
        # Anything that can't be copied easily should be initialized in 'run'.
        self.config: Config = config
        self.counter = counter
        self.minimum_classification = self.config.core.vacuum.minimum_classification
        logger.info("Connect to work queue")
        self.queue = NamedQueue(VACUUM_BUFFER_NAME, redis)
        self.worker_id = worker_id
        self.filestore: FileStore = get_filestore()
        self.engine = get_classification()
        self.apm_client = apm_client
        self.safelist: VacuumSafelist = safelist
        self.department_codes: Optional[DepartmentMap] = department_map
        self.stream_map: Optional[StreamMap] = stream_map
        self.identify = identify.Identify(use_cache=True)

        self.traffic_queue = CommsQueue('submissions', redis)

        self.ingest_queue = NamedQueue(
            "m-ingest",
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port
        )

        with self.profile_lock:
            if self.datastore is None:
                self.datastore = get_datastore()
        self.get_user()
        self.get_user_settings()

    def get_user(self) -> dict:
        if self.user_profile and time.time() - self.profile_check < PROFILE_CACHE_TIME:
            return self.user_profile

        with self.profile_lock:
            if self.user_profile and time.time() - self.profile_check < PROFILE_CACHE_TIME:
                return self.user_profile

            self.user_profile = self.datastore.user.get(self.config.core.vacuum.assemblyline_user, as_obj=False)
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

    def timed(self, name):
        return timed(name, self.apm_client)

    def get_stream(self, stream_id) -> Optional[Stream]:
        try:
            if self.stream_map:
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
            return 'UNK', 10, 'UNK_STREAM', "Unknown stream ID", self.minimum_classification

        description = stream.description or "Unknown stream ID"
        name = stream.name or "UNK_STREAM"
        dept = name.split("_", 1)[0]
        zone = stream.zone_id
        classification = stream.classification or self.minimum_classification

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
        for file_directory in self.config.core.vacuum.file_directories:
            path = os.path.join(file_directory, sha256[0], sha256[1], sha256[2], sha256[3], sha256)
            if os.path.exists(path):
                return path
        return None

    @elasticapm.capture_span(span_type=APM_SPAN_TYPE)
    def ingest(self, msg: Dict, meta_path: str, temp_file, stream_classification) -> bool:
        if 'sha256' not in msg or not msg['sha256'] or len(msg['sha256']) != 64:
            raise InvalidMessageException('SHA256 is missing form the message or is invalid.')
        sha256 = msg['sha256']

        # export metrics
        self.counter.increment('ingested')
        # protocol = msg.get('metadata', msg).get('protocol', None)
        # if protocol:
        #     self.counter.increment(f'protocol.{protocol}')

        with self.timed('safelist'):
            try:
                reason, hit = self.is_safelisted(msg)
                if reason:
                    # return to skip this safelisted notice
                    self.counter.increment('safelist')
                    logger.info("Trusting %s due to reason: %s (%s)" % (sha256, reason, hit))
                    return True
            except Exception:
                pass

            # This is the hash of a file that is SUPPOSED to be empty, just ignore this one silently.
            if sha256 == 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855':
                return True

        with self.timed('copy'):
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
            with self.timed('user_info'):
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
                active_cc = msg.get('overrides', {}).get('classification', self.minimum_classification)
                active_cc = self.engine.max_classification(active_cc, self.minimum_classification)
                active_cc = self.engine.max_classification(active_cc, stream_classification)
                s_params.update({
                    'classification': active_cc,
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

            with self.timed('get_info'):
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
                with self.timed('identify'):
                    fileinfo = self.identify.fileinfo(working_file)

                    if msg.get('metadata', {}).get('truncated'):
                        # File is malformed therefore don't send to certain services
                        s_params.update({
                            "services": {
                                "selected": ["AntiVirus", "Characterize", "Codevector", "FrankenStrings",
                                             "MetaPeek", "Safelist", "TagCheck", "VirusTotalCache", "YARA"],
                                "excluded": ["Cuckoo", "PE"]
                            }
                        })

                    # Decode cart if needed
                    extracted_path, fileinfo, al_meta = decode_file(working_file, fileinfo, self.identify)
                    if extracted_path:
                        active_file = extracted_path

            # Save the file to the filestore if needs be
            # also no need to test if exist before upload because it already does that
            if os.path.exists(active_file):
                with self.timed('upload'):
                    self.filestore.upload(active_file, fileinfo['sha256'])

            # Freshen file object
            with self.timed('freshen'):
                expiry = now_as_iso(s_params['ttl'] * 24 * 60 * 60) if s_params.get('ttl', None) else None
                self.datastore.save_or_freshen_file(fileinfo['sha256'], fileinfo, expiry, s_params['classification'])

            with self.timed('create'):
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
            with self.timed('post'):
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
            with self.timed('cleanup'):
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
        try:
            with self.timed('load_json'):
                if not os.path.exists(meta_path):
                    logger.warning("File %s was not found..." % meta_path)
                    self.counter.increment('skipped')
                    return

                if not os.path.getsize(meta_path):
                    raise EmptyMetaException("Metadata file '%s' is empty." % meta_path)

                with open(meta_path, 'rb') as fh:
                    try:
                        msg = json.load(fh)
                    except Exception as error:
                        logger.info('File %s failed to parsed as JSON. Will try other parsing methods...' % meta_path)
                        fh.seek(0)
                        sample = str(fh.read(2000))
                        raise NotJSONException(f"File '{meta_path}' is not a JSON Metadata "
                                               f"file because {error}: {sample}")

            with self.timed('client_info'):
                dept, zone, stream_name, description, stream_classification = self.client_info(msg['metadata']['stream'])
                if 'department' not in msg['metadata']:
                    msg['metadata']['department'] = dept
                if 'zone' not in msg['metadata']:
                    msg['metadata']['zone'] = zone
                if 'stream_name' not in msg['metadata']:
                    msg['metadata']['stream_name'] = stream_name
                if description and 'description' not in msg['metadata']:
                    msg['metadata']['stream_description'] = description
                if self.department_codes:
                    if 'ip_src' in msg['metadata']:
                        dep = self.department_codes[msg['metadata']['ip_src']]
                        if dep:
                            msg['metadata']['dep_src'] = dep
                    if 'ip_dst' in msg['metadata']:
                        dep = self.department_codes[msg['metadata']['ip_dst']]
                        if dep:
                            msg['metadata']['dep_dst'] = dep
                msg['metadata']['transport'] = 'tcp'

            with self.timed('ingest_outer'):
                with tempfile.NamedTemporaryFile(dir=self.config.core.vacuum.worker_cache_directory) as temp_file:
                    with self.timed('ingest'):
                        self.ingest(msg, meta_path, temp_file, stream_classification)
                        flush_file = True

        except EmptyMetaException as eme:
            logger.warning(str(eme))
            flush_file = True
            self.counter.increment('skipped')
        except NotJSONException as nje:
            logger.warning(str(nje))
            self.counter.increment('skipped')
        except Exception as e:
            self.counter.increment('errors')
            logger.exception("%s: %s" % (e.__class__.__name__, str(e)))
        finally:
            if flush_file:
                with self.timed('cleanup_meta'):
                    try:
                        os.unlink(meta_path)
                    except Exception as error:
                        logger.exception(f"Exception caught deleting file: {meta_path} {error}")
                    finally:
                        if os.path.exists(meta_path):
                            logger.warning("File '%s' could not be deleted by vacuum worker" % meta_path)

    def run(self):
        logger.info('Waiting for files...')
        while not stop_event.is_set():
            if self.ingest_queue.length() > 100000:
                time.sleep(10)
                continue

            if not FileProcessor.total_rounds.acquire(blocking=False):
                return

            try:
                file = self.queue.pop(blocking=True, timeout=2)
                if file:
                    if self.apm_client:
                        self.apm_client.begin_transaction(APM_SPAN_TYPE)

                    self.process_file(file)

                    if self.apm_client:
                        self.apm_client.end_transaction(APM_SPAN_TYPE, 'error')

            except EmptyMetaException as eme:
                self.counter.increment('skipped')
                logger.error(str(eme))
            except Exception as e:
                self.counter.increment('errors')
                logger.exception("Unhandled Exception: %s" % str(e))
                if self.apm_client:
                    self.apm_client.end_transaction(APM_SPAN_TYPE, 'error')

            finally:
                time.sleep(1000000)


def main():
    config = get_config()
    vacuum_config = config.core.vacuum
    signal.signal(signal.SIGTERM, sigterm_handler)

    # Initialize logging
    init_logging('assemblyline.vacuum')
    logger = logging.getLogger('assemblyline.vacuum')
    logger.info('Vacuum worker starting up...')

    apm_client = None
    if config.core.metrics.apm_server.server_url:
        elasticapm.instrument()
        apm_client = elasticapm.Client(server_url=config.core.metrics.apm_server.server_url,
                                       service_name="vacuum_worker")

    container_id = os.environ.get("HOSTNAME", 'vacuum-worker') + '-'
    FileProcessor.total_rounds = threading.Semaphore(value=1000*vacuum_config.worker_threads)

    department_map, stream_map = None, None
    if vacuum_config.department_map_url:
        department_map = DepartmentMap.load(vacuum_config.department_map_url)
    if vacuum_config.stream_map_url:
        stream_map = StreamMap.load(vacuum_config.stream_map_url)

    redis = get_redis_client(host=config.core.redis.nonpersistent.host,
                             port=config.core.redis.nonpersistent.port, private=False)

    counter = MetricsFactory(metrics_type='vacuum', schema=Metrics, name='vacuum',
                             redis=redis, config=config)

    args = dict(
        config=config,
        department_map=department_map,
        stream_map=stream_map,
        counter=counter,
        redis=redis,
    )

    workers = [FileProcessor(container_id + str(0), apm_client=apm_client, **args)]
    workers.extend([
        FileProcessor(container_id + str(ii), **args)
        for ii in range(1, vacuum_config.worker_threads)
    ])
    [w.start() for w in workers]

    while not stop_event.is_set() and workers:
        workers = [w for w in workers if w.is_alive()]
        time.sleep(1)

    while workers:
        workers = [w for w in workers if w.is_alive()]
        logger.info(f'stopping; {len(workers)} remaining workers')
        time.sleep(1)


if __name__ == '__main__':
    main()
