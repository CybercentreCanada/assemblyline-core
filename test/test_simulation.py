"""
A test of ingest+dispatch running in one process.

Needs the datastore and filestore to be running, otherwise these test are stand alone.
"""
from __future__ import annotations
import hashlib
import json
import typing
import time
import threading
import logging
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Any

import pytest

from assemblyline.common import forge
from assemblyline.common.forge import get_service_queue
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.uid import get_random_id
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.models.config import Config
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.service_delta import ServiceDelta
from assemblyline.odm.models.submission import Submission
from assemblyline.odm.models.user import User
from assemblyline.odm.randomizer import random_model_obj
from assemblyline.odm.messages.submission import Submission as SubmissionInput
from assemblyline.remote.datatypes.queues.named import NamedQueue

import assemblyline_core
from assemblyline_core.plumber.run_plumber import Plumber
from assemblyline_core.dispatching import dispatcher
from assemblyline_core.dispatching.client import DispatchClient
from assemblyline_core.dispatching.dispatcher import Dispatcher
from assemblyline_core.ingester.ingester import IngestTask, Ingester
from assemblyline_core.server_base import ServerBase, get_service_stage_hash, ServiceStage

from test_scheduler import dummy_service

if TYPE_CHECKING:
    from redis import Redis

RESPONSE_TIMEOUT = 60


@pytest.fixture(scope='module')
def redis(redis_connection: Redis[Any]):
    redis_connection.flushdb()
    yield redis_connection
    redis_connection.flushdb()


_global_semaphore = threading.Semaphore(value=1)
print_lock = threading.Lock()


class MockService(ServerBase):
    """Replaces everything past the dispatcher.

    Including service API, in the future probably include that in this test.
    """

    def __init__(self, name, datastore, redis, filestore):
        super().__init__('assemblyline.service.'+name)
        self.service_name = name
        self.datastore = datastore
        self.filestore = filestore
        self.queue = get_service_queue(name, redis)
        self.dispatch_client = DispatchClient(self.datastore, redis)
        self.hits = dict()
        self.drops = dict()

    def try_run(self):
        while self.running:
            task = self.dispatch_client.request_work('worker', self.service_name, '0', timeout=3)
            if not task:
                continue
            self.log.info(f"{self.service_name} has received a job {task.sid}")

            file = self.filestore.get(task.fileinfo.sha256)

            instructions = json.loads(file)
            instructions = instructions.get(self.service_name, {})
            self.log.info(f"{self.service_name} following instruction: {instructions}")
            hits = self.hits[task.fileinfo.sha256] = self.hits.get(task.fileinfo.sha256, 0) + 1

            if instructions.get('hold', False):
                queue = get_service_queue(self.service_name, self.dispatch_client.redis)
                queue.push(0, task.as_primitives())
                self.log.info(f"{self.service_name} Requeued task to {queue.name} holding for {instructions['hold']}")
                _global_semaphore.acquire(blocking=True, timeout=instructions['hold'])
                continue

            if instructions.get('lock', False):
                _global_semaphore.acquire(blocking=True, timeout=instructions['lock'])

            if 'drop' in instructions:
                if instructions['drop'] >= hits:
                    self.drops[task.fileinfo.sha256] = self.drops.get(task.fileinfo.sha256, 0) + 1
                    continue

            if instructions.get('failure', False):
                error = Error(instructions['error'])
                error.sha256 = task.fileinfo.sha256
                self.dispatch_client.service_failed(task.sid, error=error, error_key=get_random_id())
                continue

            partial = False
            temp_data = {entry.name: entry.value for entry in task.temporary_submission_data}
            with print_lock:
                print(self.service_name)
                print('instructions', instructions)
                print('temp', temp_data)
            if 'partial' in instructions:
                partial = True
                requirements = instructions['partial']
                for key, value in requirements.items():
                    if value in temp_data.get(key, ''):
                        partial = False
                    else:
                        partial = True
                        break

            if partial:
                print(self.service_name, "will produce partial results")

            result_data = {
                'archive_ts': None,
                'classification': 'U',
                'response': {
                    'service_version': '0',
                    'service_tool_version': '0',
                    'service_name': self.service_name,
                },
                'result': {},
                'partial': partial,
                'sha256': task.fileinfo.sha256,
                'expiry_ts': time.time() + 600
            }

            result_data.update(instructions.get('result', {}))
            result_data['response'].update(instructions.get('response', {}))

            result = Result(result_data)
            try:
                result_key = instructions['result_key']
            except KeyError:
                result_key = result.build_key(get_random_id())
            self.dispatch_client.service_finished(task.sid, result_key, result,
                                                  temporary_data=instructions.get('temporary_data'))


class CoreSession:
    def __init__(self, config, ingest):
        self.ds: typing.Optional[AssemblylineDatastore] = None
        self.filestore = None
        self.redis = None
        self.config: Config = config
        self.ingest: Ingester = ingest
        self.dispatcher: Dispatcher

    @property
    def ingest_queue(self):
        return self.ingest.ingest_queue


@pytest.fixture(autouse=True)
def log_config(caplog):
    caplog.set_level(logging.INFO, logger='assemblyline')


class MetricsCounter:
    def __init__(self, redis):
        self.redis = redis
        self.channel = None
        self.data = {}

    def clear(self):
        self.data = {}

    def sync_messages(self):
        read = 0
        for metric_message in self.channel.listen(blocking=False):
            if metric_message is None:
                break
            read += 1
            try:
                existing = self.data[metric_message['type']]
            except KeyError:
                existing = self.data[metric_message['type']] = {}

            for key, value in metric_message.items():
                if isinstance(value, (int, float)):
                    existing[key] = existing.get(key, 0) + value
        return read

    def __enter__(self):
        self.channel = forge.get_metrics_sink(self.redis)
        self.sync_messages()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sync_messages()
        for key in list(self.data.keys()):
            shortened = {k: v for k, v in self.data[key].items() if v > 0}
            if shortened:
                self.data[key] = shortened
            else:
                del self.data[key]
        self.channel.close()

        print("Metrics During Test")
        for key, value in self.data.items():
            print(key, value)

    def expect(self, channel, name, value):
        start_time = time.time()
        while time.time() - start_time < RESPONSE_TIMEOUT:
            if channel in self.data:
                if self.data[channel].get(name, 0) >= value:
                    # self.data[channel][name] -= value
                    return

            if self.sync_messages() == 0:
                time.sleep(0.1)
                continue
        pytest.fail(f"Did not get expected metric {name}={value} on metrics channel {channel}")


@pytest.fixture(scope='function')
def metrics(redis):
    with MetricsCounter(redis) as counter:
        yield counter


@pytest.fixture(scope='module')
def core(request, redis, filestore, config, clean_datastore: AssemblylineDatastore):
    # Block logs from being initialized, it breaks under pytest if you create new stream handlers
    from assemblyline.common import log as al_log
    al_log.init_logging = lambda *args: None
    dispatcher.TIMEOUT_EXTRA_TIME = 1
    dispatcher.TIMEOUT_TEST_INTERVAL = 3
    # al_log.init_logging("simulation")

    ds = clean_datastore

    # Register services
    stages = get_service_stage_hash(redis)

    services = []
    for svc, stage, details in [('pre', 'EXTRACT', {'extra_data': True, 'monitored_keys': ['passwords']}), ('core-a', 'CORE', {}), ('core-b', 'CORE', {}), ('finish', 'POST', {'extra_data': True})]:
        ds.service.save(f'{svc}_0', dummy_service(svc, stage, docid=f'{svc}_0', **details))
        ds.service_delta.save(svc, ServiceDelta({
            'name': svc,
            'version': '0',
            'enabled': True
        }))
        stages.set(svc, ServiceStage.Running)
        services.append(MockService(svc, ds, redis, filestore))

    user = random_model_obj(User)
    user.uname = "user"
    ds.user.save("user", user)

    ds.service.commit()
    ds.service_delta.commit()
    ds.user.commit()

    listed_services = ds.list_all_services(full=True)
    assert len(listed_services) == 4

    ingester = Ingester(datastore=ds, redis=redis, persistent_redis=redis, config=config)

    fields = CoreSession(config, ingester)
    fields.redis = redis
    fields.ds = ds

    fields.config = config
    forge.config_cache[None] = fields.config

    threads = []
    fields.filestore = filestore
    fields.dispatcher = Dispatcher(datastore=ds, redis=redis, redis_persist=redis, config=config)
    fields.pre_service = services[0]
    threads: list[ServerBase] = [
        # Start the ingester components
        ingester,

        # Start the dispatcher
        fields.dispatcher,

        # Start plumber
        Plumber(datastore=ds, redis=redis, redis_persist=redis, delay=0.5, config=config),
    ]

    threads = threads + services

    for t in threads:
        t.daemon = True
        t.start()

    def stop_core():
        [tr.stop() for tr in threads]
        [tr.raising_join() for tr in threads]
    request.addfinalizer(stop_core)
    return fields


def ready_body(core, body=None):
    out = {
        'salt': get_random_id(),
    }
    out.update(body or {})
    out = json.dumps(out).encode()
    sha256 = hashlib.sha256()
    sha256.update(out)
    core.filestore.put(sha256.hexdigest(), out)

    with NamedTemporaryFile() as file:
        file.write(out)
        file.flush()
        with forge.get_identify(use_cache=False) as identify:
            fileinfo = identify.fileinfo(file.name)
        core.ds.save_or_freshen_file(sha256.hexdigest(), fileinfo, now_as_iso(500), 'U', redis=core.redis)

    return sha256.hexdigest(), len(out)


def ready_extract(core, children):
    if not isinstance(children, list):
        children = [children]

    body = {
        'pre': {
            'response': {
                'extracted': [{
                    'name': child,
                    'sha256': child,
                    'description': 'abc',
                    'classification': 'U'
                } for child in children]
            }
        }
    }
    return ready_body(core, body)


def test_deduplication(core, metrics):
    global _global_semaphore
    # -------------------------------------------------------------------------------
    # Submit two identical jobs, check that they get deduped by ingester
    sha, size = ready_body(core, {
        'pre': {'lock': 60}
    })

    _global_semaphore = threading.Semaphore(value=0)

    for _ in range(2):
        core.ingest_queue.push(SubmissionInput(dict(
            metadata={},
            params=dict(
                description="file abc123",
                services=dict(selected=''),
                submitter='user',
                groups=['user'],
            ),
            notification=dict(
                queue='output-queue-one',
                threshold=0
            ),
            files=[dict(
                sha256=sha,
                size=size,
                name='abc123'
            )]
        )).as_primitives())

    metrics.expect('ingester', 'duplicates', 1)
    _global_semaphore.release()

    notification_queue = NamedQueue('nq-output-queue-one', core.redis)
    first_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)

    # One of the submission will get processed fully
    assert first_task is not None
    first_task = IngestTask(first_task)
    first_submission: Submission = core.ds.submission.get(first_task.submission.sid)
    assert first_submission.state == 'completed'
    assert len(first_submission.files) == 1
    assert len(first_submission.errors) == 0
    assert len(first_submission.results) == 4

    # The other will get processed as a duplicate
    # (Which one is the 'real' one and which is the duplicate isn't important for our purposes)
    second_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    second_task = IngestTask(second_task)
    assert second_task.submission.sid == first_task.submission.sid

    # -------------------------------------------------------------------------------
    # Submit the same body, but change a parameter so the cache key misses,
    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=''),
            submitter='user',
            groups=['user'],
            max_extracted=10000
        ),
        notification=dict(
            queue='2',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())
    _global_semaphore.release()

    notification_queue = NamedQueue('nq-2', core.redis)
    third_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    assert third_task

    # The third task should not be deduplicated by ingester, so will have a different submission
    third_task = IngestTask(third_task)
    third_submission: Submission = core.ds.submission.get(third_task.submission.sid)
    assert third_submission.state == 'completed'
    assert first_submission.sid != third_submission.sid
    assert len(third_submission.files) == 1
    assert len(third_submission.results) == 4

    metrics.expect('ingester', 'submissions_ingested', 3)
    metrics.expect('ingester', 'submissions_completed', 2)
    metrics.expect('ingester', 'files_completed', 2)
    metrics.expect('dispatcher', 'submissions_completed', 2)
    metrics.expect('dispatcher', 'files_completed', 2)


def test_ingest_retry(core: CoreSession, metrics):
    # -------------------------------------------------------------------------------
    #
    sha, size = ready_body(core)
    original_retry_delay = assemblyline_core.ingester.ingester._retry_delay
    assemblyline_core.ingester.ingester._retry_delay = 1

    attempts = []
    failures = []
    original_submit = core.ingest.submit

    def fail_once(task):
        attempts.append(task)
        if len(attempts) > 1:
            original_submit(task)
        else:
            failures.append(task)
            raise ValueError()
    core.ingest.submit = fail_once

    try:
        core.ingest_queue.push(SubmissionInput(dict(
            metadata={},
            params=dict(
                description="file abc123",
                services=dict(selected=''),
                submitter='user',
                groups=['user'],
            ),
            notification=dict(
                queue='output-queue-one',
                threshold=0
            ),
            files=[dict(
                sha256=sha,
                size=size,
                name='abc123'
            )]
        )).as_primitives())

        notification_queue = NamedQueue('nq-output-queue-one', core.redis)
        first_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)

        # One of the submission will get processed fully
        assert first_task is not None
        first_task = IngestTask(first_task)
        first_submission: Submission = core.ds.submission.get(first_task.submission.sid)
        assert len(attempts) == 2
        assert len(failures) == 1
        assert first_submission.state == 'completed'
        assert len(first_submission.files) == 1
        assert len(first_submission.errors) == 0
        assert len(first_submission.results) == 4

        metrics.expect('ingester', 'submissions_ingested', 1)
        metrics.expect('ingester', 'submissions_completed', 1)
        metrics.expect('ingester', 'files_completed', 1)
        metrics.expect('ingester', 'duplicates', 0)
        metrics.expect('dispatcher', 'submissions_completed', 1)
        metrics.expect('dispatcher', 'files_completed', 1)

    finally:
        core.ingest.submit = original_submit
        assemblyline_core.ingester.ingester._retry_delay = original_retry_delay


def test_ingest_timeout(core: CoreSession, metrics: MetricsCounter):
    # -------------------------------------------------------------------------------
    #
    sha, size = ready_body(core)
    original_max_time = assemblyline_core.ingester.ingester._max_time
    assemblyline_core.ingester.ingester._max_time = 1

    attempts = []
    original_submit = core.ingest.submit_client.submit

    def _fail(**args):
        attempts.append(args)
    core.ingest.submit_client.submit = _fail

    try:
        si = SubmissionInput(dict(
            metadata={},
            params=dict(
                description="file abc123",
                services=dict(selected=''),
                submitter='user',
                groups=['user'],
            ),
            notification=dict(
                queue='ingest-timeout',
                threshold=0
            ),
            files=[dict(
                sha256=sha,
                size=size,
                name='abc123'
            )]
        ))
        core.ingest_queue.push(si.as_primitives())

        sha256 = si.files[0].sha256
        scan_key = si.params.create_filescore_key(sha256)

        # Make sure the scanning table has been cleared
        time.sleep(0.5)
        for _ in range(60):
            if not core.ingest.scanning.exists(scan_key):
                break
            time.sleep(0.1)
        assert not core.ingest.scanning.exists(scan_key)
        assert len(attempts) == 1

        # Wait until we get feedback from the metrics channel
        metrics.expect('ingester', 'submissions_ingested', 1)
        metrics.expect('ingester', 'timed_out', 1)

    finally:
        core.ingest.submit_client.submit = original_submit
        assemblyline_core.ingester.ingester._max_time = original_max_time


def test_service_crash_recovery(core, metrics):
    # This time have the service 'crash'
    sha, size = ready_body(core, {
        'pre': {'drop': 1}
    })

    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=''),
            submitter='user',
            groups=['user'],
            max_extracted=10000
        ),
        notification=dict(
            queue='watcher-recover',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())

    notification_queue = NamedQueue('nq-watcher-recover', core.redis)
    dropped_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    assert dropped_task
    dropped_task = IngestTask(dropped_task)
    sub = core.ds.submission.get(dropped_task.submission.sid)
    assert len(sub.errors) == 0  # No error raised if the service succeeds on retry
    assert len(sub.results) == 4
    assert core.pre_service.drops[sha] == 1
    assert core.pre_service.hits[sha] == 2

    # Wait until we get feedback from the metrics channel
    metrics.expect('ingester', 'submissions_ingested', 1)
    metrics.expect('ingester', 'submissions_completed', 1)
    metrics.expect('service', 'fail_recoverable', 1)
    metrics.expect('dispatcher', 'service_timeouts', 1)
    metrics.expect('dispatcher', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'files_completed', 1)


def test_service_retry_limit(core, metrics):
    # This time have the service 'crash'
    sha, size = ready_body(core, {
        'pre': {'drop': 3}
    })

    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=''),
            submitter='user',
            groups=['user'],
            max_extracted=10000
        ),
        notification=dict(
            queue='watcher-recover',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())

    notification_queue = NamedQueue('nq-watcher-recover', core.redis)
    dropped_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    assert dropped_task
    dropped_task = IngestTask(dropped_task)
    sub = core.ds.submission.get(dropped_task.submission.sid)
    assert len(sub.errors) == 1
    assert len(sub.results) == 3
    assert core.pre_service.drops[sha] == 3
    assert core.pre_service.hits[sha] == 3

    # Wait until we get feedback from the metrics channel
    metrics.expect('ingester', 'submissions_ingested', 1)
    metrics.expect('ingester', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'service_timeouts', 3)
    metrics.expect('service', 'fail_recoverable', 3)
    metrics.expect('service', 'fail_nonrecoverable', 1)
    metrics.expect('dispatcher', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'files_completed', 1)


def test_dropping_early(core, metrics):
    # -------------------------------------------------------------------------------
    # This time have a file get marked for dropping by a service
    sha, size = ready_body(core, {
        'pre': {'result': {'drop_file': True}}
    })

    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=''),
            submitter='user',
            groups=['user'],
            max_extracted=10000
        ),
        notification=dict(
            queue='drop',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())

    notification_queue = NamedQueue('nq-drop', core.redis)
    dropped_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    dropped_task = IngestTask(dropped_task)
    sub = core.ds.submission.get(dropped_task.submission.sid)
    assert len(sub.files) == 1
    assert len(sub.results) == 1

    metrics.expect('ingester', 'submissions_ingested', 1)
    metrics.expect('ingester', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'files_completed', 1)


def test_service_error(core, metrics):
    # -------------------------------------------------------------------------------
    # Have a service produce an error
    # -------------------------------------------------------------------------------
    # This time have a file get marked for dropping by a service
    sha, size = ready_body(core, {
        'core-a': {
            'error': {
                'archive_ts': None,
                'sha256': 'a'*64,
                'response': {
                    'message': 'words',
                    'status': 'FAIL_NONRECOVERABLE',
                    'service_name': 'core-a',
                    'service_tool_version': 0,
                    'service_version': '0'
                },
                'expiry_ts': time.time() + 500
            },
            'failure': True,
        }
    })

    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=''),
            submitter='user',
            groups=['user'],
            max_extracted=10000
        ),
        notification=dict(
            queue='error',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())

    notification_queue = NamedQueue('nq-error', core.redis)
    task = IngestTask(notification_queue.pop(timeout=RESPONSE_TIMEOUT))
    sub = core.ds.submission.get(task.submission.sid)
    assert len(sub.files) == 1
    assert len(sub.results) == 3
    assert len(sub.errors) == 1

    metrics.expect('ingester', 'submissions_ingested', 1)
    metrics.expect('ingester', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'files_completed', 1)


def test_extracted_file(core, metrics):
    sha, size = ready_extract(core, ready_body(core)[0])

    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=''),
            submitter='user',
            groups=['user'],
            max_extracted=10000
        ),
        notification=dict(
            queue='text-extracted-file',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())

    notification_queue = NamedQueue('nq-text-extracted-file', core.redis)
    task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    assert task
    task = IngestTask(task)
    sub = core.ds.submission.get(task.submission.sid)
    assert len(sub.files) == 1
    assert len(sub.results) == 8
    assert len(sub.errors) == 0

    metrics.expect('ingester', 'submissions_ingested', 1)
    metrics.expect('ingester', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'files_completed', 2)


def test_depth_limit(core, metrics):
    # Make a nested set of files that goes deeper than the max depth by one
    sha, size = ready_body(core)
    for _ in range(core.config.submission.max_extraction_depth + 1):
        sha, size = ready_extract(core, sha)

    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=''),
            submitter='user',
            groups=['user'],
            # Make sure we can extract enough files that we will definitely hit the depth limit first
            max_extracted=core.config.submission.max_extraction_depth + 10
        ),
        notification=dict(
            queue='test-depth-limit',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())

    notification_queue = NamedQueue('nq-test-depth-limit', core.redis)
    start = time.time()
    task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    print("notification time waited", time.time() - start)
    assert task is not None
    task = IngestTask(task)
    sub: Submission = core.ds.submission.get(task.submission.sid)
    assert len(sub.files) == 1
    # We should only get results for each file up to the max depth
    assert len(sub.results) == 4 * core.config.submission.max_extraction_depth
    assert len(sub.errors) == 1

    metrics.expect('ingester', 'submissions_ingested', 1)
    metrics.expect('ingester', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'files_completed', core.config.submission.max_extraction_depth)


def test_max_extracted_in_one(core, metrics):
    # Make a set of files that is bigger than max_extracted (3 in this case)
    children = [ready_body(core)[0] for _ in range(5)]
    sha, size = ready_extract(core, children)
    max_extracted = 3

    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=''),
            submitter='user',
            groups=['user'],
            max_extracted=max_extracted
        ),
        notification=dict(
            queue='test-extracted-in-one',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())

    notification_queue = NamedQueue('nq-test-extracted-in-one', core.redis)
    start = time.time()
    task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    print("notification time waited", time.time() - start)
    assert task is not None
    task = IngestTask(task)
    sub: Submission = core.ds.submission.get(task.submission.sid)
    assert len(sub.files) == 1
    # We should only get results for each file up to the max depth
    assert len(sub.results) == 4 * (1 + 3)
    assert len(sub.errors) == 2  # The number of children that errored out

    metrics.expect('ingester', 'submissions_ingested', 1)
    metrics.expect('ingester', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'files_completed', max_extracted + 1)


def test_max_extracted_in_several(core, metrics):
    # Make a set of in a non trivial tree, that add up to more than 3 (max_extracted) files
    children = [
        ready_extract(core, [ready_body(core)[0], ready_body(core)[0]])[0],
        ready_extract(core, [ready_body(core)[0], ready_body(core)[0]])[0]
    ]
    sha, size = ready_extract(core, children)

    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=''),
            submitter='user',
            groups=['user'],
            max_extracted=3
        ),
        notification=dict(
            queue='test-extracted-in-several',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())

    notification_queue = NamedQueue('nq-test-extracted-in-several', core.redis)
    task = IngestTask(notification_queue.pop(timeout=RESPONSE_TIMEOUT))
    sub: Submission = core.ds.submission.get(task.submission.sid)
    assert len(sub.files) == 1
    # We should only get results for each file up to the max depth
    assert len(sub.results) == 4 * (1 + 3)  # 4 services, 1 original file, 3 extracted files
    assert len(sub.errors) == 3  # The number of children that errored out

    metrics.expect('ingester', 'submissions_ingested', 1)
    metrics.expect('ingester', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'files_completed', 4)


def test_caching(core: CoreSession, metrics):
    sha, size = ready_body(core)

    def run_once():
        core.ingest_queue.push(SubmissionInput(dict(
            metadata={},
            params=dict(
                description="file abc123",
                services=dict(selected=''),
                submitter='user',
                groups=['user'],
            ),
            notification=dict(
                queue='1',
                threshold=0
            ),
            files=[dict(
                sha256=sha,
                size=size,
                name='abc123'
            )]
        )).as_primitives())

        notification_queue = NamedQueue('nq-1', core.redis)
        first_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)

        # One of the submission will get processed fully
        assert first_task is not None
        first_task = IngestTask(first_task)
        first_submission: Submission = core.ds.submission.get(first_task.submission.sid)
        assert first_submission.state == 'completed'
        assert len(first_submission.files) == 1
        assert len(first_submission.errors) == 0
        assert len(first_submission.results) == 4
        return first_submission.sid

    sid1 = run_once()
    metrics.expect('ingester', 'cache_miss', 1)
    metrics.clear()

    sid2 = run_once()
    metrics.expect('ingester', 'cache_hit_local', 1)
    metrics.clear()
    assert sid1 == sid2

    core.ingest.cache = {}

    sid3 = run_once()
    metrics.expect('ingester', 'cache_hit', 1)
    metrics.clear()
    assert sid1 == sid3


def test_plumber_clearing(core, metrics):
    global _global_semaphore
    _global_semaphore = threading.Semaphore(value=0)
    start = time.time()

    try:
        # Have the plumber cancel tasks
        sha, size = ready_body(core, {
            'pre': {'hold': 60}
        })

        core.ingest_queue.push(SubmissionInput(dict(
            metadata={},
            params=dict(
                description="file abc123",
                services=dict(selected=''),
                submitter='user',
                groups=['user'],
                max_extracted=10000
            ),
            notification=dict(
                queue='test_plumber_clearing',
                threshold=0
            ),
            files=[dict(
                sha256=sha,
                size=size,
                name='abc123'
            )]
        )).as_primitives())

        metrics.expect('ingester', 'submissions_ingested', 1)
        service_queue = get_service_queue('pre', core.redis)

        start = time.time()
        while service_queue.length() < 1:
            if time.time() - start > RESPONSE_TIMEOUT:
                pytest.fail(f'Found { service_queue.length()}')
            time.sleep(0.1)

        service_delta = core.ds.service_delta.get('pre')
        service_delta['enabled'] = False
        core.ds.service_delta.save('pre', service_delta)

        notification_queue = NamedQueue('nq-test_plumber_clearing', core.redis)
        dropped_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
        dropped_task = IngestTask(dropped_task)
        sub = core.ds.submission.get(dropped_task.submission.sid)
        assert len(sub.files) == 1
        assert len(sub.results) == 3
        assert len(sub.errors) == 1
        error = core.ds.error.get(sub.errors[0])
        assert "disabled" in error.response.message

        metrics.expect('ingester', 'submissions_completed', 1)
        metrics.expect('dispatcher', 'submissions_completed', 1)
        metrics.expect('dispatcher', 'files_completed', 1)
        metrics.expect('service', 'fail_recoverable', 1)

    finally:
        _global_semaphore.release()
        service_delta = core.ds.service_delta.get('pre')
        service_delta['enabled'] = True
        core.ds.service_delta.save('pre', service_delta)


def test_filter(core: CoreSession, metrics):
    from assemblyline.common.postprocess import SubmissionFilter, PostprocessAction
    filter_string = "params.submitter: user"
    core.dispatcher.postprocess_worker.actions['test_process'] = \
        SubmissionFilter(filter_string), PostprocessAction({
            'enabled': True,
            'raise_alert': True,
            'filter': filter_string,
        })

    try:
        sha, size = ready_extract(core, ready_body(core)[0])

        core.ingest_queue.push(SubmissionInput(dict(
            metadata={},
            params=dict(
                description="file abc123",
                services=dict(selected=''),
                submitter='user',
                groups=['user'],
                max_extracted=10000,
                generate_alert=True,
            ),
            notification=dict(
                queue='text-filter',
                threshold=0
            ),
            files=[dict(
                sha256=sha,
                size=size,
                name='abc123'
            )]
        )).as_primitives())

        notification_queue = NamedQueue('nq-text-filter', core.redis)
        task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
        assert task
        task = IngestTask(task)
        sub = core.ds.submission.get(task.submission.sid)
        assert len(sub.files) == 1
        assert len(sub.results) == 8
        assert len(sub.errors) == 0

        metrics.expect('ingester', 'submissions_ingested', 1)
        metrics.expect('ingester', 'submissions_completed', 1)
        metrics.expect('dispatcher', 'submissions_completed', 1)
        metrics.expect('dispatcher', 'files_completed', 2)

        alert = core.dispatcher.postprocess_worker.alert_queue.pop(timeout=5)
        assert alert['submission']['sid'] == sub['sid']

    finally:
        core.dispatcher.postprocess_worker.actions.pop('test_process')


def test_tag_filter(core: CoreSession, metrics):
    from assemblyline.common.postprocess import SubmissionFilter, PostprocessAction
    filter_string = "tags.file.behavior: exist"
    core.dispatcher.postprocess_worker.actions['test_process'] = \
        SubmissionFilter(filter_string), PostprocessAction({
            'enabled': True,
            'raise_alert': True,
            'filter': filter_string,
        })

    try:
        sha, size = ready_body(core, {
            'pre': {'result': {'result': {
                'sections': [
                    {
                        'body': 'info',
                        'body_format': 'TEXT',
                        'classification': 'U',
                        'depth': 0,
                        'tags': {
                            'file': {
                                'behavior': ['exist']
                            }
                        },
                        'title_text': 'title'
                    }
                ]
            }}}
        })

        core.ingest_queue.push(SubmissionInput(dict(
            metadata={},
            params=dict(
                description="file abc123",
                services=dict(selected=''),
                submitter='user',
                groups=['user'],
                max_extracted=10000,
                generate_alert=True,
            ),
            notification=dict(
                queue='tag-filter',
                threshold=0
            ),
            files=[dict(
                sha256=sha,
                size=size,
                name='abc123'
            )]
        )).as_primitives())

        notification_queue = NamedQueue('nq-tag-filter', core.redis)
        task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
        assert task
        task = IngestTask(task)
        sub = core.ds.submission.get(task.submission.sid)
        assert len(sub.files) == 1
        assert len(sub.results) == 4
        assert len(sub.errors) == 0

        metrics.expect('ingester', 'submissions_ingested', 1)
        metrics.expect('ingester', 'submissions_completed', 1)
        metrics.expect('dispatcher', 'submissions_completed', 1)
        metrics.expect('dispatcher', 'files_completed', 1)

        alert: dict = core.dispatcher.postprocess_worker.alert_queue.pop(timeout=5)
        assert alert['submission']['sid'] == sub['sid']

    finally:
        core.dispatcher.postprocess_worker.actions.pop('test_process')


def test_partial(core: CoreSession, metrics):
    # Have pre produce a partial result, then have core-a update a monitored key
    sha, size = ready_body(core, {
        'pre': {'partial': {'passwords': 'test_temp_data_monitoring'}},
    })

    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=[]),
            submitter='user',
            groups=['user'],
            max_extracted=10000
        ),
        notification=dict(
            queue='temp-data-monitor',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())

    notification_queue = NamedQueue('nq-temp-data-monitor', core.redis)
    dropped_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    assert dropped_task
    dropped_task = IngestTask(dropped_task)
    sub: Submission = core.ds.submission.get(dropped_task.submission.sid)
    assert len(sub.errors) == 0
    assert len(sub.results) == 4, 'results'
    assert core.pre_service.hits[sha] == 1, 'pre_service.hits'

    # Wait until we get feedback from the metrics channel
    metrics.expect('ingester', 'submissions_ingested', 1)
    metrics.expect('ingester', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'files_completed', 1)

    partial_results = 0
    for res in sub.results:
        result = core.ds.get_single_result(res, as_obj=True)
        assert result is not None, res
        if result.partial:
            partial_results += 1
    assert partial_results == 1, 'partial_results'


def test_temp_data_monitoring(core: CoreSession, metrics):
    # Have pre produce a partial result, then have core-a update a monitored key
    sha, size = ready_body(core, {
        'pre': {'partial': {'passwords': 'test_temp_data_monitoring'}},
        'core-a': {'temporary_data': {'passwords': ['test_temp_data_monitoring']}},
        'final': {'temporary_data': {'passwords': ['some other password']}},
    })

    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=[]),
            submitter='user',
            groups=['user'],
            max_extracted=10000
        ),
        notification=dict(
            queue='temp-data-monitor',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())

    notification_queue = NamedQueue('nq-temp-data-monitor', core.redis)
    dropped_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    assert dropped_task
    dropped_task = IngestTask(dropped_task)
    sub: Submission = core.ds.submission.get(dropped_task.submission.sid)
    assert len(sub.errors) == 0
    assert len(sub.results) == 4, 'results'
    assert core.pre_service.hits[sha] == 2, 'pre_service.hits'

    # Wait until we get feedback from the metrics channel
    metrics.expect('ingester', 'submissions_ingested', 1)
    metrics.expect('ingester', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'files_completed', 1)

    partial_results = 0
    for res in sub.results:
        result = core.ds.get_single_result(res, as_obj=True)
        assert result is not None, res
        if result.partial:
            partial_results += 1
    assert partial_results == 0, 'partial_results'


def test_complex_extracted(core: CoreSession, metrics):
    # stages to this processing when everything goes well
    # 1. extract a file that will process to produce a partial result
    # 2. hold a few seconds on the second stage of the root file to let child start
    # 3. on the last stage of the root file produce the password
    dispatcher.TIMEOUT_EXTRA_TIME = 10

    child_sha, _ = ready_body(core, {
        'pre': {'partial': {'passwords': 'test_temp_data_monitoring'}},
    })

    sha, size = ready_body(core, {
        'pre': {
            'response': {
                'extracted': [{
                    'name': child_sha,
                    'sha256': child_sha,
                    'description': 'abc',
                    'classification': 'U'
                }]
            }
        },
        'core-a': {'lock': 5},
        'finish': {'temporary_data': {'passwords': ['test_temp_data_monitoring']}},
    })

    core.ingest_queue.push(SubmissionInput(dict(
        metadata={},
        params=dict(
            description="file abc123",
            services=dict(selected=''),
            submitter='user',
            groups=['user'],
            max_extracted=10000
        ),
        notification=dict(
            queue='complex-extracted-file',
            threshold=0
        ),
        files=[dict(
            sha256=sha,
            size=size,
            name='abc123'
        )]
    )).as_primitives())

    # Wait for the extract file to finish
    metrics.expect('dispatcher', 'files_completed', 1)
    _global_semaphore.release()

    # Wait for the entire submission to finish
    notification_queue = NamedQueue('nq-complex-extracted-file', core.redis)
    dropped_task = notification_queue.pop(timeout=RESPONSE_TIMEOUT)
    assert dropped_task
    dropped_task = IngestTask(dropped_task)
    sub: Submission = core.ds.submission.get(dropped_task.submission.sid)
    assert len(sub.errors) == 0
    assert len(sub.results) == 8, 'results'
    assert core.pre_service.hits[sha] == 1, 'pre_service.hits[root]'
    assert core.pre_service.hits[child_sha] >= 2, 'pre_service.hits[child]'

    # Wait until we get feedback from the metrics channel
    metrics.expect('ingester', 'submissions_ingested', 1)
    metrics.expect('ingester', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'submissions_completed', 1)
    metrics.expect('dispatcher', 'files_completed', 2)

    partial_results = 0
    for res in sub.results:
        result = core.ds.get_single_result(res, as_obj=True)
        assert result is not None, res
        if result.partial:
            partial_results += 1
    assert partial_results == 0, 'partial_results'
