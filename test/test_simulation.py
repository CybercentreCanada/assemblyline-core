"""
A test of ingest+dispatch running in one process.

Needs the datastore and filestore to be running, otherwise these test are stand alone.
"""

import hashlib
import json
import typing
import time
import threading
import logging
from tempfile import NamedTemporaryFile
from typing import List

import pytest

from assemblyline.common import forge, identify
from assemblyline.common.forge import get_service_queue
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.uid import get_random_id
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.models.config import Config
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.result import Result
from assemblyline.odm.models.service import Service
from assemblyline.odm.models.submission import Submission
from assemblyline.odm.messages.submission import Submission as SubmissionInput
from assemblyline.remote.datatypes.queues.named import NamedQueue

import assemblyline_core
from assemblyline_core.plumber.run_plumber import Plumber
from assemblyline_core.dispatching.client import DispatchClient
from assemblyline_core.dispatching.run_files import FileDispatchServer
from assemblyline_core.dispatching.run_submissions import SubmissionDispatchServer
from assemblyline_core.ingester.ingester import IngestTask
from assemblyline_core.ingester.run_ingest import IngesterInput
from assemblyline_core.ingester.run_internal import IngesterInternals
from assemblyline_core.ingester.run_submit import IngesterSubmitter
from assemblyline_core.server_base import ServerBase, get_service_stage_hash, ServiceStage
from assemblyline_core.watcher.server import WatcherServer

from .mocking import MockCollection
from .test_scheduler import dummy_service


@pytest.fixture(scope='module')
def redis(redis_connection):
    redis_connection.flushdb()
    yield redis_connection
    redis_connection.flushdb()


_global_semaphore = threading.Semaphore()


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
            print(self.service_name, 'has received a job', task.sid)

            file = self.filestore.get(task.fileinfo.sha256)

            instructions = json.loads(file)
            instructions = instructions.get(self.service_name, {})
            print(self.service_name, 'following instruction:', instructions)
            hits = self.hits[task.fileinfo.sha256] = self.hits.get(task.fileinfo.sha256, 0) + 1

            if instructions.get('semaphore', False):
                _global_semaphore.acquire(blocking=True, timeout=instructions['semaphore'])
                continue

            if 'drop' in instructions:
                if instructions['drop'] >= hits:
                    self.drops[task.fileinfo.sha256] = self.drops.get(task.fileinfo.sha256, 0) + 1
                    continue

            if instructions.get('failure', False):
                error = Error(instructions['error'])
                error.sha256 = task.fileinfo.sha256
                self.dispatch_client.service_failed(task.sid, error=error,
                                                    error_key=get_random_id())
                continue

            result_data = {
                'archive_ts': time.time() + 300,
                'classification': 'U',
                'response': {
                    'service_version': '0',
                    'service_tool_version': '0',
                    'service_name': self.service_name,
                },
                'result': {
                },
                'sha256': task.fileinfo.sha256,
                'expiry_ts': time.time() + 600
            }

            result_data.update(instructions.get('result', {}))
            result_data['response'].update(instructions.get('response', {}))

            result = Result(result_data)
            result_key = instructions.get('result_key', get_random_id())
            self.dispatch_client.service_finished(task.sid, result_key, result)


class CoreSession:
    def __init__(self):
        self.ds: typing.Optional[AssemblylineDatastore] = None
        self.filestore = None
        self.redis = None
        self.config: typing.Optional[Config] = None
        self.ingest: typing.Optional[IngesterInput] = None
        self.ingest_submit: typing.Optional[IngesterSubmitter] = None
        self.ingest_internals: typing.Optional[IngesterInternals] = None
        self.ingest_queue = None


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

        print("Metrics During Test")
        for key, value in self.data.items():
            print(key, value)

    def expect(self, channel, name, value):
        start_time = time.time()
        while time.time() - start_time < 10:
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
def core(request, redis, filestore, config):
    # Block logs from being initialized, it breaks under pytest if you create new stream handlers
    from assemblyline.common import log as al_log
    al_log.init_logging = lambda *args: None
    # al_log.init_logging("simulation")

    fields = CoreSession()
    fields.redis = redis
    fields.ds = ds = forge.get_datastore()

    fields.config = config
    forge.config_singletons[False, None] = fields.config

    threads = []
    fields.filestore = filestore
    threads: List[ServerBase] = [
        # Start the ingester components
        IngesterInput(datastore=ds, redis=redis, persistent_redis=redis),
        IngesterSubmitter(datastore=ds, redis=redis, persistent_redis=redis),
        IngesterInternals(datastore=ds, redis=redis, persistent_redis=redis),

        # Start the dispatcher
        FileDispatchServer(datastore=ds, redis=redis, redis_persist=redis),
        SubmissionDispatchServer(datastore=ds, redis=redis, redis_persist=redis),

        # Start plumber
        Plumber(datastore=ds, redis=redis, redis_persist=redis, delay=0.5),
    ]

    stages = get_service_stage_hash(redis)
    ingester_input_thread = typing.cast(IngesterInput, threads[0])
    fields.ingest = ingester_input_thread
    fields.ingest_queue = ingester_input_thread.ingester.ingest_queue
    fields.ingest_submit = threads[1]
    fields.ingest_internals = threads[2]

    ds.ds.service = MockCollection(Service)
    ds.ds.service_delta = MockCollection(Service)
    ds.service.save('pre_0', dummy_service('pre', 'EXTRACT'))
    ds.service_delta.save('pre', dummy_service('pre', 'EXTRACT'))
    stages.set('pre', ServiceStage.Running)

    threads.append(MockService('pre', ds, redis, filestore))
    fields.pre_service = threads[-1]
    ds.service.save('core-a_0', dummy_service('core-a', 'CORE'))
    ds.service_delta.save('core-a', dummy_service('core-a', 'CORE'))
    stages.set('core-a', ServiceStage.Running)

    threads.append(MockService('core-a', ds, redis, filestore))
    ds.service.save('core-b_0', dummy_service('core-b', 'CORE'))
    ds.service_delta.save('core-b', dummy_service('core-b', 'CORE'))
    threads.append(MockService('core-b', ds, redis, filestore))
    stages.set('core-b', ServiceStage.Running)

    ds.service.save('finish_0', dummy_service('finish', 'POST'))
    ds.service_delta.save('finish', dummy_service('finish', 'POST'))
    threads.append(MockService('finish', ds, redis, filestore))
    stages.set('finish', ServiceStage.Running)

    for t in threads:
        t.daemon = True
        t.start()

    def stop_core():
        [tr.close() for tr in threads]
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
    # -------------------------------------------------------------------------------
    # Submit two identical jobs, check that they get deduped by ingester
    sha, size = ready_body(core)

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

    notification_queue = NamedQueue('nq-output-queue-one', core.redis)
    first_task = notification_queue.pop(timeout=5)

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
    second_task = notification_queue.pop(timeout=5)
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

    notification_queue = NamedQueue('nq-2', core.redis)
    third_task = notification_queue.pop(timeout=5)
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
    metrics.expect('ingester', 'duplicates', 1)
    metrics.expect('dispatcher', 'submissions_completed', 2)
    metrics.expect('dispatcher', 'files_completed', 2)


def test_ingest_retry(core, metrics):
    # -------------------------------------------------------------------------------
    #
    sha, size = ready_body(core)
    original_retry_delay = assemblyline_core.ingester.ingester._retry_delay
    assemblyline_core.ingester.ingester._retry_delay = 1

    attempts = []
    failures = []
    original_submit = core.ingest_submit.ingester.submit

    def fail_once(task):
        attempts.append(task)
        if len(attempts) > 1:
            original_submit(task)
        else:
            failures.append(task)
            raise ValueError()
    core.ingest_submit.ingester.submit = fail_once

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
        first_task = notification_queue.pop(timeout=30)

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
        core.ingest_submit.ingester.submit = original_submit
        assemblyline_core.ingester.ingester._retry_delay = original_retry_delay


def test_ingest_timeout(core, metrics):
    # -------------------------------------------------------------------------------
    #
    sha, size = ready_body(core)
    original_max_time = assemblyline_core.ingester.ingester._max_time
    assemblyline_core.ingester.ingester._max_time = 1

    attempts = []
    original_submit = core.ingest_submit.ingester.submit_client.submit

    def _fail(**args):
        attempts.append(args)
    core.ingest_submit.ingester.submit_client.submit = _fail

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
            if not core.ingest.ingester.scanning.exists(scan_key):
                break
            time.sleep(0.1)
        assert not core.ingest.ingester.scanning.exists(scan_key)
        assert len(attempts) == 1

        # Wait until we get feedback from the metrics channel
        metrics.expect('ingester', 'submissions_ingested', 1)
        metrics.expect('ingester', 'timed_out', 1)

    finally:
        core.ingest_submit.ingester.submit_client.submit = original_submit
        assemblyline_core.ingester.ingester._max_time = original_max_time


def test_watcher_recovery(core, metrics):
    watch = WatcherServer(redis=core.redis, redis_persist=core.redis)
    watch.start()
    try:
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
        dropped_task = notification_queue.pop(timeout=16)
        assert dropped_task
        dropped_task = IngestTask(dropped_task)
        sub = core.ds.submission.get(dropped_task.submission.sid)
        assert len(sub.errors) == 0
        assert len(sub.results) == 4
        assert core.pre_service.drops[sha] == 1
        assert core.pre_service.hits[sha] == 2

        # Wait until we get feedback from the metrics channel
        metrics.expect('ingester', 'submissions_ingested', 1)
        metrics.expect('ingester', 'submissions_completed', 1)
        metrics.expect('watcher', 'expired', 1)
        metrics.expect('service', 'fail_recoverable', 1)
        metrics.expect('dispatcher', 'submissions_completed', 1)
        metrics.expect('dispatcher', 'files_completed', 1)

    finally:
        watch.stop()
        watch.join()


def test_service_retry_limit(core, metrics):
    watch = WatcherServer(redis=core.redis, redis_persist=core.redis)
    watch.start()
    try:
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
        dropped_task = notification_queue.pop(timeout=16)
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
        metrics.expect('watcher', 'expired', 3)
        metrics.expect('service', 'fail_recoverable', 3)
        metrics.expect('service', 'fail_nonrecoverable', 1)
        metrics.expect('dispatcher', 'submissions_completed', 1)
        metrics.expect('dispatcher', 'files_completed', 1)

    finally:
        watch.stop()
        watch.join()


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
    dropped_task = notification_queue.pop(timeout=5)
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
                'archive_ts': time.time() + 250,
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
    task = IngestTask(notification_queue.pop(timeout=5))
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
    task = notification_queue.pop(timeout=5)
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
    task = notification_queue.pop(timeout=10)
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
    task = notification_queue.pop(timeout=10)
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
    task = IngestTask(notification_queue.pop(timeout=10))
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
        first_task = notification_queue.pop(timeout=5)

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

    core.ingest.ingester.cache = {}

    sid3 = run_once()
    metrics.expect('ingester', 'cache_hit', 1)
    metrics.clear()
    assert sid1 == sid3


def test_plumber_clearing(core, metrics):
    global _global_semaphore
    _global_semaphore = threading.Semaphore(value=0)

    start = time.time()
    watch = WatcherServer(redis=core.redis, redis_persist=core.redis)
    watch.start()

    try:
        # Have the plumber cancel tasks
        sha, size = ready_body(core, {
            'pre': {'semaphore': 60}
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

        service_queue = get_service_queue('pre', core.redis)
        time.sleep(0.5)
        while service_queue.length() == 0 and time.time() - start < 20:
            time.sleep(0.1)

        service_delta = core.ds.service_delta.get('pre')
        service_delta['enabled'] = False
        core.ds.service_delta.save('pre', service_delta)

        notification_queue = NamedQueue('nq-test_plumber_clearing', core.redis)
        dropped_task = notification_queue.pop(timeout=5)
        dropped_task = IngestTask(dropped_task)
        sub = core.ds.submission.get(dropped_task.submission.sid)
        assert len(sub.files) == 1
        assert len(sub.results) == 3
        assert len(sub.errors) == 1

        error = core.ds.error.get(sub.errors[0])
        assert "disabled" in error.response.message

        metrics.expect('ingester', 'submissions_ingested', 1)
        metrics.expect('ingester', 'submissions_completed', 1)
        metrics.expect('dispatcher', 'submissions_completed', 1)
        metrics.expect('dispatcher', 'files_completed', 1)
        metrics.expect('watcher', 'expired', 1)
        metrics.expect('service', 'fail_recoverable', 1)

    finally:
        _global_semaphore.release()
        service_delta = core.ds.service_delta.get('pre')
        service_delta['enabled'] = True
        core.ds.service_delta.save('pre', service_delta)
        watch.stop()
        watch.join()
