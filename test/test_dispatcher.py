import logging
import time
from unittest import mock

import json
import pytest

from assemblyline.common.forge import get_service_queue, get_classification
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.file import File
from assemblyline.odm.models.result import Result
from assemblyline.odm.randomizer import random_model_obj, random_minimal_obj, get_random_hash
from assemblyline.odm import models
from assemblyline.common.metrics import MetricsFactory

from assemblyline_core.dispatching.client import DispatchClient
from assemblyline_core.dispatching.dispatcher import Dispatcher, Submission
from assemblyline_core.dispatching.schedules import Scheduler as RealScheduler

# noinspection PyUnresolvedReferences
from assemblyline_core.dispatching.timeout import TimeoutTable
from mocking import ToggleTrue
from test_scheduler import dummy_service


logger = logging.getLogger('assemblyline.test')


class DRPScheduler(RealScheduler):
    def __init__(self, *args, **kwargs):
        pass

    def build_schedule(self, *args):
        return [
            {'extract': ''},
            {'sandbox': ''},
        ]

    @property
    def services(self):
        return {
            'extract': dummy_service('extract', 'pre', extra_data=True),
            'sandbox': dummy_service('sandbox', 'core', 'Dynamic Analysis')
        }


class Scheduler(RealScheduler):
    def __init__(self, *args, **kwargs):
        pass

    def build_schedule(self, *args):
        return [
            {'extract': '', 'wrench': ''},
            {'av-a': '', 'av-b': '', 'frankenstrings': ''},
            {'xerox': ''}
        ]

    @property
    def services(self):
        return {
            'extract': dummy_service('extract', 'pre', extra_data=True),
            'wrench': dummy_service('wrench', 'pre'),
            'av-a': dummy_service('av-a', 'core'),
            'av-b': dummy_service('av-b', 'core'),
            'frankenstrings': dummy_service('frankenstrings', 'core'),
            'xerox': dummy_service('xerox', 'post'),
        }


def make_result(file_hash, service):
    new_result: Result = random_minimal_obj(Result)
    new_result.sha256 = file_hash
    new_result.response.service_name = service
    return new_result


def make_error(file_hash, service, recoverable=True):
    new_error: Error = random_model_obj(Error)
    new_error.response.service_name = service
    new_error.sha256 = file_hash
    if recoverable:
        new_error.response.status = 'FAIL_RECOVERABLE'
    else:
        new_error.response.status = 'FAIL_NONRECOVERABLE'
    return new_error


def wait_result(task, file_hash, service):
    for _ in range(10):
        if (file_hash, service) in task.service_results:
            return True
        time.sleep(0.05)


def wait_error(task, file_hash, service):
    for _ in range(10):
        if (file_hash, service) in task.service_errors:
            return True
        time.sleep(0.05)


@pytest.fixture(autouse=True)
def log_config(caplog):
    caplog.set_level(logging.INFO, logger='assemblyline')
    from assemblyline.common import log as al_log
    al_log.init_logging = lambda *args: None


@mock.patch('assemblyline_core.dispatching.dispatcher.Scheduler', Scheduler)
@mock.patch('assemblyline_core.dispatching.dispatcher.MetricsFactory', new=mock.MagicMock(spec=MetricsFactory))
def test_simple(clean_redis, clean_datastore):
    ds = clean_datastore
    redis = clean_redis

    def service_queue(name): return get_service_queue(name, redis)

    file = random_model_obj(File)
    file_hash = file.sha256
    file.type = 'unknown'
    ds.file.save(file_hash, file)

    sub: Submission = random_model_obj(models.submission.Submission)
    sub.sid = sid = 'first-submission'
    sub.params.ignore_cache = False
    sub.params.max_extracted = 5
    sub.to_be_deleted = False
    sub.params.classification = get_classification().UNRESTRICTED
    sub.params.initial_data = json.dumps({'cats': 'big'})
    sub.files = [dict(sha256=file_hash, name='file')]

    disp = Dispatcher(ds, redis, redis)
    disp.running = ToggleTrue()
    client = DispatchClient(ds, redis, redis)
    client.dispatcher_data_age = time.time()
    client.dispatcher_data.append(disp.instance_id)

    # Submit a problem, and check that it gets added to the dispatch hash
    # and the right service queues
    logger.info('==== first dispatch')
    # task = SubmissionTask(sub.as_primitives(), 'some-completion-queue')
    client.dispatch_submission(sub)
    disp.pull_submissions()
    disp.service_worker(disp.process_queue_index(sid))
    task = disp.tasks.get(sid)

    assert task.queue_keys[(file_hash, 'extract')] is not None
    assert task.queue_keys[(file_hash, 'wrench')] is not None
    assert service_queue('extract').length() == 1
    assert service_queue('wrench').length() == 1

    # Making the same call again will queue it up again
    logger.info('==== second dispatch')
    disp.dispatch_file(task, file_hash)

    assert task.queue_keys[(file_hash, 'extract')] is not None
    assert task.queue_keys[(file_hash, 'wrench')] is not None
    assert service_queue('extract').length() == 1  # the queue doesn't pile up
    assert service_queue('wrench').length() == 1

    logger.info('==== third dispatch')
    job = client.request_work('0', 'extract', '0')
    assert job.temporary_submission_data == [
        {'name': 'cats', 'value': 'big'},
        {"name": "ancestry", "value": [[{"type": "unknown", "parent_relation": "ROOT", "sha256": file.sha256}]]}
    ]
    client.service_failed(sid, 'abc123', make_error(file_hash, 'extract'))
    # Deliberately do in the wrong order to make sure that works
    disp.pull_service_results()
    disp.service_worker(disp.process_queue_index(sid))

    assert task.queue_keys[(file_hash, 'extract')] is not None
    assert task.queue_keys[(file_hash, 'wrench')] is not None
    assert service_queue('extract').length() == 1

    # Mark extract as finished, wrench as failed
    logger.info('==== fourth dispatch')
    client.request_work('0', 'extract', '0')
    client.request_work('0', 'wrench', '0')
    client.service_finished(sid, 'extract-result', make_result(file_hash, 'extract'))
    client.service_failed(sid, 'wrench-error', make_error(file_hash, 'wrench', False))
    for _ in range(2):
        disp.pull_service_results()
        disp.service_worker(disp.process_queue_index(sid))

    assert wait_error(task, file_hash, 'wrench')
    assert wait_result(task, file_hash, 'extract')
    assert service_queue('av-a').length() == 1
    assert service_queue('av-b').length() == 1
    assert service_queue('frankenstrings').length() == 1

    # Have the AVs fail, frankenstrings finishes
    logger.info('==== fifth dispatch')
    client.request_work('0', 'av-a', '0')
    client.request_work('0', 'av-b', '0')
    client.request_work('0', 'frankenstrings', '0')
    client.service_failed(sid, 'av-a-error', make_error(file_hash, 'av-a', False))
    client.service_failed(sid, 'av-b-error', make_error(file_hash, 'av-b', False))
    client.service_finished(sid, 'f-result', make_result(file_hash, 'frankenstrings'))
    for _ in range(3):
        disp.pull_service_results()
        disp.service_worker(disp.process_queue_index(sid))

    assert wait_result(task, file_hash, 'frankenstrings')
    assert wait_error(task, file_hash, 'av-a')
    assert wait_error(task, file_hash, 'av-b')
    assert service_queue('xerox').length() == 1

    # Finish the xerox service and check if the submission completion got checked
    logger.info('==== sixth dispatch')
    client.request_work('0', 'xerox', '0')
    client.service_finished(sid, 'xerox-result-key', make_result(file_hash, 'xerox'))
    disp.pull_service_results()
    disp.service_worker(disp.process_queue_index(sid))
    disp.save_submission()

    assert wait_result(task, file_hash, 'xerox')
    assert disp.tasks.get(sid) is None


@mock.patch('assemblyline_core.dispatching.dispatcher.MetricsFactory', mock.MagicMock())
@mock.patch('assemblyline_core.dispatching.dispatcher.Scheduler', Scheduler)
def test_dispatch_extracted(clean_redis, clean_datastore):
    redis = clean_redis
    ds = clean_datastore

    # def service_queue(name): return get_service_queue(name, redis)

    # Setup the fake datastore
    file_hash = get_random_hash(64)
    second_file_hash = get_random_hash(64)

    for fh in [file_hash, second_file_hash]:
        obj = random_model_obj(models.file.File)
        obj.sha256 = fh
        ds.file.save(fh, obj)

    # Inject the fake submission
    submission = random_model_obj(models.submission.Submission)
    submission.to_be_deleted = False
    submission.files = [dict(name='./file', sha256=file_hash)]
    sid = submission.sid = 'first-submission'

    disp = Dispatcher(ds, redis, redis)
    disp.running = ToggleTrue()
    client = DispatchClient(ds, redis, redis)
    client.dispatcher_data_age = time.time()
    client.dispatcher_data.append(disp.instance_id)

    # Launch the submission
    client.dispatch_submission(submission)
    disp.pull_submissions()
    disp.service_worker(disp.process_queue_index(sid))

    # Finish one service extracting a file
    job = client.request_work('0', 'extract', '0')
    assert job.fileinfo.sha256 == file_hash
    assert job.filename == './file'
    new_result: Result = random_minimal_obj(Result)
    new_result.sha256 = file_hash
    new_result.response.service_name = 'extract'
    new_result.response.extracted = [dict(sha256=second_file_hash, name='second-*',
                                          description='abc', classification='U')]
    client.service_finished(sid, 'extracted-done', new_result)

    # process the result
    disp.pull_service_results()
    disp.service_worker(disp.process_queue_index(sid))
    disp.service_worker(disp.process_queue_index(sid))

    #
    job = client.request_work('0', 'extract', '0')
    assert job.fileinfo.sha256 == second_file_hash
    assert job.filename == 'second-*'


@mock.patch('assemblyline_core.dispatching.dispatcher.MetricsFactory', mock.MagicMock())
@mock.patch('assemblyline_core.dispatching.dispatcher.Scheduler', DRPScheduler)
def test_dispatch_extracted_bypass_drp(clean_redis, clean_datastore):
    # Dynamic Recursion Prevention is to prevent services belonging to the 'Dynamic Analysis' from analyzing the children
    # of files they've analyzed.

    # The bypass should allow services to specify files to run through Dynamic Analysis regardless of the
    # Dynamic Recursion Prevention parameter.

    redis = clean_redis
    ds = clean_datastore

    # def service_queue(name): return get_service_queue(name, redis)

    # Setup the fake datastore
    file_hash = get_random_hash(64)
    second_file_hash = get_random_hash(64)

    for fh in [file_hash, second_file_hash]:
        obj = random_model_obj(models.file.File)
        obj.sha256 = fh
        ds.file.save(fh, obj)

    # Inject the fake submission
    submission = random_model_obj(models.submission.Submission)
    submission.to_be_deleted = False
    submission.params.ignore_dynamic_recursion_prevention = False
    submission.params.services.selected = ['extract', 'sandbox']
    submission.files = [dict(name='./file', sha256=file_hash)]
    sid = submission.sid = 'first-submission'

    disp = Dispatcher(ds, redis, redis)
    disp.running = ToggleTrue()
    client = DispatchClient(ds, redis, redis)
    client.dispatcher_data_age = time.time()
    client.dispatcher_data.append(disp.instance_id)

    # Launch the submission
    client.dispatch_submission(submission)
    disp.pull_submissions()
    disp.service_worker(disp.process_queue_index(sid))

    # 'extract' service extracts a file and yields a result
    job = client.request_work('0', 'extract', '0')
    assert job.fileinfo.sha256 == file_hash
    assert job.filename == './file'
    new_result: Result = random_minimal_obj(Result)
    new_result.sha256 = file_hash
    new_result.response.service_name = 'extract'
    # This extracted file should be able to bypass Dynamic Recursion Prevention
    new_result.response.extracted = [dict(sha256=second_file_hash, name='second-*',
                                          description='abc', classification='U', allow_dynamic_recursion=True)]
    client.service_finished(sid, 'extract-done', new_result)

    # process the result
    disp.pull_service_results()
    disp.service_worker(disp.process_queue_index(sid))
    disp.service_worker(disp.process_queue_index(sid))

    # Then 'sandbox' service will analyze the same file, give result
    job = client.request_work('0', 'sandbox', '0')
    assert job.fileinfo.sha256 == file_hash
    assert job.filename == './file'
    new_result: Result = random_minimal_obj(Result)
    new_result.sha256 = file_hash
    new_result.response.service_name = 'sandbox'
    client.service_finished(sid, 'sandbox-done', new_result)

    # process the result
    disp.pull_service_results()
    disp.service_worker(disp.process_queue_index(sid))
    disp.service_worker(disp.process_queue_index(sid))

    # 'extract' service should have a task for the extracted file, give results to move onto next stage
    job = client.request_work('0', 'extract', '0')
    assert job.fileinfo.sha256 == second_file_hash
    assert job.filename == 'second-*'
    new_result: Result = random_minimal_obj(Result)
    new_result.sha256 = second_file_hash
    new_result.response.service_name = 'extract'
    client.service_finished(sid, 'extract-done', new_result)

    # process the result
    disp.pull_service_results()
    disp.service_worker(disp.process_queue_index(sid))
    disp.service_worker(disp.process_queue_index(sid))

    # 'sandbox' should have a task for the extracted file
    #disp.dispatch_file(disp.tasks.get(sid), second_file_hash)
    job = client.request_work('0', 'sandbox', '0')
    assert job.fileinfo.sha256 == second_file_hash
    assert job.filename == 'second-*'


mock_time = mock.Mock()
mock_time.return_value = 0


@mock.patch('time.time', mock_time)
def test_timeout():
    table = TimeoutTable()
    table.set('first', 5, 1)
    table.set('second', 10, 2)
    table.set('third', 15, 3)
    table.set('fourth', 20, 4)

    # Expire one thing
    mock_time.return_value = 6
    items = table.timeouts()
    assert len(items) == 1
    assert items['first'] == 1

    # Replace the data and expiry
    table.set('second', 20, 5)  # second now expires at 26

    # Expire two things
    mock_time.return_value = 21
    items = table.timeouts()
    assert len(items) == 2
    assert items['third'] == 3
    assert items['fourth'] == 4

    # Expire nothing
    assert len(table.timeouts()) == 0

    # Expire the final thing
    mock_time.return_value = 30
    items = table.timeouts()
    assert len(items) == 1
    assert items['second'] == 5

    # Expire nothing
    assert len(table.timeouts()) == 0
