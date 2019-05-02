import logging
from unittest import mock

import assemblyline.odm.models.file
import assemblyline.odm.models.submission
from assemblyline.odm.randomizer import random_model_obj
from assemblyline.odm import models
from assemblyline.common.metrics import MetricsFactory

from al_core.dispatching.scheduler import Scheduler as RealScheduler
from al_core.dispatching.dispatcher import Dispatcher, DispatchHash, service_queue_name, FileTask, NamedQueue, SubmissionTask

from .mocking import MockDatastore, clean_redis
from .test_scheduler import dummy_service



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
            'extract': dummy_service('extract', 'pre'),
            'wrench': dummy_service('wrench', 'pre'),
            'av-a': dummy_service('av-a', 'core'),
            'av-b': dummy_service('av-b', 'core'),
            'frankenstrings': dummy_service('frankenstrings', 'core'),
            'xerox': dummy_service('xerox', 'post'),
        }


@mock.patch('al_core.dispatching.dispatcher.Scheduler', Scheduler)
@mock.patch('al_core.dispatching.dispatcher.MetricsFactory', new=mock.MagicMock(spec=MetricsFactory))
def test_dispatch_file(clean_redis):

    service_queue = lambda name: NamedQueue(service_queue_name(name), clean_redis)

    ds = MockDatastore(collections=['submission', 'result', 'service', 'error', 'file', 'filescore'])
    file_hash = 'totally-a-legit-hash'
    sub = random_model_obj(models.submission.Submission)
    sub.sid = sid = 'first-submission'
    sub.params.ignore_cache = False

    disp = Dispatcher(ds, clean_redis, clean_redis, logging)
    disp.active_tasks.add(sid, SubmissionTask(dict(submission=sub)).as_primitives())
    dh = DispatchHash(sid=sid, client=clean_redis)
    print('==== first dispatch')
    # Submit a problem, and check that it gets added to the dispatch hash
    # and the right service queues
    file_task = FileTask({
        'sid': 'first-submission',
        'file_info': dict(sha256=file_hash, type='unknown', magic='a', md5='a', mime='a', sha1='a', size=10),
        'depth': 0,
        'max_files': 5
    })
    disp.dispatch_file(file_task)

    assert dh.dispatch_time(file_hash, 'extract') > 0
    assert dh.dispatch_time(file_hash, 'wrench') > 0
    assert service_queue('extract').length() == 1
    assert len(service_queue('wrench')) == 1

    # Making the same call again should have no effect
    print('==== second dispatch')
    disp.dispatch_file(file_task)

    assert dh.dispatch_time(file_hash, 'extract') > 0
    assert dh.dispatch_time(file_hash, 'wrench') > 0
    assert len(service_queue('extract')) == 1
    assert len(service_queue('wrench')) == 1
    # assert len(mq) == 4

    # Push back the timestamp in the dispatch hash to simulate a timeout,
    # make sure it gets pushed into that service queue again
    print('==== third dispatch')
    [service_queue(name).delete() for name in disp.scheduler.services]
    dh.fail_recoverable(file_hash, 'extract')

    disp.dispatch_file(file_task)

    assert dh.dispatch_time(file_hash, 'extract') > 0
    assert dh.dispatch_time(file_hash, 'wrench') > 0
    assert service_queue('extract').length() == 1
    # assert len(mq) == 1

    # Mark extract as finished, wrench as failed
    print('==== fourth dispatch')
    [service_queue(name).delete() for name in disp.scheduler.services]
    dh.finish(file_hash, 'extract', 'result-key', 0, 'U')
    dh.fail_nonrecoverable(file_hash, 'wrench', 'error-key')

    disp.dispatch_file(file_task)

    assert dh.finished(file_hash, 'extract')
    assert dh.finished(file_hash, 'wrench')
    assert len(service_queue('av-a')) == 1
    assert len(service_queue('av-b')) == 1
    assert len(service_queue('frankenstrings')) == 1

    # Have the AVs fail, frankenstrings finishes
    print('==== fifth dispatch')
    [service_queue(name).delete() for name in disp.scheduler.services]
    dh.fail_nonrecoverable(file_hash, 'av-a', 'error-a')
    dh.fail_nonrecoverable(file_hash, 'av-b', 'error-b')
    dh.finish(file_hash, 'frankenstrings', 'result-key', 0, 'U')

    disp.dispatch_file(file_task)

    assert dh.finished(file_hash, 'av-a')
    assert dh.finished(file_hash, 'av-b')
    assert dh.finished(file_hash, 'frankenstrings')
    assert len(service_queue('xerox')) == 1

    # Finish the xerox service and check if the submission completion got checked
    print('==== sixth dispatch')
    [service_queue(name).delete() for name in disp.scheduler.services]
    dh.finish(file_hash, 'xerox', 'result-key', 0, 'U')

    disp.dispatch_file(file_task)

    assert dh.finished(file_hash, 'xerox')
    assert len(disp.submission_queue) == 1


@mock.patch('al_core.dispatching.dispatcher.MetricsFactory', mock.MagicMock())
@mock.patch('al_core.dispatching.dispatcher.Scheduler', Scheduler)
def test_dispatch_submission(clean_redis):
    ds = MockDatastore(collections=['submission', 'result', 'service', 'error', 'file'])
    file_hash = 'totally_a_legit_hash'

    ds.file.save(file_hash, random_model_obj(models.file.File))
    ds.file.get(file_hash).sha256 = file_hash
    # ds.file.get(file_hash).sha256 = ''

    submission = random_model_obj(models.submission.Submission)
    submission.files.clear()
    submission.files.append(dict(
        name='./file',
        sha256=file_hash
    ))

    submission.sid = 'first-submission'

    disp = Dispatcher(ds, logger=logging, redis=clean_redis, redis_persist=clean_redis)
    # Submit a problem, and check that it gets added to the dispatch hash
    # and the right service queues
    task = SubmissionTask(dict(submission=submission))
    disp.dispatch_submission(task)

    file_task = FileTask(disp.file_queue.pop())
    assert file_task.sid == submission.sid
    assert file_task.file_info.sha256 == file_hash
    assert file_task.depth == 0
    assert file_task.file_info.type == ds.file.get(file_hash).type

    dh = DispatchHash(submission.sid, clean_redis)
    for service_name in disp.scheduler.services.keys():
        dh.fail_nonrecoverable(file_hash, service_name, 'error-code')

    disp.dispatch_submission(task)
    assert ds.submission.get(submission.sid).state == 'completed'
    assert ds.submission.get(submission.sid).errors == ['error-code']*len(disp.scheduler.services)
