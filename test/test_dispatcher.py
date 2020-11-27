import logging
from unittest import mock

import assemblyline.odm.models.file
import assemblyline.odm.models.submission
from assemblyline.common.forge import get_service_queue, get_classification
from assemblyline.odm.randomizer import random_model_obj, get_random_hash
from assemblyline.odm import models
from assemblyline.common.metrics import MetricsFactory

from assemblyline_core.dispatching.dispatcher import Dispatcher, DispatchHash, FileTask, \
    SubmissionTask, depths_from_tree, Scheduler as RealScheduler

# noinspection PyUnresolvedReferences
from .mocking import MockDatastore, clean_redis
from .test_scheduler import dummy_service


def test_depth_calculation():
    tree = {
        'a': [None, 'c'],  # Root node, also gets extracted by c
        'b': ['a'],  # Second layer, extracted by the root
        'c': ['b'],  # Third layer, extracted by b
        'd': ['b', 'a'],  # Second layer, extracted by root, but also its peer b,
        # but being a child of root should trump that
        'x': ['y'],  # orphan files, shouldn't stop the results from being calculated, though they shouldn't exist
    }
    depths = depths_from_tree(tree)
    assert depths['a'] == 0
    assert depths['b'] == 1
    assert depths['d'] == 1
    assert depths['c'] == 2
    assert 'x' not in depths
    assert 'y' not in depths


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


@mock.patch('assemblyline_core.dispatching.dispatcher.Scheduler', Scheduler)
@mock.patch('assemblyline_core.dispatching.dispatcher.MetricsFactory', new=mock.MagicMock(spec=MetricsFactory))
def test_dispatch_file(clean_redis):
    service_queue = lambda name: get_service_queue(name, clean_redis)

    ds = MockDatastore(collections=['submission', 'result', 'service', 'error', 'file', 'filescore'])
    file_hash = get_random_hash(64)
    sub = random_model_obj(models.submission.Submission)
    sub.sid = sid = 'first-submission'
    sub.params.ignore_cache = False

    disp = Dispatcher(ds, clean_redis, clean_redis, logging)
    disp.active_submissions.add(sid, SubmissionTask(dict(submission=sub)).as_primitives())
    dh = DispatchHash(sid=sid, client=clean_redis)
    print('==== first dispatch')
    # Submit a problem, and check that it gets added to the dispatch hash
    # and the right service queues
    file_task = FileTask({
        'sid': 'first-submission',
        'min_classification': get_classification().UNRESTRICTED,
        'file_info': dict(sha256=file_hash, type='unknown', magic='a', md5=get_random_hash(32),
                          mime='a', sha1=get_random_hash(40), size=10),
        'depth': 0,
        'max_files': 5
    })
    disp.dispatch_file(file_task)

    assert dh.dispatch_key(file_hash, 'extract') is not None
    assert dh.dispatch_key(file_hash, 'wrench') is not None
    assert service_queue('extract').length() == 1
    assert service_queue('wrench').length() == 1

    # Making the same call again will queue it up again
    print('==== second dispatch')
    disp.dispatch_file(file_task)

    assert dh.dispatch_key(file_hash, 'extract') is not None
    assert dh.dispatch_key(file_hash, 'wrench') is not None
    assert service_queue('extract').length() == 1  # the queue doesn't pile up
    assert service_queue('wrench').length() == 1
    # assert len(mq) == 4

    # Push back the timestamp in the dispatch hash to simulate a timeout,
    # make sure it gets pushed into that service queue again
    print('==== third dispatch')
    [service_queue(name).delete() for name in disp.scheduler.services]
    dh.fail_recoverable(file_hash, 'extract')

    disp.dispatch_file(file_task)

    assert dh.dispatch_key(file_hash, 'extract') is not None
    assert dh.dispatch_key(file_hash, 'wrench') is not None
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
    assert service_queue('av-a').length() == 1
    assert service_queue('av-b').length() == 1
    assert service_queue('frankenstrings').length() == 1

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
    assert service_queue('xerox').length() == 1

    # Finish the xerox service and check if the submission completion got checked
    print('==== sixth dispatch')
    [service_queue(name).delete() for name in disp.scheduler.services]
    dh.finish(file_hash, 'xerox', 'result-key', 0, 'U')

    disp.dispatch_file(file_task)

    assert dh.finished(file_hash, 'xerox')
    assert len(disp.submission_queue) == 1


@mock.patch('assemblyline_core.dispatching.dispatcher.MetricsFactory', mock.MagicMock())
@mock.patch('assemblyline_core.dispatching.dispatcher.Scheduler', Scheduler)
def test_dispatch_submission(clean_redis):
    ds = MockDatastore(collections=['submission', 'result', 'service', 'error', 'file'])
    file_hash = get_random_hash(64)

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
    assert ds.submission.get(submission.sid).errors == ['error-code'] * len(disp.scheduler.services)


@mock.patch('assemblyline_core.dispatching.dispatcher.MetricsFactory', mock.MagicMock())
@mock.patch('assemblyline_core.dispatching.dispatcher.Scheduler', Scheduler)
def test_dispatch_extracted(clean_redis):
    # Setup the fake datastore
    ds = MockDatastore(collections=['submission', 'result', 'service', 'error', 'file'])
    file_hash = get_random_hash(64)
    second_file_hash = get_random_hash(64)

    for fh in [file_hash, second_file_hash]:
        ds.file.save(fh, random_model_obj(models.file.File))
        ds.file.get(fh).sha256 = fh

    # Inject the fake submission
    submission = random_model_obj(models.submission.Submission)
    submission.files.clear()
    submission.files.append(dict(
        name='./file',
        sha256=file_hash
    ))
    submission.sid = 'first-submission'

    # Launch the dispatcher
    disp = Dispatcher(ds, logger=logging, redis=clean_redis, redis_persist=clean_redis)

    # Launch the submission
    task = SubmissionTask(dict(submission=submission))
    disp.dispatch_submission(task)

    # Check that the right values were sent to the
    file_task = FileTask(disp.file_queue.pop(timeout=1))
    assert file_task.sid == submission.sid
    assert file_task.file_info.sha256 == file_hash
    assert file_task.depth == 0
    assert file_task.file_info.type == ds.file.get(file_hash).type

    # Finish the services
    dh = DispatchHash(submission.sid, clean_redis)
    for service_name in disp.scheduler.services.keys():
        dh.finish(file_hash, service_name, 'error-code', 0, 'U')

    # But one of the services extracted a file
    dh.add_file(second_file_hash, 10, file_hash)

    # But meanwhile, dispatch_submission has been recalled on the submission
    disp.dispatch_submission(task)

    # It should see the missing file, and we should get a new file dispatch message for it
    # to make sure it is getting processed properly, this should be at depth 1, the first layer of
    # extracted files
    file_task = disp.file_queue.pop(timeout=1)
    assert file_task is not None
    file_task = FileTask(file_task)
    assert file_task.sid == submission.sid
    assert file_task.file_info.sha256 == second_file_hash
    assert file_task.depth == 1
    assert file_task.file_info.type == ds.file.get(second_file_hash).type

    # Finish the second file
    for service_name in disp.scheduler.services.keys():
        dh.finish(second_file_hash, service_name, 'error-code', 0, 'U')

    # And now we should get the finished submission
    disp.dispatch_submission(task)
    submission = ds.submission.get(submission.sid)
    assert submission.state == 'completed'
    assert submission.errors == []
    assert len(submission.results) == 2 * len(disp.scheduler.services)
