# import concurrent.futures
#
# import pytest
#
# from assemblyline.datastore.helper import AssemblylineDatastore
# from assemblyline.datastore.stores.es_store import ESStore
#
# from al_core.middleman.run_ingest import ingester
# from al_core.middleman.run_internal import run_internals
# from al_core.middleman.run_submit import submitter
#
# from al_core.dispatching.run_files import FileDispatchServer
# from al_core.dispatching.run_submissions import SubmissionDispatchServer
#
# from al_core.mocking import clean_redis
#
#
# class SetupException(Exception):
#     pass
#
#
# @pytest.fixture(scope='module')
# def es_connection():
#     try:
#         document_store = ESStore(['127.0.0.1'])
#     except SetupException:
#         return pytest.skip("Connection to the Elasticsearch server failed. This test cannot be performed...")
#
#     return AssemblylineDatastore(document_store)
#
#
# class MockService:
#     pass
# #     def __init__(self, name, dispatcher):
# #         self.name = name
# #         self.queue = NamedQueue(service_queue_name(name))
# #         self.thread = threading.Thread(target=self.run)
# #         self.thread.daemon = True
# #         self.dispatcher = dispatcher
# #         self.thread.start()
# #
# #     def run(self):
# #         while True:
# #             task = ServiceTask(self.queue.pop())
# #             time.sleep(random.random())
# #
# #             if random.random() < 0.001:
# #                 continue
# #
# #             if random.random() < 0.01:
# #                 self.dispatcher.service_failed(task)
# #                 continue
# #
# #             result = random_model_obj(Result)
# #             self.dispatcher.service_finished(task, result)
#
#
# def test_simulate_core(es_connection, clean_redis):
#     pool = concurrent.futures.ThreadPoolExecutor(500)
#
#     # Start the middleman components
#     pool.submit(ingester, datastore=es_connection, redis=clean_redis, redis_persist=clean_redis)
#     pool.submit(submitter, datastore=es_connection, redis=clean_redis, redis_persist=clean_redis)
#     pool.submit(run_internals, datastore=es_connection, redis=clean_redis, redis_persist=clean_redis)
#
#     # Start the dispatcher
#     file_dispatcher = FileDispatchServer(datastore=es_connection, redis=clean_redis, redis_persist=clean_redis)
#     submission_dispatcher = SubmissionDispatchServer(datastore=es_connection, redis=clean_redis, redis_persist=clean_redis)
#     file_dispatcher.start()
#     submission_dispatcher.start()
#
#     assert False
#
#
#     #
#     # from assemblyline.common import log
#     # log.init_logging()
#     #
#     # # Create a configuration with a set of services
#     # class MockScheduler(Scheduler):
#     #     def __init__(self, *args, **kwargs):
#     #         pass
#     #
#     #     def services(self):
#     #         return {
#     #             'extract': Service(dict(
#     #                 name='extract',
#     #                 category='static',
#     #                 stage='pre',
#     #             ))
#     #         }
#     #
#     #     # def build_schedule(self, *args):
#     #     #     return [
#     #     #         ['extract', 'wrench'],
#     #     #         ['av-a', 'av-b', 'frankenstrings'],
#     #     #         ['xerox']
#     #     #     ]
#     #     #
#     #     # def build_service_config(self, *args):
#     #     #     return {}
#     #     #
#     #     # def service_failure_limit(self, *args):
#     #     #     return 5
#     #
#     # with mock.patch('dispatcher.Scheduler', MockScheduler):
#     #     # Start the dispatch servers
#     #     submission_server = SubmissionDispatchServer(redis_connection, es_connection)
#     #     file_server = FileDispatchServer(redis_connection, es_connection)
#     #     submission_server.start()
#     #     file_server.start()
#     #
#     #     # Create a set of daemons that act like those services exist
#     #     sched = MockScheduler(es_connection)
#     #     for name in sched.services():
#     #         print(f'Creating mock service {name}')
#     #         MockService(name, submission_server.dispatcher)
#     #
#     #     # Start sending randomly generated jobs
#     #     submissions = []
#     #     for _ in range(10):
#     #         sub = random_model_obj(models.submission.Submission)
#     #         sub.status = 'incomplete'
#     #         es_connection.submissions.save(sub.sid, sub)
#     #         submission_server.dispatcher.submission_queue.push(sub.json())
#     #         submissions.append(sub.sid)
#     #
#     #     # Wait for all of the jobs to finish
#     #     while len(submission_server.dispatcher.submission_queue) > 0 or len(submission_server.dispatcher.file_queue) > 0:
#     #         print(len(submission_server.dispatcher.submission_queue), len(submission_server.dispatcher.file_queue))
#     #         time.sleep(1)
#     #
#     #     submission_server.stop()
#     #     file_server.stop()
#     #
#     #     submission_server.join()
#     #     file_server.join()
#     #
#     #     # Verify that all of the jobs have reasonable results
#     #     for sid in submissions:
#     #         sub = es_connection.submissions.get(sid)
#     #         assert sub.status == 'complete'
#     #         # TODO check that results exist
