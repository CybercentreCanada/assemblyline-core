"""
A daemon that cleans up tasks from the service queues when a service is disabled/deleted.

When a service is turned off by the orchestrator or deleted by the user, the service task queue needs to be
emptied. The status of all the services will be periodically checked and any service that is found to be
disabled or deleted for which a service queue exists, the dispatcher will be informed that the task(s)
had an error.
"""
import threading
from copy import deepcopy
from typing import Optional

from assemblyline.common.constants import service_queue_name
from assemblyline.common.forge import get_service_queue
from assemblyline.common.isotime import DAY_IN_SECONDS, now_as_iso
from assemblyline.odm.models.apikey import get_apikey_id
from assemblyline.odm.models.error import Error
from assemblyline.odm.models.service import Service
from assemblyline.odm.models.user import load_roles, load_roles_form_acls
from assemblyline.odm.models.user_settings import SubmissionProfileParams
from assemblyline.remote.datatypes import retry_call
from assemblyline.remote.datatypes.queues.named import NamedQueue
from assemblyline_core.dispatching.client import DispatchClient
from assemblyline_core.server_base import CoreBase, ServiceStage

DAY = 60 * 60 * 24
TASK_DELETE_CHUNK = 10000


class Plumber(CoreBase):
    def __init__(self, logger=None, shutdown_timeout: Optional[float] = None, config=None,
                 redis=None, redis_persist=None, datastore=None, delay=60):
        super().__init__('assemblyline.plumber', logger, shutdown_timeout, config=config, redis=redis,
                         redis_persist=redis_persist, datastore=datastore)
        self.delay = float(delay)
        self.datastore.ds.switch_user("plumber")
        self.dispatch_client = DispatchClient(datastore=self.datastore, redis=self.redis,
                                              redis_persist=self.redis_persist, logger=self.log)

        self.flush_threads: dict[str, threading.Thread] = {}
        self.stop_signals: dict[str, threading.Event] = {}
        self.service_limit: dict[str, int] = {}

    def stop(self):
        for sig in self.stop_signals.values():
            sig.set()
        super().stop()

    def try_run(self):
        # Start a task cleanup thread
        tc_thread = threading.Thread(target=self.cleanup_old_tasks, daemon=True, name="datastore_task_cleanup")
        tc_thread.start()

        # Start a notification queue cleanup thread
        nq_thread = threading.Thread(target=self.cleanup_notification_queues, daemon=True,
                                     name="redis_notification_queue_cleanup")
        nq_thread.start()

        # Start a user apikey cleanup thread
        ua_thread = threading.Thread(target=self.user_apikey_cleanup, daemon=True, name="user_apikey_cleanup")
        ua_thread.start()

        # Start a user setting migration thread
        usm_thread = threading.Thread(target=self.migrate_user_settings, daemon=True, name="migrate_user_settings")
        usm_thread.start()

        self.service_queue_plumbing()

    def service_queue_plumbing(self):
        # Get an initial list of all the service queues
        service_queues: dict[str, Optional[Service]]
        service_queues = {queue.decode('utf-8').lstrip('service-queue-'): None
                          for queue in self.redis.keys(service_queue_name('*'))}

        while self.running:
            # Reset the status of the service queues
            service_queues = {service_name: None for service_name in service_queues}

            # Update the service queue status based on current list of services
            for service in self.datastore.list_all_services(full=True):
                service_queues[service.name] = service

            for service_name, service in service_queues.items():
                # For disabled or othewise unavailable services purge the queue
                current_stage = self.get_service_stage(service_name, ServiceStage.Running)
                if not service or not service.enabled or current_stage != ServiceStage.Running:
                    while True:
                        task = self.dispatch_client.request_work('plumber', service_name=service_name,
                                                                 service_version='0', blocking=False)
                        if task is None:
                            break

                        error = Error(dict(
                            archive_ts=None,
                            created='NOW',
                            expiry_ts=now_as_iso(task.ttl * DAY) if task.ttl else None,
                            response=dict(
                                message='The service was disabled while processing this task.',
                                service_name=task.service_name,
                                service_version='0',
                                status='FAIL_NONRECOVERABLE',
                            ),
                            sha256=task.fileinfo.sha256,
                            type="TASK PRE-EMPTED",
                        ))

                        error_key = error.build_key(task=task)
                        self.dispatch_client.service_failed(task.sid, error_key, error)
                        self.heartbeat()

                # For services that are enabled but limited
                if not service or not service.enabled or service.max_queue_length == 0:
                    if service_name in self.stop_signals:
                        self.stop_signals[service_name].set()
                        self.service_limit.pop(service_name)
                        self.flush_threads.pop(service_name)
                elif service and service.enabled and service.max_queue_length > 0:
                    self.service_limit[service_name] = service.max_queue_length
                    thread = self.flush_threads.get(service_name)
                    if not thread or not thread.is_alive():
                        self.stop_signals[service_name] = threading.Event()
                        thread = threading.Thread(
                            target=self.watch_service, args=[service_name],
                            daemon=True, name=service_name)
                        self.flush_threads[service_name] = thread
                        thread.start()

            # Wait a while before checking status of all services again
            self.sleep_with_heartbeat(self.delay)

    def cleanup_notification_queues(self):
        self.log.info("Cleaning up notification queues for old messages...")
        while self.running:
            # Finding all possible notification queues
            keys = [k.decode() for k in retry_call(self.redis_persist.keys, "nq-*")]
            if not keys:
                self.log.info('There are no queues right now in the system')
            for k in keys:
                self.log.info(f'Checking for old message in queue: {k}')
                # Peek the first message of the queue
                q = NamedQueue(k, self.redis_persist)
                msg = q.peek_next()
                if msg:
                    current_time = now_as_iso(-1 * self.config.core.plumber.notification_queue_max_age)
                    task_time = msg.get('notify_time', None) or msg.get('ingest_time', None)

                    # If the message too old, cleanup
                    if task_time is None or task_time <= current_time:
                        self.log.warning(
                            f"Messages on queue {k} by "
                            f"{msg.get('submission', {}).get('params', {}).get('submitter', 'unknown')}"
                            f" are older then the maximum queue age ({task_time}), removing queue")
                        q.delete()
                    else:
                        self.log.info('All messages are recent enough')
                else:
                    self.log.info('There are no messages in the queue')

            # wait for next run
            self.sleep(self.config.core.plumber.notification_queue_interval)
        self.log.info("Done cleaning up notification queues")

    def cleanup_old_tasks(self):
        self.log.info("Cleaning up task index for old completed tasks...")
        while self.running:
            deleted = self.datastore.task_cleanup(deleteable_task_age=DAY, max_tasks=TASK_DELETE_CHUNK)
            if not deleted:
                self.sleep(self.delay)
            else:
                self.log.info(f"Cleaned up {deleted} tasks that were already completed")
        self.log.info("Done cleaning up task index")

    def watch_service(self, service_name):
        self.log.info(f"Watching {service_name} service queue...")
        service_queue = get_service_queue(service_name, self.redis)
        while self.running and not self.stop_signals[service_name].is_set():
            while service_queue.length() > self.service_limit[service_name]:
                task = self.dispatch_client.request_work('plumber', service_name=service_name,
                                                         service_version='0', blocking=False, low_priority=True)
                if task is None:
                    break

                error = Error(dict(
                    archive_ts=None,
                    created='NOW',
                    expiry_ts=now_as_iso(task.ttl * 24 * 60 * 60) if task.ttl else None,
                    response=dict(
                        message="Task canceled due to execesive queuing.",
                        service_name=task.service_name,
                        service_version='0',
                        status='FAIL_NONRECOVERABLE',
                    ),
                    sha256=task.fileinfo.sha256,
                    type="TASK PRE-EMPTED",
                ))

                error_key = error.build_key(task=task)
                self.dispatch_client.service_failed(task.sid, error_key, error)
            self.sleep(2)
        self.log.info(f"Done watching {service_name} service queue")

    def user_apikey_cleanup(self):
        expiry_ts = None
        if self.config.auth.apikey_max_dtl is not None:
            expiry_ts = now_as_iso(self.config.auth.apikey_max_dtl * DAY_IN_SECONDS)

        changes_made = False
        for user in self.datastore.user.stream_search(query="*", fl="uname,apikeys,type,roles", as_obj=False):
            changes_made = True
            uname = user['uname']
            apikeys = user['apikeys']

            for key in apikeys:
                old_apikey = apikeys[key]
                key_id = get_apikey_id(key, uname)

                roles = None
                if old_apikey['acl'] == ["C"]:

                    roles = [r for r in old_apikey['roles']
                                if r in load_roles(user['type'], user.get('roles'))]

                else:
                    roles = [r for r in load_roles_form_acls(old_apikey['acl'], roles)
                            if r in load_roles(user['type'], user.get('roles'))]
                new_apikey = {
                    "password": old_apikey['password'],
                    "acl": old_apikey['acl'],
                    "uname": uname,
                    "key_name": key,
                    "roles": roles,
                    "expiry_ts": expiry_ts
                }
                self.datastore.apikey.save(key_id, new_apikey)

        if changes_made:
            # Commit changes made to indices
            self.datastore.apikey.commit()

            # Update permissions for API keys based on submission customization
            self.datastore.apikey.update_by_query('roles:"submission_create" AND NOT roles:"submission_customize"',
                                                  [(self.datastore.apikey.UPDATE_APPEND, 'roles', 'submission_customize')])

    def migrate_user_settings(self):
        service_list = self.datastore.list_all_services(as_obj=False)

        # Migrate user settings to the new format
        for doc in self.datastore.user_settings.scan_with_search_after(query={
            "bool": {
                "must": {
                    "query_string": {
                        "query": "*",
                    }
                }
            }
        }):
            user_settings = doc["_source"]
            if user_settings.get('submission_profiles'):
                # User settings already migrated, skip
                continue

            # Create the list of submission profiles
            submission_profiles = {
                # Grant everyone a default of which all settings from before 4.6 should be transferred
                "default" : SubmissionProfileParams(user_settings).as_primitives(strip_null=True)
            }

            profile_error_checks = {}
            # Try to apply the original settings to the system-defined profiles
            for profile in self.config.submission.profiles:
                updates = deepcopy(user_settings)
                profile_error_checks.setdefault(profile.name, 0)
                validated_profile = profile.params.as_primitives(strip_null=True)

                updates.setdefault("services", {})
                updates["services"].setdefault("selected", [])
                updates["services"].setdefault("excluded", [])

                # Append the exclusion list set by the profile
                updates['services']['excluded'] = updates['services']['excluded'] + \
                    list(validated_profile.get("services", {}).get("excluded", []))

                # Ensure the selected services are not in the excluded list
                for service in service_list:
                    if service['name'] in updates['services']['selected'] and service['name'] in updates['services']['excluded']:
                        updates['services']['selected'].remove(service['name'])
                        profile_error_checks[profile.name] += 1
                    elif service['category'] in updates['services']['selected'] and service['category'] in updates['services']['excluded']:
                        updates['services']['selected'].remove(service['category'])
                        profile_error_checks[profile.name] += 1


                # Check the services parameters
                for param_type, list_of_params in profile.restricted_params.items():
                    # Check if there are restricted submission parameters
                    if param_type == "submission":
                        requested_params = (set(list_of_params) & set(updates.keys())) - set({'services', 'service_spec'})
                        if requested_params:
                            # Track the number of errors for each profile
                            profile_error_checks[profile.name] += len(requested_params)
                            for param in requested_params:
                                # Remove the parameter from the updates
                                updates.pop(param, None)

                    # Check if there are restricted service parameters
                    else:
                        service_spec = updates.get('service_spec', {}).get(param_type, {})
                        requested_params = set(list_of_params) & set(service_spec)
                        if requested_params:
                            # Track the number of errors for each profile
                            profile_error_checks[profile.name] += len(requested_params)
                            for param in requested_params:
                                # Remove the parameter from the updates
                                service_spec.pop(param, None)

                            if not service_spec:
                                # Remove the service spec if empty
                                updates['service_spec'].pop(param_type, None)
                submission_profiles[profile.name] = SubmissionProfileParams(updates).as_primitives(strip_null=True)

            # Assign the profile with the least number of errors
            user_settings['submission_profiles'] = submission_profiles
            user_settings['preferred_submission_profile'] = sorted(profile_error_checks.items(), key=lambda x: x[1])[0][0]

            self.datastore.user_settings.save(doc["_id"], user_settings)




if __name__ == '__main__':
    with Plumber() as server:
        server.serve_forever()
