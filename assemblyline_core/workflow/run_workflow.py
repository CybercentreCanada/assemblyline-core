#!/usr/bin/env python

import elasticapm
import time

from assemblyline_core.server_base import ServerBase
from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso
from assemblyline.common.str_utils import safe_str

from assemblyline.datastore.exceptions import SearchException
from assemblyline.odm.models.alert import Event
from assemblyline.odm.models.workflow import Workflow


class WorkflowManager(ServerBase):
    def __init__(self):
        super().__init__('assemblyline.workflow')

        self.config = forge.get_config()
        self.datastore = forge.get_datastore(self.config)
        self.start_ts = f"{self.datastore.ds.now}/{self.datastore.ds.day}-1{self.datastore.ds.day}"

        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = forge.get_apm_client("workflow")
        else:
            self.apm_client = None

    def stop(self):
        if self.apm_client:
            elasticapm.uninstrument()
        super().stop()

    def get_last_reporting_ts(self, p_start_ts):
        # Start of transaction
        if self.apm_client:
            self.apm_client.begin_transaction("Get last reporting timestamp")

        self.log.info(f"Finding reporting timestamp for the last alert since {p_start_ts}...")
        result = None
        while result is None:
            try:
                result = self.datastore.alert.search(f"reporting_ts:[{p_start_ts} TO *]",
                                                     sort='reporting_ts desc', rows=1, fl='reporting_ts', as_obj=False)
            except SearchException as e:
                self.log.warning(f"Failed to load last reported alert from the datastore, retrying... :: {e}")
                continue

        items = result.get('items', [{}]) or [{}]

        ret_val = items[0].get("reporting_ts", p_start_ts)

        # End of transaction
        if self.apm_client:
            elasticapm.label(start_ts=p_start_ts, reporting_ts=ret_val)
            self.apm_client.end_transaction('get_last_reporting_ts', 'new_ts' if ret_val != p_start_ts else 'same_ts')

        return ret_val

    def try_run(self):
        self.datastore.alert.commit()
        while self.running:
            self.heartbeat()
            end_ts = self.get_last_reporting_ts(self.start_ts)
            if self.start_ts != end_ts:
                # Start of transaction
                if self.apm_client:
                    self.apm_client.begin_transaction("Load workflows")

                workflow_queries = [Workflow({
                    'status': "TRIAGE",
                    'name': "Triage all with no status",
                    'creator': "SYSTEM",
                    'edited_by': "SYSTEM",
                    'query': "NOT status:*",
                    'workflow_id': "DEFAULT"
                })]

                try:
                    for item in self.datastore.workflow.stream_search("status:MALICIOUS"):
                        workflow_queries.append(item)

                    for item in self.datastore.workflow.stream_search("status:NON-MALICIOUS"):
                        workflow_queries.append(item)

                    for item in self.datastore.workflow.stream_search("status:ASSESS"):
                        workflow_queries.append(item)

                    for item in self.datastore.workflow.stream_search('-status:["" TO *]'):
                        workflow_queries.append(item)
                except SearchException as e:
                    self.log.warning(f"Failed to load workflows from the datastore, retrying... :: {e}")

                    # End of transaction
                    if self.apm_client:
                        elasticapm.label(number_of_workflows=len(workflow_queries))
                        self.apm_client.end_transaction('loading_workflows', 'search_exception')
                    continue

                # End of transaction
                if self.apm_client:
                    elasticapm.label(number_of_workflows=len(workflow_queries))
                    self.apm_client.end_transaction('loading_workflows', 'success')

                for workflow in workflow_queries:
                    # Only action workflow if it's enabled
                    if not workflow.enabled:
                        continue

                    # Start of transaction
                    if self.apm_client:
                        self.apm_client.begin_transaction("Execute workflows")
                        elasticapm.label(query=workflow.query,
                                         labels=workflow.labels,
                                         status=workflow.status,
                                         priority=workflow.priority,
                                         user=workflow.creator)

                    self.log.info(f'Executing workflow filter: {workflow.name}')
                    labels = workflow.labels or []
                    status = workflow.status or None
                    priority = workflow.priority or None

                    if not status and not labels and not priority:
                        # End of transaction
                        if self.apm_client:
                            self.apm_client.end_transaction(workflow.name, 'no_action')
                        continue

                    fq = [f"reporting_ts:[{self.start_ts} TO {end_ts}]", "NOT extended_scan:submitted"]

                    event_data = Event({'entity_type': 'workflow',
                                        'entity_id': workflow.workflow_id,
                                        'entity_name': workflow.name})
                    operations = []
                    fq_items = []
                    if labels:
                        operations.extend([(self.datastore.alert.UPDATE_APPEND_IF_MISSING, 'label', lbl)
                                           for lbl in labels])
                        for label in labels:
                            fq_items.append(f'label:"{label}"')
                        event_data.labels = labels
                    if priority:
                        operations.append((self.datastore.alert.UPDATE_SET, 'priority', priority))
                        fq_items.append("priority:*")
                        event_data.priority = priority
                    if status:
                        operations.append((self.datastore.alert.UPDATE_SET, 'status', status))
                        fq_items.append("(status:MALICIOUS OR status:NON-MALICIOUS OR status:ASSESS)")
                        event_data.status = status

                    fq.append(f"NOT ({' AND '.join(fq_items)})")
                    # Add event to alert's audit history
                    operations.append((self.datastore.alert.UPDATE_APPEND, 'events', event_data))

                    try:
                        count = self.datastore.alert.update_by_query(workflow.query, operations, filters=fq)
                        if self.apm_client:
                            elasticapm.label(affected_alerts=count)

                        if count:
                            self.log.info(f"{count} Alert(s) were affected by this filter.")
                            if workflow.workflow_id != "DEFAULT":
                                seen = now_as_iso()
                                operations = [
                                    (self.datastore.workflow.UPDATE_INC, 'hit_count', count),
                                    (self.datastore.workflow.UPDATE_SET, 'last_seen', seen),
                                ]
                                if not workflow.first_seen:
                                    # Set first seen for workflow if not set
                                    operations.append((self.datastore.workflow.UPDATE_SET, 'first_seen', seen))
                                self.datastore.workflow.update(workflow.workflow_id, operations)

                    except SearchException:
                        self.log.warning(f"Invalid query '{safe_str(workflow.query or '')}' in workflow "
                                         f"'{workflow.name or 'unknown'}' by '{workflow.creator or 'unknown'}'")

                        # End of transaction
                        if self.apm_client:
                            self.apm_client.end_transaction(workflow.name, 'search_exception')

                        continue

                    # End of transaction
                    if self.apm_client:
                        self.apm_client.end_transaction(workflow.name, 'success')

                # Marking all alerts for the time period as their workflow completed
                # Start of transaction
                if self.apm_client:
                    self.apm_client.begin_transaction("Mark alerts complete")

                self.log.info(f'Marking all alerts between {self.start_ts} and {end_ts} as workflow completed...')
                wc_query = f"reporting_ts:[{self.start_ts} TO {end_ts}]"
                wc_operations = [(self.datastore.alert.UPDATE_SET, 'workflows_completed', True)]
                try:
                    wc_count = self.datastore.alert.update_by_query(wc_query, wc_operations)
                    if self.apm_client:
                        elasticapm.label(affected_alerts=wc_count)

                    if wc_count:
                        self.log.info(f"{count} Alert(s) workflows marked as completed.")

                    # End of transaction
                    if self.apm_client:
                        self.apm_client.end_transaction("workflows_completed", 'success')

                except SearchException as e:
                    self.log.warning(f"Failed to update alerts workflows_completed field. [{str(e)}]")

                    # End of transaction
                    if self.apm_client:
                        self.apm_client.end_transaction("workflows_completed", 'search_exception')

            else:
                self.log.info("Skipping all workflows since there where no new alerts in the specified time period.")

            time.sleep(30)
            self.start_ts = end_ts


if __name__ == "__main__":
    with WorkflowManager() as wm:
        wm.serve_forever()
