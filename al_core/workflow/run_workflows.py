#!/usr/bin/env python
import time

from al_core.server_base import ServerBase
from assemblyline.common import forge
from assemblyline.common.str_utils import safe_str

from assemblyline.datastore import SearchException


class WorkflowManager(ServerBase):
    def __init__(self):
        super().__init__('assemblyline.workflow')

        self.config = forge.get_config()
        self.datastore = forge.get_datastore(self.config)
        self.filestore = forge.get_filestore(self.config)
        self.start_ts = f"{self.datastore.ds.now}/{self.datastore.ds.day}-1{self.datastore.ds.day}"


    def get_last_reporting_ts(self, p_start_ts):
        self.log.info("Finding reporting timestamp for the last alert since {start_ts}...".format(start_ts=p_start_ts))
        result = self.datastore.alert.search(f"reporting_ts:[{p_start_ts} TO *]",
                                             sort='reporting_ts desc', rows=1, fl='reporting_ts', as_obj=False)
        items = result.get('items', [{}]) or [{}]
        return items[0].get("reporting_ts", p_start_ts)

    def try_run(self):
        while self.running:
            end_ts = self.get_last_reporting_ts(self.start_ts)
            if self.start_ts != end_ts:
                workflow_queries = [{
                    'status': "TRIAGE",
                    'name': "Triage all with no status",
                    'created_by': "SYSTEM",
                    'query': "NOT status:*"
                }]

                for item in self.datastore.workflow.stream_search("status:MALICIOUS"):
                    workflow_queries.append(item)

                for item in self.datastore.workflow.stream_search("status:NON-MALICIOUS"):
                    workflow_queries.append(item)

                for item in self.datastore.workflow.stream_search("status:ASSESS"):
                    workflow_queries.append(item)

                for item in self.datastore.workflow.stream_search('-status:["" TO *]'):
                    workflow_queries.append(item)

                for aq in workflow_queries:
                    self.log.info(f'Executing workflow filter: {aq["name"]}')
                    labels = aq.get('label', [])
                    status = aq.get('status', None)
                    priority = aq.get('priority', None)

                    if not status and not labels and not priority:
                        continue

                    fq = ["reporting_ts:[{start_ts} TO {end_ts}]".format(start_ts=self.start_ts, end_ts=end_ts)]

                    operations = []
                    fq_items = []
                    if labels:
                        operations.extend([(self.datastore.alert.UPDATE_APPEND, 'label', lbl) for lbl in labels])
                        for label in labels:
                            fq_items.append("label:\"{label}\"".format(label=label))
                    if priority:
                        operations.append((self.datastore.alert.UPDATE_SET, 'priority', priority))
                        fq_items.append("priority:*")
                    if status:
                        operations.append((self.datastore.alert.UPDATE_SET, 'status', status))
                        fq_items.append("status:*")

                    fq.append("NOT ({exclusion})".format(exclusion=" AND ".join(fq_items)))

                    try:
                        count = self.datastore.alert.update_by_query(aq['query'], operations, filters=fq)
                        # for item in ds.stream_search('alert', aq['query'], fq=fq):
                        #     count += 1
                        #     item_status = item.get('status', None)
                        #     if PRIORITY_LVL[status] <= PRIORITY_LVL[item_status]:
                        #         if status not in [None, "", "TRIAGE"]:
                        #             labels.append("CONFLICT.%s" % status)
                        #         status = item_status
                        #
                        #     msg = {
                        #         "label": labels,
                        #         "priority": priority,
                        #         "status": status,
                        #         "event_id": item['_yz_rk']
                        #     }
                        #     worker_msg = {
                        #         "action": "workflow",
                        #         "search_item": msg,
                        #         "original_msg": msg
                        #     }
                        #     action_queue_map[determine_worker_id(item['_yz_rk'])].push(QUEUE_PRIORITY, worker_msg)

                        if count:
                            self.log.info("{count} Alert(s) were affected by this filter.".format(count=count))
                            if 'id' in aq:
                                self.datastore.increment_workflow_counter(aq['id'], count)
                    except SearchException:
                        self.log.warning(f"Invalid query '{safe_str(aq.get('query', ''))}' in workflow "
                                         f"'{aq.get('name', 'unknown')}' by '{aq.get('created_by', 'unknown')}'")
                        continue

            else:
                self.log.info("Skipping all workflows since there where no new alerts in the specified time period.")

            time.sleep(30)
            self.start_ts = end_ts


if __name__ == "__main__":
    with WorkflowManager() as wm:
        wm.serve_forever()
