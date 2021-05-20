import json
import tempfile

import elasticapm
import sys

import elasticsearch
import time

from assemblyline_core.metrics.helper import with_retries, ensure_indexes
from assemblyline_core.server_base import ServerBase

from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso

from packaging import version


class ESMetricsServer(ServerBase):
    """
    There can only be one of these type of metrics server running because it gathers elasticsearch metrics for
    the whole cluster.
    """

    def __init__(self, config=None):
        super().__init__('assemblyline.es_metrics', shutdown_timeout=15)
        self.config = config or forge.get_config()
        self.target_hosts = self.config.core.metrics.elasticsearch.hosts

        self.index_interval = 10.0
        self.old_node_data = {}
        self.old_cluster_data = {}
        self.old_index_data = {}
        self.old_index_time = 0.0
        self.old_node_time = 0.0
        self.old_cluster_time = 0.0

        if not self.target_hosts:
            self.log.error("No elasticsearch cluster defined to store metrics. All gathered stats will be ignored...")
            sys.exit(1)

        self.input_es = None
        self.target_es = None
        self.is_datastream = False

        if self.config.core.metrics.apm_server.server_url is not None:
            self.log.info(f"Exporting application metrics to: {self.config.core.metrics.apm_server.server_url}")
            elasticapm.instrument()
            self.apm_client = elasticapm.Client(server_url=self.config.core.metrics.apm_server.server_url,
                                                service_name="es_metrics")
        else:
            self.apm_client = None

    def get_node_metrics(self):
        if self.apm_client:
            self.apm_client.begin_transaction('metrics')

        try:
            es_nodes = with_retries(self.log, self.input_es.nodes.stats, level='shards')

            cur_time = time.time()
            if self.old_node_time:
                divider = cur_time - self.old_node_time
            else:
                divider = self.index_interval
            self.old_node_time = cur_time

            node_metrics = {}
            for node, stats in es_nodes['nodes'].items():
                state = "Online"
                name = stats["name"]
                index_count = 0
                shard_count = 0
                for index, shards in stats['indices']['shards'].items():
                    index_count += 1
                    for shard in shards:
                        for s_name, s_stats in shard.items():
                            shard_count += 1
                            if not s_stats['routing']['state'] == "STARTED":
                                state = "Degraded"

                gc_collectors = stats['jvm']['gc']['collectors']
                # Compute current values
                if name in self.old_node_data:
                    # GC
                    old_gc_count = gc_collectors['old']['collection_count'] - self.old_node_data[name]['ogcc']
                    old_gc_time = gc_collectors['old']['collection_time_in_millis'] - self.old_node_data[name]['ogct']
                    young_gc_count = gc_collectors['young']['collection_count'] - self.old_node_data[name]['ygcc']
                    young_gc_time = \
                        gc_collectors['young']['collection_time_in_millis'] - self.old_node_data[name]['ygct']

                    # Rates
                    get_rate = (stats['indices']['get']['total'] - self.old_node_data[name]['gr'])
                    index_rate = (stats['indices']['indexing']['index_total'] - self.old_node_data[name]['ir'])
                    search_rate = (stats['indices']['search']['query_total'] - self.old_node_data[name]['sr'])

                    # Times
                    get_time = stats['indices']['get']['time_in_millis'] - self.old_node_data[name]['gt']
                    index_time = stats['indices']['indexing']['index_time_in_millis'] - self.old_node_data[name]['it']
                    search_time = stats['indices']['search']['query_time_in_millis'] - self.old_node_data[name]['st']

                    # Latency
                    get_latency = float(get_time / (get_rate or 1.0))
                    index_latency = float(index_time / (index_rate or 1.0))
                    search_latency = float(search_time / (search_rate or 1.0))

                    # CGroup
                    cg = stats['os']['cgroup']
                    cg_nanos = cg['cpuacct']['usage_nanos'] - self.old_node_data[name]['cgn']
                    cg_throttled = cg['cpu']['stat']['time_throttled_nanos'] - self.old_node_data[name]['cgt']
                else:
                    old_gc_count = old_gc_time = young_gc_count = young_gc_time = 0.0
                    get_rate = index_rate = search_rate = 0.0
                    get_latency = index_latency = search_latency = 0.0
                    cg_nanos = cg_throttled = 0.0

                # Save old fields values
                self.old_node_data.setdefault(name, {})
                self.old_node_data[name]['ogcc'] = gc_collectors['old']['collection_count']
                self.old_node_data[name]['ogct'] = gc_collectors['old']['collection_time_in_millis']
                self.old_node_data[name]['ygcc'] = gc_collectors['young']['collection_count']
                self.old_node_data[name]['ygct'] = gc_collectors['young']['collection_time_in_millis']
                self.old_node_data[name]['gr'] = stats['indices']['get']['total']
                self.old_node_data[name]['ir'] = stats['indices']['indexing']['index_total']
                self.old_node_data[name]['sr'] = stats['indices']['search']['query_total']
                self.old_node_data[name]['gt'] = stats['indices']['get']['time_in_millis']
                self.old_node_data[name]['it'] = stats['indices']['indexing']['index_time_in_millis']
                self.old_node_data[name]['st'] = stats['indices']['search']['query_time_in_millis']
                self.old_node_data[name]['cgn'] = stats['os']['cgroup']['cpuacct']['usage_nanos']
                self.old_node_data[name]['cgt'] = stats['os']['cgroup']['cpu']['stat']['time_throttled_nanos']

                # Build Metrics document
                timestamp = now_as_iso()
                metric = {
                    "timestamp": timestamp,
                    "node": {
                        "name": name,
                        "state": state,
                        "roles": stats["roles"],
                        "transport_address": stats['transport_address']
                    },
                    "clients": {
                        "current": stats['http']['current_open'],
                        "total": stats['http']['total_opened']
                    },
                    "cpu": {
                        "percent": stats["process"]["cpu"]["percent"],
                        "cgroup": {
                            "timing": {
                                "usage": cg_nanos / divider,
                                "throttled": cg_throttled / divider
                            },
                            "count": {
                                "periods": stats['os']['cgroup']['cpu']['stat']['number_of_elapsed_periods'],
                                "throttled": stats['os']['cgroup']['cpu']['stat']['number_of_times_throttled']
                            }

                        }
                    },
                    "disk": {
                        "total": stats["fs"]["total"]["total_in_bytes"],
                        "available": stats["fs"]["total"]["available_in_bytes"],
                    },
                    "file_descriptors": {
                        "open": stats['process']['open_file_descriptors'],
                        "max": stats['process']['max_file_descriptors']
                    },
                    # This is an aggregate over time. Should we compute the value from the last time ?
                    "gc": {
                        "old": {
                            "count": old_gc_count / divider,
                            "time": old_gc_time / divider,
                        },
                        "young": {
                            "count": young_gc_count / divider,
                            "time": young_gc_time / divider,
                        }
                    },
                    "indices": {
                        "count": index_count,
                        "docs": {
                            "count": stats['indices']['docs']['count'],
                            "deleted": stats['indices']['docs']['deleted'],
                            "size": stats['indices']['store']['size_in_bytes'],
                        },
                        "latency": {
                            "get": get_latency,
                            "index": index_latency,
                            "search": search_latency,
                        },
                        "rate": {
                            "get": get_rate / divider,
                            "index": index_rate / divider,
                            "search": search_rate / divider,
                        },
                        "time": {
                            "get": get_latency * ((get_rate / divider) or 1.0),
                            "index": index_latency * ((index_rate / divider) or 1.0),
                            "search": search_latency * ((search_rate / divider) or 1.0),
                        },
                        "segments": stats['indices']['segments']['count'],
                        "memory": {
                            "total": stats['indices']['segments']['memory_in_bytes'],
                            "docs": stats['indices']['segments']['doc_values_memory_in_bytes'],
                            "terms": stats['indices']['segments']['terms_memory_in_bytes'],
                            "points": stats['indices']['segments']['points_memory_in_bytes'],
                            "stored_fields": stats['indices']['segments']['stored_fields_memory_in_bytes'],
                            "index_writer": stats['indices']['segments']['index_writer_memory_in_bytes'],
                            "norms": stats['indices']['segments']['norms_memory_in_bytes'],
                            "fixed_bitsets": stats['indices']['segments']['fixed_bit_set_memory_in_bytes'],
                            "term_vectors": stats['indices']['segments']['term_vectors_memory_in_bytes'],
                            "version_map": stats['indices']['segments']['version_map_memory_in_bytes'],
                            "query_cache": stats['indices']['query_cache']['memory_size_in_bytes'],
                            "request_cache": stats['indices']['request_cache']['memory_size_in_bytes'],
                            "fielddata": stats['indices']['fielddata']['memory_size_in_bytes']
                        }
                    },
                    "jvm": {
                        "max": stats["jvm"]["mem"]["heap_max_in_bytes"],
                        "used": stats["jvm"]["mem"]["heap_used_in_bytes"]
                    },
                    "shards": {
                        "count": shard_count
                    },
                    "system": {
                        "load": stats["os"]["cpu"]["load_average"]["1m"]
                    },
                    "thread": {
                        "write": {
                            "queue": stats['thread_pool']['write']['queue'],
                            "rejection": stats['thread_pool']['write']['rejected'],
                        },
                        "search": {
                            "queue": stats['thread_pool']['search']['queue'],
                            "rejection": stats['thread_pool']['search']['rejected'],
                        },
                        "get": {
                            "queue": stats['thread_pool']['get']['queue'],
                            "rejection": stats['thread_pool']['get']['rejected'],
                        },
                    }
                }
                if self.is_datastream:
                    metric['@timestamp'] = timestamp
                node_metrics[node] = metric
            return node_metrics
        finally:
            if self.apm_client:
                self.apm_client.end_transaction('gather_node_metrics', 'success')

    def get_cluster_metrics(self):
        if self.apm_client:
            self.apm_client.begin_transaction('metrics')

        try:
            cluster_stats = with_retries(self.log, self.input_es.cluster.stats)
            cluster_health = with_retries(self.log, self.input_es.cluster.health)
            indices_metrics = with_retries(self.log, self.input_es.indices.stats, level='cluster')

            cur_time = time.time()
            if self.old_cluster_time:
                divider = cur_time - self.old_cluster_time
            else:
                divider = self.index_interval
            self.old_cluster_time = cur_time

            # Compute current values
            if len(self.old_cluster_data) != 0:
                all_metrics = indices_metrics['_all']['total']
                # Rates
                get_rate = (all_metrics['get']['total'] - self.old_cluster_data['gr'])
                index_rate = (all_metrics['indexing']['index_total'] - self.old_cluster_data['ir'])
                search_rate = (all_metrics['search']['query_total'] - self.old_cluster_data['sr'])

                # Times
                get_time = all_metrics['get']['time_in_millis'] - self.old_cluster_data['gt']
                index_time = all_metrics['indexing']['index_time_in_millis'] - self.old_cluster_data['it']
                search_time = all_metrics['search']['query_time_in_millis'] - self.old_cluster_data['st']

                # Latency
                get_latency = float(get_time / (get_rate or 1.0))
                index_latency = float(index_time / (index_rate or 1.0))
                search_latency = float(search_time / (search_rate or 1.0))

            else:
                get_rate = index_rate = search_rate = 0.0
                get_latency = index_latency = search_latency = 0.0

            # Save old fields values
            self.old_cluster_data['gr'] = indices_metrics['_all']['total']['get']['total']
            self.old_cluster_data['ir'] = indices_metrics['_all']['total']['indexing']['index_total']
            self.old_cluster_data['sr'] = indices_metrics['_all']['total']['search']['query_total']
            self.old_cluster_data['gt'] = indices_metrics['_all']['total']['get']['time_in_millis']
            self.old_cluster_data['it'] = indices_metrics['_all']['total']['indexing']['index_time_in_millis']
            self.old_cluster_data['st'] = indices_metrics['_all']['total']['search']['query_time_in_millis']

            jvm_mem = cluster_stats['nodes']["jvm"]["mem"]
            fs = cluster_stats['nodes']["fs"]
            timestamp = now_as_iso()
            metric = {
                "timestamp": timestamp,
                "name": cluster_health['cluster_name'],
                "status": cluster_health['status'],
                "indices": {
                    "count": cluster_stats['indices']["count"],
                    "docs": {
                        "count": cluster_stats['indices']["docs"]["count"],
                        "size": cluster_stats['indices']["store"]["size_in_bytes"]
                    },
                    "latency": {
                        "get": get_latency,
                        "index": index_latency,
                        "search": search_latency,
                    },
                    "rate": {
                        "get": get_rate / divider,
                        "index": index_rate / divider,
                        "search": search_rate / divider,
                    },
                    "time": {
                        "get": get_latency * ((get_rate / divider) or 1.0),
                        "index": index_latency * ((index_rate / divider) or 1.0),
                        "search": search_latency * ((search_rate / divider) or 1.0),
                    },
                    "shards": {
                        "initializing": cluster_health["initializing_shards"],
                        "delayed": cluster_health["delayed_unassigned_shards"],
                        "relocating": cluster_health["relocating_shards"],
                        "primary": cluster_health["active_primary_shards"],
                        "active": cluster_health["active_shards"],
                        "unassigned": cluster_health['unassigned_shards']
                    }
                },
                "nodes": {
                    "count": cluster_health['number_of_nodes'],
                    "heap": {
                        "total": jvm_mem["heap_max_in_bytes"],
                        "used": jvm_mem["heap_used_in_bytes"],
                        "available": jvm_mem["heap_max_in_bytes"] - jvm_mem["heap_used_in_bytes"]
                    },
                    "fs": {
                        "total": fs["total_in_bytes"],
                        "used": fs["total_in_bytes"] - fs["available_in_bytes"],
                        "available": fs["available_in_bytes"],
                    },
                }
            }
            if self.is_datastream:
                metric['@timestamp'] = timestamp
            return metric
        finally:
            if self.apm_client:
                self.apm_client.end_transaction('gather_cluster_metrics', 'success')

    def get_index_metrics(self):
        if self.apm_client:
            self.apm_client.begin_transaction('metrics')

        try:
            health = {x['index']: x['health'] for x in with_retries(self.log, self.input_es.cat.indices, format='json')}
            shards = {}
            for x in with_retries(self.log, self.input_es.cat.shards, format='json'):
                shards.setdefault(x['index'], {"total": 0, "unassigned": 0})
                shards[x['index']]['total'] += 1
                if x['state'] != "STARTED":
                    shards[x['index']]['unassigned'] += 1

            es_indices = with_retries(self.log, self.input_es.indices.stats, level='indices')

            cur_time = time.time()
            if self.old_index_time:
                divider = cur_time - self.old_index_time
            else:
                divider = self.index_interval
            self.old_index_time = cur_time

            indices_metrics = {}
            for name, stats in es_indices['indices'].items():

                # Compute current values
                if name in self.old_index_data:
                    p_stat = stats['primaries']
                    # Rates
                    get_rate = (stats['total']['get']['total'] - self.old_index_data[name]['gr'])
                    index_rate = (stats['total']['indexing']['index_total'] - self.old_index_data[name]['ir'])
                    search_rate = (stats['total']['search']['query_total'] - self.old_index_data[name]['sr'])
                    get_p_rate = (p_stat['get']['total'] - self.old_index_data[name]['pgr'])
                    index_p_rate = (p_stat['indexing']['index_total'] - self.old_index_data[name]['pir'])
                    search_p_rate = (p_stat['search']['query_total'] - self.old_index_data[name]['psr'])

                    # Times
                    get_time = stats['total']['get']['time_in_millis'] - self.old_index_data[name]['gt']
                    index_time = stats['total']['indexing']['index_time_in_millis'] - self.old_index_data[name]['it']
                    search_time = stats['total']['search']['query_time_in_millis'] - self.old_index_data[name]['st']
                    get_p_time = p_stat['get']['time_in_millis'] - self.old_index_data[name]['pgt']
                    index_p_time = p_stat['indexing']['index_time_in_millis'] - self.old_index_data[name]['pit']
                    search_p_time = p_stat['search']['query_time_in_millis'] - self.old_index_data[name]['pst']

                    # Latency
                    get_latency = float(get_time / (get_rate or 1.0))
                    index_latency = float(index_time / (index_rate or 1.0))
                    search_latency = float(search_time / (search_rate or 1.0))
                    get_p_latency = float(get_p_time / (get_p_rate or 1.0))
                    index_p_latency = float(index_p_time / (index_p_rate or 1.0))
                    search_p_latency = float(search_p_time / (search_p_rate or 1.0))

                else:
                    get_rate = index_rate = search_rate = 0.0
                    get_latency = index_latency = search_latency = 0.0
                    get_p_rate = index_p_rate = search_p_rate = 0.0
                    get_p_latency = index_p_latency = search_p_latency = 0.0

                # Save old fields values
                self.old_index_data.setdefault(name, {})
                self.old_index_data[name]['gr'] = stats['total']['get']['total']
                self.old_index_data[name]['ir'] = stats['total']['indexing']['index_total']
                self.old_index_data[name]['sr'] = stats['total']['search']['query_total']
                self.old_index_data[name]['gt'] = stats['total']['get']['time_in_millis']
                self.old_index_data[name]['it'] = stats['total']['indexing']['index_time_in_millis']
                self.old_index_data[name]['st'] = stats['total']['search']['query_time_in_millis']

                self.old_index_data[name]['pgr'] = stats['primaries']['get']['total']
                self.old_index_data[name]['pir'] = stats['primaries']['indexing']['index_total']
                self.old_index_data[name]['psr'] = stats['primaries']['search']['query_total']
                self.old_index_data[name]['pgt'] = stats['primaries']['get']['time_in_millis']
                self.old_index_data[name]['pit'] = stats['primaries']['indexing']['index_time_in_millis']
                self.old_index_data[name]['pst'] = stats['primaries']['search']['query_time_in_millis']

                timestamp = now_as_iso()
                metric = {
                    "name": name,
                    "timestamp": timestamp,
                    "status": health.get(name, 'red'),
                    "shards": shards.get(name, {"total": 0, "unassigned": 0}),
                    "docs": {
                        "total": stats['total']['docs']['count']
                    },
                    "latency": {
                        "get": {
                            "total": get_latency,
                            "primaries": get_p_latency
                        },
                        "index": {
                            "total": index_latency,
                            "primaries": index_p_latency
                        },
                        "search": {
                            "total": search_latency,
                            "primaries": search_p_latency,
                        }
                    },
                    "rate": {
                        "get": {
                            "total": get_rate / divider,
                            "primaries": get_p_rate / divider
                        },
                        "index": {
                            "total": index_rate / divider,
                            "primaries": index_p_rate / divider
                        },
                        "search": {
                            "total": search_rate / divider,
                            "primaries": search_p_rate / divider,
                        }
                    },
                    "time": {
                        "get": {
                            "total": get_latency * ((get_rate / divider) or 1.0),
                            "primaries": get_p_latency * ((get_p_rate / divider) or 1.0)
                        },
                        "index": {
                            "total": index_latency * ((index_rate / divider) or 1.0),
                            "primaries": index_p_latency * ((index_p_rate / divider) or 1.0)
                        },
                        "search": {
                            "total": search_latency * ((search_rate / divider) or 1.0),
                            "primaries": search_p_latency * ((search_p_rate / divider) or 1.0),
                        }
                    },
                    "segments": {
                        "total": stats['total']['segments']['count'],
                        "primaries": stats['primaries']['segments']['count'],
                    },
                    "merges_size": {
                        "primaries": stats['primaries']['merges']['current_size_in_bytes'],
                        "total": stats['total']['merges']['current_size_in_bytes']
                    },
                    "size": {
                        "primaries": stats['primaries']['store']['size_in_bytes'],
                        "total": stats['total']['store']['size_in_bytes']
                    },
                    "memory": {
                        "total": stats['total']['segments']['memory_in_bytes'],
                        "docs": stats['total']['segments']['doc_values_memory_in_bytes'],
                        "terms": stats['total']['segments']['terms_memory_in_bytes'],
                        "points": stats['total']['segments']['points_memory_in_bytes'],
                        "stored_fields": stats['total']['segments']['stored_fields_memory_in_bytes'],
                        "index_writer": stats['total']['segments']['index_writer_memory_in_bytes'],
                        "norms": stats['total']['segments']['norms_memory_in_bytes'],
                        "fixed_bitsets": stats['total']['segments']['fixed_bit_set_memory_in_bytes'],
                        "term_vectors": stats['total']['segments']['term_vectors_memory_in_bytes'],
                        "version_map": stats['total']['segments']['version_map_memory_in_bytes'],
                        "query_cache": stats['total']['query_cache']['memory_size_in_bytes'],
                        "request_cache": stats['total']['request_cache']['memory_size_in_bytes'],
                        "fielddata": stats['total']['fielddata']['memory_size_in_bytes']
                    }
                }
                if self.is_datastream:
                    metric['@timestamp'] = timestamp

                indices_metrics[name] = metric

            return indices_metrics
        finally:
            if self.apm_client:
                self.apm_client.end_transaction('gather_index_metrics', 'success')

    def try_run(self):
        # If our connection to the metrics database requires a custom ca cert, prepare it
        ca_certs = None
        if self.config.core.metrics.elasticsearch.host_certificates:
            with tempfile.NamedTemporaryFile(delete=False) as ca_certs_file:
                ca_certs = ca_certs_file.name
                ca_certs_file.write(self.config.core.metrics.elasticsearch.host_certificates.encode())

        # Open connections to the input and output databases
        self.input_es = elasticsearch.Elasticsearch(hosts=self.config.datastore.hosts,
                                                    connection_class=elasticsearch.RequestsHttpConnection,
                                                    max_retries=0)
        self.target_es = elasticsearch.Elasticsearch(hosts=self.target_hosts,
                                                     connection_class=elasticsearch.RequestsHttpConnection,
                                                     max_retries=0,
                                                     ca_certs=ca_certs)
        # Check if target_es supports datastreams (>=7.9)
        es_metric_indices = ['es_cluster', 'es_nodes', 'es_indices']
        self.is_datastream = version.parse(self.target_es.info()['version']['number']) >= version.parse("7.9")
        ensure_indexes(self.log, self.target_es, self.config.core.metrics.elasticsearch,
                       es_metric_indices, datastream_enabled=self.is_datastream)

        # Were data streams created for one of the indices specified?
        try:
            if not self.target_es.indices.get_index_template(name="al_metrics_es_cluster_ds"):
                self.is_datastream = False
        except elasticsearch.exceptions.TransportError:
            self.is_datastream = False

        while self.running:
            self.heartbeat()
            start_time = time.time()

            # CLUSTER
            self.log.info("Gathering cluster metrics ...")
            cluster_metrics = self.get_cluster_metrics()
            self.log.debug(cluster_metrics)

            # NODES
            self.log.info("Gathering node metrics ...")
            node_metrics = self.get_node_metrics()
            self.log.debug(node_metrics)

            # INDICES
            self.log.info("Gathering indices metrics ...")
            index_metrics = self.get_index_metrics()
            self.log.debug(index_metrics)

            # Saving Metrics
            self.log.info("Saving metrics ...")
            if self.apm_client:
                self.apm_client.begin_transaction('metrics')

            suffix = ""
            action = "index"
            # Datastreams only allow create actions via Bulk API; use _ds for distinction between normal indices
            if self.is_datastream:
                action = "create"
                suffix = "_ds"
            plan = [json.dumps({action: {"_index": f"al_metrics_es_cluster{suffix}"}}), json.dumps(cluster_metrics)]

            for metric in node_metrics.values():
                plan.append(json.dumps({action: {"_index": f"al_metrics_es_nodes{suffix}"}}))
                plan.append(json.dumps(metric))

            for metric in index_metrics.values():
                plan.append(json.dumps({action: {"_index": f"al_metrics_es_indices{suffix}"}}))
                plan.append(json.dumps(metric))

            with_retries(self.log, self.target_es.bulk, body="\n".join(plan))
            if self.apm_client:
                self.apm_client.end_transaction('saving_metrics', 'success')

            # Wait and repeat
            elapsed_time = (time.time() - start_time)
            sleep_time = max(0.0, self.index_interval - elapsed_time)
            if sleep_time < 5:
                self.log.warning(f"Metrics gathering is taking longer than it should! [{int(elapsed_time)}s]")

            self.log.info(f'Waiting {int(sleep_time)}s for next run ...')
            time.sleep(sleep_time)


if __name__ == '__main__':
    cfg = forge.get_config()
    with ESMetricsServer(config=cfg) as metricsd:
        metricsd.serve_forever()
