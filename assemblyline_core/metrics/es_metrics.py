import elasticsearch
import time

from pprint import pprint

from assemblyline.common import forge
from assemblyline.common.isotime import now_as_iso

config = forge.get_config()

es_hosts = config.datastore.hosts
es = elasticsearch.Elasticsearch(hosts=config.datastore.hosts, connection_class=elasticsearch.RequestsHttpConnection)

INDEX_INTERVAL = 10

old_data = {}
while True:
    node_stats = es.nodes.stats()
    for node, stats in node_stats['nodes'].items():
        name = stats["name"]

        # Compute current values
        #    GC
        if name in old_data:
            old_gc_count = stats['jvm']['gc']['collectors']['old']['collection_count'] - old_data[name]['o_ogcc']
            old_gc_time = \
                stats['jvm']['gc']['collectors']['old']['collection_time_in_millis'] - old_data[name]['o_ogct']
            young_gc_count = stats['jvm']['gc']['collectors']['young']['collection_count'] - old_data[name]['o_ygcc']
            young_gc_time = \
                stats['jvm']['gc']['collectors']['young']['collection_time_in_millis'] - old_data[name]['o_ygct']

            #    Rates
            get_rate = (stats['indices']['get']['total'] - old_data[name]['o_gr'])
            index_rate = (stats['indices']['indexing']['index_total'] - old_data[name]['o_ir'])
            search_rate = (stats['indices']['search']['query_total'] - old_data[name]['o_sr'])

            #    Times
            get_time = stats['indices']['get']['time_in_millis'] - old_data[name]['o_gt']
            index_time = stats['indices']['indexing']['index_time_in_millis'] - old_data[name]['o_it']
            search_time = stats['indices']['search']['query_time_in_millis'] - old_data[name]['o_st']

            # Latency
            get_latency = get_time / get_rate
            index_latency = index_time / index_rate
            search_latency = search_time / search_rate
        else:
            old_gc_count = old_gc_time = young_gc_count = young_gc_time = 0
            get_rate = index_rate = search_rate = 0
            get_time = index_time = search_time = 0
            get_latency = index_latency = search_latency = 0

        # Save old fields values
        old_data.setdefault(name, {})
        old_data[name]['o_ogcc'] = stats['jvm']['gc']['collectors']['old']['collection_count']
        old_data[name]['o_ogct'] = stats['jvm']['gc']['collectors']['old']['collection_time_in_millis']
        old_data[name]['o_ygcc'] = stats['jvm']['gc']['collectors']['young']['collection_count']
        old_data[name]['o_ygct'] = stats['jvm']['gc']['collectors']['young']['collection_time_in_millis']
        old_data[name]['o_gr'] = stats['indices']['get']['total']
        old_data[name]['o_ir'] = stats['indices']['indexing']['index_total']
        old_data[name]['o_sr'] = stats['indices']['search']['query_total']
        old_data[name]['o_gt'] = stats['indices']['get']['time_in_millis']
        old_data[name]['o_it'] = stats['indices']['indexing']['index_time_in_millis']
        old_data[name]['o_st'] = stats['indices']['search']['query_time_in_millis']

        # Build Metrics document
        metric = {
            "timestamp": now_as_iso(),
            "node": {
                "name": name
            },
            "clients": {
                "current": stats['http']['current_open'],
                "total": stats['http']['total_opened']
            },
            "cpu": {
                "percent": stats["process"]["cpu"]["percent"]
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
                    "count": old_gc_count / INDEX_INTERVAL,
                    "time": old_gc_time / INDEX_INTERVAL,
                },
                "young": {
                    "count": young_gc_count / INDEX_INTERVAL,
                    "time": young_gc_time / INDEX_INTERVAL,
                }
            },
            "indices": {
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
                    "get": int(get_rate / INDEX_INTERVAL),
                    "index": int(index_rate / INDEX_INTERVAL),
                    "search": int(search_rate / INDEX_INTERVAL),
                },
                "time": {
                    "get": get_latency * int(get_rate / INDEX_INTERVAL),
                    "index": index_latency * int(index_rate / INDEX_INTERVAL),
                    "search": search_latency * int(search_rate / INDEX_INTERVAL),
                },
                "segments": stats['indices']['segments']['count']
            },
            "jvm": {
                "max": stats["jvm"]["mem"]["heap_max_in_bytes"],
                "used": stats["jvm"]["mem"]["heap_used_in_bytes"]
            },
            "system": {
                "load": stats["os"]["cpu"]["load_average"]["1m"]
            }
        }

        # TODO: Index memory, read/write thread queues, cgroup CPU/CFS
        pprint(metric)

    time.sleep(INDEX_INTERVAL)
