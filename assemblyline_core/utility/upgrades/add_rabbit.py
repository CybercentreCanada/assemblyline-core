from assemblyline.common import forge
from assemblyline.remote.datatypes.queues.priority import PriorityQueue
from assemblyline.remote.queues.connection import get_rabbit_connection
from assemblyline.remote.datatypes import get_client
from assemblyline.remote.queues.named import NamedQueue
from assemblyline_core.ingester.ingester import IngestTask, connect_ingest_backlog, connect_ingest_queue, BINS


def drain_queues():
    """
    When queues are moved from redis to rabbit the data needs to be moved
    to make the update simple.
    """
    config = forge.get_config()

    rabbit = get_rabbit_connection(config.core.rabbit_mq)
    redis_persistent = get_client(
        host=config.core.redis.persistent.host,
        port=config.core.redis.persistent.port,
        private=False,
    )

    # Move any ingest queue content to rabbit
    old_ingest_queue = NamedQueue('m-ingest', redis_persistent)
    ingest_queue = connect_ingest_queue(rabbit)
    while True:
        row = old_ingest_queue.pop()
        if not row:
            break
        ingest_queue.push(row)
        rabbit.process(0)

    # Internal. Unique requests are placed in and processed from this queue.
    old_unique_queue = PriorityQueue('m-unique', redis_persistent)
    backlog_queues = connect_ingest_backlog(rabbit)
    while True:
        row = old_unique_queue.blocking_pop()
        if not row:
            break

        try:
            task = IngestTask(row)
            priority = task.params.priority

            for (_, (low, high)), queue in zip(BINS, backlog_queues):
                if low <= priority <= high:
                    queue.push(priority - low, row)
                    break
            else:
                backlog_queues[0].push(0, row)
            rabbit.process(0)

        except Exception:
            pass
