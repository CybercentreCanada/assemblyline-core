from .middleman import forge, NamedQueue, IngestTask
from .middleman import _completeq_name, _ingestq_name
from .middleman import get_client, now


class MiddlemanClient:
    """A convience object that wraps the input/output queues of the middleman."""

    def __init__(self):
        # Create a config cache that will refresh config values periodically
        self.config = forge.CachedObject(forge.get_config)

        # Connect to the redis servers
        self.redis = get_client(
            db=self.config.core.redis.nonpersistent.db,
            host=self.config.core.redis.nonpersistent.host,
            port=self.config.core.redis.nonpersistent.port,
            private=False,
        )
        self.persistent_redis = get_client(
            db=self.config.core.redis.persistent.db,
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )

        # MM Input. An external process creates a record when any submission completes.
        self.complete_queue = NamedQueue(_completeq_name, self.redis)

        # MM Input. An external process places submission requests on this queue.
        self.ingest_queue = NamedQueue(_ingestq_name, self.persistent_redis)

    def ingest(self, **kwargs):
        ing_conf = self.config.core.middleman

        # Fill in fields that have a hard coded default
        kwargs['deep_scan'] = kwargs.get('deep_scan', False)
        kwargs['ignore_dynamic_recursion_prevention'] = kwargs.get('ignore_dynamic_recursion_prevention', False)
        kwargs['ignore_cache'] = kwargs.get('ignore_cache', False)
        kwargs['ignore_filtering'] = kwargs.get('ignore_filtering', False)
        kwargs['completed_queue'] = _completeq_name

        # Fill in fields that have a default set in the configuration
        kwargs['max_extracted'] = kwargs.get('max_extracted', ing_conf.default_max_extracted)
        kwargs['max_supplementary'] = kwargs.get('max_supplementary', ing_conf.default_max_supplementary)

        if 'description' not in kwargs or not kwargs['description']:
            kwargs['description'] = ': '.join((ing_conf.description_prefix, kwargs['sha256'] or ''))

        # Fill in fields that the submitter shouldn't have any say over
        kwargs['ingest_time'] = now()

        # Type/field check then push into middleman
        self.ingest_queue.push(IngestTask(kwargs).json())
