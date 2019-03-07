import copy

from .ingester import forge, NamedQueue, IngestTask
from .ingester import _completeq_name, _ingestq_name
from .ingester import get_client, now
from assemblyline.odm.models.submission import INGEST_SUBMISSION_DEFAULTS


class IngesterClient:
    """A convience object that wraps the input queue of the ingester."""

    def __init__(self, persistent_redis=None):
        # Create a config cache that will refresh config values periodically
        self.config = forge.CachedObject(forge.get_config)

        self.persistent_redis = persistent_redis or get_client(
            db=self.config.core.redis.persistent.db,
            host=self.config.core.redis.persistent.host,
            port=self.config.core.redis.persistent.port,
            private=False,
        )

        # MM Input. An external process places submission requests on this queue.
        self.ingest_queue = NamedQueue(_ingestq_name, self.persistent_redis)

    def ingest(self, **kwargs):
        # Load a snapshot of ingest parameters as of right now.
        # self.config is a timed cache
        ing_conf = self.config.core.ingester

        # In case the submission has 'all default' parameters
        kwargs['params'] = kwargs.get('params', {})

        # If there are missing fields, fill them in with the alternative model defaults
        params = copy.deepcopy(INGEST_SUBMISSION_DEFAULTS)
        params.update(kwargs['params'])
        kwargs['params'] = params

        # Fill in fields that may be found twice
        if 'classification' in kwargs and 'classification' not in params:
            params['classification'] = kwargs['classification']

        # Fill in fields that have a default set in the configuration
        params['max_extracted'] = params.get('max_extracted', ing_conf.default_max_extracted)
        params['max_supplementary'] = params.get('max_supplementary', ing_conf.default_max_supplementary)

        if 'description' not in params or not params['description']:
            params['description'] = ': '.join((ing_conf.description_prefix, kwargs.get('sha256', '')))

        params['submitter'] = params.get('submitter', ing_conf.default_user)
        services = params['services'] = params.get('services', {})

        if 'selected' not in services:
            services['selected'] = ing_conf.default_services
            services['resubmit'] = ing_conf.default_resubmit_services

        if 'resubmit' not in services:
            services['resubmit'] = ing_conf.default_resubmit_services

        # Fill in fields that the submitter shouldn't have any say over
        kwargs['ingest_time'] = now()

        # Type/field check then push into ingester
        self.ingest_queue.push(IngestTask(kwargs).as_primitives())
