from __future__ import annotations
from typing import Dict, cast

import logging
import os
import re

from assemblyline.common.forge import CachedObject
from assemblyline.datastore.helper import AssemblylineDatastore
from assemblyline.odm.models.config import Config
from assemblyline.odm.models.service import Service
from assemblyline.odm.models.submission import Submission
from assemblyline_core.server_base import get_service_stage_hash, ServiceStage


# If you are doing development and you want the system to route jobs ignoring the service setup/teardown
# set an environment variable SKIP_SERVICE_SETUP to true for all dispatcher containers
SKIP_SERVICE_SETUP = os.environ.get('SKIP_SERVICE_SETUP', 'false').lower() in ['true', '1']


class Scheduler:
    """This object encapsulates building the schedule for a given file type for a submission."""

    def __init__(self, datastore: AssemblylineDatastore, config: Config, redis):
        self.datastore = datastore
        self.config = config
        self.services = cast(Dict[str, Service], CachedObject(self._get_services))
        self.service_stage = get_service_stage_hash(redis)

    def build_schedule(self, submission: Submission, file_type: str) -> list[dict[str, Service]]:
        all_services = dict(self.services)

        # Load the selected and excluded services by category
        excluded = self.expand_categories(submission.params.services.excluded)
        runtime_excluded = self.expand_categories(submission.params.services.runtime_excluded)
        if not submission.params.services.selected:
            selected = [s for s in all_services.keys()]
        else:
            selected = self.expand_categories(submission.params.services.selected)

        # Add all selected, accepted, and not rejected services to the schedule
        schedule: list[dict[str, Service]] = [{} for _ in self.config.services.stages]
        services = list(set(selected) - set(excluded) - set(runtime_excluded))
        selected = []
        skipped = []
        for name in services:
            service = all_services.get(name, None)

            if not service:
                skipped.append(name)
                logging.warning(f"Service configuration not found: {name}")
                continue

            accepted = not service.accepts or re.match(service.accepts, file_type)
            rejected = bool(service.rejects) and re.match(service.rejects, file_type)

            if accepted and not rejected:
                schedule[self.stage_index(service.stage)][name] = service
                selected.append(name)
            else:
                skipped.append(name)

        return schedule

    def expand_categories(self, services: list[str]) -> list[str]:
        """Expands the names of service categories found in the list of services.

        Args:
            services (list): List of service category or service names.
        """
        if services is None:
            return []

        services = list(services)
        categories = self.categories()

        found_services = []
        seen_categories: set[str] = set()
        while services:
            name = services.pop()

            # If we found a new category mix in it's content
            if name in categories:
                if name not in seen_categories:
                    # Add all of the items in this group to the list of
                    # things that we need to evaluate, and mark this
                    # group as having been seen.
                    services.extend(categories[name])
                    seen_categories.update(name)
                continue

            # If it isn't a category, its a service
            found_services.append(name)

        # Use set to remove duplicates, set is more efficient in batches
        return list(set(found_services))

    def categories(self) -> Dict[str, list[str]]:
        all_categories: dict[str, list[str]] = {}
        for service in self.services.values():
            try:
                all_categories[service.category].append(service.name)
            except KeyError:
                all_categories[service.category] = [service.name]
        return all_categories

    def stage_index(self, stage):
        return self.config.services.stages.index(stage)

    def _get_services(self):
        stages = self.service_stage.items()

        # noinspection PyUnresolvedReferences
        return {x.name: x for x in self.datastore.list_all_services(full=True)
                if x.enabled and (stages.get(x.name) == ServiceStage.Running or SKIP_SERVICE_SETUP)}
