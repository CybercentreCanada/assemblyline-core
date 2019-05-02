"""
This object encapsulates the

"""
import re
import logging
from typing import Dict, List

from assemblyline.odm.models.service import Service
from assemblyline.common.forge import CachedObject


# def normalize_data(data):
#     if isinstance(data, dict):
#         return tuple((k, normalize_data(data[k])) for k in sorted(data.keys()))
#     elif isinstance(data, (list, tuple)):
#         return tuple(normalize_data(v) for v in data)
#     else:
#         return data
#
#
# def config_hash(config):
#     return str(hash(normalize_data(config)))
#


class Scheduler:
    def __init__(self, datastore, config):
        self.datastore = datastore
        self.config = config
        self.services: Dict[str, Service] = CachedObject(self._get_services)

    @property
    def system_category(self):
        return self.config.services.system_category

    def build_schedule(self, submission, file_type: str) -> List[Dict[str, Service]]:
        all_services = dict(self.services)

        # Load the selected and excluded services by category
        excluded = self.expand_categories(submission.params.services.excluded)
        if not submission.params.services.selected:
            selected = [s for s in all_services.keys()]
        else:
            selected = self.expand_categories(submission.params.services.selected)

        # Add in all system services, they are always needed, and can't be excluded
        system_services = [k for k, v in all_services.items() if v.category == self.system_category]

        # Add all selected, accepted, and not rejected services to the schedule
        schedule: List[Dict[str, Service]] = [{} for _ in self.config.services.stages]
        services = list((set(selected) - set(excluded)) | set(system_services))
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

    def expand_categories(self, services: list):
        """Expands the names of service categories found in the list of services.

        Args:
            services (list): List of service catagory or service names.
        """
        if services is None:
            return []

        services = list(services)
        categories = self.categories()

        found_services = []
        seen_categories = set()
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

        # Use set to remove duplicates, set is more efficent in batches
        return list(set(found_services))

    def categories(self):
        all_categories = {}
        for service in self.services.values():
            try:
                all_categories[service.category].append(service.name)
            except KeyError:
                all_categories[service.category] = [service.name]
        return all_categories

    def stage_index(self, stage):
        return self.config.services.stages.index(stage)

    def _get_services(self):
        return {x.name: x for x in self.datastore.list_all_services(full=True) if x.enabled}
