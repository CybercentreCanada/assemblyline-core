import re
import logging
import time
from assemblyline import odm
from assemblyline.odm.models.result import Result


def normalize_data(data):
    if isinstance(data, dict):
        return tuple((k, normalize_data(data[k])) for k in sorted(data.keys()))
    elif isinstance(data, (list, tuple)):
        return tuple(normalize_data(v) for v in data)
    else:
        return data


def config_hash(config):
    return str(hash(normalize_data(config)))


@odm.model()
class Service(odm.Model):
    name = odm.Keyword()
    category = odm.Keyword()
    stage = odm.Keyword()
    accepts = odm.Keyword(index=False, default_set=True)
    rejects = odm.Keyword(index=False, default_set=True)


class ConfigManager:
    REFRESH_SECONDS = 5
    system_category = 'system'

    def __init__(self, datastore):
        self.datastore = datastore
        self._seed = None
        self._update_time = 0

    def build_schedule(self, submission, file_type: str):
        all_services = self.services()

        # Load the selected and excluded services by catagory
        excluded = self.expand_categories(submission.excluded_categories)
        if not submission.selected_categories:
            selected = [s for s in all_services.keys()]
        else:
            selected = self.expand_categories(submission.selected_categories)

        # Add in all system services, they are always needed, and can't be excluded
        for k, v in all_services.items():
            if v.category == self.system_category:
                selected.append(k)

        # Add all selected, accepted, and not rejected services to the schedule
        schedule = [{} for _ in self.stages()]
        services = list(set(selected) - set(excluded))
        selected = []
        skipped = []
        for name in services:
            service = all_services.get(name, None)
            print(name, service)

            if not service:
                skipped.append(name)
                logging.warning(f"Service configuration not found: {name}")
                continue

            print(bool(re.match(service.accepts, file_type)))
            print(bool(re.match(service.rejects, file_type)))

            if re.match(service.accepts, file_type) and (not service.rejects or not re.match(service.rejects, file_type)):
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

    def build_result_key(self, file_hash, service_name, config_hash):
        # TODO get service version from config
        return Result.build_key(
            service_name=service_name,
            version='0',
            file_hash=file_hash,
            conf_key=config_hash,
        )

    def categories(self):
        all_categories = {}
        for service in self.services().values():
            try:
                all_categories[service.category].append(service.name)
            except KeyError:
                all_categories[service.category] = [service.name]
        return all_categories

    def stage_index(self, stage):
        return self.stages().index(stage)

    def stages(self):
        return self.seed['services']['stages']

    def service_failure_limit(self, service):
        return self.seed['services']['master_list'][service].get('failure_limit', 5)

    @property
    def seed(self):
        if time.time() - self._update_time > self.REFRESH_SECONDS:
            self._seed = self.datastore.blobs.get('seed')
            self._update_time = time.time()
        return self._seed

    def services(self):
        return {name: Service(dict(name=name, **data)) for name, data in self.seed['services']['master_list'].items()}
