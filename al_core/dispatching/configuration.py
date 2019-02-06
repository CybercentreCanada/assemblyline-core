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


@odm.model(index=True)
class DispatchConfig(odm.Model):
    stages = odm.List(odm.Keyword())


@odm.model(index=True, store=True)
class Service(odm.Model):
    name = odm.Keyword()
    category = odm.Keyword()
    stage = odm.Keyword()
    accepts = odm.Keyword(default='')
    rejects = odm.Keyword(default='')
    failure_limit = odm.Integer(default=5)
    params = odm.Mapping(odm.Keyword(), default={})


class Scheduler:
    REFRESH_SECONDS = 5
    system_category = 'system'

    def __init__(self, datastore, config):
        self.datastore = datastore
        datastore.register('services', Service)
        self._services = datastore.services
        self._cached = None
        self._update_time = 0
        self.config = config

    def build_schedule(self, submission, file_type: str):
        all_services = self.services()

        # Load the selected and excluded services by catagory
        excluded = self.expand_categories(submission.params.services.excluded)
        if not submission.params.services.selected:
            selected = [s for s in all_services.keys()]
        else:
            selected = self.expand_categories(submission.params.services.selected)

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
        return self.config.core.dispatcher.stages

    def services(self):
        if time.time() - self._update_time > self.REFRESH_SECONDS:
            self._cached = {ser.name: ser for ser in self._services.search('*:*', fl='*', rows=1000)['items']}
            self._update_time = time.time()
        return self._cached

    def service(self, service_name):
        cached_services = self.services()
        return cached_services[service_name]

    def service_timeout(self, service_name):
        return 60*60

    def service_failure_limit(self, service_name):
        return self.service(service_name).failure_limit

    def build_service_config(self, service_name, submission):
        """
        Determine the parameter mapping for a service.

        Combine the default and submission specific service parameters to
        produce the final configuration for this submission.
        """
        params = dict(self.service(service_name).params)
        params.update(submission.params.service_spec.get(service_name, {}))
        return params
