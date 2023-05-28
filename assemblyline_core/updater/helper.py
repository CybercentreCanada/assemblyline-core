import json
import requests
import re
import socket
import string

from assemblyline.common.version import FRAMEWORK_VERSION, SYSTEM_VERSION
from assemblyline.odm.models.config import Config as SystemConfig
from assemblyline.odm.models.service import Service as ServiceConfig

from base64 import b64encode
from collections import defaultdict
from logging import Logger
from packaging.version import parse, Version
from typing import Any, Dict, List, Tuple

DEFAULT_DOCKER_REGISTRY = "registry.hub.docker.com"


class ContainerRegistry():
    def __init__(self, server, headers: Dict[str, str] = None, verify: bool = True,
                 proxies: Dict[str, str] = None, *args, **kwargs):
        self.server = server
        self.session = requests.Session()
        self.session.headers = headers
        self.session.verify = verify
        self.session.proxies = proxies

    def _make_request(self, path: str) -> Dict[str, Any]:
        request_path = f"{self.server}{path}"
        resp = None
        try:
            resp = self.session.get(f"https://{request_path}")
        except requests.exceptions.SSLError:
            # Connect to insecure registry over HTTP (development only)
            if not self.session.verify:
                resp = self.session.get(f"http://{request_path}")
        # Test for valid response
        if resp and resp.ok:
            return resp.json()
        return None

    # Provide a means of obtaining a list of tags from a container registry
    def get_image_tags(self, image_name) -> List[str]:
        raise NotImplementedError()

    # Provide a means of obtaining the compatible operating system for the container image
    def get_image_os(self, image_name, image_tag) -> str:
        raise NotImplementedError()


class DockerHub(ContainerRegistry):
    def __init__(self, update_channel, proxies: Dict[str, str] = None, *args, **kwargs):
        super().__init__(DEFAULT_DOCKER_REGISTRY, None, True, proxies)
        self.update_channel = update_channel

    def get_image_tags(self, image_name) -> List[str]:
        resp = self._make_request(f"/v2/repositories/{image_name}/tags?page_size=5&page=1&name={self.update_channel}")
        if resp:
            return [x['name'] for x in resp['results']]
        return []

    def get_image_os(self, image_name, image_tag) -> str:
        resp = self._make_request(f"/v2/repositories/{image_name}/tags/{image_tag}")
        if resp:
            return resp['images'][0]['os']
        return None


# Ref: https://docs.docker.com/registry/spec/api/#detail
class DockerRegistry(ContainerRegistry):
    def get_image_tags(self, image_name) -> List[str]:
        # Find latest tag for each types
        resp = self._make_request(f"/v2/{image_name}/tags/list")
        if resp:
            return resp['tags'] or []
        return []

    def get_image_os(self, image_name, image_tag) -> str:
        resp = self._make_request(f"/v2/{image_name}/manifests/{image_tag}")
        if resp:
            # Retrieve OS compatibilty from historical record
            return json.loads(resp['history'][0]['v1Compatibility'])['os']

        # Unable to determine the OS compatibility
        return None


# Ref: https://github.com/goharbor/harbor/blob/main/api/v2.0/swagger.yaml
class HarborRegistry(ContainerRegistry):
    def _get_project_repo_ids(self, image_name) -> Tuple[str, str]:
        # Determine project/repo IDs from image name
        project_id, repo_id = image_name.split('/', 1)
        repo_id = repo_id.replace('/', "%2F")
        return project_id, repo_id

    def get_image_tags(self, image_name) -> List[str]:
        project_id, repo_id = self._get_project_repo_ids(image_name)
        resp = self._make_request(f"/api/v2.0/projects/{project_id}/repositories/{repo_id}/artifacts?page_size=0")
        if resp:
            return [tag['name'] for image in resp if image['tags'] for tag in image['tags']]
        return []

    def get_image_os(self, image_name, image_tag) -> str:
        project_id, repo_id = self._get_project_repo_ids(image_name)
        resp = self._make_request(f"/api/v2.0/projects/{project_id}/repositories/{repo_id}/artifacts/{image_tag}")
        if resp:
            # Retrieve OS compatibilty from reference
            return resp['references'][0]['platform']['os']

        # Unable to determine the OS compatibility
        return None


REGISTRY_TYPE_MAPPING = {
    'dockerhub': DockerHub,
    'docker': DockerRegistry,
    'harbor': HarborRegistry
}


def get_latest_tag_for_service(service_config: ServiceConfig, system_config: SystemConfig, logger: Logger, prefix: str = ""):
    def process_image(image):
        # Find which server to search in
        server = image.split("/")[0]
        if server != "cccs":
            if ":" in server:
                image_name = image[len(server) + 1:]
            else:
                try:
                    socket.gethostbyname_ex(server)
                    image_name = image[len(server) + 1:]
                except socket.gaierror:
                    server = DEFAULT_DOCKER_REGISTRY
                    image_name = image
        else:
            server = DEFAULT_DOCKER_REGISTRY
            image_name = image

        # Split repo name without the tag
        image_name = image_name.rsplit(":", 1)[0]

        return server, image_name

    # Extract info
    service_name = service_config.name
    image = service_config.docker_config.image
    update_channel = service_config.update_channel

    # Fix service image for calling Docker API
    image_variables = defaultdict(str)
    image_variables.update(system_config.services.image_variables)
    image_variables.update(system_config.services.update_image_variables)
    searchable_image = string.Template(image).safe_substitute(image_variables)

    # Get authentication
    auth = None
    auth_config = None
    server, image_name = process_image(searchable_image)

    if not (service_config.docker_config.registry_username and service_config.docker_config.registry_password):
        # If the passed in service configuration is missing registry credentials, check against system configuration
        for registry in system_config.services.registries:
            if server.startswith(registry['name']):
                # Apply the credentials that the system is configured to use with the registry
                service_config.docker_config.registry_username = registry['username']
                service_config.docker_config.registry_password = registry['password']
                service_config.docker_config.registry_type = registry['type']
                break

    if service_config.docker_config.registry_username and service_config.docker_config.registry_password:
        auth_config = {
            'username': service_config.docker_config.registry_username,
            'password': service_config.docker_config.registry_password
        }
        upass = f"{service_config.docker_config.registry_username}:{service_config.docker_config.registry_password}"
        auth = f"Basic {b64encode(upass.encode()).decode()}"

    proxies = None
    for reg_conf in system_config.core.updater.registry_configs:
        if reg_conf.name == server:
            proxies = reg_conf.proxies or None
            break

    registry_type = 'dockerhub' if server == DEFAULT_DOCKER_REGISTRY else service_config.docker_config.registry_type
    registry_args = {
        'server': server,
        'headers': {'Authorization': auth},
        'verify': not system_config.services.allow_insecure_registry,
        'proxies': proxies,
        'update_channel': update_channel
    }

    registry: ContainerRegistry = REGISTRY_TYPE_MAPPING[registry_type](**registry_args)
    tags = registry.get_image_tags(image_name)

    tag_name = None
    # Pre-filter tags to only consider 'compatible' tags relative to the running system
    tags = [t for t in tags
            if re.match(f"({FRAMEWORK_VERSION})[.]({SYSTEM_VERSION})[.]\\d+[.]({update_channel})\\d+", t)]

    if not tags:
        logger.warning(f"{prefix}Cannot fetch latest tag for service {service_name} - {image_name}"
                       f" => [server: {server}, repo_name: {image_name}, channel: {update_channel}]")
    else:
        for t in tags:
            t_version = Version(t.replace(update_channel, ""))
            # Tag name gets assigned to the first viable option then relies on comparison to get the latest
            if not tag_name:
                tag_name = t
            elif t_version.major == FRAMEWORK_VERSION and t_version.minor == SYSTEM_VERSION and \
                    t_version > parse(tag_name.replace(update_channel, "")):
                tag_name = t

        logger.info(f"{prefix}Latest {service_name} tag on {update_channel.upper()} channel is: {tag_name}")

    # Fix service image for use in Kubernetes
    image_variables = defaultdict(str)
    image_variables.update(system_config.services.image_variables)
    image = string.Template(image).safe_substitute(image_variables)
    os = registry.get_image_os(image_name, tag_name)

    # Append server to image if not the default server
    if server != DEFAULT_DOCKER_REGISTRY:
        image_name = "/".join([server, image_name])

    return image_name, tag_name, auth_config, os
