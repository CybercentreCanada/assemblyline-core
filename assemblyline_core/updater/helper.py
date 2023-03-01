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

DEFAULT_DOCKER_REGISTRY = "registry.hub.docker.com"


class ContainerRegistry():
    # Provide a means of obtaining a list of tags from a container registry
    def _get_proprietary_registry_tags(self, server, image_name, auth, verify):
        raise NotImplementedError()


class DockerRegistry(ContainerRegistry):
    def _get_proprietary_registry_tags(self, server, image_name, auth, verify, proxies=None):
        # Find latest tag for each types
        url = f"https://{server}/v2/{image_name}/tags/list"

        # Get tag list
        headers = {}
        if auth:
            headers["Authorization"] = auth

        resp = None
        try:
            resp = requests.get(url, headers=headers, verify=verify, proxies=proxies)
        except requests.exceptions.SSLError:
            # Connect to insecure registry over HTTP (development only)
            if not verify:
                url = f"http://{server}/v2/{image_name}/tags/list"
                resp = requests.get(url, headers=headers, verify=verify, proxies=proxies)

        # Test for valid response
        if resp and resp.ok:
            # Test for positive list of tags
            resp_data = resp.json()
            return resp_data['tags'] or []
        return []


class HarborRegistry(ContainerRegistry):
    def _get_proprietary_registry_tags(self, server, image_name, auth, verify, proxies=None):
        # Determine project/repo IDs from image name
        project_id, repo_id = image_name.split('/', 1)
        repo_id = repo_id.replace('/', "%2F")
        url = f"https://{server}/api/v2.0/projects/{project_id}/repositories/{repo_id}/artifacts?page_size=0"

        headers = {}
        if auth:
            headers["Authorization"] = auth

        resp = None
        try:
            resp = requests.get(url, headers=headers, verify=verify, proxies=proxies)
        except requests.exceptions.SSLError:
            # Connect to insecure registry over HTTP (development only)
            if not verify:
                url = f"http://{server}/api/v2.0/projects/{project_id}/repositories/{repo_id}/artifacts"
                resp = requests.get(url, headers=headers, verify=verify, proxies=proxies)

        if resp and resp.ok:
            return [tag['name'] for image in resp.json() if image['tags'] for tag in image['tags']]
        return []


REGISTRY_TYPE_MAPPING = {
    'docker': DockerRegistry(),
    'harbor': HarborRegistry()
}


def get_latest_tag_for_service(
        service_config: ServiceConfig, system_config: SystemConfig, logger: Logger, prefix: str = ""):
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

    registry = REGISTRY_TYPE_MAPPING[service_config.docker_config.registry_type]
    proxies = None
    for reg_conf in system_config.core.updater.registry_configs:
        if reg_conf.name == server:
            proxies = reg_conf.proxies or None
            break

    if server == DEFAULT_DOCKER_REGISTRY:
        tags = _get_dockerhub_tags(image_name, update_channel, proxies)
    else:
        tags = registry._get_proprietary_registry_tags(server, image_name, auth,
                                                       not system_config.services.allow_insecure_registry, proxies)

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
    server, image_name = process_image(image)

    # Append server to image if not the default server
    if server != "registry.hub.docker.com":
        image_name = "/".join([server, image_name])

    return image_name, tag_name, auth_config


# Default for obtaining tags from DockerHub
def _get_dockerhub_tags(image_name, update_channel, proxies=None):
    # Find latest tag for each types
    url = f"https://{DEFAULT_DOCKER_REGISTRY}/v2/repositories/{image_name}/tags" \
        f"?page_size=5&page=1&name={update_channel}"

    # Get tag list
    resp = requests.get(url, proxies=proxies)

    # Test for valid response
    if resp.ok:
        # Test for positive list of tags
        resp_data = resp.json()
        return [x['name'] for x in resp_data['results']]

    return []
