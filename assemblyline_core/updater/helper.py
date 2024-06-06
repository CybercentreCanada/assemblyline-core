import requests
import re
import socket
import string
import time

from assemblyline.common.version import FRAMEWORK_VERSION, SYSTEM_VERSION
from assemblyline.odm.models.config import Config as SystemConfig
from assemblyline.odm.models.service import Service as ServiceConfig

from base64 import b64encode
from collections import defaultdict
from logging import Logger
from packaging.version import parse, Version

DEFAULT_DOCKER_REGISTRY = "hub.docker.com"


class ContainerRegistry():
    # Provide a means of obtaining a list of tags from a container registry
    def _get_proprietary_registry_tags(self, server, image_name, auth, verify, proxies=None, token_server=None):
        raise NotImplementedError()

    def _perform_request(self, url, headers, verify, proxies):
        resp = None
        try:
            resp = requests.get(url, headers=headers, verify=verify, proxies=proxies)
        except requests.exceptions.SSLError:
            # Connect to insecure registry over HTTP (development only)
            if not verify:
                url = url.replace('https://','http://')
                resp = requests.get(url, headers=headers, verify=verify, proxies=proxies)
        return resp


class DockerRegistry(ContainerRegistry):
    def _get_proprietary_registry_tags(self, server, image_name, auth, verify, proxies=None, token_server=None):
        # Find latest tag for each types
        url = f"https://{server}/v2/{image_name}/tags/list"

        # Get tag list
        headers = {}
        if auth:
            headers["Authorization"] = auth
        else:
            # Retrieve token for authentication: https://distribution.github.io/distribution/spec/auth/token/

            # Assume the token server is the same as the container image registry host if not explicitly set
            token_server = token_server if token_server else server
            token_url = f"https://{token_server}/token?scope=repository:{image_name}:pull"
            resp = self._perform_request(token_url, headers, verify, proxies)
            if resp and resp.ok:
                # Request to obtain token was successful, set Authorization header for registry API
                token = resp.json().get('token')
                headers["Authorization"] = f"Bearer {token}"

        resp = self._perform_request(url, headers, verify, proxies)
        # Test for valid response
        if resp and resp.ok:
            # Test for positive list of tags
            resp_data = resp.json()
            return resp_data['tags'] or []
        return []

class HarborRegistry(ContainerRegistry):
    def _get_proprietary_registry_tags(self, server, image_name, auth, verify, proxies=None, token_server=None):
        # Determine project/repo IDs from image name
        project_id, repo_id = image_name.split('/', 1)
        repo_id = repo_id.replace('/', "%2F")
        url = f"https://{server}/api/v2.0/projects/{project_id}/repositories/{repo_id}/artifacts?page_size=0"

        headers = {}
        if auth:
            headers["Authorization"] = auth
        else:
            # Retrieve token for authentication: https://github.com/goharbor/harbor/wiki/Harbor-FAQs#api

            # Assume the token server is the same as the container image registry host if not explicitly set
            token_server = token_server if token_server else server
            token_url = f"https://{server}/service/token?scope=repository:{image_name}:pull"
            resp = self._perform_request(token_url, headers, verify, proxies)
            if resp and resp.ok:
                # Request to obtain token was successful, set Authorization header for registry API
                token = resp.json().get('token')
                headers["Authorization"] = f"Bearer {token}"

        resp = self._perform_request(url, headers, verify, proxies)
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
        # We're authenticating using Basic Auth
        auth_config = {
            'username': service_config.docker_config.registry_username,
            'password': service_config.docker_config.registry_password
        }
        upass = f"{service_config.docker_config.registry_username}:{service_config.docker_config.registry_password}"
        auth = f"Basic {b64encode(upass.encode()).decode()}"
    elif service_config.docker_config.registry_password:
        # We're assuming that if only a password is given, then this is a token
        auth = f"Bearer {service_config.docker_config.registry_password}"

    registry = REGISTRY_TYPE_MAPPING[service_config.docker_config.registry_type]
    token_server = None
    proxies = None
    for reg_conf in system_config.core.updater.registry_configs:
        if reg_conf.name == server:
            proxies = reg_conf.proxies or None
            token_server = reg_conf.token_server or None
            break

    if server == DEFAULT_DOCKER_REGISTRY:
        tags = _get_dockerhub_tags(image_name, update_channel, proxies)
    else:
        tags = registry._get_proprietary_registry_tags(server, image_name, auth,
                                                       not system_config.services.allow_insecure_registry,
                                                       proxies, token_server)
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
    if server != DEFAULT_DOCKER_REGISTRY:
        image_name = "/".join([server, image_name])

    return image_name, tag_name, auth_config


# Default for obtaining tags from DockerHub
def _get_dockerhub_tags(image_name, update_channel, proxies=None):
    # Find latest tag for each types
    rv = []
    namespace, repository = image_name.split('/', 1)
    url = f"https://{DEFAULT_DOCKER_REGISTRY}/v2/namespaces/{namespace}/repositories/{repository}/tags"
    f"?page_size=50&page=1&name={update_channel}"

    while True:
        resp = requests.get(url, proxies=proxies)
        if resp.ok:
            resp_data = resp.json()
            rv.extend([x['name'] for x in resp_data['results']])
            # Page until there are no results left
            url = resp_data.get('next', None)
            if url is None:
                break
        elif resp.status_code == 429:
            # Based on https://docs.docker.com/docker-hub/api/latest/#tag/rate-limiting
            # We've hit the rate limit so we have to wait and try again later
            time.sleep(int(resp.headers['retry-after']) - int(time.time()))
        else:
            break

    return rv
