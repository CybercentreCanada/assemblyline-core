import os
import requests
import re
import socket
import string
import time

from assemblyline.common.version import FRAMEWORK_VERSION, SYSTEM_VERSION
from assemblyline.odm.models.config import Config as SystemConfig, ServiceRegistry
from assemblyline.odm.models.service import Service as ServiceConfig, DockerConfig

from base64 import b64encode
from collections import defaultdict
from logging import Logger
from typing import Dict, List
from packaging.version import parse, Version
from urllib.parse import urlencode

from azure.identity import DefaultAzureCredential


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

class AzureContainerRegistry(ContainerRegistry):
    def _get_proprietary_registry_tags(self, server, image_name, auth, verify, proxies=None, token_server=None):
        # Find latest tag for each types
        url = f"https://{server}/v2/{image_name}/tags/list"

        # Get tag list
        headers = {}
        if auth:
            headers["Authorization"] = auth

        # Attempt request with provided credentials alone
        resp = self._perform_request(url, headers, verify, proxies)

        if not resp:
            # Authentication with just credentials failed, moving over to generating a bearer token

            # Retrieve token for authentication: https://azure.github.io/acr/Token-BasicAuth.html#using-the-token-api
            token_url = f"https://{server}/oauth2/token?scope=repository:{image_name}:metadata_read,pull&service={server}"
            resp = self._perform_request(token_url, headers, verify, proxies)
            if resp and resp.ok:
                # Request to obtain token was successful, set Authorization header for registry API
                token = resp.json().get('access_token')
                headers["Authorization"] = f"Bearer {token}"

                resp = self._perform_request(url, headers, verify, proxies)

        # At this point, we should have a response from the API
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

def get_registry_config(docker_config: DockerConfig, system_config: SystemConfig) -> Dict[str, str]:
    # Fix service image for calling Docker API
    image_variables = defaultdict(str)
    image_variables.update(system_config.services.image_variables)
    image_variables.update(system_config.services.update_image_variables)
    searchable_image = string.Template(docker_config.image).safe_substitute(image_variables)

    server = searchable_image.split("/", 1)[0]

    # Prioritize authentication given as a system configuration
    registries: List[ServiceRegistry] = system_config.services.registries or []
    for registry in registries:
        if server.startswith(registry.name):
            # Return authentication credentials and the type of registry
            return dict(username=registry.username, password=registry.password, type=registry.type,
                        use_fic=registry.use_fic)

    # Otherwise return what's configured for the service
    return dict(username=docker_config.registry_username, password=docker_config.registry_password,
                type=docker_config.registry_type)

def get_latest_tag_for_service(service_config: ServiceConfig, system_config: SystemConfig, logger: Logger,
                               prefix: str = ""):
    """
    Retrieves the latest compatible tag for a given service from its respective Docker registry.
    This function processes the image name to determine the correct server, authenticates
    with the registry, and then fetches the latest tag that matches the service's framework
    and system version constraints. It handles different registry configurations including
    private and public ones.
    Args:
        service_config (ServiceConfig): The configuration object for the service which includes Docker configuration.
        system_config (SystemConfig): System-wide configuration which includes service updates and registry configurations.
        logger (Logger): Logger object for logging messages.
        prefix (str, optional): A prefix string to prepend to log messages for clarity in logs.
    Returns:
        tuple: Returns a tuple of the final image name, the latest tag, authentication and auto_update config used for the Docker registry.
    """
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

    # Extract and process service information
    service_name = service_config.name
    image = service_config.docker_config.image
    update_channel = service_config.update_channel
    auto_update = service_config.auto_update
    if auto_update is None:
        auto_update = system_config.services.default_auto_update

    # Fix service image for calling Docker API
    image_variables = defaultdict(str)
    image_variables.update(system_config.services.image_variables)
    image_variables.update(system_config.services.update_image_variables)
    searchable_image = string.Template(image).safe_substitute(image_variables)

    # Get authentication
    auth = None
    server, image_name = process_image(searchable_image)

    # Load in proxies and token server
    token_server = None
    proxies = None
    for reg_conf in system_config.core.updater.registry_configs:
        if reg_conf.name == server:
            proxies = reg_conf.proxies or None
            token_server = reg_conf.token_server or None
            break

    # Generate 'Authenication' header value for pulling tag list from registry
    auth_config = get_registry_config(service_config.docker_config, system_config)
    registry_type = auth_config.pop('type')
    if auth_config['username'] and auth_config['password']:
        upass = f"{auth_config['username']}:{auth_config['password']}"
        auth = f"Basic {b64encode(upass.encode()).decode()}"
    elif auth_config['password']:
        # We're assuming that if only a password is given, then this is a token
        auth = f"Bearer {auth_config['password']}"

    if server.endswith(".azurecr.io"):
        if auth_config.get('use_fic', False):
            # If the use of federated identity token is set, exchange said token to an ACR token
            try:
                credentials = DefaultAzureCredential()
                aad_token = credentials.get_token('https://management.core.windows.net/.default').token

                refresh_token = requests.post(
                    f"https://{server}/oauth2/exchange",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data=f"grant_type=access_token&service={server}&access_token={aad_token}",
                    proxies=proxies).json()["refresh_token"]

                token = requests.post(
                    f"https://{server}/oauth2/token",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    data=f"grant_type=refresh_token&service={server}&refresh_token={refresh_token}"
                        f"&scope=repository:{image_name}:metadata_read",
                    proxies=proxies).json()["access_token"]

                auth = f"Bearer {token}"
            except Exception as e:
                logger.error(f"{prefix} Failed to acquire Azure credentials: {str(e)}")

        # This is an Azure Container Registry based on the server name
        registry = AzureContainerRegistry()
    else:
        registry = REGISTRY_TYPE_MAPPING[registry_type]

    if server == DEFAULT_DOCKER_REGISTRY:
        tags = _get_dockerhub_tags(image_name, update_channel, prefix, proxies, logger=logger)
    else:
        tags = registry._get_proprietary_registry_tags(server, image_name, auth,
                                                       not system_config.services.allow_insecure_registry,
                                                       proxies, token_server)
    # Pre-filter tags to only consider 'compatible' tags relative to the running system
    tags = [tag for tag in tags
            if re.match(f"({FRAMEWORK_VERSION})\\.({SYSTEM_VERSION})\\.\\d+\\.({update_channel})\\d+", tag)]
    if not tags:
        logger.warning(f"{prefix} Cannot fetch latest tag for service {service_name} - {image_name}" \
                       f" => [server: {server}, repo_name: {image_name}, channel: {update_channel}]")
    tag_name = None
    for tag in tags:
        t_version = Version(tag.replace(update_channel, ""))
        # Tag name gets assigned to the first viable option then relies on comparison to get the latest
        if not tag_name:
            tag_name = tag
        elif t_version.major == FRAMEWORK_VERSION and t_version.minor == SYSTEM_VERSION and \
                t_version > parse(tag_name.replace(update_channel, "")):
            tag_name = tag
    logger.info(f"{prefix}Latest {service_name} tag on {update_channel.upper()} channel is: {tag_name}")

    # Fix service image for use in Kubernetes
    image_variables = defaultdict(str)
    image_variables.update(system_config.services.image_variables)
    image = string.Template(image).safe_substitute(image_variables)
    server, image_name = process_image(image)

    # Append server to image if not the default server
    if server != DEFAULT_DOCKER_REGISTRY:
        image_name = "/".join([server, image_name])

    return image_name, tag_name, auth_config, auto_update

def _get_dockerhub_tags(image_name, update_channel, prefix, proxies=None, docker_registry=DEFAULT_DOCKER_REGISTRY,
                        logger: Logger=None):
    """
    Retrieve DockerHub tags for a specific image and update channel.
    Args:
    image_name (str): Full name of the image (including namespace).
    update_channel (str): Specific channel to filter tags (e.g., stable, beta).
    proxies (dict, optional): Proxy configuration.
    docker_registry (str, optional): Docker registry URL. Defaults to DEFAULT_DOCKER_REGISTRY.
    Returns:
    list: A list of tag names that match the given criteria, or None if an error occurs.
    """
    namespace, repository = image_name.split('/', 1)
    base_url = f"https://{docker_registry}/v2/namespaces/{namespace}/repositories/{repository}/tags"
    query_params = {
        'page_size': 50,
        'page': 1,
        'name': update_channel,
        'ordering': 'last_updated'
    }
    url = f"{base_url}?{urlencode(query_params)}"
    tags = []
    while True:
        try:
            response = requests.get(url, proxies=proxies)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            response_data = response.json()
            if namespace == "cccs":
                # The tags are coming from a repository managed by the Assemblyline team
                for tag_info in response_data['results']:
                    tag_name = tag_info.get('name')
                    if tag_name and tag_name == f"{FRAMEWORK_VERSION}.{SYSTEM_VERSION}.{update_channel}":
                        # Ignore tag aliases containing the update_channel
                        continue
                    if tag_name and tag_name.startswith(f"{FRAMEWORK_VERSION}.{SYSTEM_VERSION}"):
                        # Because the order of paging is based on `last_updated`,
                        # we can return the first valid candidate
                        logger.info(f"{prefix}Valid tag found for {repository}: {tag_name}")
                        return [tag_name]
            else:
                # The tags are coming from another repository, so we're don't know the ordering of tags
                tags.extend(tag_info.get('name') for tag_info in response_data['results'] if tag_info.get('name'))
            url = response_data.get('next')
            if not url:
                logger.info(f"{prefix}No more pages left to fetch.")
                break
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                # Based on https://docs.docker.com/docker-hub/api/latest/#tag/rate-limiting
                # We've hit the rate limit so we have to wait and try again later
                logger.warning(f"{prefix}Rate limit reached for DockerHub. Retrying after sleep..")
                time.sleep(int(response.headers['retry-after']) - int(time.time()))
            else:
                logger.error(f"{prefix}HTTP error occurred: {e}")
            break
        except requests.exceptions.ConnectionError as e:
            logger.error(f"{prefix}Connection error occurred: {e}")
            break
        except requests.exceptions.Timeout as e:
            logger.error(f"{prefix}Request timed out.")
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"{prefix}An error occurred during requests: {e}")
            break
        except ValueError as e:
            logger.error(f"{prefix}JSON decode error: {e}")
            break
    if tags:
        logger.info(f"{prefix}Tags retrieved successfully: {tags}")
    return tags
