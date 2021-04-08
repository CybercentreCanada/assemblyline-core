import requests
import re
import socket
import string

from assemblyline.common.version import FRAMEWORK_VERSION, SYSTEM_VERSION
from collections import defaultdict
from base64 import b64encode
from packaging.version import parse

DEFAULT_DOCKER_REGISTRY = "registry.hub.docker.com"


def _get_proprietary_registry_tags(server, image_name, auth, verify):
    # Find latest tag for each types
    url = f"https://{server}/v2/{image_name}/tags/list"

    # Get tag list
    headers = {}
    if auth:
        headers["Authorization"] = auth
    resp = requests.get(url, headers=headers, verify=verify)

    # Test for valid response
    if resp.ok:
        # Test for positive list of tags
        resp_data = resp.json()
        return resp_data['tags']
    return []


def _get_dockerhub_tags(image_name, update_channel):
    # Find latest tag for each types
    url = f"https://{DEFAULT_DOCKER_REGISTRY}/v2/repositories/{image_name}/tags" \
          f"?page_size=5&page=1&name={update_channel}"

    # Get tag list
    resp = requests.get(url)

    # Test for valid response
    if resp.ok:
        # Test for positive list of tags
        resp_data = resp.json()
        return [x['name'] for x in resp_data['results']]

    return []


def get_latest_tag_for_service(service_config, system_config, logger):
    # Extract info
    service_name = service_config.name
    image = service_config.docker_config.image
    update_channel = service_config.update_channel

    # Fix service image
    image_variables = defaultdict(str)
    image_variables.update(system_config.services.image_variables)
    image = string.Template(image).safe_substitute(image_variables)

    # Get authentication
    auth = None
    auth_config = None
    if service_config.docker_config.registry_username and service_config.docker_config.registry_password:
        auth_config = {
            'username': service_config.docker_config.registry_username,
            'password': service_config.docker_config.registry_password
        }
        upass = f"{service_config.docker_config.registry_username}:{service_config.docker_config.registry_password}"
        auth = f"Basic {b64encode(upass.encode()).decode()}"

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

    if server == DEFAULT_DOCKER_REGISTRY:
        tags = _get_dockerhub_tags(image_name, update_channel)
    else:
        tags = _get_proprietary_registry_tags(server, image_name, auth, not system_config.services.allow_insecure_registry)

    tag_name = None
    if not tags:
        logger.warning(f"Cannot fetch latest tag for service {service_name} - {image}"
                       f" => [server: {server}, repo_name: {image_name}, channel: {update_channel}]")
    else:
        tag_name = f"{FRAMEWORK_VERSION}.{SYSTEM_VERSION}.0.{update_channel}0"
        for t in tags:
            if re.match(f"({FRAMEWORK_VERSION})[.]({SYSTEM_VERSION})[.]\d+[.]({update_channel})\d+", t):
                t_version = parse(t.replace(update_channel, ""))
                if t_version.major == FRAMEWORK_VERSION and t_version.minor == SYSTEM_VERSION and \
                        t_version > parse(tag_name.replace(update_channel, "")):
                    tag_name = t

        logger.info(f"Latest {service_name} tag on {update_channel.upper()} channel is: {tag_name}")

    # Append server to image if not the default server
    if server != "registry.hub.docker.com":
        image_name = "/".join([server, image_name])

    return image_name, tag_name, auth_config
