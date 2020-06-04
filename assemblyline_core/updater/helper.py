
import requests
import socket
import string
from collections import defaultdict


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
    auth = {}
    if service_config.docker_config.registry_username or service_config.docker_config.registry_password:
        auth['username'] = service_config.docker_config.registry_username,
        auth['password'] = service_config.docker_config.registry_password

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
                server = "registry.hub.docker.com:443"
                image_name = image
    else:
        server = "registry.hub.docker.com:443"
        image_name = image

    # Split repo name without the tag
    image_name = image_name.rsplit(":", 1)[0]

    # Find latest tag for each types
    url = f"https://{server}/v2/repositories/{image_name}/tags?ordering=last_updated"
    if update_channel:
        url += f"&name={update_channel}"

    if server == "registry.hub.docker.com:443":
        image_name = image_name
    else:
        image_name = "/".join([server, image_name])

    # Get tag list
    resp = requests.get(url)

    # Test for valid response
    tag_name = None
    if not resp.ok:
        logger.warning(f"Cannot fetch latest tag for service {service_name} - {image}"
                       f" => [server: {server}, repo_name: {image_name}, channel: {update_channel}]")
    else:
        # Test for positive list of tags
        resp_data = resp.json()
        if resp_data['results']:
            # Find latest tag
            tag_name = resp_data['results'][0]['name']

        logger.info(f"Latest {service_name} tag on {update_channel.upper()} channel is: {tag_name}")

    return image_name, tag_name, auth
