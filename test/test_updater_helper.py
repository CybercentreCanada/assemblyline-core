from unittest.mock import MagicMock, patch

from assemblyline_core.updater.helper import AzureContainerRegistry

SERVER = "registry.example"
IMAGE_NAME = "namespace/service"
FIRST_PAGE_URL = f"https://{SERVER}/v2/{IMAGE_NAME}/tags/list?n=1000"
NEXT_PAGE_URL = f"https://{SERVER}/v2/{IMAGE_NAME}/tags/list?n=1000&last=tag-b"


def _make_response(tags, links=None):
    resp = MagicMock()
    resp.ok = True
    resp.json.return_value = {"tags": tags}
    resp.links = links or {}
    return resp


def test_azure_registry_follows_pagination_links():
    first_page = _make_response(
        ["tag-a", "tag-b"],
        links= {
            'next': {
                'url': NEXT_PAGE_URL,
                'rel': 'next'
            }
        }
    )
    second_page = _make_response(["tag-c"], links=None)
    requested_urls = []

    def fake_get(url, headers=None, verify=None, proxies=None):
        requested_urls.append(url)
        if url == FIRST_PAGE_URL:
            return first_page
        if url == NEXT_PAGE_URL:
            return second_page
        raise AssertionError(f"Unexpected URL requested: {url}")

    registry = AzureContainerRegistry()
    with patch("assemblyline_core.updater.helper.requests.get", side_effect=fake_get):
        tags = registry._get_proprietary_registry_tags(
            server=SERVER, image_name=IMAGE_NAME, auth="Bearer token", verify=True
        )

    assert tags == ["tag-a", "tag-b", "tag-c"]
    assert requested_urls == [FIRST_PAGE_URL, NEXT_PAGE_URL]
