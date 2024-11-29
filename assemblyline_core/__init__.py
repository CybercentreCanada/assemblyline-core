PAUSABLE_COMPONENTS = ['ingester', 'dispatcher']

def normalize_hashlist_item(tag_type: str, tag_value: str) -> str:
    # Normalize tag data pertaining to domains or URIs
    if tag_type.endswith('.domain'):
        tag_value = tag_value.lower()
    elif tag_type.endswith('.uri'):
        hostname = tag_value.split('//', 1)[1].split('/', 1)[0]
        tag_value = tag_value.replace(hostname, hostname.lower(), 1)
    return tag_value
