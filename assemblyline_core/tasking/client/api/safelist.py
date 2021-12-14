import yaml
from gevent import config

from assemblyline.common import forge
from assemblyline_core.tasking.config import STORAGE, config


def exists(qhash, **_):
    return STORAGE.safelist.get_if_exists(qhash, as_obj=False)


def get_safelist_for_tags(tag_types, **_):
    if tag_types:
        tag_types = tag_types.split(',')

    with forge.get_cachestore('system', config=config, datastore=STORAGE) as cache:
        tag_safelist_yml = cache.get('tag_safelist_yml')
        if tag_safelist_yml:
            tag_safelist_data = yaml.safe_load(tag_safelist_yml)
        else:
            tag_safelist_data = forge.get_tag_safelist_data()

    if tag_types:
        output = {
            'match': {k: v for k, v in tag_safelist_data.get('match', {}).items() if k in tag_types or tag_types == []},
            'regex': {k: v for k, v in tag_safelist_data.get('regex', {}).items() if k in tag_types or tag_types == []},
        }
        for tag in tag_types:
            for sl in STORAGE.safelist.stream_search(f"type:tag AND enabled:true AND tag.type:{tag}", as_obj=False):
                output['match'].setdefault(sl['tag']['type'], [])
                output['match'][sl['tag']['type']].append(sl['tag']['value'])

    else:
        output = tag_safelist_data
        for sl in STORAGE.safelist.stream_search("type:tag AND enabled:true", as_obj=False):
            output['match'].setdefault(sl['tag']['type'], [])
            output['match'][sl['tag']['type']].append(sl['tag']['value'])

    return output


def get_safelist_for_signatures(**_):
    output = [
        item['signature']['name']
        for item in STORAGE.safelist.stream_search(
            "type:signature AND enabled:true", fl="signature.name", as_obj=False)]

    return output
