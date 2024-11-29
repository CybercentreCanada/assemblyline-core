
import hashlib
import random
import time
from copy import deepcopy

import pytest
from assemblyline.common.forge import get_classification
from assemblyline.common.isotime import iso_to_epoch
from assemblyline.odm.random_data import (
    create_badlists,
    create_users,
    wipe_badlist,
    wipe_users,
)
from assemblyline.odm.randomizer import get_random_hash
from assemblyline_core.badlist_client import BadlistClient, InvalidBadhash

CLASSIFICATION = get_classification()

add_hash_file = "10" + get_random_hash(62)
add_error_hash = "11" + get_random_hash(62)
update_hash = "12" + get_random_hash(62)
update_conflict_hash = "13" + get_random_hash(62)
source_hash = "14" + get_random_hash(62)

BAD_SOURCE = {
    "classification": CLASSIFICATION.UNRESTRICTED,
    "name": "BAD",
    "reason": [
        "2nd stage for implant BAD",
        "Used by actor BLAH!"
    ],
    "type": "external"}

BAD2_SOURCE = {
    "classification": CLASSIFICATION.UNRESTRICTED,
    "name": "BAD2",
    "reason": [
        "Use for phishing"
    ],
    "type": "external"}

ADMIN_SOURCE = {
    "classification": CLASSIFICATION.UNRESTRICTED,
    "name": "admin",
    "reason": [
        "It's denifitely bad",
    ],
    "type": "user"}

USER_SOURCE = {
    "classification": CLASSIFICATION.UNRESTRICTED,
    "name": "user",
    "reason": [
        "I just feel like it!",
        "I just feel like it!",
    ],
    "type": "user"}


@pytest.fixture(scope="module")
def client(datastore_connection):
    try:
        create_users(datastore_connection)
        create_badlists(datastore_connection)
        yield BadlistClient(datastore_connection)
    finally:
        wipe_users(datastore_connection)
        wipe_badlist(datastore_connection)


# noinspection PyUnusedLocal
def test_badlist_add_file(client):
    # Generate a random badlist
    sl_data = {
        'attribution': None,
        'hashes': {'md5': get_random_hash(32),
                   'sha1': get_random_hash(40),
                   'sha256': add_hash_file,
                   'ssdeep': None,
                   'tlsh': None},
        'file': {'name': ['file.txt'],
                 'size': random.randint(128, 4096),
                 'type': 'document/text'},
        'sources': [BAD_SOURCE, ADMIN_SOURCE],
        'type': 'file'
    }
    sl_data_original = deepcopy(sl_data)

    # Insert it and test return value
    qhash, op = client.add_update(sl_data)
    assert qhash == add_hash_file
    assert op == 'add'

    # Load inserted data from DB
    ds_sl = client.datastore.badlist.get(add_hash_file, as_obj=False)

    # Test dates
    added = ds_sl.pop('added', None)
    updated = ds_sl.pop('updated', None)

    # File item will live forever
    assert ds_sl.pop('expiry_ts', None) is None

    assert added == updated
    assert added is not None and updated is not None

    # Make sure tag is none
    tag = ds_sl.pop('tag', None)
    assert tag is None

    # Test classification
    classification = ds_sl.pop('classification', None)
    assert classification is not None

    # Test enabled
    enabled = ds_sl.pop('enabled', None)
    assert enabled

    # Normalize classification in sources
    for source in ds_sl['sources']:
        source['classification'] = CLASSIFICATION.normalize_classification(source['classification'])

    # Test rest
    assert ds_sl == sl_data_original


def test_badlist_add_tag(client):
    tag_type = 'network.static.ip'
    tag_value = '127.0.0.1'
    hashed_value = f"{tag_type}: {tag_value}".encode('utf8')
    expected_qhash = hashlib.sha256(hashed_value).hexdigest()

    # Generate a random badlist
    sl_data = {
        'attribution': {
            'actor': ["SOMEONE!"],
            'campaign': None,
            'category': None,
            'exploit': None,
            'implant': None,
            'family': None,
            'network': None
        },
        'dtl': 15,
        'hashes': {'sha256': expected_qhash},
        'tag': {'type': tag_type,
                'value': tag_value},
        'sources': [BAD_SOURCE, ADMIN_SOURCE],
        'type': 'tag'
    }
    sl_data_original = deepcopy(sl_data)

    # Insert it and test return value
    qhash, op = client.add_update(sl_data)
    assert qhash == expected_qhash
    assert op == 'add'

    # Load inserted data from DB
    ds_sl = client.datastore.badlist.get(expected_qhash, as_obj=False)

    # Test dates
    added = ds_sl.pop('added', None)
    updated = ds_sl.pop('updated', None)

    # Tag item will live up to a certain date
    assert ds_sl.pop('expiry_ts', None) is not None

    assert added == updated
    assert added is not None and updated is not None

    # Make sure file is None
    file = ds_sl.pop('file', {})
    assert file is None

    # Test classification
    classification = ds_sl.pop('classification', None)
    assert classification is not None

    # Test enabled
    enabled = ds_sl.pop('enabled', None)
    assert enabled

    # Test rest, dtl should not exist anymore
    sl_data_original.pop('dtl', None)

    # Normalize classification in sources
    for source in ds_sl['sources']:
        source['classification'] = CLASSIFICATION.normalize_classification(source['classification'])

    for hashtype in ['md5', 'sha1', 'ssdeep', 'tlsh']:
        ds_sl['hashes'].pop(hashtype, None)

    # Test rest
    assert ds_sl == sl_data_original


def test_badlist_add_invalid(client):
    # Generate a random badlist
    sl_data = {
        'hashes': {'sha256': add_error_hash},
        'sources': [USER_SOURCE],
        'type': 'file'}

    # Insert it and test return value
    with pytest.raises(ValueError) as conflict_exc:
        client.add_update(sl_data, user={"uname": "test"})

    assert 'for another user' in conflict_exc.value.args[0]


def test_badlist_update(client):
    # Generate a random badlist
    sl_data = {
        'attribution': {
            'actor': None,
            'campaign': None,
            'category': None,
            'exploit': None,
            'implant': ['BAD'],
            'family': None,
            'network': None},
        'hashes': {'md5': get_random_hash(32),
                   'sha1': get_random_hash(40),
                   'sha256': update_hash,
                   'ssdeep': None,
                   'tlsh': None},
        'file': {'name': [],
                 'size': random.randint(128, 4096),
                 'type': 'document/text'},
        'sources': [BAD_SOURCE],
        'type': 'file'
    }
    sl_data_original = deepcopy(sl_data)

    # Insert it and test return value
    qhash, op = client.add_update(sl_data)
    assert qhash == update_hash
    assert op == 'add'

    # Load inserted data from DB
    ds_sl = client.datastore.badlist.get(update_hash, as_obj=False)

    # Normalize classification in sources
    for source in ds_sl['sources']:
        source['classification'] = CLASSIFICATION.normalize_classification(source['classification'])

    # Test rest
    assert {k: v for k, v in ds_sl.items() if k in sl_data_original} == sl_data_original

    u_data = {
        'attribution': {'implant': ['TEST'], 'actor': ['TEST']},
        'hashes': {'sha256': update_hash, 'tlsh': 'faketlsh'},
        'sources': [USER_SOURCE],
        'type': 'file'
    }

    # Insert it and test return value
    qhash, op = client.add_update(u_data)
    assert qhash == update_hash
    assert op == 'update'

    # Load inserted data from DB
    ds_u = client.datastore.badlist.get(update_hash, as_obj=False)

    # Normalize classification in sources
    for source in ds_u['sources']:
        source['classification'] = CLASSIFICATION.normalize_classification(source['classification'])

    assert ds_u['added'] == ds_sl['added']
    assert iso_to_epoch(ds_u['updated']) > iso_to_epoch(ds_sl['updated'])
    assert len(ds_u['sources']) == 2
    assert USER_SOURCE in ds_u['sources']
    assert BAD_SOURCE in ds_u['sources']
    assert 'TEST' in ds_u['attribution']['implant']
    assert 'BAD' in ds_u['attribution']['implant']
    assert 'TEST' in ds_u['attribution']['actor']
    assert 'faketlsh' in ds_u['hashes']['tlsh']


def test_badlist_update_conflict(client):
    # Generate a random badlist
    sl_data = {'hashes': {'sha256': update_conflict_hash}, 'file': {}, 'sources': [ADMIN_SOURCE], 'type': 'file'}

    # Insert it and test return value
    qhash, op = client.add_update(sl_data)
    assert qhash == update_conflict_hash
    assert op == 'add'

    # Insert the same source with a different type
    sl_data['sources'][0]['type'] = 'external'
    with pytest.raises(InvalidBadhash) as conflict_exc:
        client.add_update(sl_data)

    assert 'has a type conflict:' in conflict_exc.value.args[0]

def test_badlist_tag_normalization(client):
    tag_type = 'network.static.uri'
    tag_value = 'https://BaD.com/About'

    normalized_value = 'https://bad.com/About'
    hashed_value = f"{tag_type}: {normalized_value}".encode('utf8')
    expected_qhash = hashlib.sha256(hashed_value).hexdigest()

    # Generate a random badlist
    sl_data = {
        'attribution': {
            'actor': ["SOMEONE!"],
            'campaign': None,
            'category': None,
            'exploit': None,
            'implant': None,
            'family': None,
            'network': None
        },
        'dtl': 15,
        'tag': {'type': tag_type,
                'value': tag_value},
        'sources': [BAD_SOURCE, ADMIN_SOURCE],
        'type': 'tag'
    }

    client.add_update(sl_data)

    # Assert that item got created with the expected ID from the normalized tag value
    assert client.datastore.badlist.exists(expected_qhash)
    time.sleep(1)

    # Assert that the tag exists in either format (within reason)
    assert client.exists_tags({tag_type: [tag_value]})
    assert client.exists_tags({tag_type: [normalized_value]})
