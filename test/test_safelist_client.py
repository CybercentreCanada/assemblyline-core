
import hashlib
import random
from copy import deepcopy

import pytest

from assemblyline.common.forge import get_classification
from assemblyline.common.isotime import iso_to_epoch, now_as_iso
from assemblyline.odm.random_data import create_users, create_safelists, wipe_users, wipe_safelist
from assemblyline.odm.randomizer import get_random_hash
from assemblyline_core.safelist_client import SafelistClient, InvalidSafehash

add_hash_file = "10" + get_random_hash(62)
add_error_hash = "11" + get_random_hash(62)
update_hash = "12" + get_random_hash(62)
update_conflict_hash = "13" + get_random_hash(62)
source_hash = "14" + get_random_hash(62)

CLASSIFICATION = get_classification()

NSRL_SOURCE = {
    "classification": CLASSIFICATION.UNRESTRICTED,
    "name": "NSRL",
    "reason": [
        "Found as test.txt on default windows 10 CD",
        "Found as install.txt on default windows XP CD"
    ],
    "type": "external"}

NSRL2_SOURCE = {
    "classification": CLASSIFICATION.UNRESTRICTED,
    "name": "NSRL2",
    "reason": [
        "File contains only AAAAs..."
    ],
    "type": "external"}

ADMIN_SOURCE = {
    "classification": CLASSIFICATION.UNRESTRICTED,
    "name": "admin",
    "reason": [
        "Generates a lot of FPs",
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
        create_safelists(datastore_connection)
        yield SafelistClient(datastore_connection)
    finally:
        wipe_users(datastore_connection)
        wipe_safelist(datastore_connection)


# noinspection PyUnusedLocal
def test_safelist_add_file(client):
    # Generate a random safelist
    sl_data = {
        'hashes': {'md5': get_random_hash(32),
                   'sha1': get_random_hash(40),
                   'sha256': add_hash_file},
        'file': {'name': ['file.txt'],
                 'size': random.randint(128, 4096),
                 'type': 'document/text'},
        'sources': [NSRL_SOURCE, ADMIN_SOURCE],
        'type': 'file'
    }
    sl_data_original = deepcopy(sl_data)

    # Insert it and test return value
    qhash, op = client.add_update(sl_data)
    assert qhash == add_hash_file
    assert op == 'add'

    # Load inserted data from DB
    ds_sl = client.datastore.safelist.get(add_hash_file, as_obj=False)

    # Test dates
    added = ds_sl.pop('added', None)
    updated = ds_sl.pop('updated', None)

    # File item will live forever
    assert ds_sl.pop('expiry_ts', None) is None

    assert added == updated
    assert added is not None and updated is not None

    # Make sure tag and signature are none
    tag = ds_sl.pop('tag', None)
    signature = ds_sl.pop('signature', None)
    assert tag is None
    assert signature is None

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


def test_safelist_add_tag(client):
    tag_type = 'network.static.ip'
    tag_value = '127.0.0.1'
    hashed_value = f"{tag_type}: {tag_value}".encode('utf8')
    expected_qhash = hashlib.sha256(hashed_value).hexdigest()
    # Generate a random safelist
    sl_data = {
        'dtl': 15,
        'hashes': {'sha256': expected_qhash},
        'tag': {'type': tag_type,
                'value': tag_value},
        'sources': [NSRL_SOURCE, ADMIN_SOURCE],
        'type': 'tag'
    }
    sl_data_original = deepcopy(sl_data)

    # Insert it and test return value
    qhash, op = client.add_update(sl_data)
    assert qhash == expected_qhash
    assert op == 'add'

    # Load inserted data from DB
    ds_sl = client.datastore.safelist.get(expected_qhash, as_obj=False)

    # Test dates
    added = ds_sl.pop('added', None)
    updated = ds_sl.pop('updated', None)

    # Tag item will live up to a certain date
    assert ds_sl.pop('expiry_ts', None) is not None

    assert added == updated
    assert added is not None and updated is not None

    # Make sure file and signature are None
    file = ds_sl.pop('file', {})
    signature = ds_sl.pop('signature', None)
    assert file is None
    assert signature is None

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

    for hashtype in ['md5', 'sha1']:
        ds_sl['hashes'].pop(hashtype, None)

    # Test rest
    assert ds_sl == sl_data_original


def test_safelist_add_signature(client):
    sig_name = 'McAfee.Eicar'
    hashed_value = f"signature: {sig_name}".encode('utf8')
    expected_qhash = hashlib.sha256(hashed_value).hexdigest()

    # Generate a random safelist
    sl_data = {
        'hashes': {'sha256': expected_qhash},
        'signature': {'name': sig_name},
        'sources': [ADMIN_SOURCE],
        'type': 'signature'
    }

    sl_data_original = deepcopy(sl_data)

    # Insert it and test return value
    qhash, op = client.add_update(sl_data)
    assert qhash == expected_qhash
    assert op == 'add'

    # Load inserted data from DB
    ds_sl = client.datastore.safelist.get(expected_qhash, as_obj=False)

    # Test dates
    added = ds_sl.pop('added', None)
    updated = ds_sl.pop('updated', None)

    # Signature item will live forever
    assert ds_sl.pop('expiry_ts', None) is None

    assert added == updated
    assert added is not None and updated is not None

    # Make sure file and signature are None
    file = ds_sl.pop('file', {})
    tag = ds_sl.pop('tag', None)
    assert file is None
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

    for hashtype in ['md5', 'sha1']:
        ds_sl['hashes'].pop(hashtype, None)

    # Test rest
    assert ds_sl == sl_data_original


def test_safelist_add_invalid(client):
    # Generate a random safelist
    sl_data = {
        'hashes': {'sha256': add_error_hash},
        'sources': [USER_SOURCE],
        'type': 'file'}

    # Insert it and test return value
    with pytest.raises(ValueError) as conflict_exc:
        client.add_update(sl_data, user={"uname": "test"})

    assert 'for another user' in conflict_exc.value.args[0]


def test_safelist_update(client):
    cl_eng = get_classification()

    # Generate a random safelist
    sl_data = {
        'hashes': {'md5': get_random_hash(32),
                   'sha1': get_random_hash(40),
                   'sha256': update_hash},
        'file': {'name': [],
                 'size': random.randint(128, 4096),
                 'type': 'document/text'},
        'sources': [NSRL_SOURCE],
        'type': 'file'
    }

    sl_data_original = deepcopy(sl_data)

    # Insert it and test return value
    qhash, op = client.add_update(sl_data)
    assert qhash == update_hash
    assert op == 'add'

    # Load inserted data from DB
    ds_sl = client.datastore.safelist.get(update_hash, as_obj=False)

    # Normalize classification in sources
    for source in ds_sl['sources']:
        source['classification'] = CLASSIFICATION.normalize_classification(source['classification'])

    # Test rest
    assert {k: v for k, v in ds_sl.items() if k in sl_data_original} == sl_data_original

    u_data = {
        'hashes': {'sha256': update_hash},
        'sources': [USER_SOURCE],
        'type': 'file'
    }

    # Insert it and test return value
    qhash, op = client.add_update(u_data)
    assert qhash == update_hash
    assert op == 'update'

    # Load inserted data from DB
    ds_u = client.datastore.safelist.get(update_hash, as_obj=False)

    # Normalize classification in sources
    for source in ds_u['sources']:
        source['classification'] = CLASSIFICATION.normalize_classification(source['classification'])

    assert ds_u['added'] == ds_sl['added']
    assert iso_to_epoch(ds_u['updated']) > iso_to_epoch(ds_sl['updated'])
    assert len(ds_u['sources']) == 2
    assert USER_SOURCE in ds_u['sources']
    assert NSRL_SOURCE in ds_u['sources']


def test_safelist_update_conflict(client):
    # Generate a random safelist
    sl_data = {'hashes': {'sha256': update_conflict_hash}, 'file': {}, 'sources': [ADMIN_SOURCE], 'type': 'file'}

    # Insert it and test return value
    qhash, op = client.add_update(sl_data)
    assert qhash == update_conflict_hash
    assert op == 'add'

    # Insert the same source with a different type
    sl_data['sources'][0]['type'] = 'external'
    with pytest.raises(InvalidSafehash) as conflict_exc:
        client.add_update(sl_data)

    assert 'has a type conflict:' in conflict_exc.value.args[0]
