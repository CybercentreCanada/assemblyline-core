import json
import random
import pytest

from assemblyline.odm.models.signature import Signature
from assemblyline.odm.randomizer import random_model_obj
from assemblyline.odm.random_data import create_signatures, wipe_signatures, create_users, wipe_users
from assemblyline_core.signature_client import SignatureClient


@pytest.fixture(scope="module")
def client(datastore_connection):
    try:
        create_users(datastore_connection)
        create_signatures(datastore_connection)
        yield SignatureClient(datastore_connection)
    finally:
        wipe_users(datastore_connection)
        wipe_signatures(datastore_connection)


# noinspection PyUnusedLocal
def test_add_update_signature(client):
    # Insert a dummy signature
    data = random_model_obj(Signature).as_primitives()
    data['status'] = "DEPLOYED"
    expected_key = f'{data["type"]}_{data["source"]}_{data["signature_id"]}'
    success, key, _ = client.add_update(data)
    assert success
    assert key == expected_key

    # Test the signature data
    client.datastore.signature.commit()
    added_sig = client.datastore.signature.get(key, as_obj=False)
    assert data == added_sig

    # Change the signature status as a user
    success, _ = client.change_status(key, "DISABLED", client.datastore.user.get('user', as_obj=False))
    assert success

    # Update signature data as an internal component
    new_sig_data = "NEW SIGNATURE DATA"
    data['data'] = new_sig_data
    success, key, _ = client.add_update(data)
    assert success
    assert expected_key == key
    modded_sig = client.datastore.signature.get(key, as_obj=False)
    assert modded_sig["data"] == new_sig_data
    # Was state kept from user setting?
    assert "DISABLED" == modded_sig.pop('status')


# noinspection PyUnusedLocal
def test_add_update_signature_many(client):

    # Insert a dummy signature
    source = "source"
    s_type = "type"
    sig_list = []
    for x in range(10):
        data = random_model_obj(Signature).as_primitives()
        data['signature_id'] = f"test_sig_{x}"
        data['name'] = f"sig_name_{x}"
        data['status'] = "DEPLOYED"
        data['source'] = source
        data['type'] = s_type
        sig_list.append(data)

    assert {'errors': False, 'success': 10, 'skipped': []} == client.add_update_many(source, s_type, sig_list)

    # Test the signature data
    client.datastore.signature.commit()
    data = random.choice(sig_list)
    key = f"{data['type']}_{data['source']}_{data['signature_id']}"
    added_sig = client.datastore.signature.get(key, as_obj=False)
    assert data == added_sig

    # Change the signature status
    success, _ = client.change_status(key, "DISABLED", client.datastore.user.get('user', as_obj=False))
    assert success

    # Update signature data
    new_sig_data = "NEW SIGNATURE DATA"
    data['data'] = new_sig_data
    assert {'errors': False, 'success': 1, 'skipped': []} == client.add_update_many(source, s_type, [data])

    # Test the signature data
    modded_sig = client.datastore.signature.get(key, as_obj=False)
    assert modded_sig["data"] == new_sig_data
    # Was state kept?
    assert "DISABLED" == modded_sig.pop('status')


# noinspection PyUnusedLocal
def test_download_signatures(client):
    resp = client.download()
    assert resp.startswith(b"PK")
    assert b"YAR_SAMPLE" in resp
    assert b"ET_SAMPLE" in resp


# noinspection PyUnusedLocal
def test_update_available(client):
    assert client.update_available()
    assert not client.update_available(since='2030-01-01T00:00:00.000000Z')
