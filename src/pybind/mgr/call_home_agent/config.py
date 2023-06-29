import os
import sys
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import base64
import json

call_home_keys = '/usr/share/ceph/mgr/call_home_agent/ceph_call_home'  # default location of the key file

decryption_key = b'yDVH70MMpzBnu5Y1dKfJrw=='
decyption_nonce = b'1K6HRTiLD80laBi6'

def get_settings() -> dict:
    if 'UNITTEST' in os.environ:
        return {'api_key': 'test_api_key', 'private_key': 'test_private_key'}

    try:
        encrypted_keys = _load_encrypted_keys()
        aes_key = base64.b64decode(decryption_key)
        nonce = base64.b64decode(decyption_nonce)
        aesgcm = AESGCM(aes_key)
        clear_keys = aesgcm.decrypt(nonce, encrypted_keys, b'')
        keys = json.loads(clear_keys)
        return keys
    except Exception as e:
        raise Exception(f"Error getting encrypted settings: {e}")

def _load_encrypted_keys() -> bytes:
    key_file = os.environ.get('CALLHOMEKEYSFILE', call_home_keys)
    if not os.path.isfile(key_file):
        raise Exception(f"Can't find key file {key_file}")

    with open(key_file, 'rb') as f:
        return f.read()
