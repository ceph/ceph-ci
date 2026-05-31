# helper functions for cephadm dealing with IBM license acceptance -------------

import hashlib
import json

from ceph.utils import datetime_now


def get_license_acceptance_key_value_entry_name(ceph_version: str, license_text: str) -> str:
    license_hash = hashlib.md5(str(license_text).encode('utf-8')).hexdigest()
    return f'{ceph_version.replace(" ", "_")}_license_{license_hash}'


def generate_license_acceptance_key_value_entry(
    ceph_version: str,
    license_text: str,
    image_digest: str
) -> str:
    return json.dumps({
        'ceph_version': ceph_version,
        'image_id': image_digest,
        'accept_time': str(datetime_now()),
        'license': license_text
    })
