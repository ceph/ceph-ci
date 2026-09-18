#!/usr/bin/env python3

import logging as log
import os
import sys
import botocore.exceptions
from common import create_user, boto_connect, connect_with_retry, make_body, \
    object_stat, wait_for_storage_class, get_compression_type, \
    get_crypt_mode, has_crypt_attr, is_aead_crypt_mode

"""
Tests that a lifecycle transition re-encrypts an object with the algorithm
the gateway is configured with now, rather than the one the object was
written with.

The test runs in two phases around a cipher change. The suite starts with
the default aes-256-cbc, runs the put phase, sets aes-256-gcm, restarts
the gateway, then runs the transition phase. Objects written in the put
phase therefore predate the cipher change, which is what an operator
upgrading an existing cluster has.

TRANSITION_REENCRYPT_PHASE selects the phase. TRANSITION_REENCRYPT_EXPECT
names the mode the sse-kms objects should carry once they have
transitioned, so the same script covers a zonegroup with the
transition-reencrypt feature enabled and one without it.

The qa suite configures:
  STANDARD  - no compression
  TEPID     - no compression
  LUKEWARM  - zstd compression
"""

USER = 'transition-reencrypt-tester'
DISPLAY_NAME = 'Transition Reencrypt Testing'
ACCESS_KEY = 'LCREENCRYPT012345678'
SECRET_KEY = 'lcreencryptsecretkey0123456789abcdefghijk'
BUCKET_NAME = 'transition-reencrypt-bucket'
KMS_KEY_ID = 'testkey-1'

# same compression either side, so only a mode change can drive a rewrite
SAME_KEY = 'same-object'
# compression differs, so a rewrite happens with or without the feature
RECOMP_KEY = 'recomp-object'
SSEC_KEY = 'ssec-object'
PLAIN_KEY = 'plain-object'
PROBE_KEY = 'probe-object'

OBJECT_SIZE = 4 * 1024

SSEC_ARGS = {
    'SSECustomerAlgorithm': 'AES256',
    'SSECustomerKey': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
    'SSECustomerKeyMD5': 'DWygnHRtgiJ77HCm+1rvHw==',
}
KMS_ARGS = {
    'ServerSideEncryption': 'aws:kms',
    'SSEKMSKeyId': KMS_KEY_ID,
}

TRANSITIONS = {
    SAME_KEY: 'TEPID',
    RECOMP_KEY: 'LUKEWARM',
    SSEC_KEY: 'TEPID',
    PLAIN_KEY: 'TEPID',
}


def put_lifecycle_rules(client):
    """Set one transition rule per object, firing immediately."""
    rules = [{
        'ID': f'{key}-to-{storage_class}',
        'Filter': {'Prefix': key},
        'Status': 'Enabled',
        'Transitions': [{'Days': 0, 'StorageClass': storage_class}],
    } for key, storage_class in TRANSITIONS.items()]

    client.put_bucket_lifecycle_configuration(
        Bucket=BUCKET_NAME, LifecycleConfiguration={'Rules': rules})
    log.info(f'set {len(rules)} lifecycle transition rules')


def run_put_phase():
    """Write objects under whichever cipher the gateway is running now."""
    log.info('=== put phase ===')
    create_user(USER, DISPLAY_NAME, ACCESS_KEY, SECRET_KEY)

    conn = boto_connect(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client

    try:
        bucket = conn.Bucket(BUCKET_NAME)
        bucket.objects.all().delete()
        bucket.delete()
    except botocore.exceptions.ClientError:
        pass

    conn.create_bucket(Bucket=BUCKET_NAME)
    body = make_body(OBJECT_SIZE)

    for key in (SAME_KEY, RECOMP_KEY):
        log.info(f'uploading {key} with sse-kms')
        client.put_object(Bucket=BUCKET_NAME, Key=key, Body=body, **KMS_ARGS)

    log.info(f'uploading {SSEC_KEY} with sse-c')
    client.put_object(Bucket=BUCKET_NAME, Key=SSEC_KEY, Body=body, **SSEC_ARGS)

    log.info(f'uploading {PLAIN_KEY} unencrypted')
    client.put_object(Bucket=BUCKET_NAME, Key=PLAIN_KEY, Body=body)

    for key in (SAME_KEY, RECOMP_KEY):
        mode = get_crypt_mode(object_stat(BUCKET_NAME, key))
        assert mode == 'SSE-KMS', f'{key} was written as {mode}, expected SSE-KMS'

    mode = get_crypt_mode(object_stat(BUCKET_NAME, SSEC_KEY))
    assert mode == 'SSE-C-AES256', \
        f'{SSEC_KEY} was written as {mode}, expected SSE-C-AES256'

    mode = get_crypt_mode(object_stat(BUCKET_NAME, PLAIN_KEY))
    assert mode is None, f'{PLAIN_KEY} was written encrypted as {mode}'

    log.info('put phase passed')


def run_transition_phase(expected_mode):
    """Transition each object and check what it was re-encrypted with."""
    log.info(f'=== transition phase, expecting {expected_mode} ===')
    conn = connect_with_retry(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client
    bucket = conn.Bucket(BUCKET_NAME)
    body = make_body(OBJECT_SIZE)

    # a fresh upload proves the cipher change took effect, so a failure
    # below is not mistaken for a transition bug. this holds whether or
    # not the transition-reencrypt feature is enabled
    log.info('probing the configured cipher')
    client.put_object(Bucket=BUCKET_NAME, Key=PROBE_KEY, Body=body, **KMS_ARGS)
    probe_mode = get_crypt_mode(object_stat(BUCKET_NAME, PROBE_KEY))
    assert probe_mode == 'SSE-KMS-GCM', \
        f'a fresh upload is {probe_mode}, so the cipher change did not apply'
    bucket.Object(PROBE_KEY).delete()

    put_lifecycle_rules(client)

    expect_gcm = is_aead_crypt_mode(expected_mode)

    for key in (SAME_KEY, RECOMP_KEY):
        log.info(f'--- {key} -> {TRANSITIONS[key]} ---')
        stat = wait_for_storage_class(BUCKET_NAME, key, TRANSITIONS[key])

        mode = get_crypt_mode(stat)
        assert mode == expected_mode, \
            f'{key} transitioned as {mode}, expected {expected_mode}'

        salt = has_crypt_attr(stat, 'salt')
        align = has_crypt_attr(stat, 'prefetch-align')
        if expect_gcm:
            assert salt, f'{key} is {mode} but has no crypt.salt'
            assert align, f'{key} is {mode} but has no crypt.prefetch-align'
        else:
            assert not salt, f'{key} is {mode} but carries a crypt.salt'
            assert not align, \
                f'{key} is {mode} but carries a crypt.prefetch-align'

        assert bucket.Object(key).get()['Body'].read() == body, \
            f'data mismatch after transitioning {key}'

    assert get_compression_type(object_stat(BUCKET_NAME, RECOMP_KEY)) == 'zstd', \
        f'{RECOMP_KEY} was not recompressed for {TRANSITIONS[RECOMP_KEY]}'
    assert get_compression_type(object_stat(BUCKET_NAME, SAME_KEY)) is None, \
        f'{SAME_KEY} was compressed by a transition that changes no codec'

    # the gateway has no stored key for sse-c, so a transition can move the
    # object but can never re-encrypt it
    log.info(f'--- {SSEC_KEY} -> {TRANSITIONS[SSEC_KEY]} ---')
    stat = wait_for_storage_class(BUCKET_NAME, SSEC_KEY, TRANSITIONS[SSEC_KEY])
    mode = get_crypt_mode(stat)
    assert mode == 'SSE-C-AES256', \
        f'{SSEC_KEY} transitioned as {mode}, expected SSE-C-AES256'
    response = client.get_object(Bucket=BUCKET_NAME, Key=SSEC_KEY, **SSEC_ARGS)
    assert response['Body'].read() == body, \
        f'data mismatch after transitioning {SSEC_KEY}'

    log.info(f'--- {PLAIN_KEY} -> {TRANSITIONS[PLAIN_KEY]} ---')
    stat = wait_for_storage_class(BUCKET_NAME, PLAIN_KEY, TRANSITIONS[PLAIN_KEY])
    mode = get_crypt_mode(stat)
    assert mode is None, f'{PLAIN_KEY} was encrypted by a transition as {mode}'
    assert bucket.Object(PLAIN_KEY).get()['Body'].read() == body, \
        f'data mismatch after transitioning {PLAIN_KEY}'

    bucket.objects.all().delete()
    bucket.delete()
    log.info('transition phase passed')


def main():
    phase = os.environ.get('TRANSITION_REENCRYPT_PHASE')
    if phase == 'put':
        run_put_phase()
    elif phase == 'transition':
        expected = os.environ.get('TRANSITION_REENCRYPT_EXPECT')
        if not expected:
            sys.exit('TRANSITION_REENCRYPT_EXPECT must name the expected crypt mode')
        run_transition_phase(expected)
    else:
        sys.exit(f'TRANSITION_REENCRYPT_PHASE must be put or transition, got {phase!r}')


if __name__ == '__main__':
    main()
