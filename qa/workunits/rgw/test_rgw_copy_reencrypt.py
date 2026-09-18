#!/usr/bin/env python3

import io
import logging as log
import os
import sys
import boto3.s3.transfer
import botocore.exceptions
from common import create_user, boto_connect, connect_with_retry, make_body, \
    object_stat, get_compression_type, get_storage_class, get_crypt_mode, \
    has_crypt_attr, get_crypt_attr_raw

"""
Tests that CopyObject re-encrypts an object with the encryption algorithm
the gateway is configured with now, rather than the one the object was
written with, and recompresses it for the destination storage class.

The test runs in two phases around a cipher change. The suite starts with
the default aes-256-cbc, runs the put phase, sets aes-256-gcm, restarts
the gateway, then runs the copy phase. Objects written in the put phase
therefore predate the cipher change, which is what an operator upgrading
an existing cluster has.

The phase is selected by COPY_REENCRYPT_PHASE.

The qa suite configures:
  STANDARD  - no compression
  LUKEWARM  - zstd compression
"""

USER = 'copy-reencrypt-tester'
DISPLAY_NAME = 'CopyObject Reencrypt Testing'
ACCESS_KEY = 'COPYREENC0123456789A'
SECRET_KEY = 'copyreencsecretkey0123456789abcdefghijklm'
BUCKET_NAME = 'copy-reencrypt-bucket'
KMS_KEY_ID = 'testkey-1'

SMALL_KEY = 'small'
LARGE_KEY = 'large'
INPLACE_KEY = 'multipart-inplace'
SSEC_KEY = 'ssec'
PROBE_KEY = 'gcm-probe'

SMALL_SIZE = 4 * 1024
LARGE_SIZE = 32 * 1024 * 1024
INPLACE_SIZE = 9 * 1024 * 1024
MULTIPART_THRESHOLD = 8 * 1024 * 1024

SSEC_ARGS = {
    'SSECustomerAlgorithm': 'AES256',
    'SSECustomerKey': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
    'SSECustomerKeyMD5': 'DWygnHRtgiJ77HCm+1rvHw==',
}
SSEC_COPY_SOURCE_ARGS = {
    'CopySourceSSECustomerAlgorithm': 'AES256',
    'CopySourceSSECustomerKey': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
    'CopySourceSSECustomerKeyMD5': 'DWygnHRtgiJ77HCm+1rvHw==',
}
KMS_ARGS = {
    'ServerSideEncryption': 'aws:kms',
    'SSEKMSKeyId': KMS_KEY_ID,
}


def put_object(client, key, body, extra_args):
    """Upload body, using multipart above the threshold."""
    if len(body) > MULTIPART_THRESHOLD:
        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=MULTIPART_THRESHOLD,
            multipart_chunksize=MULTIPART_THRESHOLD,
        )
        client.upload_fileobj(io.BytesIO(body), BUCKET_NAME, key,
                              ExtraArgs=dict(extra_args),
                              Config=transfer_config)
    else:
        client.put_object(Bucket=BUCKET_NAME, Key=key, Body=body, **extra_args)


def verify_encrypted(key, expected_mode):
    """Check the stored encryption mode of an object."""
    stat = object_stat(BUCKET_NAME, key)

    mode = get_crypt_mode(stat)
    log.info(f'{key}: crypt_mode={mode} storage_class={get_storage_class(stat)} '
             f'compression={get_compression_type(stat)}')
    assert mode == expected_mode, \
        f'{key} crypt mode is {mode}, expected {expected_mode}'

    return stat


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

    log.info(f'uploading {SMALL_KEY} with sse-kms')
    put_object(client, SMALL_KEY, make_body(SMALL_SIZE), KMS_ARGS)

    log.info(f'uploading {LARGE_KEY} with sse-kms, multipart')
    put_object(client, LARGE_KEY, make_body(LARGE_SIZE), KMS_ARGS)

    log.info(f'uploading {INPLACE_KEY} with sse-kms, multipart')
    put_object(client, INPLACE_KEY, make_body(INPLACE_SIZE), KMS_ARGS)

    log.info(f'uploading {SSEC_KEY} with sse-c')
    put_object(client, SSEC_KEY, make_body(SMALL_SIZE), SSEC_ARGS)

    for key in (SMALL_KEY, LARGE_KEY, INPLACE_KEY, SSEC_KEY):
        stat = object_stat(BUCKET_NAME, key)
        mode = get_crypt_mode(stat)
        assert mode is not None, f'{key} was not encrypted'
        log.info(f'{key}: crypt_mode={mode}')

    log.info('put phase passed')


def run_copy_phase():
    """Re-encrypt each object by copying it onto itself."""
    log.info('=== copy phase ===')
    conn = connect_with_retry(ACCESS_KEY, SECRET_KEY)
    client = conn.meta.client
    bucket = conn.Bucket(BUCKET_NAME)

    # a fresh upload proves the cipher change took effect, so that a
    # failure here is not mistaken for a copy bug
    log.info('probing the configured cipher')
    bucket.put_object(Key=PROBE_KEY, Body=make_body(SMALL_SIZE), **KMS_ARGS)
    verify_encrypted(PROBE_KEY, 'SSE-KMS-GCM')
    bucket.Object(PROBE_KEY).delete()

    small_body = make_body(SMALL_SIZE)
    large_body = make_body(LARGE_SIZE)
    ssec_body = make_body(SMALL_SIZE)

    log.info('--- re-encrypt sse-kms object in place ---')
    client.copy_object(Bucket=BUCKET_NAME, Key=SMALL_KEY,
                       CopySource={'Bucket': BUCKET_NAME, 'Key': SMALL_KEY},
                       MetadataDirective='COPY', **KMS_ARGS)
    stat = verify_encrypted(SMALL_KEY, 'SSE-KMS-GCM')
    assert has_crypt_attr(stat, 'salt'), 'AEAD object missing crypt.salt'
    first_salt = get_crypt_attr_raw(BUCKET_NAME, SMALL_KEY, 'salt')
    assert bucket.Object(SMALL_KEY).get()['Body'].read() == small_body, \
        'data mismatch after re-encrypting in place'

    # a second re-encryption has a salt to rotate away from, which the
    # first does not: the object was written under CBC, which stores none
    log.info('--- re-encrypt the same object again, salt must rotate ---')
    client.copy_object(Bucket=BUCKET_NAME, Key=SMALL_KEY,
                       CopySource={'Bucket': BUCKET_NAME, 'Key': SMALL_KEY},
                       MetadataDirective='COPY', **KMS_ARGS)
    stat = verify_encrypted(SMALL_KEY, 'SSE-KMS-GCM')
    assert has_crypt_attr(stat, 'salt'), 'AEAD object missing crypt.salt'
    second_salt = get_crypt_attr_raw(BUCKET_NAME, SMALL_KEY, 'salt')
    assert second_salt != first_salt, \
        'crypt.salt did not rotate across re-encryption'
    assert bucket.Object(SMALL_KEY).get()['Body'].read() == small_body, \
        'data mismatch after re-encrypting a second time'

    # a multipart source with no storage class change, so the copy is legal
    # only because the request names an encryption
    log.info('--- re-encrypt multipart object in place ---')
    inplace_body = make_body(INPLACE_SIZE)
    before = client.head_object(Bucket=BUCKET_NAME, Key=INPLACE_KEY)['ETag']
    assert '-' in before.strip('"'), \
        f'{INPLACE_KEY} was expected to carry a multipart ETag, got {before}'
    client.copy_object(Bucket=BUCKET_NAME, Key=INPLACE_KEY,
                       CopySource={'Bucket': BUCKET_NAME, 'Key': INPLACE_KEY},
                       MetadataDirective='COPY', **KMS_ARGS)
    verify_encrypted(INPLACE_KEY, 'SSE-KMS-GCM')
    after = client.head_object(Bucket=BUCKET_NAME, Key=INPLACE_KEY)['ETag']
    assert after == before, \
        f'{INPLACE_KEY} ETag changed across re-encryption, {before} -> {after}'
    assert bucket.Object(INPLACE_KEY).get()['Body'].read() == inplace_body, \
        'data mismatch after re-encrypting a multipart object in place'

    log.info('--- re-encrypt and recompress multipart object ---')
    client.copy_object(Bucket=BUCKET_NAME, Key=LARGE_KEY,
                       CopySource={'Bucket': BUCKET_NAME, 'Key': LARGE_KEY},
                       MetadataDirective='COPY', StorageClass='LUKEWARM',
                       **KMS_ARGS)
    stat = verify_encrypted(LARGE_KEY, 'SSE-KMS-GCM')
    sc = get_storage_class(stat)
    assert sc == 'LUKEWARM', \
        f'{LARGE_KEY} storage class is {sc}, expected LUKEWARM'
    ct = get_compression_type(stat)
    assert ct == 'zstd', f'{LARGE_KEY} compression is {ct}, expected zstd'
    orig_size = stat['compression']['orig_size']
    assert orig_size == len(large_body), \
        f'{LARGE_KEY} orig_size is {orig_size}, expected {len(large_body)}'
    assert bucket.Object(LARGE_KEY).get()['Body'].read() == large_body, \
        'data mismatch after re-encrypting and recompressing'

    log.info('--- re-encrypt sse-c object in place ---')
    client.copy_object(Bucket=BUCKET_NAME, Key=SSEC_KEY,
                       CopySource={'Bucket': BUCKET_NAME, 'Key': SSEC_KEY},
                       MetadataDirective='COPY',
                       **SSEC_ARGS, **SSEC_COPY_SOURCE_ARGS)
    verify_encrypted(SSEC_KEY, 'SSE-C-AES256-GCM')
    response = client.get_object(Bucket=BUCKET_NAME, Key=SSEC_KEY, **SSEC_ARGS)
    assert response['Body'].read() == ssec_body, \
        'data mismatch after re-encrypting sse-c object'

    log.info('--- copy onto itself with no encryption headers is rejected ---')
    try:
        client.copy_object(Bucket=BUCKET_NAME, Key=SMALL_KEY,
                           CopySource={'Bucket': BUCKET_NAME, 'Key': SMALL_KEY},
                           MetadataDirective='COPY')
        raise AssertionError('copy onto itself without changes was accepted')
    except botocore.exceptions.ClientError as e:
        status = e.response['ResponseMetadata']['HTTPStatusCode']
        code = e.response['Error']['Code']
        assert status == 400 and code == 'InvalidRequest', \
            f'expected 400 InvalidRequest, got {status} {code}'

    bucket.objects.all().delete()
    bucket.delete()
    log.info('copy phase passed')


def main():
    phase = os.environ.get('COPY_REENCRYPT_PHASE')
    if phase == 'put':
        run_put_phase()
    elif phase == 'copy':
        run_copy_phase()
    else:
        sys.exit(f'COPY_REENCRYPT_PHASE must be put or copy, got {phase!r}')


if __name__ == '__main__':
    main()
