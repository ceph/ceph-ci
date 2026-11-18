#!/usr/bin/env python3

"""
Helpers shared by the rgw workunits. README.md says how they find radosgw.
"""

import errno
import os
import subprocess
import logging as log
import boto3
import botocore.exceptions
import random
import json
from time import sleep

log.basicConfig(format = '%(message)s', level=log.DEBUG)
log.getLogger('botocore').setLevel(log.CRITICAL)
log.getLogger('boto3').setLevel(log.CRITICAL)
log.getLogger('urllib3').setLevel(log.CRITICAL)

def exec_cmd(cmd, wait = True, **kwargs):
    check_retcode = kwargs.pop('check_retcode', True)
    kwargs['shell'] = True
    kwargs['stdout'] = subprocess.PIPE
    proc = subprocess.Popen(cmd, **kwargs)
    log.info(proc.args)
    if wait:
        out, _ = proc.communicate()
        if check_retcode:
            assert(proc.returncode == 0)
            return out
        return (out, proc.returncode)
    return ''
    
def create_user(uid, display_name, access_key, secret_key):
    _, ret = exec_cmd(f'radosgw-admin user create --uid {uid} --display-name "{display_name}" --access-key {access_key} --secret {secret_key}', check_retcode=False)
    assert(ret == 0 or ret == errno.EEXIST)

def rgw_endpoints():
    """
    The radosgw endpoints to try, in order.

    AWS_ENDPOINT_URL, if set, is the only endpoint. Otherwise try RGW_HOST
    (default: localhost) on port 80, then 8000, then 443 with ssl.
    """
    url = os.environ.get('AWS_ENDPOINT_URL')
    if url:
        return [url]
    host = os.environ.get('RGW_HOST', 'localhost')
    return [f'http://{host}:80', f'http://{host}:8000', f'https://{host}:443']

def boto_connect(access_key, secret_key, config=None):
    """
    A boto3 s3 resource for the first of rgw_endpoints() that answers.
    If none does, raises the connection error from the last one.
    """
    def try_connect(endpoint):
        conn = boto3.resource('s3',
                              aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key,
                              use_ssl=endpoint.startswith('https://'),
                              endpoint_url=endpoint,
                              verify=False,
                              config=config,
                              )
        try:
            list(conn.buckets.limit(1)) # just verify we can list buckets
        except botocore.exceptions.ConnectionError as e:
            print(e)
            raise
        print('connected to', endpoint)
        return conn
    endpoints = rgw_endpoints()
    for idx, endpoint in enumerate(endpoints):
        try:
            return try_connect(endpoint)
        except botocore.exceptions.ConnectionError:
            if idx == len(endpoints) - 1:
                raise

def put_objects(bucket, key_list):
    objs = []
    for key in key_list:
        o = bucket.put_object(Key=key, Body=b"some_data")
        objs.append((o.key, o.version_id))
    return objs

def create_unlinked_objects(conn, bucket, key_list):
    # creates an unlinked/unlistable object for each key in key_list
    
    object_versions = []
    try:
        exec_cmd('ceph config set client rgw_debug_inject_set_olh_err 2')
        exec_cmd('ceph config set client rgw_debug_inject_olh_cancel_modification_err true')
        sleep(1)
        for key in key_list:
            tag = str(random.randint(0, 1_000_000))
            try:
                bucket.put_object(Key=key, Body=b"some_data", Metadata = {
                    'tag': tag,
                })
            except Exception as e:
                log.debug(e)
            out = exec_cmd(f'radosgw-admin bi list --bucket {bucket.name} --object {key}')
            instance_entries = filter(
                lambda x: x['type'] == 'instance',
                json.loads(out.replace(b'\x80', b'0x80')))
            found = False
            for ie in instance_entries:
                instance_id = ie['entry']['instance']
                ov = conn.ObjectVersion(bucket.name, key, instance_id).head()
                if ov['Metadata'] and ov['Metadata']['tag'] == tag:
                    object_versions.append((key, instance_id))
                    found = True
                    break
            if not found:
                raise Exception(f'failed to create unlinked object for key={key}')
    finally:
        exec_cmd('ceph config rm client rgw_debug_inject_set_olh_err')
        exec_cmd('ceph config rm client rgw_debug_inject_olh_cancel_modification_err')
    return object_versions

