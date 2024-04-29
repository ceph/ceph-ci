import boto3
import valkey as redis
import os
import botocore
import json
import string
import random
import unittest
import subprocess
from botocore.client import Config
import logging
import hashlib
import shutil

logging.basicConfig(filename="boto.log", level=logging.DEBUG)

from botocore.handlers import validate_bucket_name

endpoint = 'http://localhost:8000'

access = 'test3'
secret = 'test3'

def generate_random(size, part_size=5*1024*1024):
    """
    Generate the specified number random data.
    (actually each MB is a repetition of the first KB)
    """
    chunk = 1024
    allowed = string.ascii_letters
    for x in range(0, size, part_size):
        strpart = ''.join([allowed[random.randint(0, len(allowed) - 1)] for _ in range(chunk)])
        s = ''
        left = size - x
        this_part_size = min(left, part_size)
        for y in range(this_part_size // chunk):
            s = s + strpart
        if this_part_size > len(s):
            s = s + strpart[0:this_part_size - len(s)]
        yield s
        if (x == size):
            return

def _multipart_upload(bucket_name, key, size, part_size=5*1024*1024, client=None, content_type=None, metadata=None, resend_parts=[]):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """

    if content_type == None and metadata == None:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    else:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key, Metadata=metadata, ContentType=content_type)

    upload_id = response['UploadId']
    s = ''
    parts = []
    for i, part in enumerate(generate_random(size, part_size)):
        # part_num is necessary because PartNumber for upload_part and in parts must start at 1 and i starts at 0
        part_num = i+1
        s += part
        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)
        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num})
        if i in resend_parts:
            client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)

    return (upload_id, s, parts)

ACCESS_KEY = 'test3'
SECRET_KEY = 'test3'

client = boto3.client(service_name='s3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            endpoint_url=endpoint,
            use_ssl=False,
            verify=False)

s3 = boto3.resource('s3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            endpoint_url=endpoint,
            use_ssl=False,
            verify=False)
bucket = s3.Bucket('bkt')

bucket.create()
obj = s3.Object(bucket_name='bkt', key='test.txt')

def get_body(response):
    body = response['Body']
    got = body.read()
    if type(got) is bytes:
        got = got.decode()
    return got

class D4NFilterTestCase(unittest.case.TestCase):

    @classmethod
    def setUpClass(cls):
        print("D4NFilterTest setup.")

        try:
            cls._connection = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            cls._connection.ping()
        except:
            print("ERROR: Redis instance not running.")
            raise

    def test_small_object(self):
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        test_txt = 'test'
        
        response_put = obj.put(Body=test_txt)
        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200)
        
        # first get call
        response_get = obj.get()
        self.assertEqual(response_get.get('ResponseMetadata').get('HTTPStatusCode'), 200)

        # check logs to ensure object was retrieved from storage backend
        res = subprocess.call(['grep', '"D4NFilterObject::iterate:: iterate(): Fetching object from backend store"', '/ceph/build/out/radosgw.8000.log'])
        self.assertGreaterEqual(res, 1)

        # retrieve and compare cache contents
        body = get_body(response_get)
        self.assertEqual(body, "test")

        bucketID = subprocess.check_output(['ls', '/tmp/rgw_d4n_datacache/']).decode('latin-1').strip()
        datacache_path = '/tmp/rgw_d4n_datacache/' + bucketID + '/test.txt/'
        datacache = subprocess.check_output(['ls', '-a', datacache_path])
        datacache = datacache.decode('latin-1').strip().splitlines()
        if '#' in datacache[3]: # datablock key
          datacache = datacache[3]
        else:
          datacache = datacache[2]

        output = subprocess.check_output(['md5sum', datacache_path + datacache]).decode('latin-1')
        self.assertEqual(output.splitlines()[0].split()[0], hashlib.md5("test".encode('utf-8')).hexdigest())
        
        data = {}
        for entry in r.scan_iter(match="*_test.txt_0_4"):
            data = r.hgetall(entry)

            # directory entry comparisons
            self.assertEqual(data.get('blockID'), '0')
            self.assertEqual(data.get('size'), '4')
            self.assertEqual(data.get('deleteMarker'), '0')
            self.assertEqual(data.get('globalWeight'), '0')
            self.assertEqual(data.get('hosts'), '127.0.0.1:8000')
            self.assertEqual(data.get('objName'), 'test.txt')
            self.assertEqual(data.get('bucketName'), bucketID)
            self.assertEqual(data.get('dirty'), '0')
            self.assertEqual(data.get('hosts'), '127.0.0.1:8000')


        # second get call
        response_get = obj.get()
        self.assertEqual(response_get.get('ResponseMetadata').get('HTTPStatusCode'), 200)

        # check logs to ensure object was retrieved from cache
        oid_in_cache = bucketID + "#" + data.get('version') + "test.txt#0" + data.get('size')
        res = subprocess.call(['grep', '"D4NFilterObject::iterate:: iterate(): READ FROM CACHE: oid="' + oid_in_cache, '/ceph/build/out/radosgw.8000.log'])
        self.assertGreaterEqual(res, 1)

        # retrieve and compare cache contents
        body = get_body(response_get)
        self.assertEqual(body, "test")

        datacache = subprocess.check_output(['ls', '-a', datacache_path])
        datacache = datacache.decode('latin-1').strip().splitlines()
        if '#' in datacache[3]: # datablock key
          datacache = datacache[3]
        else:
          datacache = datacache[2]
        output = subprocess.check_output(['md5sum', datacache_path + datacache]).decode('latin-1')
        self.assertEqual(output.splitlines()[0].split()[0], hashlib.md5("test".encode('utf-8')).hexdigest())

        data = {}
        for entry in r.scan_iter(match="*test.txt_0_4"):
            data = r.hgetall(entry)

            # directory entries should remain consistent
            self.assertEqual(data.get('blockID'), '0')
            self.assertEqual(data.get('deleteMarker'), '0')
            self.assertEqual(data.get('size'), '4')
            self.assertEqual(data.get('globalWeight'), '0')
            self.assertEqual(data.get('objName'), 'test.txt')
            self.assertEqual(data.get('bucketName'), bucketID)
            self.assertEqual(data.get('dirty'), '0')
            self.assertEqual(data.get('hosts'), '127.0.0.1:8000')

    def test_large_object(self):
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

        key="mymultipart"
        bucket_name="bkt"
        content_type='text/bla'
        objlen = 30 * 1024 * 1024
        metadata = {'foo': 'bar'}

        (upload_id, multipart_data, parts) = _multipart_upload(bucket_name=bucket_name, key=key, size=objlen, client=client, content_type=content_type, metadata=metadata)
        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

        file_path = os.path.dirname(__file__)+'mymultipart'

        # first get
        s3.Object(bucket_name, key).download_file(file_path)

        # check logs to ensure object was retrieved from storage backend
        res = subprocess.call(['grep', '"D4NFilterObject::iterate:: iterate(): Fetching object from backend store"', '/ceph/build/out/radosgw.8000.log'])
        self.assertGreaterEqual(res, 1)

        # retrieve and compare cache contents
        with open(file_path, 'r') as body:
            self.assertEqual(body.read(), multipart_data)

        bucketID = subprocess.check_output(['ls', '/tmp/rgw_d4n_datacache/']).decode('latin-1').strip()
        datacache_path = '/tmp/rgw_d4n_datacache/' + bucketID + '/mymultipart/'
        datacache = subprocess.check_output(['ls', '-a', datacache_path])
        datacache = datacache.decode('latin-1').splitlines()[2:]

        for file in datacache:
            if '#' in file: # data blocks
                ofs = int(file.split("#")[1])
                size = int(file.split("#")[2])
                output = subprocess.check_output(['md5sum', datacache_path + file]).decode('latin-1')
                self.assertEqual(output.splitlines()[0].split()[0], hashlib.md5(multipart_data[ofs:ofs+size].encode('utf-8')).hexdigest())

        data = {}
        for entry in r.scan_iter(match="*_mymultipart_*"):
            data = r.hgetall(entry)
            entry_name = entry.split("_")

            if len(entry_name) == 6: # versioned block
                self.assertEqual(data.get('blockID'), entry_name[4])
                self.assertEqual(data.get('deleteMarker'), '0')
                self.assertEqual(data.get('size'), entry_name[5])
                self.assertEqual(data.get('globalWeight'), '0')
                self.assertEqual(data.get('objName'), '_:null_mymultipart')
                self.assertEqual(data.get('bucketName'), bucketID)
                self.assertEqual(data.get('dirty'), '0')
                self.assertEqual(data.get('hosts'), '127.0.0.1:8000')
                continue

            # directory entry comparisons
            self.assertEqual(data.get('blockID'), entry_name[2])
            self.assertEqual(data.get('deleteMarker'), '0')
            self.assertEqual(data.get('size'), entry_name[3])
            self.assertEqual(data.get('globalWeight'), '0')
            self.assertEqual(data.get('objName'), 'mymultipart')
            self.assertEqual(data.get('bucketName'), bucketID)
            self.assertEqual(data.get('dirty'), '0')
            self.assertEqual(data.get('hosts'), '127.0.0.1:8000')

        # second get
        s3.Object(bucket_name, key).download_file(file_path)

        # check logs to ensure object was retrieved from cache
        oid_in_cache = bucketID + "#" + data.get('version') + "mymultipart#0" + data.get('size')
        res = subprocess.call(['grep', '"D4NFilterObject::iterate:: iterate(): READ FROM CACHE: oid="' + oid_in_cache, '/ceph/build/out/radosgw.8000.log'])
        self.assertGreaterEqual(res, 1)

        # retrieve and compare cache contents
        with open(file_path, 'r') as body:
            self.assertEqual(body.read(), multipart_data)

        datacache = subprocess.check_output(['ls', '-a', datacache_path])
        datacache = datacache.decode('latin-1').splitlines()[2:]

        for file in datacache:
            if '#' in file: # data blocks
                ofs = int(file.split("#")[1])
                size = int(file.split("#")[2])
                output = subprocess.check_output(['md5sum', datacache_path + file]).decode('latin-1')
                self.assertEqual(output.splitlines()[0].split()[0], hashlib.md5(multipart_data[ofs:ofs+size].encode('utf-8')).hexdigest())

        data = {}
        for entry in r.scan_iter(match="*_mymultipart_*"):
            data = r.hgetall(entry)
            entry_name = entry.split("_")

            if len(entry_name) == 6: # versioned block
                self.assertEqual(data.get('blockID'), entry_name[4])
                self.assertEqual(data.get('deleteMarker'), '0')
                self.assertEqual(data.get('size'), entry_name[5])
                self.assertEqual(data.get('globalWeight'), '0')
                self.assertEqual(data.get('objName'), '_:null_mymultipart')
                self.assertEqual(data.get('bucketName'), bucketID)
                self.assertEqual(data.get('dirty'), '0')
                self.assertEqual(data.get('hosts'), '127.0.0.1:8000')
                continue

            # directory entries should remain consistent
            self.assertEqual(data.get('blockID'), entry_name[2])
            self.assertEqual(data.get('deleteMarker'), '0')
            self.assertEqual(data.get('size'), entry_name[3])
            self.assertEqual(data.get('globalWeight'), '0')
            self.assertEqual(data.get('objName'), 'mymultipart')
            self.assertEqual(data.get('bucketName'), bucketID)
            self.assertEqual(data.get('dirty'), '0')
            self.assertEqual(data.get('hosts'), '127.0.0.1:8000')

        #filter_client = [client for client in r.client_list()
        #                   if client.get('name') in ['D4N.Filter']]
        #r.client_kill_filter(_id=filter_client[0].get('id'))

    @classmethod
    def tearDownClass(cls):
        print("D4NFilterTest teardown.")
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

        # delete bucket
        response_delete = bucket.object_versions.delete();
        for res in response_delete:
            print(res.get('ResponseMetadata').get('HTTPStatusCode'))
            #self.assertEqual(res.get('ResponseMetadata').get('HTTPStatusCode'), 200)
 
        response_delete = bucket.delete();
        print(res.get('ResponseMetadata').get('HTTPStatusCode'))
        #self.assertEqual(response_delete.get('ResponseMetadata').get('HTTPStatusCode'), 200)
         
        data = list(r.scan_iter(match='*test.txt*'))
        print(len(data))
        #self.assertEqual(len(data), 0)

        r.flushall()

if __name__ == '__main__':

    unittest.main()

