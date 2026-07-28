#!/usr/bin/env bash
set -ex
mydir=`dirname $0`

python3 -m venv $mydir
source $mydir/bin/activate
pip install pip --upgrade
pip install valkey
pip install configobj
pip install boto3

#mount -t tmpfs -o size=15 tmpfs /tmp/rgw_d4n_datacache

# create user
radosgw-admin user create --uid=test3 --display-name=test3 --access-key=test3 --secret-key=test3 2>/dev/null

# run test
$mydir/bin/python3 $mydir/test_rgw_d4n_remote.py

deactivate
echo OK.
