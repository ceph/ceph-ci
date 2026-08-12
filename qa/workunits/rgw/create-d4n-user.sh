#!/usr/bin/env bash
set -ex

# create user
radosgw-admin user create --uid=test3 --display-name=test3 --access-key=test3 --secret-key=test3 2>/dev/null

echo OK.
