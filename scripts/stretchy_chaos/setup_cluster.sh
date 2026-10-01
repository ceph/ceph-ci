#!/bin/bash
# Recreate the 2-DC stretch EC vstart cluster used by stretchy_cokey.py.
# Runs the cluster from $CEPH_BUILD (see env.sh).
set -ex
. "$(dirname "$0")/env.sh"
cd $CEPH_BUILD
env -u CEPH_KEYRING -u CEPH_CONF CEPH_PORT=30000 CEPH_ARGS='--bluestore_block_size=4294967296 --bluestore_block_wal_size=134217728 --bluestore_block_db_size=536870912' MON=3 OSD=${OSD:-8} MDS=0 MGR=1 RGW=0 ../src/vstart.sh -n -d --without-dashboard \
    -o 'osd_pool_default_pg_autoscale_mode=off'
sed -i -E 's/^(\s*debug (osd|mon|mgr|paxos|auth|monc|client|mgrc)) = [0-9/]+\s*$/\1 = 1\/20/; s/^(\s*debug ms) = [0-9/]+\s*$/\1 = 0\/5/' ceph.conf
for kv in debug_osd=1/20 debug_ms=0/5 debug_bluestore=1/10 debug_bluefs=1/10 debug_bdev=1/10 debug_rocksdb=1/5; do
    ceph config set osd ${kv%%=*} ${kv#*=}
done
ceph tell osd.\* config set debug_osd 1/20 >/dev/null
ceph tell osd.\* config set debug_bdev 1/10 >/dev/null
ceph tell osd.\* config set debug_bluestore 1/10 >/dev/null
ceph tell mon.\* config set debug_mon 1/20 >/dev/null
ceph tell mon.\* config set debug_ms 0/5 >/dev/null
ceph tell mon.\* config set debug_paxos 1/10 >/dev/null
ceph config set osd osd_crush_update_on_start false
ceph config set osd bluestore_debug_inject_read_err true
half=$(( ${OSD:-8} / 2 ))
for dc in dc1 dc2; do ceph osd crush add-bucket $dc datacenter; ceph osd crush move $dc root=default; done
ceph osd crush add-bucket h1 host; ceph osd crush move h1 datacenter=dc1
ceph osd crush add-bucket h2 host; ceph osd crush move h2 datacenter=dc2
for o in $(seq 0 $((half-1))); do ceph osd crush set osd.$o 1.0 host=h1; done
for o in $(seq $half $((${OSD:-8}-1))); do ceph osd crush set osd.$o 1.0 host=h2; done
ceph mon set_location a datacenter=dc1
ceph mon set_location b datacenter=dc2
ceph mon set_location c datacenter=arbiter
ceph osd pool create ecs erasure --k ${K:-2} --m ${M:-1} --num_zones 2 --osd_failure_domain osd --pg_num 16
ceph osd pool set ecs allow_ec_overwrites true
ceph osd pool create rbd replicated --pg_num 8
ceph osd pool application enable rbd rbd
ceph osd pool application enable ecs rbd
