#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7310" # git grep '\<7310\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7311" # git grep '\<7311\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7312" # git grep '\<7312\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# stretch unset must keep an EC pool's size: its size is set by num_zones.
function TEST_stretch_unset_keeps_ec_size() {
    local dir=$1

    two_zone_cluster $dir || return 1
    ceph osd pool create data0 erasure --num-zones 2 --k 2 --m 1 || return 1
    local rule=$(ceph osd pool get data0 crush_rule -f json | jq -r .crush_rule)
    ceph osd pool stretch unset data0 $rule 3 2 2>&1 | grep "size must stay 6" || return 1
    test "$(pool_field data0 size)" = 6 || return 1
    ceph osd pool stretch unset data0 $rule 6 2 || return 1
}

main mon-stretch-stretch-unset-ec-size "$@"
