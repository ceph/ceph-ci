#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON="127.0.0.1:7330" # git grep '\<7330\>' : there must be only one
    run_stretch_tests "$@"
}

# A multi-zone EC pool needs a profile generated from k and m. Without them
# the pool takes the default profile and builds the shared erasure-code
# rule, which later single-zone EC pools also use, as a stretch rule.
function TEST_multi_zone_ec_pool_requires_k_m() {
    local dir=$1

    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    ceph osd pool create p0 erasure --num-zones 2 2>&1 | grep "multi-zone erasure coded pools require k and m" || return 1
    ! ceph osd pool ls | grep -qx p0 || return 1
}

main mon-stretch-pool-create-multi-zone-km "$@"
