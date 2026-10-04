#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7306" # git grep '\<7306\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7307" # git grep '\<7307\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7308" # git grep '\<7308\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# osd pool stretch set in degraded stretch mode must leave the pool peering in
# the surviving zone, as the other stretch pools do.
function TEST_stretch_set_in_degraded_stretch_mode() {
    local dir=$1

    ec_stretch_cluster_without_dc2 $dir || return 1
    local rule=$(ceph osd pool get data0 crush_rule -f json | jq -r .crush_rule)
    ceph osd pool stretch set data0 2 2 datacenter $rule 6 2 || return 1
    test "$(pool_field data0 peering_crush_bucket_count)" == 1 || return 1
    ceph osd getmap -o $dir/osdmap || return 1
    timeout 120 rados -p data0 put obj $dir/osdmap || return 1

    restart_mon $dir b --public-addr $CEPH_MON_B || return 1
    wait_for_quorum 300 3 || return 1
    for osd in 3 4 5; do
        activate_osd $dir $osd || return 1
    done
    wait_for_stretch_state 0 0 || return 1
    test "$(pool_field data0 peering_crush_bucket_count)" == 2 || return 1
    test "$(pool_field data0 peering_crush_bucket_mandatory_member)" == 2147483647 || return 1
}

main mon-stretch-stretch-set-degraded "$@"
