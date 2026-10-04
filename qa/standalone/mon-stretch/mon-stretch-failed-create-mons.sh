#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7320" # git grep '\<7320\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7321" # git grep '\<7321\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7322" # git grep '\<7322\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# A stretch pool create that fails must not leave the monitors in stretch mode
function TEST_failed_stretch_pool_create_leaves_mons() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    for osd in 0 1 2 3 4 5; do
        run_osd $dir $osd || return 1
    done
    ceph config set osd osd_crush_update_on_start false || return 1
    for dc in dc1 dc2; do
        ceph osd crush add-bucket $dc datacenter || return 1
        ceph osd crush move $dc root=default || return 1
    done
    # dc2 weighs twice as much as dc1
    for osd in 0 1 2 3 4 5; do
        ceph osd crush add-bucket host$osd host || return 1
        ceph osd crush move host$osd datacenter=dc$((osd / 3 + 1)) || return 1
        ceph osd crush set osd.$osd $((osd / 3 + 1)).0 host=host$osd || return 1
    done
    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=arbiter || return 1

    ! ceph osd pool create data0 erasure --num-zones 2 --k 2 --m 1 || return 1
    sleep 5
    test "$(ceph mon dump -f json | jq .stretch_mode)" = false || return 1
    test "$(ceph mon dump -f json | jq -r .tiebreaker_mon)" = "" || return 1
}

main mon-stretch-failed-create-mons "$@"
