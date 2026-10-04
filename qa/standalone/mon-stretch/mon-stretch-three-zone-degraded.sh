#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7303" # git grep '\<7303\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7304" # git grep '\<7304\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7305" # git grep '\<7305\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# A 3-zone stretch EC pool must again need all three zones once the cluster
# leaves degraded stretch mode.
function TEST_three_zone_ec_pool_after_degraded_stretch_mode() {
    local dir=$1

    run_mon $dir a --public-addr $CEPH_MON_A || return 1
    run_mon $dir b --public-addr $CEPH_MON_B || return 1
    run_mon $dir c --public-addr $CEPH_MON_C || return 1
    wait_for_quorum 300 3 || return 1
    run_mgr $dir x || return 1
    for osd in 0 1 2 3 4 5 6 7 8; do
        run_osd $dir $osd || return 1
    done

    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=arbiter || return 1
    for dc in dc1 dc2; do
        ceph osd crush add-bucket $dc datacenter || return 1
        ceph osd crush move $dc root=default || return 1
    done
    # osd.6-8 wait outside the datacenters for the third zone
    ceph osd crush add-bucket spare root || return 1
    for osd in 0 1 2 3 4 5 6 7 8; do
        local loc=datacenter=dc$((osd / 3 + 1))
        [ $osd -ge 6 ] && loc=root=spare
        ceph osd crush add-bucket host$osd host || return 1
        ceph osd crush move host$osd $loc || return 1
        ceph osd crush set osd.$osd 1.0 host=host$osd || return 1
    done
    ceph config set osd osd_crush_update_on_start false || return 1

    # a 2-zone pool puts the cluster in stretch mode
    ceph osd pool create data1 erasure --num-zones 2 --k 2 --m 1 || return 1
    wait_for_clean || return 1

    ceph osd crush add-bucket dc3 datacenter || return 1
    ceph osd crush move dc3 root=default || return 1
    for osd in 6 7 8; do
        ceph osd crush move host$osd datacenter=dc3 || return 1
    done
    ceph osd pool create data0 erasure --num-zones 3 --k 2 --m 1 || return 1
    local rule=$(ceph osd pool get data0 crush_rule -f json | jq -r .crush_rule)
    ceph osd pool stretch set data0 3 3 datacenter $rule 9 2 || return 1
    # data1's rule now spans three datacenters, which a 2-zone pool cannot use
    ceph osd pool delete data1 data1 --yes-i-really-really-mean-it || return 1
    wait_for_clean || return 1

    kill_daemons $dir KILL mon.b || return 1
    for osd in 3 4 5; do
        kill_daemons $dir KILL osd.$osd || return 1
    done
    ceph osd down osd.3 osd.4 osd.5
    wait_for_stretch_state 1 0 || return 1

    restart_mon $dir b --public-addr $CEPH_MON_B || return 1
    wait_for_quorum 300 3 || return 1
    for osd in 3 4 5; do
        activate_osd $dir $osd || return 1
    done
    wait_for_stretch_state 0 0 || return 1
    ceph osd pool ls detail
    test "$(pool_field data0 peering_crush_bucket_count)" == 3 || return 1
    wait_for_clean || return 1
}

main mon-stretch-three-zone-degraded "$@"
