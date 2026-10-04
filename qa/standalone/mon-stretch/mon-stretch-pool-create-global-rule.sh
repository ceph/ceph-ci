#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7317" # git grep '\<7317\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7318" # git grep '\<7318\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7319" # git grep '\<7319\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# As on main, a replicated pool created without a rule in stretch mode uses
# the stretch rule rather than a new single-zone rule.
function TEST_stretch_mode_pool_create_without_rule() {
    local dir=$1
    run_mon $dir a --public-addr=$CEPH_MON_A || return 1
    run_mon $dir b --public-addr=$CEPH_MON_B || return 1
    run_mon $dir c --public-addr=$CEPH_MON_C || return 1
    for osd in 0 1 2 3; do
        run_osd $dir $osd || return 1
    done
    for dc in dc1 dc2; do
        ceph osd crush add-bucket $dc datacenter || return 1
        ceph osd crush move $dc root=default || return 1
    done
    for osd in 0 1 2 3; do
        ceph osd crush add-bucket host$osd host || return 1
        ceph osd crush move host$osd datacenter=dc$((osd / 2 + 1)) || return 1
        ceph osd crush set osd.$osd 1.0 host=host$osd || return 1
    done
    ceph mon set_location a datacenter=dc1 || return 1
    ceph mon set_location b datacenter=dc2 || return 1
    ceph mon set_location c datacenter=arbiter || return 1
    ceph osd crush rule create-stretch-replicated --rule-name=stretch_rule || return 1
    ceph osd pool create stretched 8 8 replicated stretch_rule || return 1
    ceph mon set election_strategy connectivity || return 1
    # the command can be resent after the election it causes
    ceph mon enable_stretch_mode c stretch_rule datacenter
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = true || return 1

    ceph osd pool create plain 8 || return 1
    ceph osd pool get plain crush_rule | grep -w stretch_rule || return 1
    # such a pool gets the stretch rule, so options that would build a rule
    # for it are refused rather than ignored
    for opt in "--root default" "--zone_failure_domain datacenter" "--osd_failure_domain host" "--class hdd"; do
        ceph osd pool create plain2 8 $opt 2>&1 | grep "without num_zones in stretch mode" || return 1
    done
    ! ceph osd pool ls | grep -qx plain2 || return 1
}

main mon-stretch-pool-create-global-rule "$@"
