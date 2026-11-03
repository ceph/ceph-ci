#!/usr/bin/env bash

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON="127.0.0.1:7359" # git grep '\<7359\>' : there must be only one
    run_stretch_tests "$@"
}

# A single-zone pool builds a rule from the placement options when they differ
# from the cluster default, so --root, --osd_failure_domain and --class are
# honoured; only --zone_failure_domain, meaningless for one zone, is refused.
function TEST_single_zone_replicated_placement_options() {
    local dir=$1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    run_osd $dir 3 || return 1
    # run_osd binds each OSD to 'hdd', so drop that before setting 'ssd'
    ceph osd crush rm-device-class osd.0 osd.1 osd.2 osd.3 || return 1
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 osd.3 || return 1
    # a non-default root to exercise --root (default is the stock root)
    ceph osd crush add-bucket altroot root || return 1
    ceph osd crush add-bucket althost host || return 1
    ceph osd crush move althost root=altroot || return 1
    ceph osd crush create-or-move osd.3 1.0 root=altroot host=althost || return 1

    # --zone_failure_domain is meaningless for one zone and is still refused
    expect_failure $dir \
        "zone_failure_domain requires num_zones > 1 for a replicated pool" \
        ceph osd pool create rep 8 replicated --zone_failure_domain datacenter \
        || return 1
    ! ceph osd pool ls | grep -qx rep || return 1

    # --root, --osd_failure_domain and --class now generate a <pool>-single-zone rule
    ceph osd pool create rep_root 8 replicated --root altroot || return 1
    ceph osd crush rule dump rep_root-single-zone | grep -q altroot || return 1
    ceph osd pool create rep_osd 8 replicated --osd_failure_domain osd || return 1
    ceph osd crush rule dump rep_osd-single-zone | grep -q '"type": "osd"' || return 1
    ceph osd pool create rep_cls 8 replicated --class ssd || return 1
    ceph osd crush rule dump rep_cls-single-zone | grep -q '~ssd' || return 1

    # a plain pool keeps the default replicated rule (no generated rule)
    ceph osd pool create plain 8 replicated --num_zones 1 || return 1
    ! ceph osd crush rule ls | grep -qx plain-single-zone || return 1

    # --no-class overrides an inherited default class; --class + --no-class conflict
    ceph config set mon osd_pool_default_class ssd || return 1
    ceph osd pool create rep_nc 8 replicated --no-class || return 1
    local nc=$(ceph osd pool get rep_nc crush_rule | awk '{print $2}')
    ! ceph osd crush rule dump "$nc" | grep -q '~ssd' || return 1
    ceph config set mon osd_pool_default_class "" || return 1
    expect_failure $dir "cannot specify both --class and --no-class" \
        ceph osd pool create rep_both 8 replicated --class ssd --no-class || return 1

    # with a named rule the options conflict with the rule
    expect_failure $dir "cannot specify both crush rule and crush parameters" \
        ceph osd pool create rep2 8 8 replicated replicated_rule --root altroot \
        || return 1
}

# A single-zone erasure coded pool takes --class, and osd_pool_default_class
# reaches an EC pool that names no profile; a named profile's class still wins.
function TEST_single_zone_erasure_placement_options() {
    local dir=$1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1
    ceph osd crush rm-device-class osd.0 osd.1 osd.2 || return 1
    ceph osd crush set-device-class ssd osd.0 osd.1 osd.2 || return 1

    ceph osd pool create ec_cls 8 8 erasure --class ssd || return 1
    local prof=$(ceph osd pool get ec_cls erasure_code_profile | awk '{print $2}')
    ceph osd erasure-code-profile get "$prof" | grep -q 'crush-device-class=ssd' \
        || return 1

    ceph config set mon osd_pool_default_class ssd || return 1
    ceph osd pool create ec_def 8 8 erasure || return 1
    prof=$(ceph osd pool get ec_def erasure_code_profile | awk '{print $2}')
    ceph osd erasure-code-profile get "$prof" | grep -q 'crush-device-class=ssd' \
        || return 1
    ceph config set mon osd_pool_default_class "" || return 1
}

main mon-stretch-replicated-placement-options "$@"
