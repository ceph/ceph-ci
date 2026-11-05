#!/usr/bin/env bash
#
# A pool migration finishes when its source PG loses its local reservation
# while a copy is in flight and gets it back before that copy finishes.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7169" # git grep '\<7169\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function wait_for_migration() {
    local pool=$1
    for i in $(seq 1 120) ; do
        ceph osd dump --format=json | \
            jq -e ".pools[] | select(.pool_name == \"$pool\") | has(\"migration_src\") | not" > /dev/null && return 0
        sleep 1
    done
    ceph osd pool ls detail
    return 1
}

function wait_for_log() {
    local log=$1
    local pattern=$2
    for i in $(seq 1 60) ; do
        grep -q "$pattern" $log && return 0
        sleep 1
    done
    return 1
}

function wait_for_objecter_op() {
    local id=$1
    for i in $(seq 1 60) ; do
        test "$(CEPH_ARGS='' ceph --admin-daemon $(get_asok_path osd.$id) objecter_requests | jq '.ops | length')" -gt 0 && return 0
        sleep 1
    done
    return 1
}

function TEST_defer_with_copy_in_flight() {
    local dir=$1
    local osd_args="--osd_mclock_override_recovery_settings=true --osd_recovery_max_active=1"

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in 0 1 ; do
        run_osd $dir $id $osd_args || return 1
    done
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
    ceph osd set-require-min-compat-client umbrella || return 1

    # the source PG is on osd.0 and the target PG on osd.1
    for id in 0 1 ; do
        ceph osd crush rm-device-class osd.$id || return 1
        ceph osd crush set-device-class class$id osd.$id || return 1
        ceph osd crush rule create-replicated rule$id default osd class$id || return 1
    done
    ceph osd pool create src 1 1 replicated rule0 || return 1
    ceph osd pool set src size 1 --yes-i-really-mean-it || return 1
    wait_for_clean || return 1
    echo hello > $dir/obj
    for i in $(seq 1 4) ; do
        rados -p src put obj$i $dir/obj || return 1
    done

    # the source delete of the first object copied holds the PG's only
    # recovery slot for 10 seconds, so the next copy starts after the cluster
    # is paused and stays in flight
    ceph tell osd.0 config set osd_debug_pool_migration_hold_source_delete 10 || return 1
    ceph osd pool migrate src --pool_type replicated --pg_num 1 --size 1 --rule rule1 --yes-i-really-mean-it || return 1
    wait_for_log $dir/osd.0.log "injecting a read" || return 1
    ceph osd pause || return 1
    wait_for_objecter_op 0 || return 1
    test $(grep -c "pool migration copying" $dir/osd.0.log) = 2 || return 1

    # the PG loses its local reservation and gets it back, and its recovery
    # runs while the copy drains
    ceph tell osd.0 config set osd_recovery_max_active 2 || return 1
    ceph tell osd.0 config set osd_min_recovery_priority 253 || return 1
    wait_for_log $dir/osd.0.log "waiting for 1 in-flight migrations to drain" || return 1
    ceph tell osd.0 config set osd_min_recovery_priority 0 || return 1
    wait_for_log $dir/osd.0.log "quiescing (reason=3), skipping new migration work" || return 1
    ceph osd unpause || return 1

    wait_for_migration src || return 1
    for i in $(seq 1 4) ; do
        rados -p src get obj$i $dir/got || return 1
        cmp $dir/obj $dir/got || return 1
    done
}

main osd-pool-migration-defer-in-flight "$@"
