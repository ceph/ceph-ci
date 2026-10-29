#!/usr/bin/env bash
#
# Pool migration of pools over zones: ceph osd pool migrate keeps or changes
# num_zones as ceph osd pool create gives it, and the pools of a migration,
# and the source pools that finished migrations leave, can change num_zones
# and enter and leave stretch mode while the data stays readable.

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh
source $CEPH_ROOT/qa/standalone/mon-stretch/mon-stretch-helpers.sh

function run() {
    export CEPH_MON_A="127.0.0.1:7391" # git grep '\<7391\>' : there must be only one
    export CEPH_MON_B="127.0.0.1:7392" # git grep '\<7392\>' : there must be only one
    export CEPH_MON_C="127.0.0.1:7393" # git grep '\<7393\>' : there must be only one
    export CEPH_MON="$CEPH_MON_A,$CEPH_MON_B,$CEPH_MON_C"
    run_stretch_tests "$@"
}

# a field of a pool, including the source pools that ceph osd pool ls hides
function dump_field() {
    ceph osd dump -f json | jq -r --arg p $1 ".pools[]|select(.pool_name==\$p)|.$2"
}

function profile_of() {
    ceph osd pool get $1 erasure_code_profile -f json | jq -r .erasure_code_profile
}

function setup_migration() {
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
    ceph osd set-require-min-compat-client umbrella || return 1
    ceph osd pool set noautoscale || return 1
}

# migrate one PG at a time, slowly, so that a test can act on a migration in
# progress; fast undoes it
function slow_migration() {
    ceph config set mon mon_pool_migration_max_pg_ratio 0.01 || return 1
    ceph tell 'osd.*' config set osd_recovery_sleep 1 || return 1
}

function fast_migration() {
    ceph config set mon mon_pool_migration_max_pg_ratio 1 || return 1
    ceph tell 'osd.*' config set osd_recovery_sleep 0 || return 1
}

# write count objects named prefix<i> to pool, keeping what was written
function write_objects() {
    local dir=$1 pool=$2 prefix=$3 count=$4 i
    for i in $(seq 0 $((count - 1))); do
        dd if=/dev/urandom of=$dir/$prefix$i bs=4k count=$((1 + i % 16)) status=none
        rados -p $pool put $prefix$i $dir/$prefix$i || return 1
    done
}

function check_objects() {
    local dir=$1 pool=$2 prefix=$3 count=$4 i
    for i in $(seq 0 $((count - 1))); do
        timeout 120 rados -p $pool get $prefix$i $dir/got || return 1
        cmp $dir/got $dir/$prefix$i || return 1
    done
}

function migration_done() {
    ceph osd dump -f json |
        jq -e --arg p $1 '.pools[]|select(.pool_name==$p)|has("migration_src")|not' > /dev/null
}

function wait_for_migration() {
    local pool=$1 i
    for i in $(seq 1 300); do
        migration_done $pool && return 0
        sleep 2
    done
    ceph osd pool ls detail --show-all
    return 1
}

# a replicated rule that takes 2 hosts in each of dc1 and dc2
function add_stretch_rule() {
    local dir=$1
    ceph osd getcrushmap > $dir/crushmap || return 1
    crushtool --decompile $dir/crushmap > $dir/crushmap.txt || return 1
    sed 's/^# end crush map$//' $dir/crushmap.txt > $dir/crushmap_modified.txt || return 1
    cat >> $dir/crushmap_modified.txt << EOF
rule stretch_rule {
        id 1
        type replicated
        step take dc1
        step chooseleaf firstn 2 type host
        step emit
        step take dc2
        step chooseleaf firstn 2 type host
        step emit
}

# end crush map
EOF
    crushtool --compile $dir/crushmap_modified.txt -o $dir/crushmap.bin || return 1
    ceph osd setcrushmap -i $dir/crushmap.bin || return 1
}

function TEST_two_zone_ec_copy() {
    local dir=$1
    two_zone_cluster $dir || return 1
    setup_migration || return 1
    ceph osd pool create ec 8 8 erasure --num_zones 2 --k 2 --m 1 || return 1
    wait_for_clean || return 1
    write_objects $dir ec obj 40 || return 1

    ceph osd pool migrate ec --inherit-params || return 1
    test "$(dump_field ec num_zones)" = 2 || return 1
    test "$(dump_field ec size)" = 6 || return 1
    test "$(dump_field ec pg_num)" = 8 || return 1
    check_objects $dir ec obj 40 || return 1
    wait_for_migration ec || return 1
    wait_for_clean || return 1
    check_objects $dir ec obj 40 || return 1
}

# The same k and m with another failure domain: the target's profile takes
# the name of the source's, which is renamed first
function TEST_two_zone_ec_to_one_zone() {
    local dir=$1
    two_zone_cluster $dir || return 1
    setup_migration || return 1
    ceph osd pool create ec 8 8 erasure --num_zones 2 --k 2 --m 1 || return 1
    wait_for_clean || return 1
    write_objects $dir ec obj 40 || return 1
    local before=$(ceph osd erasure-code-profile get ec-k2-m1 -f json | jq -cS .)

    ceph osd pool migrate ec --pool_type erasure --k 2 --m 1 --num_zones 1 \
        --osd_failure_domain osd --enable_ec_optimizations || return 1
    test "$(dump_field ec num_zones)" = 1 || return 1
    test "$(dump_field ec size)" = 3 || return 1
    test "$(profile_of ec)" = ec-k2-m1 || return 1
    test "$(profile_of .migrate-1)" = ec-k2-m1.migrate-1 || return 1
    test "$(ceph osd erasure-code-profile get ec-k2-m1.migrate-1 -f json | jq -cS .)" = "$before" || return 1
    check_objects $dir ec obj 40 || return 1
    wait_for_migration ec || return 1
    wait_for_clean || return 1
    check_objects $dir ec obj 40 || return 1
}

function TEST_one_zone_ec_to_two_zones() {
    local dir=$1
    two_zone_cluster $dir || return 1
    setup_migration || return 1
    ceph osd pool create ec 8 8 erasure --k 3 --m 1 --enable_ec_optimizations || return 1
    wait_for_clean || return 1
    write_objects $dir ec obj 40 || return 1

    ceph osd pool migrate ec --pool_type erasure --k 2 --m 1 --num_zones 2 \
        --enable_ec_optimizations || return 1
    test "$(dump_field ec num_zones)" = 2 || return 1
    test "$(dump_field ec size)" = 6 || return 1
    test "$(profile_of ec)" = ec-k2-m1 || return 1
    check_objects $dir ec obj 40 || return 1
    wait_for_migration ec || return 1
    wait_for_clean || return 1
    check_objects $dir ec obj 40 || return 1
}

# The first pool over two zones turns on stretch mode, and the election that
# follows makes the monitors run the migration again
function TEST_one_zone_replicated_to_two_zones() {
    local dir=$1
    two_zone_cluster $dir || return 1
    setup_migration || return 1
    ceph osd pool create rep 8 8 replicated || return 1
    wait_for_clean || return 1
    write_objects $dir rep obj 40 || return 1

    ceph osd pool migrate rep --pool_type replicated --num_zones 2 --replica 2 || return 1
    test "$(dump_field rep num_zones)" = 2 || return 1
    test "$(dump_field rep size)" = 4 || return 1
    check_objects $dir rep obj 40 || return 1
    wait_for_migration rep || return 1
    wait_for_clean || return 1
    check_objects $dir rep obj 40 || return 1
}

# a copy over two zones, then a migration of the copy to one zone
function TEST_two_zone_replicated() {
    local dir=$1
    two_zone_cluster $dir || return 1
    setup_migration || return 1
    ceph osd pool create rep 8 8 replicated --num_zones 2 --replica 2 || return 1
    wait_for_clean || return 1
    write_objects $dir rep obj 40 || return 1

    ceph osd pool migrate rep --inherit-params || return 1
    test "$(dump_field rep num_zones)" = 2 || return 1
    test "$(dump_field rep size)" = 4 || return 1
    wait_for_migration rep || return 1
    wait_for_clean || return 1
    check_objects $dir rep obj 40 || return 1

    ceph osd pool migrate rep --pool_type replicated --num_zones 1 --size 3 || return 1
    test "$(dump_field rep num_zones)" = 1 || return 1
    test "$(dump_field rep size)" = 3 || return 1
    check_objects $dir rep obj 40 || return 1
    wait_for_migration rep || return 1
    wait_for_clean || return 1
    check_objects $dir rep obj 40 || return 1
}

# num_zones changes for the target and the source of a migration in progress,
# and for the source pool that the finished migration leaves
function TEST_num_zones_during_migration() {
    local dir=$1
    two_zone_cluster $dir || return 1
    setup_migration || return 1
    ceph osd pool create ec 16 16 erasure --k 2 --m 1 --enable_ec_optimizations || return 1
    wait_for_clean || return 1
    write_objects $dir ec obj 60 || return 1

    slow_migration || return 1
    ceph osd pool migrate ec 32 32 --pool_type erasure --k 2 --m 1 \
        --enable_ec_optimizations || return 1
    ! migration_done ec || return 1
    ceph osd pool set ec num_zones 2 --zone_failure_domain datacenter || return 1
    ceph osd pool set .migrate-1 num_zones 2 --zone_failure_domain datacenter || return 1
    test "$(dump_field ec size)" = 6 || return 1
    test "$(dump_field .migrate-1 size)" = 6 || return 1
    ! migration_done ec || return 1
    check_objects $dir ec obj 60 || return 1

    fast_migration || return 1
    wait_for_migration ec || return 1
    wait_for_clean || return 1
    check_objects $dir ec obj 60 || return 1

    ceph osd pool set .migrate-1 num_zones 1 --yes-i-really-mean-it || return 1
    ceph osd pool set ec num_zones 1 --yes-i-really-mean-it || return 1
    wait_for_clean || return 1
    check_objects $dir ec obj 60 || return 1
}

# Stretch mode turns on while a pool migrates, a migration in stretch mode
# takes the stretch defaults as a new pool does, and stretch mode turns off
# with the source pools of finished migrations
function TEST_stretch_mode_and_migration() {
    local dir=$1
    two_zone_cluster $dir || return 1
    add_stretch_rule $dir || return 1
    setup_migration || return 1
    ceph osd pool create rep 16 16 replicated || return 1
    wait_for_clean || return 1
    write_objects $dir rep obj 60 || return 1

    slow_migration || return 1
    ceph osd pool migrate rep --inherit-params || return 1
    ! migration_done rep || return 1
    ceph mon enable_stretch_mode c stretch_rule datacenter || return 1
    for pool in rep .migrate-1; do
        test "$(dump_field $pool num_zones)" = 2 || return 1
        test "$(dump_field $pool size)" = 4 || return 1
    done
    check_objects $dir rep obj 60 || return 1
    fast_migration || return 1
    wait_for_migration rep || return 1
    wait_for_clean || return 1
    check_objects $dir rep obj 60 || return 1

    ceph osd pool migrate rep --pool_type replicated || return 1
    test "$(dump_field rep num_zones)" = 2 || return 1
    test "$(dump_field rep size)" = 4 || return 1
    wait_for_migration rep || return 1
    wait_for_clean || return 1
    check_objects $dir rep obj 60 || return 1

    ceph mon disable_stretch_mode --yes-i-really-mean-it || return 1
    for pool in rep .migrate-1 .migrate-2; do
        test "$(dump_field $pool num_zones)" = 1 || return 1
    done
    test "$(ceph osd dump -f json | jq .stretch_mode.stretch_mode_enabled)" = false || return 1
    wait_for_clean || return 1
    check_objects $dir rep obj 60 || return 1
}

main mon-stretch-pool-migrate "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && ../qa/run-standalone.sh mon-stretch-pool-migrate.sh"
# End:
