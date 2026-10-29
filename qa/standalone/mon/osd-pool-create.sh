#!/usr/bin/env bash
#
# Copyright (C) 2013, 2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7105" # git grep '\<7105\>' : there must be only one
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "
    export CEPH_ARGS

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

# Before http://tracker.ceph.com/issues/8307 the invalid profile was created
function TEST_erasure_invalid_profile() {
    local dir=$1
    run_mon $dir a || return 1
    local poolname=pool_erasure
    local notaprofile=not-a-valid-erasure-code-profile
    ! ceph osd pool create $poolname 12 12 erasure $notaprofile || return 1
    ! ceph osd erasure-code-profile ls | grep $notaprofile || return 1
}

function TEST_erasure_crush_rule() {
    local dir=$1
    run_mon $dir a || return 1
    #
    # choose the crush rule used with an erasure coded pool
    #
    local crush_rule=myrule
    ! ceph osd crush rule ls | grep $crush_rule || return 1
    ceph osd crush rule create-erasure $crush_rule
    ceph osd crush rule ls | grep $crush_rule
    local poolname
    poolname=pool_erasure1
    ! ceph --format json osd dump | grep '"crush_rule":1' || return 1
    ceph osd pool create $poolname 12 12 erasure default $crush_rule
    ceph --format json osd dump | grep '"crush_rule":1' || return 1
    #
    # a crush rule by the name of the pool is implicitly created
    #
    poolname=pool_erasure2
    ceph osd erasure-code-profile set myprofile
    ceph osd pool create $poolname 12 12 erasure myprofile
    ceph osd crush rule ls | grep $poolname || return 1
    #
    # a non existent crush rule given in argument is an error
    # http://tracker.ceph.com/issues/9304
    #
    poolname=pool_erasure3
    ! ceph osd pool create $poolname 12 12 erasure myprofile INVALIDRULE || return 1
}

function TEST_erasure_code_profile_default() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd erasure-code-profile rm default || return 1
    ! ceph osd erasure-code-profile ls | grep default || return 1
    ceph osd pool create $poolname 12 12 erasure default
    ceph osd erasure-code-profile ls | grep default || return 1
}

function TEST_erasure_crush_stripe_unit() {
    local dir=$1
    # the default stripe unit is used to initialize the pool
    run_mon $dir a --public-addr $CEPH_MON
    stripe_unit=$(ceph-conf --show-config-value osd_pool_erasure_code_stripe_unit)
    eval local $(ceph osd erasure-code-profile get myprofile | grep k=)
    stripe_width = $((stripe_unit * k))
    ceph osd pool create pool_erasure 12 12 erasure
    ceph --format json osd dump | tee $dir/osd.json
    grep '"stripe_width":'$stripe_width $dir/osd.json > /dev/null || return 1
}

function TEST_erasure_crush_stripe_unit_padded() {
    local dir=$1
    # setting osd_pool_erasure_code_stripe_unit modifies the stripe_width
    # and it is padded as required by the default plugin
    profile+=" plugin=isa"
    profile+=" technique=reed_sol_van"
    k=4
    profile+=" k=$k"
    profile+=" m=2"
    actual_stripe_unit=2048
    desired_stripe_unit=$((actual_stripe_unit - 1))
    actual_stripe_width=$((actual_stripe_unit * k))
    run_mon $dir a \
        --osd_pool_erasure_code_stripe_unit $desired_stripe_unit \
        --osd_pool_default_erasure_code_profile "$profile" || return 1
    ceph osd pool create pool_erasure 12 12 erasure
    ceph osd dump | tee $dir/osd.json
    grep "stripe_width $actual_stripe_width" $dir/osd.json > /dev/null || return 1
}

function TEST_erasure_code_pool() {
    local dir=$1
    run_mon $dir a || return 1
    ceph --format json osd dump > $dir/osd.json
    local expected='"erasure_code_profile":"default"'
    ! grep "$expected" $dir/osd.json || return 1
    ceph osd pool create erasurecodes 12 12 erasure
    ceph --format json osd dump | tee $dir/osd.json
    grep "$expected" $dir/osd.json > /dev/null || return 1

    ceph osd pool create erasurecodes 12 12 erasure 2>&1 | \
        grep 'already exists' || return 1
    ceph osd pool create erasurecodes 12 12 2>&1 | \
        grep 'cannot change to type replicated' || return 1
}

function TEST_replicated_pool_with_rule() {
    local dir=$1
    run_mon $dir a
    local rule=rule0
    local root=host1
    ceph osd crush add-bucket $root host
    local failure_domain=osd
    local poolname=mypool
    ceph osd crush rule create-simple $rule $root $failure_domain || return 1
    ceph osd crush rule ls | grep $rule
    ceph osd pool create $poolname 12 12 replicated $rule || return 1
    rule_id=`ceph osd crush rule dump $rule | grep "rule_id" | awk -F[' ':,] '{print $4}'`
    ceph osd pool get $poolname crush_rule  2>&1 | \
        grep "crush_rule: $rule_id" || return 1
    #non-existent crush rule
    ceph osd pool create newpool 12 12 replicated non-existent 2>&1 | \
        grep "doesn't exist" || return 1
}

function TEST_erasure_code_pool_lrc() {
    local dir=$1
    run_mon $dir a || return 1

    ceph osd erasure-code-profile set LRCprofile \
             plugin=lrc \
             mapping=DD_ \
             layers='[ [ "DDc", "" ] ]' || return 1

    ceph --format json osd dump > $dir/osd.json
    local expected='"erasure_code_profile":"LRCprofile"'
    local poolname=erasurecodes
    ! grep "$expected" $dir/osd.json || return 1
    ceph osd pool create $poolname 12 12 erasure LRCprofile
    ceph --format json osd dump | tee $dir/osd.json
    grep "$expected" $dir/osd.json > /dev/null || return 1
    ceph osd crush rule ls | grep $poolname || return 1
}

function TEST_replicated_pool() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd pool create replicated 12 12 replicated replicated_rule || return 1
    ceph osd pool create replicated 12 12 replicated replicated_rule 2>&1 | \
        grep 'already exists' || return 1
    # default is replicated
    ceph osd pool create replicated1 12 12 || return 1
    # default is replicated, pgp_num = pg_num
    ceph osd pool create replicated2 12 || return 1
    ceph osd pool create replicated 12 12 erasure 2>&1 | \
        grep 'cannot change to type erasure' || return 1
}

# A create resent before the first one is committed replies once the pool
# exists
function TEST_replicated_pool_create_resent() {
    local dir=$1
    run_mon $dir a || return 1
    ceph tell mon.\* config set paxos_propose_interval 5 || return 1
    # an OSDMap change just committed delays the next proposal
    ceph osd set noout || return 1

    timeout 60 ceph osd pool create resent 4 4 replicated &
    local pid=$!
    sleep 1
    timeout 60 ceph osd pool create resent 4 4 replicated || return 1
    ceph osd pool get resent size || return 1
    wait $pid || return 1
}

function TEST_no_pool_delete() {
    local dir=$1
    run_mon $dir a || return 1
    ceph osd pool create foo 1 || return 1
    ceph tell mon.a injectargs -- --no-mon-allow-pool-delete || return 1
    ! ceph osd pool delete foo foo --yes-i-really-really-mean-it || return 1
    ceph tell mon.a injectargs -- --mon-allow-pool-delete || return 1
    ceph osd pool delete foo foo --yes-i-really-really-mean-it || return 1
}

function TEST_utf8_cli() {
    local dir=$1
    run_mon $dir a || return 1
    # Hopefully it's safe to include literal UTF-8 characters to test
    # the fix for http://tracker.ceph.com/issues/7387.  If it turns out
    # to not be OK (when is the default encoding *not* UTF-8?), maybe
    # the character '黄' can be replaced with the escape $'\xe9\xbb\x84'
    OLDLANG="$LANG"
    export LANG=en_US.UTF-8
    ceph osd pool create 黄 16 || return 1
    ceph osd lspools 2>&1 | \
        grep "黄" || return 1
    ceph -f json-pretty osd dump | \
        python3 -c "import json; import sys; json.load(sys.stdin)" || return 1
    ceph osd pool delete 黄 黄 --yes-i-really-really-mean-it
    export LANG="$OLDLANG"
}

function check_pool_priority() {
    local dir=$1
    shift
    local pools=$1
    shift
    local spread="$1"
    shift
    local results="$1"

    setup $dir || return 1

    EXTRA_OPTS="--debug_allow_any_pool_priority=true"
    export EXTRA_OPTS
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    # Add pool 0 too
    for i in $(seq 0 $pools)
    do
      num=$(expr $i + 1)
      ceph osd pool create test${num} 1 1
    done

    wait_for_clean || return 1
    for i in $(seq 0 $pools)
    do
	num=$(expr $i + 1)
	ceph osd pool set test${num} recovery_priority $(expr $i \* $spread)
    done

    #grep "recovery_priority.*pool set" out/mon.a.log

    bin/ceph osd dump

    # Restart everything so mon converts the priorities
    kill_daemons
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    activate_osd $dir 0 || return 1
    activate_osd $dir 1 || return 1
    activate_osd $dir 2 || return 1
    sleep 5

    grep convert $dir/mon.a.log
    ceph osd dump

    pos=1
    for i in $(ceph osd dump | grep ^pool | sed 's/.*recovery_priority //' | awk '{ print $1 }')
    do
      result=$(echo $results | awk "{ print \$${pos} }")
      # A value of 0 is an unset value so sed/awk gets "pool"
      if test $result = "0"
      then
        result="pool"
      fi
      test "$result" = "$i" || return 1
      pos=$(expr $pos + 1)
    done
}

function TEST_pool_pos_only_prio() {
   local dir=$1
   check_pool_priority $dir 20 5 "0 0 1 1 2 2 3 3 4 4 5 5 6 6 7 7 8 8 9 9 10" || return 1
}

function TEST_pool_neg_only_prio() {
   local dir=$1
   check_pool_priority $dir 20 -5 "0 0 -1 -1 -2 -2 -3 -3 -4 -4 -5 -5 -6 -6 -7 -7 -8 -8 -9 -9 -10" || return 1
}

function TEST_pool_both_prio() {
   local dir=$1
   check_pool_priority $dir 20 "5 - 50" "-10 -9 -8 -7 -6 -5 -4 -3 -2 -1 0 1 2 3 4 5 6 7 8 9 10" || return 1
}

function TEST_pool_both_prio_no_neg() {
   local dir=$1
   check_pool_priority $dir 20 "2 - 4" "-4 -2 0 0 1 1 2 2 3 3 4 5 5 6 6 7 7 8 8 9 10" || return 1
}

function TEST_pool_both_prio_no_pos() {
   local dir=$1
   check_pool_priority $dir 20 "2 - 36" "-10 -9 -8 -8 -7 -7 -6 -6 -5 -5 -4 -3 -3 -2 -2 -1 -1 0 0 2 4" || return 1
}

# A migrate with no options creates the pool that a create with no options
# would; --inherit-params keeps every setting of the source and takes no
# other options
function TEST_pool_migrate_inherit_params() {
  local dir=$1

  run_mon $dir a || return 1
  ceph osd erasure-code-profile set ec-prof plugin=isa k=2 m=1 crush-failure-domain=osd || return 1
  ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
  ceph osd set-require-min-compat-client umbrella || return 1

  ceph osd pool create defaulted 4 4 erasure ec-prof || return 1
  ceph osd pool migrate defaulted || return 1

  ceph config set mon osd_pool_default_flag_ec_optimizations true || return 1
  ceph osd pool create inherited 4 4 erasure ec-prof || return 1
  ceph config set mon osd_pool_default_flag_ec_optimizations false || return 1
  ceph osd pool set inherited allow_ec_overwrites true || return 1
  ceph osd pool set inherited min_size 2 || return 1
  ceph osd pool set inherited nodelete true || return 1
  ceph osd pool set inherited noscrub true || return 1
  ceph osd pool set inherited compression_mode aggressive || return 1
  ceph osd pool application enable inherited rbd || return 1
  ceph osd pool set-quota inherited max_objects 1000 || return 1
  expect_failure $dir "cannot be combined" \
    ceph osd pool migrate inherited --inherit-params --size 2 || return 1
  ceph osd pool migrate inherited --inherit-params || return 1

  ceph osd dump --format=json > $dir/osdmap.json || return 1
  # the source's pg_num freeze during the migration is not a setting
  pool_settings() {
    jq -S ".pools[] | select(.pool_name == \"$1\") |
      del(.pool, .pool_name, .create_time, .last_change, .migration_src,
          .migration_target, .migration_root, .migrating_pgs,
          .lowest_migrated_pg, .flags, .pg_autoscale_mode) |
      .flags_names |= (split(\",\") - [\"nopgchange\"] | join(\",\"))" $dir/osdmap.json
  }
  test "$(pool_settings defaulted | jq .type)" = 1 || return 1
  test "$(pool_settings inherited | jq -r .erasure_code_profile)" = ec-prof || return 1
  diff <(pool_settings .migrate-2) <(pool_settings inherited) || return 1
}

# A second migration of a pool prepared in the update of the first must not
# rename the pool again: that left two pools of its name
function TEST_pool_migrate_twice_in_one_update() {
  local dir=$1

  run_mon $dir a || return 1
  ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
  ceph osd set-require-min-compat-client umbrella || return 1
  ceph osd pool create mtwice 4 4 replicated || return 1
  ceph tell mon.\* config set paxos_propose_interval 5 || return 1
  # an OSDMap change just committed delays the next proposal
  ceph osd set noout || return 1

  timeout 60 ceph osd pool migrate mtwice --pool_type replicated &
  local pid=$!
  sleep 1
  timeout 60 ceph osd pool migrate mtwice --pool_type replicated || return 1
  wait $pid || return 1
  ceph osd dump --format=json > $dir/osdmap.json || return 1
  test "$(jq '[.pools[] | select(.pool_name == "mtwice")] | length' $dir/osdmap.json)" = 1 || return 1
  test "$(jq '[.pools[] | select(.pool_name | startswith(".migrate-"))] | length' $dir/osdmap.json)" = 1 || return 1
}

# The monitors run a command again after an election: a migration of a pool
# that is migrating to what it asks for succeeds and changes nothing
function TEST_pool_migrate_again() {
  local dir=$1

  run_mon $dir a || return 1
  ceph osd erasure-code-profile set ec-prof plugin=isa k=2 m=1 crush-failure-domain=osd || return 1
  ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
  ceph osd set-require-min-compat-client umbrella || return 1

  ceph osd pool create again 4 4 replicated || return 1
  local to_ec="--pool_type erasure --erasure_code_profile ec-prof --enable_ec_optimizations"
  ceph osd pool migrate again $to_ec --pg_num 8 || return 1
  ceph osd pool migrate again $to_ec --pg_num 8 || return 1
  expect_failure $dir "ongoing migration" ceph osd pool migrate again $to_ec --pg_num 16 || return 1
  expect_failure $dir "ongoing migration" ceph osd pool migrate again || return 1
  ceph osd pool migrate again --inherit-params || return 1

  ceph osd pool create copied 4 4 erasure ec-prof || return 1
  ceph osd pool migrate copied --inherit-params || return 1
  ceph osd pool migrate copied --inherit-params || return 1
  expect_failure $dir "cannot be combined" \
    ceph osd pool migrate copied --inherit-params --size 2 || return 1
  expect_failure $dir "ongoing migration" \
    ceph osd pool migrate copied --pool_type replicated || return 1

  test "$(ceph osd dump --format=json | jq '[.pools[] | select(.pool_name | startswith(".migrate-"))] | length')" = 2 || return 1
}

function TEST_osd_pool_create_migrate_from() {
  local dir=$1

  setup $dir || return 1
  run_mon $dir a || return 1
  run_mgr $dir x || return 1
  for id in $(seq 0 2); do
    run_osd $dir $id || return 1
  done
  create_rbd_pool || return 1
  wait_for_clean || return 1

  SRC_POOL_NAME="ecpool"
  REPPOOL_NAME="metadatapool"

  #setup the cluster 
  #create an erasure code profile
  ceph osd erasure-code-profile set ec-prof plugin=isa k=2 m=1 crush-failure-domain=osd

  #create an erasure coded pool
  ceph osd pool create $SRC_POOL_NAME erasure ec-prof || return 1
  ceph osd pool set $SRC_POOL_NAME allow_ec_overwrites true || return 1
  #rbd pool init $SRC_POOL_NAME || return 1
  ceph osd pool application enable $SRC_POOL_NAME rbd || return 1

  #create a replicated pool
  ceph osd pool create $REPPOOL_NAME replicated || return 1
  rbd pool init $REPPOOL_NAME || return 1

  #create an rbd image with data pool set to the erasure coded pool
  rbd create -s 10G --data-pool $SRC_POOL_NAME $REPPOOL_NAME/vol0 || return 1

  #write some data to the image
  rbd bench -p $REPPOOL_NAME --image vol0 --io-size 1K --io-threads 1 --io-total 10000K --io-pattern rand --io-type write || return 1

  #pool migration is experimental and needs min compat client umbrella
  ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
  ceph osd set-require-min-compat-client umbrella || return 1

  #disable autoscaling
  ceph osd pool set noautoscale || return 1

  #resize the ec pool to have small number of pgs
  ceph osd pool set $SRC_POOL_NAME pg_num 2 || return 1

  #wait for the merge to finish: a pool cannot start migrating while its
  #pg_num is changing
  local i
  for ((i = 0; i < 600; i++)); do
    test "$(ceph osd dump --format=json | jq ".pools[] | select(.pool_name == \"$SRC_POOL_NAME\") |
      .pg_num == 2 and .pg_num_pending == 2")" = true && break
    sleep 1
  done
  test $i -lt 600 || return 1

  #migrate the pool, the new pool takes over the name and the old pool is
  #renamed to .migrate-1
  ceph osd pool migrate $SRC_POOL_NAME --inherit-params || return 1
  TGT_POOL_NAME=$SRC_POOL_NAME
  SRC_POOL_NAME=".migrate-1"
   
  #reenable autoscaling
  ceph osd pool unset noautoscale || return 1

  #check that changing the pg_num on either pool is blocked
  expect_failure $dir "while the pool is migrating" ceph osd pool set $SRC_POOL_NAME pg_num 10 || return 1
  expect_failure $dir "while the pool is migrating" ceph osd pool set $TGT_POOL_NAME pg_num 10 || return 1

  #check that the daemonServer was blocked from changing the pg_num on the
  #source pool
  #(the plain osd dump does not list the source of a migration)
  SRC_POOL_ID=$(ceph osd dump --format=json | jq ".pools[] | select(.pool_name == \"$SRC_POOL_NAME\") | .pool")
  TGT_POOL_ID=$(ceph osd dump --format=json | jq ".pools[] | select(.pool_name == \"$TGT_POOL_NAME\") | .pool")
  echo "Source pool id: $SRC_POOL_ID"
  echo "Target pool id: $TGT_POOL_ID"
  
  grep "Pool $SRC_POOL_ID has the flag NOPGCHANGE so will be skipped" $dir/mgr.x.log || return 1

  teardown $dir || return 1
}

function pool_field() {
  local pool=$1
  local field=$2
  ceph osd dump --format=json | jq -r ".pools[] | select(.pool_name == \"$pool\") | .$field"
}

# --enable_ec_optimizations must give a pool what osd_pool_default_flag_ec_optimizations does
function TEST_enable_ec_optimizations() {
  local dir=$1

  run_mon $dir a || return 1
  ceph osd erasure-code-profile set ec-prof plugin=isa k=2 m=1 crush-failure-domain=osd || return 1

  ceph config set mon osd_pool_default_flag_ec_optimizations true || return 1
  ceph osd pool create default_opt 4 4 erasure ec-prof || return 1
  ceph config set mon osd_pool_default_flag_ec_optimizations false || return 1
  local stripe_width=$(pool_field default_opt stripe_width)

  ceph osd pool create requested_opt 4 4 erasure ec-prof --enable_ec_optimizations || return 1
  test "$(pool_field requested_opt stripe_width)" = "$stripe_width" || return 1
  pool_field requested_opt flags_names | grep -q split_reads || return 1

  ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
  ceph osd set-require-min-compat-client umbrella || return 1
  ceph osd pool create migrated 4 4 erasure ec-prof || return 1
  ceph osd pool migrate migrated --pool_type erasure --erasure_code_profile ec-prof --enable_ec_optimizations || return 1
  test "$(pool_field migrated stripe_width)" = "$stripe_width" || return 1
  pool_field migrated flags_names | grep -q split_reads || return 1
}

function run_mon_for_pool_migration() {
    local dir=$1
    run_mon $dir a || return 1
    ceph config set global enable_experimental_unrecoverable_data_corrupting_features poolmigration || return 1
    ceph osd set-require-min-compat-client umbrella || return 1
}

function rule_of() {
    ceph osd pool get $1 crush_rule -f json | jq -r .crush_rule
}

function profile_of() {
    ceph osd pool get $1 erasure_code_profile -f json | jq -r .erasure_code_profile
}

# the OSDs create the PGs of a pool only while it has the creating flag
function creating() {
    pool_field $1 flags_names | tr , '\n' | grep -qx creating
}

function TEST_pool_migrate_copies_pool() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd erasure-code-profile set mprofile k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd pool create mec 8 8 erasure mprofile || return 1
    ceph osd pool create mrep 4 4 replicated || return 1
    ceph osd pool set mrep size 2 || return 1

    # --inherit-params takes no pool option, the zone options among them
    expect_failure $dir "cannot be combined" \
        ceph osd pool migrate mec --inherit-params --num_zones 1 || return 1
    ceph osd pool ls | grep -qx mec || return 1
    ! ceph osd pool ls --show-all | grep -q 'migrate-' || return 1

    ceph osd pool migrate mec --inherit-params || return 1
    test "$(pool_field mec type)" = 3 || return 1
    test "$(pool_field mec pg_num)" = 8 || return 1
    test "$(pool_field mec size)" = 3 || return 1
    test "$(pool_field mec num_zones)" = 1 || return 1
    test "$(pool_field .migrate-1 migration_target)" = "$(pool_field mec pool)" || return 1

    ceph osd pool migrate mrep --inherit-params || return 1
    test "$(pool_field mrep type)" = 1 || return 1
    test "$(pool_field mrep pg_num)" = 4 || return 1
    test "$(pool_field mrep size)" = 2 || return 1
}

function TEST_pool_migrate_takes_create_arguments() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd pool create mec erasure --k 2 --m 1 --osd_failure_domain osd || return 1
    ceph osd pool create mrep 4 4 replicated || return 1

    ceph osd pool migrate mec --pool_type erasure --k 4 --m 2 --osd_failure_domain osd \
        --enable_ec_optimizations || return 1
    test "$(profile_of mec)" = mec-k4-m2 || return 1
    test "$(pool_field mec size)" = 6 || return 1

    # a migration that the checks of osd pool create refuse changes nothing
    expect_failure $dir "num_zones must be from 1 to" \
        ceph osd pool migrate mrep --num_zones 3 || return 1
    ceph osd pool create later 4 4 replicated || return 1
    ceph osd pool ls | grep -qx mrep || return 1
    test "$(ceph osd pool ls --show-all | grep -c 'migrate-')" = 1 || return 1
}

function TEST_pool_migrate_gives_target_own_rule_and_profile() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd erasure-code-profile set pcopy k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd pool create mcopy 4 4 erasure pcopy || return 1
    test "$(rule_of mcopy)" = mcopy || return 1
    local rule=$(pool_field mcopy crush_rule)
    local steps=$(ceph osd crush rule dump mcopy -f json | jq -c 'del(.rule_id, .rule_name)')

    # the source keeps its rule and profile under new names, and the target
    # has copies named after it
    ceph osd pool migrate mcopy --inherit-params || return 1
    creating mcopy || return 1
    test "$(pool_field .migrate-1 crush_rule)" = $rule || return 1
    test "$(rule_of .migrate-1)" = mcopy.migrate-1 || return 1
    test "$(profile_of .migrate-1)" = pcopy.migrate-1 || return 1
    test "$(pool_field mcopy crush_rule)" != $rule || return 1
    test "$(rule_of mcopy)" = mcopy || return 1
    test "$(profile_of mcopy)" = mcopy-k2-m1 || return 1
    test "$(ceph osd crush rule dump mcopy -f json | jq -c 'del(.rule_id, .rule_name)')" = "$steps" || return 1
    test "$(ceph osd erasure-code-profile get mcopy-k2-m1 -f json | jq -cS .)" = \
        "$(ceph osd erasure-code-profile get pcopy.migrate-1 -f json | jq -cS .)" || return 1
    ! ceph osd erasure-code-profile ls | grep -qx pcopy || return 1

    # a new profile needs a new rule
    ceph osd pool create mnew erasure --k 2 --m 1 --osd_failure_domain osd || return 1
    rule=$(pool_field mnew crush_rule)
    ceph osd pool migrate mnew --pool_type erasure --k 3 --m 1 --osd_failure_domain osd \
        --enable_ec_optimizations || return 1
    creating mnew || return 1
    test "$(rule_of mnew)" = mnew || return 1
    test "$(profile_of mnew)" = mnew-k3-m1 || return 1
    test "$(pool_field mnew crush_rule)" != $rule || return 1
    test "$(pool_field .migrate-2 crush_rule)" = $rule || return 1
    test "$(rule_of .migrate-2)" = mnew.migrate-2 || return 1
    test "$(profile_of .migrate-2)" = mnew-k2-m1.migrate-2 || return 1

    ceph osd crush rule create-replicated myrule default osd || return 1
    ceph osd pool create mrep 4 4 replicated myrule || return 1
    rule=$(pool_field mrep crush_rule)
    ceph osd pool migrate mrep --inherit-params || return 1
    creating mrep || return 1
    test "$(rule_of mrep)" = mrep || return 1
    test "$(pool_field .migrate-3 crush_rule)" = $rule || return 1
    test "$(rule_of .migrate-3)" = myrule.migrate-3 || return 1
}

function TEST_pool_migrate_keeps_shared_rule_and_profile() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd erasure-code-profile set pshared k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd crush rule create-erasure rshared pshared || return 1
    ceph osd pool create mshared 4 4 erasure pshared rshared || return 1
    ceph osd pool create other 4 4 erasure pshared rshared || return 1
    ceph osd pool create mdefault 4 4 replicated || return 1
    local default_rule=$(rule_of mdefault)
    local steps=$(ceph osd crush rule dump $default_rule -f json | jq -c 'del(.rule_id, .rule_name)')

    # the target has copies of the rules and profiles that keep their names
    ceph osd pool migrate mshared --inherit-params || return 1
    test "$(rule_of .migrate-1)" = rshared || return 1
    test "$(profile_of .migrate-1)" = pshared || return 1
    test "$(rule_of mshared)" = mshared || return 1
    test "$(profile_of mshared)" = mshared-k2-m1 || return 1
    ceph osd pool migrate mdefault --inherit-params || return 1
    test "$(rule_of .migrate-2)" = $default_rule || return 1
    test "$(rule_of mdefault)" = mdefault || return 1
    test "$(ceph osd crush rule dump mdefault -f json | jq -c 'del(.rule_id, .rule_name)')" = "$steps" || return 1

    # and of the defaults for a new pool type
    ceph osd pool create mtoec 4 4 replicated || return 1
    ceph osd pool migrate mtoec --pool_type erasure --enable_ec_optimizations || return 1
    test "$(rule_of mtoec)" = mtoec || return 1
    profile_of mtoec | grep -qx 'mtoec-k[0-9]*-m[0-9]*' || return 1
    ceph osd pool create mtorep 4 4 erasure pshared rshared || return 1
    ceph osd pool migrate mtorep --pool_type replicated || return 1
    test "$(rule_of mtorep)" = mtorep || return 1

    # a rule or profile that the command names is shared, and keeps its name
    ceph osd erasure-code-profile set pown k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd pool create mown 4 4 erasure pown || return 1
    ceph osd pool migrate mown --pool_type erasure --erasure_code_profile pown --rule mown \
        --enable_ec_optimizations || return 1
    for pool in mown .migrate-5 ; do
        test "$(rule_of $pool)" = mown || return 1
        test "$(profile_of $pool)" = pown || return 1
    done
    ! ceph osd crush rule ls | grep -q migrate || return 1
    ! ceph osd erasure-code-profile ls | grep -q migrate || return 1
}

function TEST_pool_migrate_name_clash() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd erasure-code-profile set pclash k=2 m=1 crush-failure-domain=osd || return 1
    ceph osd pool create mclash 4 4 erasure pclash || return 1

    # the name for the source's rule is taken
    ceph osd crush rule create-erasure mclash.migrate-1 pclash || return 1
    expect_failure $dir "EEXIST.*rule 'mclash.migrate-1'" \
        ceph osd pool migrate mclash --inherit-params || return 1
    ceph osd crush rule rm mclash.migrate-1 || return 1
    # the name for the source's profile is taken
    ceph osd erasure-code-profile set pclash.migrate-1 k=2 m=1 crush-failure-domain=osd || return 1
    expect_failure $dir "EEXIST.*profile 'pclash.migrate-1'" \
        ceph osd pool migrate mclash --inherit-params || return 1
    ceph osd erasure-code-profile rm pclash.migrate-1 || return 1
    # the name for the target's profile is taken, with other contents
    ceph osd erasure-code-profile set mclash-k2-m1 k=2 m=1 crush-failure-domain=host || return 1
    expect_failure $dir "EEXIST.*'mclash-k2-m1'" \
        ceph osd pool migrate mclash --inherit-params || return 1
    ceph osd erasure-code-profile rm mclash-k2-m1 || return 1
    # the name for the target's rule is another pool's rule
    ceph osd pool create mrep 4 4 replicated || return 1
    ceph osd crush rule create-erasure mrep pclash || return 1
    ceph osd pool create other 4 4 erasure pclash mrep || return 1
    expect_failure $dir "EEXIST.*rule 'mrep'" \
        ceph osd pool migrate mrep --inherit-params || return 1

    # the failures renamed nothing, and left nothing behind
    ceph osd pool create later 4 4 replicated || return 1
    ! ceph osd pool ls --show-all | grep -q migrate || return 1
    test "$(rule_of mclash)" = mclash || return 1
    test "$(profile_of mclash)" = pclash || return 1
    test "$(rule_of mrep)" = "$(rule_of later)" || return 1
    ! ceph osd erasure-code-profile ls | grep -q '^mclash-' || return 1
}

function TEST_pool_migrate_changes_profile_contents() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd pool create mec erasure --k 2 --m 1 --osd_failure_domain osd || return 1
    local before=$(ceph osd erasure-code-profile get mec-k2-m1 -f json | jq -cS .)

    # the target's profile has the name of the source's, with other contents
    ceph osd pool migrate mec --pool_type erasure --k 2 --m 1 --osd_failure_domain host \
        --enable_ec_optimizations || return 1
    test "$(profile_of mec)" = mec-k2-m1 || return 1
    ceph osd erasure-code-profile get mec-k2-m1 | grep -q 'failure-domain=host' || return 1
    ceph osd crush rule dump mec -f json | jq -e '[.steps[] | select(.type == "host")] | length > 0' || return 1
    test "$(profile_of .migrate-1)" = mec-k2-m1.migrate-1 || return 1
    test "$(ceph osd erasure-code-profile get mec-k2-m1.migrate-1 -f json | jq -cS .)" = "$before" || return 1
    test "$(rule_of .migrate-1)" = mec.migrate-1 || return 1

    # a failure once the source's profile has its new name gives it back the old one
    ceph osd pool create mundo erasure --k 2 --m 1 --osd_failure_domain osd || return 1
    before=$(ceph osd erasure-code-profile get mundo-k2-m1 -f json | jq -cS .)
    ceph osd crush rule create-replicated mundo.migrate-2 default osd || return 1
    expect_failure $dir "EEXIST.*rule 'mundo.migrate-2'" \
        ceph osd pool migrate mundo --pool_type erasure --k 2 --m 1 --osd_failure_domain host \
        --enable_ec_optimizations || return 1
    ceph osd pool ls | grep -qx mundo || return 1
    test "$(profile_of mundo)" = mundo-k2-m1 || return 1
    test "$(ceph osd erasure-code-profile get mundo-k2-m1 -f json | jq -cS .)" = "$before" || return 1
    ! ceph osd erasure-code-profile ls | grep -q '^mundo-k2-m1.migrate' || return 1
}

# The monitors run a command again after an election: a migration of a pool
# that is being migrated to the layout it asks for changes nothing, also when
# its options give the target its own rule and erasure code profile
function TEST_pool_migrate_again_to_layout() {
    local dir=$1
    run_mon_for_pool_migration $dir || return 1
    ceph osd pool create mrep 4 4 replicated || return 1
    ceph osd pool create mec 4 4 erasure --k 2 --m 1 --osd_failure_domain osd \
        --enable_ec_optimizations || return 1

    ceph osd pool migrate mrep --pool_type replicated --size 2 || return 1
    local target=$(pool_field mrep pool)
    ceph osd pool migrate mrep --pool_type replicated --size 2 || return 1
    test "$(pool_field mrep pool)" = $target || return 1
    expect_failure $dir "ongoing migration" \
        ceph osd pool migrate mrep --pool_type replicated --size 3 || return 1
    expect_failure $dir "ongoing migration" \
        ceph osd pool migrate .migrate-1 --pool_type replicated --size 2 || return 1

    ceph osd pool migrate mec --pool_type erasure --k 4 --m 2 --osd_failure_domain osd \
        --enable_ec_optimizations || return 1
    target=$(pool_field mec pool)
    ceph osd pool migrate mec --pool_type erasure --k 4 --m 2 --osd_failure_domain osd \
        --enable_ec_optimizations || return 1
    test "$(pool_field mec pool)" = $target || return 1
    expect_failure $dir "ongoing migration" \
        ceph osd pool migrate mec --pool_type erasure --k 4 --m 2 --osd_failure_domain host \
        --enable_ec_optimizations || return 1
    test "$(ceph osd pool ls --show-all | grep -c 'migrate-')" = 2 || return 1
}
main osd-pool-create "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-pool-create.sh"
# End:
