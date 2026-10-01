#!/bin/bash
# One stretch EC chaos run, start to finish, on the vstart cluster in
# $CEPH_BUILD (see env.sh):
#
#   1. stop the running cluster (by its pid files) and wipe it
#   2. create a fresh 2-zone cluster + 'ecs' pool (setup_cluster.sh)
#   3. optionally rolling-restart mons/OSDs onto a binary snapshot
#      (upgrade_cluster.sh)
#   4. run stretchy_cokey.py with every read policy and zone failover variant
#   5. summarise: result, cycles, findings, crashes, asserts, final health
#
# usage: run_chaos.sh [options]
#   -s DIR    binary snapshot to run (default: the binaries in $CEPH_BUILD)
#   -b DIR    build dir to snapshot first into a new dir under $CHAOS_SNAPS
#             (e.g. a worktree's build/); overrides -s
#   -S SEED   chaos seed (default random)
#   -t SECS   wall-clock limit for the chaos phase (default 36000)
#   -c N      number of cycles, 0 = until failure/limit (default 0)
#   -n NAME   run name (default chaos-<date>-s<seed>)
#   -k K -m M EC profile (default 2+1)
#   -x        stop the cluster after the run (default: leave it for inspection)
#   -y        do not ask before destroying the existing cluster
# Extra arguments after -- are passed to stretchy_cokey.py.
set -e
. "$(dirname "$0")/env.sh"
B=$CEPH_BUILD
SNAP=
BUILD_DIR= SEED= LIMIT=36000 CYCLES=0 NAME= STOP=0 YES=0
export K=2 M=1
while getopts "s:b:S:t:c:n:k:m:xyh" o; do
    case $o in
        s) SNAP=$OPTARG ;; b) BUILD_DIR=$OPTARG ;; S) SEED=$OPTARG ;;
        t) LIMIT=$OPTARG ;; c) CYCLES=$OPTARG ;; n) NAME=$OPTARG ;;
        k) K=$OPTARG ;; m) M=$OPTARG ;; x) STOP=1 ;; y) YES=1 ;;
        h|*) sed -n '2,/^set -e/p' $0 | sed '$d; s/^# \{0,1\}//'; exit 2 ;;
    esac
done
shift $((OPTIND - 1))
SEED=${SEED:-$((RANDOM * 32768 + RANDOM))}
NAME=${NAME:-chaos-$(date +%m%d-%H%M)-s$SEED}
R=$CHAOS_RUNS/$NAME
[ -e "$R" ] && { echo "$R already exists"; exit 1; }
say() { echo "[$(date +%T)] $*"; }

running_daemons() {
    for f in $B/out/*.pid; do
        [ -e "$f" ] && kill -0 $(cat $f) 2>/dev/null && echo "$(basename $f .pid)"
    done
}

stop_cluster() {
    local pids=() p
    for f in $B/out/*.pid; do
        [ -e "$f" ] || continue
        p=$(cat $f); kill -0 $p 2>/dev/null && pids+=($p)
    done
    [ ${#pids[@]} = 0 ] && return 0
    say "stopping ${#pids[@]} daemons"
    kill ${pids[@]} 2>/dev/null || true
    for i in $(seq 1 60); do
        p=0; for x in ${pids[@]}; do kill -0 $x 2>/dev/null && p=1; done
        [ $p = 0 ] && return 0; sleep 1
    done
    kill -9 ${pids[@]} 2>/dev/null || true
}

# --- 0. preflight
if pgrep -f '^python3 [s]tretchy_cokey.py' >/dev/null; then
    echo "a chaos run is already in progress"; exit 1
fi
up=$(running_daemons | tr '\n' ' ')
if [ -n "$up" ] && [ $YES = 0 ]; then
    read -r -p "Destroy the running cluster in $B ($up)? [y/N] " a
    [ "$a" = y ] || exit 1
fi
if [ -n "$BUILD_DIR" ]; then
    SNAP=$CHAOS_SNAPS/$(basename $(dirname $(realpath $BUILD_DIR)))-$(date +%m%d-%H%M)
    say "snapshotting $BUILD_DIR -> $SNAP"
    mkdir -p $CHAOS_SNAPS
    $CHAOS_DIR/make_snapshot.sh $BUILD_DIR $SNAP
fi
[ -z "$SNAP" ] || [ -x $SNAP/bin/ceph-osd ] || { echo "no snapshot at $SNAP"; exit 1; }
# vstart devices: 8 x (4G block + 512M db + 128M wal) plus logs; dev/ is reclaimed
free=$(( $(df --output=avail -k $B | tail -1) / 1048576 ))
reclaim=$(( $(du -sk $B/dev 2>/dev/null | cut -f1 || echo 0) / 1048576 ))
if [ $((free + reclaim)) -lt 40 ]; then
    echo "$B has ${free}G free (+${reclaim}G from the old cluster); need ~40G"; exit 1
fi
mkdir -p $R
exec > >(tee -a $R/run.log) 2>&1
say "run $NAME: binaries ${SNAP:-$B/bin} ($(cat $SNAP/VERSION 2>/dev/null || git -C $B log -1 --format='%h %s')), seed $SEED, k=$K m=$M, limit ${LIMIT}s"

# --- 1+2. fresh cluster
stop_cluster
say "creating cluster"
T0=$(date +%s)
OSD=8 $CHAOS_DIR/setup_cluster.sh > $R/setup.log 2>&1 || { say "setup failed, see $R/setup.log"; exit 1; }

# --- 3. onto the snapshot
if [ -n "$SNAP" ]; then
    say "upgrading daemons to $SNAP"
    $CHAOS_DIR/upgrade_cluster.sh $SNAP > $R/upgrade.log 2>&1 || { say "upgrade failed, see $R/upgrade.log"; exit 1; }
    export CHAOS_BIN_DIR=$SNAP/bin LD_LIBRARY_PATH=$SNAP/lib:$LD_LIBRARY_PATH \
           CEPH_ARGS="--erasure_code_dir=$SNAP/lib --plugin_dir=$SNAP/lib" \
           RADOSC_LIB=$SNAP/lib/librados.so.2
fi
ceph -s | sed 's/^/    /'

# --- 4. chaos
say "chaos started (tail -f $R/chaos.out)"
set +e
( cd $CHAOS_DIR && timeout $LIMIT python3 stretchy_cokey.py --cycles $CYCLES --seed $SEED \
    --quiesce-every 30 --revive-timeout 2400 --clean-timeout 2400 \
    --read-policies none,localize,balance \
    --zf-variants standard,osds_first,flap,surviving_loss,mon_only \
    --rundir $R/chaos "$@" > $R/chaos.out 2>&1 )
rc=$?
set -e

# --- 5. conclusion
{
echo "=============== $NAME summary ==============="
echo "binaries : ${SNAP:-$B/bin}  $(cat $SNAP/VERSION 2>/dev/null || git -C $B log -1 --format='%h %s')"
echo "seed     : $SEED    k=$K m=$M"
echo "duration : $(( ($(date +%s) - T0) / 60 )) min"
last=$(grep -oE 'cycle [0-9]+' $R/chaos.out | tail -1)
case $rc in
    0)   res="PASS: completed $CYCLES cycles" ;;
    124) res="PASS: time limit reached, no failure" ;;
    *)   res="STOPPED rc=$rc: $(grep -m1 'FATAL' $R/chaos.out | sed 's/.*>> //')" ;;
esac
echo "result   : $res"
echo "cycles   : ${last#cycle }"
echo "actions  :"
awk '{print $3}' $R/chaos/timeline.log 2>/dev/null | sed 's/\[.*//' | sort | uniq -c | sort -rn | sed 's/^/    /'
echo "findings :"
[ -s $R/chaos/findings.log ] && sed 's/^/    /' $R/chaos/findings.log || echo "    none"
echo "crashes  :"
ceph crash ls 2>/dev/null | sed 's/^/    /' | grep -v '^    ID' || true
echo "asserts/aborts in daemon logs:"
grep -aHE 'FAILED ceph_assert|\*\*\* Caught signal' $B/out/{osd,mon}.*.log 2>/dev/null \
    | sort -u | head -20 | sed 's/^/    /' || true
echo "duplicate OSDs in acting sets:"
ceph pg ls-by-pool ecs -f json 2>/dev/null | python3 -c '
import json, sys
for p in json.load(sys.stdin)["pg_stats"]:
    a = [o for o in p["acting"] if o != 2147483647]
    if len(a) != len(set(a)):
        print(f"    {p[\"pgid\"]} acting {p[\"acting\"]} {p[\"state\"]}")' || true
echo "health   : $(ceph health 2>/dev/null)"
echo "diag     : $(ls -d $R/chaos/diag-* 2>/dev/null | tr '\n' ' ')"
} | tee $R/SUMMARY

# --- 6. teardown
if [ $STOP = 1 ]; then
    stop_cluster
    say "cluster stopped"
else
    say "cluster left running for inspection (stop with: $0 ... -x, or src/stop.sh)"
fi
exit $rc
