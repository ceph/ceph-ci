#!/usr/bin/env python3
"""
Chaos testing for a pool on the vstart cluster in $CEPH_BUILD (source env.sh).
Works on replicated and erasure coded pools, single or multi-zone: it reads
the pool's type, size and num_zones and only uses the actions and clients
that apply.  Random OSD faults run while verifying workloads:

  rados-plain       ceph_test_rados
  rados-local[-z]   ceph_test_rados with localized reads (one per zone)
  rados-balanced    ceph_test_rados with balanced reads
  ioseq             ceph_test_rados_io_sequence
  rbd               random rbd writes with data in the pool, read back and compared
  probe-*           io_probe.py clients, watched for stuck ops

Actions for every pool, keeping each zone within its failure budget (m for
an EC pool, otherwise size - min_size, or zone size - 1 with zones):
  mark_down, out_in, kill_restart, upmap_items, rm_upmap, repeer, deep_scrub

Multi-zone pools (num_zones > 1, which enables stretch mode) also get:
  upmap_zone, upmap_flip  move or swap a PG's zone blocks
  zone_partial            (EC) kill enough of a zone's OSDs to drop it below k
  zone_failover           whole zone failures (zone OSDs and zone monitor via
                          scripts/zone_thrasher.py), run as a state machine
                          across cycles so other actions and workloads continue
                          while a zone is down:
    standard        kill zone OSDs+mon, wait degraded, hold, revive mon then OSDs
    osds_first      as standard, but revive OSDs before the mon and do not nudge
                    the monitor; getting stuck in degraded mode is recorded
    flap            once the revived zone is recovering, kill the other zone
    surviving_loss  while degraded, also kill a failure budget of OSDs in the
                    surviving zone
    osds_only       kill every OSD in a zone but leave its mon running
    mon_only        kill only the zone mon

Stops (leaving the cluster as it is) on: daemon crash signature, a daemon
dying that we did not kill, workload failure/miscompare, scrub inconsistency,
or PGs (and stretch mode) not returning to healthy after everything is revived.
Non-fatal oddities are appended to <rundir>/findings.log with diagnostics.
"""

import argparse
import json
import os
import random
import signal
import socket
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import zone_thrasher  # noqa: E402

BUILD = os.environ["CEPH_BUILD"]
OUT = f"{BUILD}/out"
CONF = f"{BUILD}/ceph.conf"
# Daemon/tool binaries: default the main build; CHAOS_BIN_DIR points at a
# snapshot of a fixed build (its lib/ must be first in LD_LIBRARY_PATH).
BIN = os.environ.get("CHAOS_BIN_DIR", f"{BUILD}/bin")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def ceph(cmd, timeout=90, quiet=False):
    try:
        r = subprocess.run(f"ceph --connect-timeout 30 {cmd}", shell=True,
                           capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        if not quiet:
            log(f"  ceph {cmd}: timed out")
        return None
    if r.returncode != 0:
        if not quiet:
            log(f"  ceph {cmd}: rc={r.returncode} {r.stderr.strip()[:200]}")
        return None
    return r.stdout


def ceph_json(cmd, quiet=False):
    out = ceph(f"{cmd} -f json", quiet=quiet)
    try:
        return json.loads(out) if out else None
    except json.JSONDecodeError:
        return None


def daemon_env():
    env = dict(os.environ)
    env.pop("CEPH_KEYRING", None)
    return env


class Daemons:
    """Kill/start vstart OSDs and mons; remembers which ones we stopped."""

    def __init__(self):
        self.stopped = set()      # ("osd", 3) / ("mon", "a")
        self.starting = {}        # key -> (Popen, time)
        self.killed_pid = {}      # key -> pid we signalled

    def pid(self, key):
        typ, i = key
        try:
            pid = int(open(f"{OUT}/{typ}.{i}.pid").read().strip())
            os.kill(pid, 0)
            return pid
        except (OSError, ValueError):
            return None

    def kill(self, key, sig=signal.SIGKILL):
        pid = self.pid(key)
        self.stopped.add(key)
        self.starting.pop(key, None)
        if pid:
            os.kill(pid, sig)
            self.killed_pid[key] = pid
            log(f"  kill -{sig.name} {key[0]}.{key[1]} (pid {pid})")

    def start(self, key):
        typ, i = key
        if key not in self.stopped:
            return
        old = self.killed_pid.pop(key, None) or self.pid(key)
        if old:
            # SIGTERM may still be shutting down (pid file can go before sockets)
            for _ in range(120):
                try:
                    os.kill(old, 0)
                except OSError:
                    break
                time.sleep(1)
        if typ == "mon":
            # A client reconnecting to a dead mon can self-connect on its port
            # (vstart ports are in the ephemeral range), leaving a TIME_WAIT
            # that blocks the bind for ~60s: retry until the mon starts.
            for attempt in range(12):
                r = subprocess.run(f"{BIN}/ceph-mon -i {i} -c {CONF}",
                                   shell=True, cwd=BUILD, env=daemon_env(),
                                   capture_output=True)
                if r.returncode == 0:
                    break
                log(f"  mon.{i} start rc={r.returncode}, retrying ({attempt})")
                time.sleep(10)
            self.stopped.discard(key)
            log(f"  started mon.{i}")
            return
        p = subprocess.Popen(f"{BIN}/ceph-{typ} -i {i} -c {CONF}",
                             shell=True, cwd=BUILD, env=daemon_env(),
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        self.stopped.discard(key)
        self.starting[key] = (p, time.time())
        log(f"  starting {typ}.{i}")

    def is_dead(self, key):
        if key in self.stopped:
            return False
        if key in self.starting:
            p, t = self.starting[key]
            if p.poll() is None and time.time() - t < 300:
                return False
            del self.starting[key]
            if p.returncode:
                log(f"!!! ceph-{key[0]} -i {key[1]} exited rc={p.returncode}")
                return True
        return self.pid(key) is None


class Cluster:
    def __init__(self, pool):
        self.pool = pool
        pools = ceph_json("osd pool ls detail")
        p = next(p for p in pools if p["pool_name"] == pool)
        self.pool_id = p["pool_id"]
        self.size = p["size"]
        self.min_size = p["min_size"]
        self.num_zones = p.get("options", {}).get("num_zones", 1)
        self.multi_zone = self.num_zones > 1
        self.zone_size = self.size // self.num_zones
        self.erasure = bool(p.get("erasure_code_profile"))
        if self.erasure:
            prof = ceph_json(f"osd erasure-code-profile get {p['erasure_code_profile']}")
            self.k, self.m = int(prof["k"]), int(prof["m"])
            self.budget = self.m
        elif self.multi_zone:
            self.budget = self.zone_size - 1
        else:
            self.budget = self.size - self.min_size
        if self.multi_zone:
            zone_thrasher.CEPH_BIN = "ceph"
            topo = zone_thrasher.discover_zones()
            self.tiebreak = topo["tiebreak_mon"]
            self.zones = {z["name"]: z["osds"] for z in topo["zones"]}
            self.zone_mon = {z["name"]: z["mon"] for z in topo["zones"]}
            self.all_mons = sorted([m for m in self.zone_mon.values() if m] +
                                   [self.tiebreak])
        else:
            self.tiebreak = None
            self.zones = {"all": sorted(int(o) for o in ceph_json("osd ls"))}
            self.zone_mon = {}
            self.all_mons = sorted(m["name"] for m in ceph_json("mon dump")["mons"])
        self.osd_zone = {o: z for z, osds in self.zones.items() for o in osds}
        self.all_osds = sorted(self.osd_zone)
        kind = f"erasure k={self.k} m={self.m}" if self.erasure else \
            f"replicated size={self.size} min_size={self.min_size}"
        log(f"pool {pool} id={self.pool_id} {kind} num_zones={self.num_zones} "
            f"budget/zone={self.budget} zones={self.zones} mons={self.all_mons}")

    def pgs(self):
        d = ceph_json(f"pg ls-by-pool {self.pool}", quiet=True)
        return d["pg_stats"] if d else []

    def stretch(self):
        if not self.multi_zone:
            return {}
        d = ceph_json("osd dump", quiet=True)
        return d.get("stretch_mode", {}) if d else None

    def quorum(self):
        d = ceph_json("quorum_status", quiet=True)
        return d.get("quorum_names", []) if d else []


class Workloads:
    def __init__(self, cluster, args, rundir):
        self.c = cluster
        self.args = args
        self.rundir = rundir
        self.procs = {}
        self.n = 0
        self.failed = []

    def specs(self):
        pool = self.c.pool
        pols = self.args.read_policies.split(",")
        here = os.path.dirname(os.path.abspath(__file__))
        rados_ops = ("--op read 100 --op write 50 --op append 50 --op delete 20 "
                     "--op snap_create 20 --op snap_remove 20 --op rollback 20 "
                     "--op copy_from 20 --op setattr 25 --op rmattr 25")
        common = (f"{BIN}/ceph_test_rados --pool {pool} --max-ops 4000 --objects 50 "
                  f"--max-in-flight 16 --size 400000 --min-stride-size 1000 "
                  f"--max-stride-size 80000")
        specs = {}
        if "rados" in self.args.workloads:
            if "none" in pols:
                specs["rados-plain"] = f"{common} {rados_ops}"
            if "localize" in pols:
                if self.c.multi_zone:
                    for z in sorted(self.c.zones):
                        specs[f"rados-local-{z}"] = (
                            f"{common} --localize-reads "
                            f"--crush-location datacenter={z} {rados_ops}")
                else:
                    specs["rados-local"] = f"{common} --localize-reads {rados_ops}"
            if "balance" in pols:
                specs["rados-balanced"] = f"{common} --balance-reads {rados_ops}"
        if "ioseq" in self.args.workloads:
            pct = 100 if "balance" in pols else 0
            specs["ioseq"] = (
                f"{BIN}/ceph_test_rados_io_sequence --pool {pool} --parallel 4 "
                f"--allow_pool_scrubbing --allow_pool_deep_scrubbing "
                f"--balanced_read_percentage {pct} --seed {{seed}}")
        if "ioseqrec" in self.args.workloads:
            specs["ioseqrec"] = (
                f"{BIN}/ceph_test_rados_io_sequence --pool {pool} --parallel 1 "
                f"--testrecovery --object ioseqrec --object_copy ioseqrec_copy "
                f"--allow_pool_scrubbing --allow_pool_deep_scrubbing "
                f"--balanced_read_percentage 0 --seed {{seed}}")
        if "rbd" in self.args.workloads:
            specs["rbd"] = f"python3 {here}/rbd_roundtrip.py {{seed}} {pool}"
        if "probe" in self.args.workloads:
            zones = sorted(self.c.zones) if self.c.multi_zone else [None]
            for pol in pols:
                for z in zones:
                    name = f"probe-{pol}-{z}" if z else f"probe-{pol}"
                    zone_arg = f"--zone {z} " if z else ""
                    specs[name] = (f"python3 {here}/io_probe.py --pool {pool} "
                                   f"--policy {pol} {zone_arg}--duration 1800 "
                                   f"--out {self.rundir}/{name}.jsonl --name {name}")
        return specs

    def stuck_probes(self, limit):
        """Probes whose current op has been outstanding longer than limit."""
        bad = []
        for name in self.procs:
            if not name.startswith("probe-"):
                continue
            f = f"{self.rundir}/{name}.jsonl"
            try:
                with open(f, "rb") as fh:
                    fh.seek(max(0, os.path.getsize(f) - 4096))
                    lines = fh.read().decode(errors="replace").splitlines()
            except OSError:
                continue
            for line in reversed(lines):
                if '"inflight"' in line:
                    try:
                        r = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if r["inflight"] > limit:
                        bad.append((name, r["inflight"], r["op"]))
                    break
        return bad

    def launch(self, name, cmd):
        self.n += 1
        cmd = cmd.format(seed=random.randint(1, 1 << 30))
        logf = f"{self.rundir}/{name}.{self.n}.log"
        f = open(logf, "w")
        f.write(f"# {cmd}\n")
        f.flush()
        p = subprocess.Popen(cmd, shell=True, stdout=f, stderr=subprocess.STDOUT,
                             cwd=BUILD, start_new_session=True)
        self.procs[name] = (p, logf, cmd)
        log(f"  workload {name} started (pid {p.pid}) -> {logf}")

    def start_all(self):
        for name, cmd in self.specs().items():
            self.launch(name, cmd)

    def poll(self, restart=True):
        for name, (p, logf, cmd) in list(self.procs.items()):
            rc = p.poll()
            if rc is None:
                continue
            del self.procs[name]
            if rc != 0:
                log(f"!!! workload {name} FAILED rc={rc}, see {logf}")
                self.failed.append((name, rc, logf))
            else:
                log(f"  workload {name} completed OK")
                if name.startswith("rados-"):
                    self.remove_rados_objects(p.pid)
                if restart:
                    self.launch(name, self.specs()[name])
        return not self.failed

    def remove_rados_objects(self, pid):
        """Clean up after a finished ceph_test_rados (objects <hostname><pid>-<n>
        and the snaps holding their clones) in the background, so a long run
        does not fill the small test devices."""
        here = os.path.dirname(os.path.abspath(__file__))
        prefix = f"{socket.gethostname()}{pid}-"
        subprocess.Popen(
            ["timeout", "1800", "python3", f"{here}/rados_cleanup.py",
             self.c.pool, prefix],
            cwd=BUILD, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True)

    def stop_all(self):
        for name, (p, logf, cmd) in self.procs.items():
            try:
                os.killpg(p.pid, signal.SIGTERM)
            except OSError:
                pass
        self.procs = {}

    def wait_idle(self, timeout):
        deadline = time.time() + timeout
        while self.procs and time.time() < deadline:
            self.poll(restart=False)
            time.sleep(5)
        return not self.procs


class LogWatcher:
    PATTERNS = ("FAILED ceph_assert", "Caught signal", "ceph_abort",
                "terminate called", "heap-use-after-free", "AddressSanitizer")

    def __init__(self):
        self.offsets = {f: os.path.getsize(f) for f in self.files()}

    def files(self):
        return [f"{OUT}/{f}" for f in os.listdir(OUT)
                if f.split(".")[0] in ("osd", "mon", "mgr") and f.endswith(".log")]

    def scan(self):
        hits = []
        for f in self.files():
            off = self.offsets.get(f, 0)
            size = os.path.getsize(f)
            if size < off:
                off = 0
            if size == off:
                continue
            with open(f, "rb") as fh:
                fh.seek(off)
                data = fh.read(size - off).decode(errors="replace")
            self.offsets[f] = size
            for line in data.splitlines():
                if any(p in line for p in self.PATTERNS):
                    hits.append((f, line.strip()[:300]))
        return hits


class ZoneFailover:
    """One zone failover, advanced once per chaos cycle."""

    VARIANTS = ("standard", "osds_first", "flap", "surviving_loss",
                "osds_only", "mon_only")

    def __init__(self, chaos, variant, zone):
        self.ch = chaos
        self.c = chaos.c
        self.variant = variant
        self.zone = zone
        self.other = next(z for z in self.c.zones if z != zone)
        self.state = "start"
        self.t = time.time()
        self.hold = random.randint(*chaos.args.hold_cycles)
        self.hold_until = None
        self.sig = random.choice([signal.SIGKILL, signal.SIGTERM])
        self.extra = []
        self.flapped = False
        self.flap_revived = False
        self.nudged = False

    def done(self):
        return self.state == "done"

    def kill_zone(self, zone, osds=True, mon=True):
        d = self.ch.d
        if osds:
            for o in self.c.zones[zone]:
                d.kill(("osd", o), self.sig)
        if mon and self.c.zone_mon.get(zone):
            d.kill(("mon", self.c.zone_mon[zone]), self.sig)

    def revive_zone(self, zone, mon_first=True):
        d = self.ch.d
        mon = ("mon", self.c.zone_mon[zone]) if self.c.zone_mon.get(zone) else None
        if mon_first and mon:
            d.start(mon)
            self.wait_quorum(120)
        for o in self.c.zones[zone]:
            d.start(("osd", o))
        if not mon_first and mon:
            d.start(mon)

    def wait_quorum(self, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if len(self.c.quorum()) == len(self.c.all_mons):
                return True
            time.sleep(3)
        self.ch.finding(f"quorum not restored within {timeout}s "
                        f"(quorum={self.c.quorum()})")
        return False

    def step(self):
        ch = self.ch
        st = self.c.stretch() or {}
        degraded = st.get("degraded_stretch_mode", 0)
        recovering = st.get("recovering_stretch_mode", 0)
        age = time.time() - self.t
        tag = f"zone_failover[{self.variant}:{self.zone}]"

        if self.state == "start":
            ch.record(f"{tag} kill sig={self.sig.name} hold={self.hold} cycles")
            if self.variant == "mon_only":
                self.kill_zone(self.zone, osds=False)
                self.state, self.t = "held", time.time()
                self.hold_until = ch.cycle + self.hold
            else:
                self.kill_zone(self.zone, mon=self.variant != "osds_only")
                self.state, self.t = "wait_degraded", time.time()
            return

        if self.state == "wait_degraded":
            if degraded:
                ch.record(f"{tag} degraded stretch mode after {age:.0f}s")
            elif age > ch.args.degrade_timeout:
                if self.variant == "osds_only":
                    ch.record(f"{tag} no degraded mode with zone mon up")
                else:
                    ch.finding(f"{tag} cluster did not enter degraded stretch "
                               f"mode within {age:.0f}s")
            else:
                return
            self.state, self.t = "held", time.time()
            self.hold_until = ch.cycle + self.hold
            if self.variant == "surviving_loss":
                victims = random.sample(self.c.zones[self.other],
                                        min(self.c.budget, len(self.c.zones[self.other])))
                ch.record(f"{tag} also killing surviving-zone OSDs {victims}")
                for o in victims:
                    ch.d.kill(("osd", o))
                self.extra = victims
            return

        if self.state == "held":
            if ch.cycle < self.hold_until:
                return
            if self.extra:
                ch.record(f"{tag} reviving surviving-zone OSDs {self.extra}")
                for o in self.extra:
                    ch.d.start(("osd", o))
                self.extra = []
            mon_first = self.variant != "osds_first"
            ch.record(f"{tag} revive ({'mon first' if mon_first else 'OSDs first'})")
            if self.variant == "mon_only":
                ch.d.start(("mon", self.c.zone_mon[self.zone]))
            else:
                self.revive_zone(self.zone, mon_first=mon_first)
            self.state, self.t = "recovering", time.time()
            return

        if self.state == "recovering":
            if self.variant == "flap" and not self.flapped and recovering:
                self.flapped = True
                ch.record(f"{tag} FLAP: zone {self.zone} recovering, killing "
                          f"zone {self.other}")
                self.kill_zone(self.other)
                self.flap_until = ch.cycle + random.randint(1, 4)
                return
            if self.flapped and not self.flap_revived:
                if ch.cycle >= self.flap_until:
                    ch.record(f"{tag} FLAP: reviving zone {self.other}")
                    self.revive_zone(self.other)
                    self.flap_revived = True
                    self.t = time.time()
                return
            if (degraded and not recovering and not self.nudged and
                    age > ch.args.nudge_after and
                    len(self.c.quorum()) == len(self.c.all_mons)):
                ch.finding(f"{tag} stuck in degraded (not recovering) stretch "
                           f"mode {age:.0f}s after revive with full quorum; "
                           f"nudging with force_recovery_stretch_mode")
                ceph("osd force_recovery_stretch_mode --yes-i-really-mean-it")
                self.nudged = True
                return
            pgs = self.c.pgs()
            clean = pgs and all(p["state"].startswith("active+clean") for p in pgs)
            if not degraded and not recovering and clean:
                ch.record(f"{tag} healthy again after {age:.0f}s")
                self.state = "done"
                return
            if age > ch.args.revive_timeout:
                bad = [(p["pgid"], p["state"]) for p in pgs
                       if not p["state"].startswith("active+clean")][:10]
                ch.fatal(f"{tag} not healthy {age:.0f}s after revive: "
                         f"degraded={degraded} recovering={recovering} pgs={bad}")


class Chaos:
    def __init__(self, args):
        self.args = args
        self.c = Cluster(args.pool)
        self.d = Daemons()
        self.cycle = 0
        self.rundir = args.rundir
        os.makedirs(self.rundir, exist_ok=True)
        self.w = Workloads(self.c, args, self.rundir)
        self.watch = LogWatcher()
        self.timeline = open(f"{self.rundir}/timeline.log", "a")
        self.zf = None
        self.failed = False
        self.pending_restart = {}
        self.partial_until = -1
        self.dup_acting_seen = set()

    def record(self, what):
        line = f"{time.strftime('%H:%M:%S')} cycle={self.cycle} {what}"
        self.timeline.write(line + "\n")
        self.timeline.flush()
        log(f"  >> {what}")

    def diag(self, label):
        d = f"{self.rundir}/diag-c{self.cycle}-{label}"
        os.makedirs(d, exist_ok=True)
        for name, cmd in (("status", "-s"), ("health", "health detail"),
                          ("osd_dump", "osd dump"),
                          ("pgs", f"pg ls-by-pool {self.c.pool}"),
                          ("mon_dump", "mon dump")):
            out = ceph(cmd, quiet=True)
            with open(f"{d}/{name}.txt", "w") as f:
                f.write(out or "(no output)\n")
        return d

    def finding(self, what):
        d = self.diag("finding")
        self.record(f"FINDING {what} (diag {d})")
        with open(f"{self.rundir}/findings.log", "a") as f:
            f.write(f"{time.strftime('%H:%M:%S')} cycle={self.cycle} {what}\n"
                    f"  diagnostics: {d}\n")

    def fatal(self, what):
        d = self.diag("fatal")
        self.record(f"FATAL {what} (diag {d})")
        self.failed = True

    # ---- budget helpers ---------------------------------------------------

    def unavailable(self):
        d = ceph_json("osd dump", quiet=True)
        down = {o["osd"] for o in d["osds"] if not o["up"]} if d else set()
        out = {o["osd"] for o in d["osds"] if not o["in"]} if d else set()
        down |= {i for (t, i) in self.d.stopped if t == "osd"}
        return down, out

    def zone_budget_ok(self, osd, down, out):
        """True if making osd unavailable keeps its zone within m failures."""
        z = self.c.osd_zone[osd]
        if self.zf and z == self.zf.zone:
            return False
        bad = {o for o in down | out if self.c.osd_zone.get(o) == z}
        return osd not in bad and len(bad) < self.c.budget + self.args.extra_per_zone

    def candidates(self):
        down, out = self.unavailable()
        return [o for o in self.c.all_osds if self.zone_budget_ok(o, down, out)]

    # ---- single-OSD / PG actions -----------------------------------------

    def a_mark_down(self):
        cands = self.candidates()
        if cands:
            o = random.choice(cands)
            self.record(f"mark_down osd.{o}")
            ceph(f"osd down {o}")

    def a_out_in(self):
        _, out = self.unavailable()
        if out and random.random() < 0.5:
            o = random.choice(sorted(out))
            self.record(f"in osd.{o}")
            ceph(f"osd in {o}")
            return
        cands = self.candidates()
        if cands:
            o = random.choice(cands)
            self.record(f"out osd.{o}")
            ceph(f"osd out {o}")

    def a_kill_restart(self):
        cands = self.candidates()
        if cands:
            o = random.choice(cands)
            when = self.cycle + random.randint(0, 4)
            self.record(f"kill_restart osd.{o} restart@{when}")
            self.d.kill(("osd", o))
            self.pending_restart[("osd", o)] = when

    def a_zone_partial(self):
        if self.zf:
            return
        down, out = self.unavailable()
        if down:
            return
        z = random.choice(sorted(self.c.zones))
        osds = self.c.zones[z]
        victims = random.sample(osds, len(osds) - self.c.k + 1)
        when = self.cycle + random.randint(1, 6)
        # the zone's PGs stay peered until the victims return, and the hold
        # is in cycles, so slow cycles can outlast --stuck-limit
        self.partial_until = when + 2
        self.record(f"zone_partial {z} {victims} restart@{when}")
        for o in victims:
            self.d.kill(("osd", o))
            self.pending_restart[("osd", o)] = when

    def a_zone_failover(self):
        if self.zf:
            return
        down, out = self.unavailable()
        if down:
            return
        variants = self.args.zf_variants.split(",")
        self.zf = ZoneFailover(self, random.choice(variants),
                               random.choice(sorted(self.c.zones)))
        self.zf.step()

    def _random_pg(self):
        pgs = self.c.pgs()
        return random.choice(pgs) if pgs else None

    def _zone_order(self, up):
        zs = self.c.zone_size
        return [self.c.osd_zone.get(up[i * zs]) if i * zs < len(up) else None
                for i in range(self.c.num_zones)]

    def a_upmap_zone(self, flip=False):
        pg = self._random_pg()
        if not pg:
            return
        order = self._zone_order(pg["up"])
        if None in order or len(set(order)) != len(order):
            return
        if flip:
            order = list(reversed(order))
        new = []
        for z in order:
            new += random.sample(self.c.zones[z], self.c.zone_size)
        self.record(f"upmap{'_flip' if flip else '_zone'} {pg['pgid']} "
                    f"{pg['up']} -> {new}")
        ceph(f"osd pg-upmap {pg['pgid']} {' '.join(map(str, new))}")

    def a_upmap_flip(self):
        self.a_upmap_zone(flip=True)

    def a_upmap_items(self):
        pg = self._random_pg()
        if not pg:
            return
        up = pg["up"]
        src = random.choice(up)
        z = self.c.osd_zone.get(src)
        cands = [o for o in self.c.zones.get(z, []) if o not in up]
        if cands:
            dst = random.choice(cands)
            self.record(f"upmap_items {pg['pgid']} {src}->{dst}")
            ceph(f"osd pg-upmap-items {pg['pgid']} {src} {dst}")

    def a_rm_upmap(self):
        d = ceph_json("osd dump", quiet=True) or {}
        ups = [u["pgid"] for u in d.get("pg_upmap", [])]
        items = [u["pgid"] for u in d.get("pg_upmap_items", [])]
        if ups and random.random() < 0.5:
            pg = random.choice(ups)
            self.record(f"rm_upmap {pg}")
            ceph(f"osd rm-pg-upmap {pg}")
        elif items:
            pg = random.choice(items)
            self.record(f"rm_upmap_items {pg}")
            ceph(f"osd rm-pg-upmap-items {pg}")

    def a_repeer(self):
        pg = self._random_pg()
        if pg:
            self.record(f"repeer {pg['pgid']}")
            ceph(f"pg repeer {pg['pgid']}")

    def a_deep_scrub(self):
        pg = self._random_pg()
        if pg:
            self.record(f"deep_scrub {pg['pgid']}")
            ceph(f"pg deep-scrub {pg['pgid']}")

    # ---- checks -----------------------------------------------------------

    def check(self):
        hits = self.watch.scan()
        ignored = {f for f, line in hits
                   if any(pat in line for pat in self.args.ignore_crash)}
        for f, line in hits:
            if f in ignored:
                log(f"  (ignored known crash in {os.path.basename(f)}: {line[:120]})")
                continue
            log(f"!!! crash signature in {f}: {line}")
            self.fatal(f"CRASH {os.path.basename(f)}: {line}")
        for f in ignored:
            osd = os.path.basename(f).split(".")[1]
            if os.path.basename(f).startswith("osd.") and ("osd", int(osd)) not in self.d.stopped:
                self.d.stopped.add(("osd", int(osd)))
                self.pending_restart[("osd", int(osd))] = self.cycle + 1
        keys = [("osd", o) for o in self.c.all_osds] + \
               [("mon", m) for m in self.c.all_mons]
        for key in keys:
            if self.d.is_dead(key):
                self.fatal(f"DEAD {key[0]}.{key[1]} (not killed by us)")
        if not self.w.poll():
            for name, rc, logf in self.w.failed:
                self.fatal(f"WORKLOAD_FAIL {name} rc={rc} {logf}")
            self.w.failed = []
        relaxed = self.zf or self.cycle <= self.partial_until
        limit = self.args.stuck_limit_zf if relaxed else self.args.stuck_limit
        for name, age, op in self.w.stuck_probes(limit):
            self.fatal(f"STUCK_OP {name} {op} outstanding {age:.0f}s")
        st = os.statvfs(BUILD)
        if st.f_bavail * st.f_frsize < 5 << 30:
            self.fatal(f"DISK: {BUILD} below 5G free")
        pgs = ceph_json(f"pg ls-by-pool {self.c.pool}", quiet=True)
        for p in (pgs or {}).get("pg_stats", []):
            a = [o for o in p["acting"] if o != 2147483647]
            # an OSD holding two shards may serve both until backfill moves
            # one (as calc_ec_acting allows); only a clean PG should not
            if len(a) != len(set(a)) and "clean" in p["state"] and \
                    p["pgid"] not in self.dup_acting_seen:
                self.dup_acting_seen.add(p["pgid"])
                with open(f"{self.rundir}/dup-{p['pgid']}-c{self.cycle}.query.json", "w") as f:
                    f.write(ceph(f"pg {p['pgid']} query", quiet=True) or "")
                self.finding(f"DUP_ACTING {p['pgid']} up {p['up']} acting {p['acting']} {p['state']}")
        df = ceph_json("osd df", quiet=True)
        if df:
            full = [(n["id"], n["utilization"]) for n in df.get("nodes", [])
                    if n.get("utilization", 0) > self.args.osd_full_pct]
            if full:
                self.fatal(f"ENV_OSD_FULL (test devices too small, not a Ceph bug): {full}")
        return not self.failed

    def restart_due(self):
        for key, when in list(self.pending_restart.items()):
            if when <= self.cycle:
                self.d.start(key)
                del self.pending_restart[key]

    def revive_everything(self):
        for key in sorted(self.d.stopped, key=lambda k: k[0] != "mon"):
            self.d.start(key)
            if key[0] == "mon":
                deadline = time.time() + 120
                while time.time() < deadline and \
                        len(self.c.quorum()) < len(self.c.all_mons):
                    time.sleep(3)
        self.pending_restart = {}
        _, out = self.unavailable()
        for o in out:
            ceph(f"osd in {o}")

    def wait_healthy(self, timeout):
        deadline = time.time() + timeout
        nudged = False
        start = time.time()
        while time.time() < deadline:
            st = self.c.stretch() or {}
            pgs = self.c.pgs()
            deg = st.get("degraded_stretch_mode", 0)
            rec = st.get("recovering_stretch_mode", 0)
            if not deg and not rec and pgs and \
               all(p["state"].startswith("active+clean") for p in pgs):
                return True
            if deg and not rec and not nudged and \
               time.time() - start > self.args.nudge_after:
                self.finding("quiesce: still degraded (not recovering) stretch "
                             "mode with everything up; nudging")
                ceph("osd force_recovery_stretch_mode --yes-i-really-mean-it")
                nudged = True
            if not self.check():
                return False
            time.sleep(10)
        bad = [(p["pgid"], p["state"]) for p in self.c.pgs()
               if not p["state"].startswith("active+clean")][:10]
        st = f" stretch={self.c.stretch()}" if self.c.multi_zone else ""
        self.fatal(f"NOT_HEALTHY after {timeout}s{st} pgs={bad}")
        return False

    def quiesce(self):
        log("=== quiesce ===")
        self.record("quiesce")
        self.zf = None
        self.revive_everything()
        if not self.wait_healthy(self.args.clean_timeout):
            return False
        ceph("osd unset noscrub", quiet=True)
        ceph("osd unset nodeep-scrub", quiet=True)
        stamps = {p["pgid"]: p.get("last_deep_scrub_stamp") for p in self.c.pgs()}
        for pgid in stamps:
            ceph(f"pg deep-scrub {pgid}", quiet=True)
        deadline = time.time() + self.args.clean_timeout
        while time.time() < deadline:
            pgs = self.c.pgs()
            if pgs and all(p.get("last_deep_scrub_stamp") != stamps.get(p["pgid"])
                           for p in pgs):
                break
            if not self.check():
                return False
            time.sleep(10)
        incons = ceph(f"pg ls-by-pool {self.c.pool} inconsistent", quiet=True)
        if incons and "inconsistent" in incons:
            self.fatal(f"INCONSISTENT after deep scrub:\n{incons}")
            return False
        return self.check()

    MULTI_ZONE_ACTIONS = ("zone_failover", "zone_partial", "upmap_zone", "upmap_flip")

    def applicable(self, action):
        if action in self.MULTI_ZONE_ACTIONS and not self.c.multi_zone:
            return False
        if action == "zone_failover" and not (self.c.stretch() or {}).get(
                "stretch_mode_enabled"):
            return False
        return action != "zone_partial" or self.c.erasure

    def run(self):
        actions = {n: float(w) for n, w in
                   (a.split("=") for a in self.args.actions.split(","))}
        skipped = [n for n in actions if not self.applicable(n)]
        actions = {n: w for n, w in actions.items() if n not in skipped}
        log(f"actions: {sorted(actions)}"
            + (f" (not applicable to this pool: {sorted(skipped)})" if skipped else ""))
        self.w.start_all()
        try:
            while self.args.cycles == 0 or self.cycle < self.args.cycles:
                self.cycle += 1
                zf = f"(zone failover {self.zf.variant}:{self.zf.state})" \
                    if self.zf else ""
                log(f"--- cycle {self.cycle} {zf} ---")
                self.restart_due()
                if self.zf:
                    self.zf.step()
                    if self.zf.done():
                        self.zf = None
                if self.failed:
                    return 1
                names = list(actions)
                if self.zf and self.zf.state == "recovering":
                    names = []
                for _ in range(random.randint(1, self.args.max_actions) if names else 0):
                    a = random.choices(names, [actions[n] for n in names])[0]
                    getattr(self, f"a_{a}")()
                time.sleep(self.args.delay)
                if not self.check():
                    return 1
                if self.args.quiesce_every and not self.zf and \
                   self.cycle % self.args.quiesce_every == 0:
                    if not self.quiesce():
                        return 1
            log("=== final quiesce ===")
            if not self.w.wait_idle(self.args.clean_timeout):
                log("workloads still running at the end; stopping them")
            return 0 if self.quiesce() and self.w.poll(restart=False) else 1
        finally:
            self.w.stop_all()
            if self.failed:
                log(f"stopped on failure; cluster left as-is. see {self.rundir}")


DEFAULT_ACTIONS = ("mark_down=3,out_in=2,kill_restart=3,zone_partial=1,"
                   "zone_failover=3,upmap_zone=2,upmap_flip=1,upmap_items=2,"
                   "rm_upmap=2,repeer=1,deep_scrub=1")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--pool", default="chaos")
    ap.add_argument("--cycles", type=int, default=100, help="0 = forever")
    ap.add_argument("--delay", type=float, default=8)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--max-actions", type=int, default=2)
    ap.add_argument("--extra-per-zone", type=int, default=0,
                    help="allow this many failures beyond the budget per zone")
    ap.add_argument("--actions", default=DEFAULT_ACTIONS)
    ap.add_argument("--zf-variants", default=",".join(ZoneFailover.VARIANTS))
    ap.add_argument("--hold-cycles", type=int, nargs=2, default=[2, 8])
    ap.add_argument("--degrade-timeout", type=int, default=180)
    ap.add_argument("--nudge-after", type=int, default=180)
    ap.add_argument("--revive-timeout", type=int, default=1200)
    ap.add_argument("--workloads", default="rados,ioseq,rbd,probe")
    ap.add_argument("--read-policies", default="none",
                    help="comma list of none,balance,localize for rados/probe/ioseq clients")
    ap.add_argument("--stuck-limit", type=int, default=240,
                    help="fatal if a probe op is outstanding longer than this")
    ap.add_argument("--stuck-limit-zf", type=int, default=900,
                    help="stuck limit while a zone failover or zone_partial is in progress "
                         "(flap can legitimately block IO until the zone returns)")
    ap.add_argument("--quiesce-every", type=int, default=30)
    ap.add_argument("--clean-timeout", type=int, default=1200)
    ap.add_argument("--rundir", default=None)
    ap.add_argument("--osd-full-pct", type=float, default=85.0)
    ap.add_argument("--ignore-crash", action="append",
                    default=["_shutdown_cache", "get_nref() == 1"],
                    help="substring of a crash line to treat as known")
    args = ap.parse_args()
    if args.seed is None:
        args.seed = random.randint(1, 1 << 30)
    random.seed(args.seed)
    if args.rundir is None:
        args.rundir = (f"{os.environ['CHAOS_RUNS']}/chaos-"
                       f"{time.strftime('%m%d-%H%M%S')}-s{args.seed}")
    log(f"seed={args.seed} rundir={args.rundir}")
    sys.exit(Chaos(args).run())


if __name__ == "__main__":
    main()
