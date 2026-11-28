"""
Compare client I/O performance against Crimson OSDs, with and without
routing the client ops directly to the OSD reactor core owning their PG
(core hints):
  - the OSDs listen on a port per reactor core (crimson_osd_core_listeners),
    and hint the owning core in their op replies;
  - the clients follow the hints only with objecter_use_osd_core_hints.
The OSD side is the same in both modes; only the client option differs. It
is set per run, in a client configuration file of its own: it must not be
set in the cluster's configuration.

This is the multi-node version of qa/standalone/crimson/core-hints-fio.sh,
collecting and analyzing the same data. For each object size, workload and
mode, fio (rados engine) runs on all the client nodes at once, every I/O
covering a whole object. Over the measured window (from about the end of
the ramp to about the end of the run), it gathers:
  - from fio: IOPS, and the mean and percentile completion latencies,
    the percentiles being computed from the merged latency histograms of
    all the clients (fio's json+ output);
  - from every OSD reactor ('dump_metrics' over the OSDs' admin sockets):
    the client ops served with and without a cross-core hop
    (osd_pg_shard_op_local / op_remote), busy time, tasks run, polls,
    network bytes sent;
  - from every node: TCP segments sent (/proc/net/snmp);
  - from the clients: the CPU fio used, and the objecter counters of every
    fio client (over their admin sockets): ops sent on per-core
    connections, while a PG switch was pending, PG holds, switch delays,
    unanswered ops.
It writes, under <archive>/core-hints/:
  - summary.txt / summary.tsv: a row per run;
  - comparison.txt: hints on vs off, per size, workload and repetition;
  - results.json: all the rows, and the parameters;
  - raw/: per run, every client's fio output and the snapshots.

Report only: the numbers never fail the test. It fails only if it cannot
run: fio cannot be set up, a fio run fails or does no I/O, the
measurements cannot be read, or an OSD runs out of memory; or if the reads
of a run failed. fio's rados engine counts failed reads as completed: the
test checks the OSD logs for failed full-object reads, and that the OSDs
sent the data the reads should return. A run whose reads failed is marked
invalid (column 'valid'), the others still run, and the test fails at the
end.

Configuration (the defaults are a ~20 minute run; lists set in an override
replace the defaults, but are appended to by other overrides):

    tasks:
    - crimson_core_hints_perf:
        sizes: [1k, 4k, 16k, 64k, 256k]     # object = I/O size
        workloads: [randread, randwrite, randrw]
        rwmixread: 70                        # randrw: % reads
        modes: [off, on]                     # objecter_use_osd_core_hints
        numjobs: 8                           # fio jobs per client
        iodepth: 16                          # per job
        objects_per_job: 256
        runtime: 20                          # seconds per run
        ramp: 5                              # not measured
        repeat: 1                            # alternating the mode order
        rate_fractions: []                   # e.g. [0.5]: see below
        rate_process: poisson                # fio's, for the capped runs
        pool: corehints
        pg_num: 256
        pool_size: 3
        clients: [client.0, client.1]        # default: all the clients
        client_conf: {}                      # more [client.admin] settings
        fio_version: fio-3.38                # if fio must be built

Rate-limited runs (rate_fractions): at saturation, the latencies mostly
show where the ops in flight queue up. To compare the modes at the same
load below it, each size and workload, after its two unlimited runs, runs
again in both modes with fio capped (rate_iops, split evenly among all the
fio jobs; randrw: by rwmixread) at each fraction of the lower of the two
unlimited runs' total IOPS. Their rows have the load ('50%'), the
unlimited ones 'max'.
"""
import contextlib
import json
import logging
import os
import tempfile
import time

from io import StringIO

from teuthology import misc as teuthology
from teuthology.exceptions import CommandFailedError
from teuthology.orchestra import run
from teuthology.parallel import parallel

log = logging.getLogger(__name__)

DEFAULTS = {
    'sizes': ['1k', '4k', '16k', '64k', '256k'],
    'workloads': ['randread', 'randwrite', 'randrw'],
    'rwmixread': 70,
    'modes': ['off', 'on'],
    'numjobs': 8,
    'iodepth': 16,
    'objects_per_job': 256,
    'prefill_iodepth': 16,
    'runtime': 20,
    'ramp': 5,
    'repeat': 1,
    'rate_fractions': [],
    'rate_process': 'poisson',
    'pool': 'corehints',
    'pg_num': 256,
    'pool_size': 3,
    'clients': None,
    'client_conf': {},
    'fio_version': 'fio-3.38',
    # a fio run taking this much longer than ramp + runtime is killed
    'fio_grace': 120,
    # stop after this many failed runs
    'max_failed_runs': 2,
}

# the objecter counters summed over all the fio clients (times: seconds)
CLIENT_COUNTERS = [
    'op_send', 'op_send_core', 'op_send_switch_pending', 'core_switch_hold',
    'core_switch_hold_expired', 'op_switch_held', 'op_unanswered',
    'op_unanswered_forgotten', 'op_unanswered_answered', 'op_unanswered_reset',
    'core_switch_delayed_unanswered',
]

# per reactor, in this order (see collect.py: REACTOR_KEYS)
REACTOR_KEYS = ['reactor_cpu_busy_ms', 'reactor_polls',
                'reactor_tasks_processed', 'network_bytes_sent',
                'network_bytes_received']

# what the collector counts in the OSD logs: out of memory (the reactor's
# share of it); a full-object read whose data does not match the object's
# data digest (the read fails: EILSEQ)
LOG_NEEDLES = {'bad_alloc': b'bad_alloc',
               'read_crc': b'full-object read crc'}

# The collector, run (as root) on every node. Prints a JSON object.
#   collect.py host
#   collect.py osds '{"<id>": "<asok>", ...}'
#   collect.py snap '<json: {"osds": {id: asok}, "client_glob": <glob>,
#                           "osd_logs": {id: path}}>'
# Talks to the admin sockets directly: much faster than the ceph CLI.
COLLECTOR = r'''
import glob, json, os, socket, struct, sys, time

REACTOR_KEYS = %(reactor_keys)s
LOG_NEEDLES = %(log_needles)s


def asok(path, cmd):
    s = socket.socket(socket.AF_UNIX)
    s.settimeout(20)
    s.connect(path)
    s.sendall(json.dumps(cmd).encode() + b"\0")

    def recv(n):
        buf = b""
        while len(buf) < n:
            chunk = s.recv(n - len(buf))
            if not chunk:
                raise EOFError("admin socket closed")
            buf += chunk
        return buf
    data = recv(struct.unpack(">I", recv(4))[0])
    s.close()
    return json.loads(data)


def metrics(path, group):
    d = asok(path, {"prefix": "dump_metrics", "group": group,
                    "format": "json"})
    for m in d.get("metrics", []):
        for name, v in m.items():
            if isinstance(v, dict):
                yield name, v


def osd_snapshot(path):
    ops = {}
    for name, v in metrics(path, "osd_pg_shard"):
        if v.get("op_type") != "client_request":
            continue
        e = ops.setdefault(str(v.get("shard")), [0, 0])
        if name == "osd_pg_shard_op_local":
            e[0] += v.get("value", 0)
        elif name == "osd_pg_shard_op_remote":
            e[1] += v.get("value", 0)
    reactor = {}
    for group in ("reactor_", "network_"):
        for name, v in metrics(path, group):
            val = v.get("value")
            if name in REACTOR_KEYS and isinstance(val, (int, float)):
                e = reactor.setdefault(str(v.get("shard")),
                                       [0] * len(REACTOR_KEYS))
                e[REACTOR_KEYS.index(name)] += val
    return {"ops": ops, "reactor": reactor}


def tcp():
    hdr = None
    with open("/proc/net/snmp") as f:
        for line in f:
            if not line.startswith("Tcp:"):
                continue
            if hdr is None:
                hdr = line.split()
            else:
                v = dict(zip(hdr, line.split()))
                return [int(v["InSegs"]), int(v["OutSegs"])]
    return [0, 0]


def fio_ticks():
    total = 0
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open("/proc/%%s/stat" %% pid) as f:
                st = f.read()
        except OSError:
            continue
        if st[st.index("(") + 1:st.rindex(")")] != "fio":
            continue
        v = st[st.rindex(")") + 2:].split()
        total += int(v[11]) + int(v[12])
    return total


def count_in_file(path, needle):
    # (needle: more than one byte. A chunk's tail, shorter than the needle,
    # is searched again with the next chunk)
    n = 0
    try:
        with open(path, "rb") as f:
            tail = b""
            while True:
                chunk = f.read(1 << 22)
                if not chunk:
                    break
                buf = tail + chunk
                n += buf.count(needle)
                tail = buf[-(len(needle) - 1):]
    except OSError:
        return -1
    return n


def snap(arg):
    r = {"t": time.time(), "tcp": tcp(), "fio_ticks": fio_ticks(),
         "clk_tck": os.sysconf("SC_CLK_TCK"), "osds": {}, "clients": {}}
    for osd, path in arg.get("osds", {}).items():
        r["osds"][osd] = osd_snapshot(path)
    if arg.get("client_glob"):
        for path in sorted(glob.glob(arg["client_glob"])):
            try:
                d = asok(path, {"prefix": "perf dump", "logger": "objecter"})
                r["clients"][path] = d.get("objecter", {})
            except Exception as e:
                r["clients"][path] = {"error": repr(e)}
    r["logs"] = {osd: {k: count_in_file(path, needle)
                       for k, needle in LOG_NEEDLES.items()}
                 for osd, path in arg.get("osd_logs", {}).items()}
    return r


def read(path, default=""):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return default


def host():
    model = ""
    with open("/proc/cpuinfo") as f:
        for line in f:
            if line.startswith("model name"):
                model = line.split(":", 1)[1].strip()
                break
    mem = ""
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemTotal"):
                mem = line.split(":", 1)[1].strip()
                break
    nics = {}
    for dev in sorted(os.listdir("/sys/class/net")):
        if dev == "lo" or read("/sys/class/net/%%s/operstate" %% dev) != "up":
            continue
        nics[dev] = {"speed": read("/sys/class/net/%%s/speed" %% dev),
                     "mtu": read("/sys/class/net/%%s/mtu" %% dev)}
    return {
        "hostname": socket.gethostname(),
        "cpus": os.cpu_count(),
        "cpus_usable": len(os.sched_getaffinity(0)),
        "model": model,
        "numa_nodes": len(glob.glob("/sys/devices/system/node/node[0-9]*")),
        "mem": mem,
        "kernel": os.uname().release,
        "governor": read("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor",
                         "n/a"),
        "no_turbo": read("/sys/devices/system/cpu/intel_pstate/no_turbo", "n/a"),
        "init_on_alloc": "init_on_alloc=1" in read("/proc/cmdline"),
        "nics": nics,
        # the distinct sets of SMT siblings, e.g. ["0,12", "1,13", ...]
        "cpu_siblings": sorted(
            {read(p) for p in glob.glob(
                "/sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list")},
            key=lambda s: [int(x) for x in s.replace("-", ",").split(",")]),
    }


def osd_pid(osd):
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open("/proc/%%s/cmdline" %% pid, "rb") as f:
                argv = f.read().decode(errors="replace").split("\0")
        except OSError:
            continue
        # (not the wrappers starting it: their argv[0] differs. ceph-osd:
        # the alternative installed for Crimson; crimson-osd: run directly)
        if not os.path.basename(argv[0]).startswith(("ceph-osd", "crimson-osd")):
            continue
        for i, a in enumerate(argv[:-1]):
            if a in ("-i", "--id") and argv[i + 1] == osd:
                return int(pid)
    return None


def listening(pid):
    inodes = set()
    for fd in glob.glob("/proc/%%d/fd/*" %% pid):
        try:
            link = os.readlink(fd)
        except OSError:
            continue
        if link.startswith("socket:["):
            inodes.add(link[8:-1])
    ports = set()
    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            with open(table) as f:
                next(f)
                for line in f:
                    v = line.split()
                    # state 0A: LISTEN
                    if v[3] == "0A" and v[9] in inodes:
                        ports.add(int(v[1].rsplit(":", 1)[1], 16))
        except OSError:
            continue
    return sorted(ports)


def reactors(pid):
    """{thread name: the CPUs it may run on} of a process's Seastar reactor
    threads"""
    r = {}
    for task in glob.glob("/proc/%%d/task/*" %% pid):
        name = read(task + "/comm")
        if os.path.basename(task) == str(pid):
            # shard 0 runs on the main thread, which keeps its name
            name = "reactor-0"
        if name.startswith("reactor"):
            try:
                r[name] = sorted(os.sched_getaffinity(int(os.path.basename(task))))
            except OSError:
                continue
    return r


def osds(arg):
    r = {}
    for osd, path in arg.items():
        pid = osd_pid(osd)
        e = {"pid": pid,
             "listening": listening(pid) if pid else [],
             "threads": len(os.listdir("/proc/%%d/task" %% pid)) if pid else 0,
             "reactors": reactors(pid) if pid else {}}
        for var in ("crimson_cpu_num", "crimson_cpu_set",
                    "crimson_osd_core_listeners", "crimson_memory",
                    "osd_objectstore", "seastore_data_backend_type",
                    "seastore_cachepin_size_pershard", "osd_op_queue",
                    "crimson_osd_core_hint_withhold", "debug_osd", "debug_ms"):
            try:
                e[var] = asok(path, {"prefix": "config get", "var": var}).get(var)
            except Exception as ex:
                e[var] = "error: %%r" %% ex
        r[osd] = e
    return r


cmd = sys.argv[1]
arg = json.loads(sys.argv[2]) if len(sys.argv) > 2 else {}
out = {"host": host, "osds": osds, "snap": snap}[cmd]
print(json.dumps(out(arg) if cmd != "host" else out()))
''' % {'reactor_keys': repr(REACTOR_KEYS), 'log_needles': repr(LOG_NEEDLES)}


#
# pure helpers (unit tested in tasks/tests/test_crimson_core_hints_perf.py)
#

def parse_size(s):
    """'4k' -> 4096; plain numbers are bytes"""
    s = str(s).strip().lower().rstrip('b')
    mult = {'k': 1 << 10, 'm': 1 << 20, 'g': 1 << 30}
    if s and s[-1] in mult:
        return int(float(s[:-1]) * mult[s[-1]])
    return int(s)


def fio_job(conf, pool, name, rw, bs, numjobs, iodepth, nrfiles, extra=()):
    """a fio job file: <numjobs> jobs, each on its own <nrfiles> objects of
    <bs> bytes, named after <name>. Every I/O covers a whole object."""
    # the rados engine ignores 'filesize': it splits a job's 'size' evenly
    # among its 'nrfiles' objects (without 'size', it does no I/O at all)
    lines = [
        '[global]',
        'ioengine=rados',
        'clientname=admin',
        f'conf={conf}',
        f'pool={pool}',
        f'bs={bs}',
        f'iodepth={iodepth}',
        f'numjobs={numjobs}',
        f'nrfiles={nrfiles}',
        f'size={nrfiles * bs}',
        'file_service_type=random',
        'group_reporting=1',
    ]
    lines += list(extra)
    lines += [f'[{name}]', f'rw={rw}']
    return '\n'.join(lines) + '\n'


def rate_options(rw, target_iops, fio_jobs, rwmixread, process):
    """the fio options capping <fio_jobs> jobs (over all the clients) at
    <target_iops> in total. randrw: split by rwmixread (fio's rate_iops
    caps reads and writes separately)."""
    job_iops = -(-int(target_iops) // fio_jobs)
    if rw == 'randrw':
        rate = (f'{max(1, job_iops * rwmixread // 100)},'
                f'{max(1, job_iops * (100 - rwmixread) // 100)}')
    else:
        rate = str(max(1, job_iops))
    return [f'rate_iops={rate}', f'rate_process={process}']


def parse_fio_output(text):
    """fio's JSON output (fio may print notes before it)"""
    start = text.find('{')
    if start < 0:
        raise ValueError('no JSON in the fio output')
    return json.loads(text[start:])


def percentile_from_bins(bins, pct):
    """the <pct> percentile (ns) of a latency histogram {ns: count}"""
    total = sum(bins.values())
    if not total:
        return 0
    rank = pct / 100.0 * total
    seen = 0
    for ns in sorted(bins):
        seen += bins[ns]
        if seen >= rank:
            return ns
    return max(bins)


def merge_fio(outputs):
    """the merged fio results of all the clients (one fio output each, with
    group_reporting): per direction, the IOPS and ops (summed), the mean
    completion latency (weighted by the ops), and the percentiles of the
    merged latency histograms (exact with json+; otherwise, the largest of
    the clients' percentiles)"""
    r = {}
    for ddir in ('read', 'write'):
        iops = ios = 0
        lat_sum = 0.0
        bins = {}
        pcts = {}
        exact = True
        for out in outputs:
            for job in out.get('jobs', []):
                d = job.get(ddir, {})
                n = d.get('total_ios', 0)
                iops += d.get('iops', 0)
                ios += n
                clat = d.get('clat_ns', {})
                lat_sum += clat.get('mean', 0) * n
                if not n:
                    continue
                if 'bins' in clat:
                    for ns, c in clat['bins'].items():
                        bins[int(ns)] = bins.get(int(ns), 0) + c
                else:
                    exact = False
                for p, ns in clat.get('percentile', {}).items():
                    pcts[p] = max(pcts.get(p, 0), ns)
        e = {'iops': iops, 'ios': ios,
             'mean_us': lat_sum / ios / 1000 if ios else 0,
             'exact': exact}
        for name, p in (('p50_us', 50), ('p99_us', 99), ('p999_us', 99.9)):
            if exact:
                e[name] = percentile_from_bins(bins, p) / 1000
            else:
                e[name] = pcts.get(f'{p:.6f}', 0) / 1000
        r[ddir] = e
    return r


def fio_errors(output):
    """the error count of a fio output (all its jobs)"""
    return sum(job.get('error', 0) for job in output.get('jobs', []))


def sum_client_counters(snapshot):
    """the objecter counters summed over every fio client of a snapshot
    ({host: host snapshot}), and the number of clients read"""
    tot = {k: 0 for k in CLIENT_COUNTERS}
    tot.update(delay_sum=0.0, delay_n=0, delay_max=0.0, clients=0, errors=0)
    for h in snapshot.values():
        for c in h.get('clients', {}).values():
            if 'error' in c:
                tot['errors'] += 1
                continue
            tot['clients'] += 1
            for k in CLIENT_COUNTERS:
                v = c.get(k, 0)
                tot[k] += v if isinstance(v, (int, float)) else 0
            d = c.get('core_switch_delay', {})
            if isinstance(d, dict):
                tot['delay_sum'] += d.get('sum', 0)
                tot['delay_n'] += d.get('avgcount', 0)
                tot['delay_max'] = max(tot['delay_max'], d.get('max_inc', 0))
    return tot


def osd_deltas(s0, s1):
    """per OSD reactor, over the window between two snapshots (both
    {host: host snapshot}): [osd, shard, local ops, remote ops, busy %,
    busy ms, polls, tasks, bytes sent, bytes received]"""
    rows = []
    for host, h1 in s1.items():
        h0 = s0.get(host)
        if not h0:
            continue
        ms = (h1['t'] - h0['t']) * 1000
        for osd, o1 in h1.get('osds', {}).items():
            o0 = h0['osds'].get(osd)
            if not o0:
                continue
            for shard in sorted(o1['reactor'], key=int):
                r0 = o0['reactor'].get(shard)
                if r0 is None:
                    continue
                r1 = o1['reactor'][shard]
                d = [b - a for a, b in zip(r0, r1)]
                p0 = o0['ops'].get(shard, [0, 0])
                p1 = o1['ops'].get(shard, [0, 0])
                rows.append([int(osd), int(shard), p1[0] - p0[0],
                             p1[1] - p0[1],
                             100.0 * d[0] / ms if ms > 0 else 0.0] + d)
    rows.sort(key=lambda r: (r[0], r[1]))
    return rows


def _pct(a, b):
    return 100.0 * a / b if b else 0.0


def log_counts(snapshot, key):
    """{osd: count} of an OSD log needle (see LOG_NEEDLES) in a snapshot"""
    return {osd: c.get(key, 0) for h in snapshot.values()
            for osd, c in h.get('logs', {}).items()}


def read_failures(row, s0, s1, object_bytes):
    """why the reads of a run are not valid, or None. fio's rados engine
    counts failed reads as completed: check the OSDs' read errors over the
    window, and that the OSDs sent at least half the data the reads should
    have returned."""
    before = log_counts(s0, 'read_crc')
    errors = sum(max(0, n - before.get(osd, 0))
                 for osd, n in log_counts(s1, 'read_crc').items())
    row['rd_crc_errs'] = errors
    if errors:
        return f'{errors} full-object reads failed their data digest check'
    ops = row['rd_iops'] + row['wr_iops']
    if row['rd_iops'] and row['osd_ops']:
        expected = row['rd_iops'] / ops * object_bytes
        sent = row['tx_kb_op'] * 1024
        if sent < expected / 2:
            return (f'the OSDs sent {sent:.0f} bytes per op, under half the '
                    f'{expected:.0f} the reads should return')
    return None


def summarize(meta, fio, s0, s1, counters, osd_hosts, client_hosts):
    """a summary row (dict) of one run"""
    cores = osd_deltas(s0, s1)
    local = sum(r[2] for r in cores)
    remote = sum(r[3] for r in cores)
    ops = local + remote
    busy = [r[4] for r in cores]
    row = dict(meta)
    rd, wr = fio['read'], fio['write']
    row.update({
        'rd_iops': rd['iops'], 'wr_iops': wr['iops'],
        'rd_lat_us': rd['mean_us'], 'rd_p50_us': rd['p50_us'],
        'rd_p99_us': rd['p99_us'], 'rd_p999_us': rd['p999_us'],
        'wr_lat_us': wr['mean_us'], 'wr_p50_us': wr['p50_us'],
        'wr_p99_us': wr['p99_us'], 'wr_p999_us': wr['p999_us'],
        'pct_exact': rd['exact'] and wr['exact'],
        'osd_ops': ops,
        'osd_hop_pct': _pct(remote, ops),
        'cpu_avg_pct': sum(busy) / len(busy) if busy else 0.0,
        'cpu_max_pct': max(busy) if busy else 0.0,
        'reactors': len(busy),
        'cpu_us_op': 1000.0 * sum(r[5] for r in cores) / ops if ops else 0.0,
        'polls_op': sum(r[6] for r in cores) / ops if ops else 0.0,
        'tasks_op': sum(r[7] for r in cores) / ops if ops else 0.0,
        'tx_kb_op': sum(r[8] for r in cores) / 1024.0 / ops if ops else 0.0,
    })
    segs = 0
    for host in osd_hosts:
        if host in s0 and host in s1:
            segs += s1[host]['tcp'][1] - s0[host]['tcp'][1]
    row['segs_op'] = segs / ops if ops else 0.0
    cpu = 0.0
    for host in client_hosts:
        if host in s0 and host in s1:
            secs = s1[host]['t'] - s0[host]['t']
            ticks = s1[host]['fio_ticks'] - s0[host]['fio_ticks']
            if secs > 0:
                cpu += ticks / s1[host]['clk_tck'] / secs
    row['clnt_cores'] = cpu
    c = counters
    row['clients'] = c['clients']
    row['core_send_pct'] = _pct(c['op_send_core'], c['op_send'])
    row['pend_pct'] = _pct(c['op_send_switch_pending'], c['op_send'])
    row['holds'] = f"{c['core_switch_hold']}/{c['core_switch_hold_expired']}"
    if c['delay_n']:
        row['sw_delay_ms'] = (f"{1000 * c['delay_sum'] / c['delay_n']:.1f}/"
                              f"{1000 * c['delay_max']:.1f}")
    else:
        row['sw_delay_ms'] = '-'
    row['unans'] = f"{c['op_unanswered']}/{c['op_unanswered_forgotten']}"
    return row, cores


# (key, header, format) of the summary columns
SUMMARY_COLUMNS = [
    ('size', 'size', '{}'), ('workload', 'workload', '{}'),
    ('mode', 'mode', '{}'), ('load', 'load', '{}'), ('rep', 'rep', '{}'),
    ('target_iops', 'target_iops', '{:.0f}'),
    ('rd_iops', 'rd_iops', '{:.0f}'), ('wr_iops', 'wr_iops', '{:.0f}'),
    ('rd_lat_us', 'rd_lat_us', '{:.0f}'), ('rd_p50_us', 'rd_p50_us', '{:.0f}'),
    ('rd_p99_us', 'rd_p99_us', '{:.0f}'),
    ('rd_p999_us', 'rd_p99.9_us', '{:.0f}'),
    ('wr_lat_us', 'wr_lat_us', '{:.0f}'), ('wr_p50_us', 'wr_p50_us', '{:.0f}'),
    ('wr_p99_us', 'wr_p99_us', '{:.0f}'),
    ('wr_p999_us', 'wr_p99.9_us', '{:.0f}'),
    ('osd_hop_pct', 'osd_hop_%', '{:.1f}'),
    ('core_send_pct', 'core_send_%', '{:.1f}'),
    ('pend_pct', 'pend_%', '{:.1f}'), ('holds', 'holds/exp', '{}'),
    ('sw_delay_ms', 'sw_delay_ms', '{}'), ('unans', 'unans/forg', '{}'),
    ('cpu_avg_pct', 'cpu_avg_%', '{:.1f}'),
    ('cpu_max_pct', 'cpu_max_%', '{:.1f}'),
    ('cpu_us_op', 'cpu_us/op', '{:.1f}'),
    ('tasks_op', 'tasks/op', '{:.1f}'), ('polls_op', 'polls/op', '{:.1f}'),
    ('segs_op', 'segs/op', '{:.2f}'), ('tx_kb_op', 'tx_KB/op', '{:.1f}'),
    ('clnt_cores', 'clnt_cores', '{:.1f}'), ('clients', 'clients', '{}'),
    ('rd_crc_errs', 'rd_crc_errs', '{}'), ('valid', 'valid', '{}'),
]


def format_table(rows, columns):
    """an aligned text table of <rows> (dicts), and the same as TSV"""
    cells = [[h for _, h, _ in columns]]
    for r in rows:
        cells.append([fmt.format(r[k]) if r.get(k) is not None else 'n/a'
                      for k, _, fmt in columns])
    widths = [max(len(row[i]) for row in cells) for i in range(len(columns))]
    text = '\n'.join('  '.join(c.ljust(w) for c, w in zip(row, widths)).rstrip()
                     for row in cells)
    tsv = '\n'.join('\t'.join(row) for row in cells)
    return text + '\n', tsv + '\n'


def _delta_pct(off, on):
    return 100.0 * (on - off) / off if off else 0.0


COMPARISON_COLUMNS = [
    ('size', 'size', '{}'), ('workload', 'workload', '{}'),
    ('load', 'load', '{}'), ('rep', 'rep', '{}'),
    ('iops_off', 'iops_off', '{:.0f}'), ('iops_on', 'iops_on', '{:.0f}'),
    ('iops_delta', 'iops_%', '{:+.1f}'),
    ('rd_p50', 'rd_p50_us', '{}'), ('rd_p99', 'rd_p99_us', '{}'),
    ('wr_p50', 'wr_p50_us', '{}'), ('wr_p99', 'wr_p99_us', '{}'),
    ('cpu_op_delta', 'cpu/op_%', '{:+.1f}'),
    ('cpu_max', 'cpu_max_%', '{}'),
    ('hop', 'osd_hop_%', '{}'), ('core_send_on', 'core_send_%_on', '{:.1f}'),
    ('valid', 'valid', '{}'),
]


def compare(rows):
    """hints on vs off, per (size, workload, load, rep): one row each where
    both modes ran. Pairs of values are 'off->on'."""
    by_key = {}
    for r in rows:
        key = (r['size'], r['workload'], r.get('load', 'max'), r['rep'])
        by_key.setdefault(key, {})[r['mode']] = r
    out = []
    for (size, workload, load, rep), m in by_key.items():
        off, on = m.get('off'), m.get('on')
        if not off or not on:
            continue

        def pair(key, fmt='{:.0f}'):
            return f"{fmt.format(off[key])}->{fmt.format(on[key])}"
        iops_off = off['rd_iops'] + off['wr_iops']
        iops_on = on['rd_iops'] + on['wr_iops']
        out.append({
            'size': size, 'workload': workload, 'load': load, 'rep': rep,
            'iops_off': iops_off, 'iops_on': iops_on,
            'iops_delta': _delta_pct(iops_off, iops_on),
            'rd_p50': pair('rd_p50_us'), 'rd_p99': pair('rd_p99_us'),
            'wr_p50': pair('wr_p50_us'), 'wr_p99': pair('wr_p99_us'),
            'cpu_op_delta': _delta_pct(off['cpu_us_op'], on['cpu_us_op']),
            'cpu_max': pair('cpu_max_pct'),
            'hop': pair('osd_hop_pct', '{:.1f}'),
            'core_send_on': on['core_send_pct'],
            'valid': ('yes' if off.get('valid', 'yes') == 'yes' and
                      on.get('valid', 'yes') == 'yes' else 'no'),
        })
    return out


#
# the benchmark
#

class BenchBroken(Exception):
    """the test itself cannot go on"""


class CoreHintsBench(object):

    def __init__(self, ctx, config):
        self.ctx = ctx
        self.config = config
        self.testdir = teuthology.get_testdir(ctx)
        self.dir = f'{self.testdir}/core-hints'
        self.collector = f'{self.dir}/collect.py'
        self.manager = ctx.managers['ceph']
        if ctx.archive:
            self.out = os.path.join(ctx.archive, 'core-hints')
        else:
            self.out = tempfile.mkdtemp(prefix='core-hints-')
        os.makedirs(os.path.join(self.out, 'raw'), exist_ok=True)

        # nodes: {name: {'remote', 'osds': {id: asok}, 'clients': [ids]}}
        self.nodes = {}
        for remote, roles in ctx.cluster.remotes.items():
            self.nodes[remote.shortname] = {'remote': remote, 'osds': {},
                                            'osd_logs': {}, 'clients': []}
        for remote, roles in ctx.cluster.only(teuthology.is_type('osd')).remotes.items():
            for role in roles:
                if role.startswith('osd.'):
                    osd = role.split('.', 1)[1]
                    node = self.nodes[remote.shortname]
                    node['osds'][osd] = f'/var/run/ceph/ceph-osd.{osd}.asok'
                    node['osd_logs'][osd] = f'/var/log/ceph/ceph-osd.{osd}.log'
        clients = config['clients']
        if clients is None:
            clients = [r for roles in ctx.cluster.remotes.values()
                       for r in roles if r.startswith('client.')]
        for role in clients:
            (remote,) = ctx.cluster.only(role).remotes.keys()
            self.nodes[remote.shortname]['clients'].append(role.split('.', 1)[1])
        self.osd_hosts = [n for n, v in self.nodes.items() if v['osds']]
        self.client_hosts = [n for n, v in self.nodes.items() if v['clients']]
        if not self.osd_hosts or not self.client_hosts:
            raise BenchBroken('need OSD and client roles')
        self.fio = {}
        self.rows = []
        self.failures = []
        self.broken = False
        # runs whose reads failed (see read_failures())
        self.invalid = []
        self.params = {}

    #
    # remote helpers
    #

    def _collect(self, name, cmd, arg=None):
        remote = self.nodes[name]['remote']
        args = ['sudo', 'python3', self.collector, cmd]
        if arg is not None:
            args.append(json.dumps(arg))
        out = remote.sh(args, timeout=120)
        return json.loads(out)

    def _all(self, func, names):
        """{name: func(name)}, run in parallel"""
        def one(name):
            return name, func(name)
        with parallel() as p:
            for name in names:
                p.spawn(one, name)
            return dict(p)

    def _save(self, name, data):
        path = os.path.join(self.out, name)
        with open(path, 'w') as f:
            if isinstance(data, str):
                f.write(data)
            else:
                json.dump(data, f, indent=1, sort_keys=True)
        return path

    #
    # setup
    #

    def setup(self):
        def prepare(name):
            remote = self.nodes[name]['remote']
            remote.run(args=['mkdir', '-p', self.dir, f'{self.dir}/asok',
                             f'{self.dir}/fio', f'{self.dir}/logs'])
            remote.write_file(self.collector, COLLECTOR)
            return self._collect(name, 'host')
        self.params['hosts'] = self._all(prepare, list(self.nodes))
        for name, h in sorted(self.params['hosts'].items()):
            log.info('core-hints: %s: %s', name, json.dumps(h, sort_keys=True))

        self.params['osds'] = {}
        for name in self.osd_hosts:
            info = self._collect(name, 'osds', self.nodes[name]['osds'])
            self.params['osds'].update(info)
        want = 0
        for osd, info in sorted(self.params['osds'].items()):
            log.info('core-hints: osd.%s: %s', osd, json.dumps(info, sort_keys=True))
            # the OSD's reactor threads, as found on its node
            want = len(info.get('reactors', {}))
            if not want:
                log.warning('core-hints: osd.%s: no reactor threads found', osd)
            pinned = [t for t, cpus in info.get('reactors', {}).items()
                      if len(cpus) == 1]
            if len(pinned) != want:
                log.warning('core-hints: osd.%s: %d of its %d reactors are not '
                            'pinned to a CPU', osd, want - len(pinned), want)
            if str(info.get('crimson_osd_core_listeners')).lower() != 'true':
                log.warning('core-hints: osd.%s: crimson_osd_core_listeners '
                            'is not set: the clients will get no hints', osd)
            elif want and len(info['listening']) < want:
                log.warning('core-hints: osd.%s listens on %d ports only, '
                            'with %d reactors', osd, len(info['listening']), want)
        for name in self.osd_hosts:
            cpus = self.params['hosts'][name]['cpus_usable']
            if want and cpus < want + 2:
                log.warning('core-hints: %s has %d CPUs, for %d reactors and '
                            'the alien threads', name, cpus, want)

        self.fio = self._all(self._setup_fio, self.client_hosts)
        self._all(self._client_confs, self.client_hosts)
        self._create_pool()

    def _has_rados_engine(self, remote, fio):
        proc = remote.run(args=[fio, '--enghelp=rados'], check_status=False,
                          stdout=StringIO(), stderr=StringIO())
        return proc.exitstatus == 0 and 'clientname' in proc.stdout.getvalue()

    def _setup_fio(self, name):
        """a fio with the rados engine on a client node: the distro's, or
        built from source"""
        remote = self.nodes[name]['remote']
        if self._has_rados_engine(remote, 'fio'):
            return 'fio'
        install = ('if command -v dnf >/dev/null; then '
                   'sudo dnf install -y fio fio-engine-rados || '
                   'sudo dnf install -y fio; '
                   'else sudo apt-get install -y fio; fi')
        remote.run(args=[run.Raw(install)], check_status=False)
        if self._has_rados_engine(remote, 'fio'):
            return 'fio'
        tag = self.config['fio_version']
        log.info('core-hints: %s: building fio %s', name, tag)
        src = f'{self.dir}/fio-src'
        remote.run(args=[
            'rm', '-rf', src, run.Raw('&&'),
            'git', 'clone', '--depth', '1', '-b', tag,
            'https://github.com/axboe/fio.git', src, run.Raw('&&'),
            'cd', src, run.Raw('&&'),
            './configure', run.Raw('&&'),
            'make', run.Raw('-j$(nproc)')])
        fio = f'{src}/fio'
        if not self._has_rados_engine(remote, fio):
            raise BenchBroken(f'{name}: fio has no rados engine (librados '
                              'headers missing?)')
        return fio

    def _conf(self, client, mode):
        return f'{self.dir}/client.{client}-{mode}.conf'

    def _client_confs(self, name):
        """per client and mode: the cluster's configuration, and a section
        of its own, for the client name fio uses (client.admin)"""
        remote = self.nodes[name]['remote']
        base = remote.read_file('/etc/ceph/ceph.conf').decode()
        for client in self.nodes[name]['clients']:
            for mode in self.config['modes']:
                lines = [
                    '', '[client.admin]',
                    'objecter use osd core hints = '
                    f"{'true' if mode == 'on' else 'false'}",
                    f'admin socket = {self.dir}/asok/'
                    f'fio-{client}-{mode}.$pid.$cctid.asok',
                    f'log file = {self.dir}/logs/fio-{client}-{mode}.$pid.log',
                ]
                lines += [f'{k} = {v}' for k, v in
                          self.config['client_conf'].items()]
                remote.write_file(self._conf(client, mode),
                                  base + '\n'.join(lines) + '\n')

    def _create_pool(self):
        c = self.config
        pool = c['pool']
        self.manager.raw_cluster_cmd('osd', 'pool', 'create', pool,
                                     str(c['pg_num']), str(c['pg_num']))
        self.manager.raw_cluster_cmd('osd', 'pool', 'set', pool, 'size',
                                     str(c['pool_size']),
                                     '--yes-i-really-mean-it')
        self.manager.raw_cluster_cmd('osd', 'pool', 'application', 'enable',
                                     pool, 'rados')
        self.manager.wait_for_clean()

    #
    # fio
    #

    def _job_name(self, client, size):
        return f'ch_c{client}_{size}'

    def _fio(self, tag, size, rw, mode, extra, timeout):
        """start fio on every client, for <size> objects. Returns
        [(name, client, proc, output path)]"""
        c = self.config
        bs = parse_size(size)
        procs = []
        for name in self.client_hosts:
            remote = self.nodes[name]['remote']
            for client in self.nodes[name]['clients']:
                job = fio_job(self._conf(client, mode), c['pool'],
                              self._job_name(client, size), rw, bs,
                              c['numjobs'], c['iodepth'],
                              c['objects_per_job'], extra)
                jobfile = f'{self.dir}/{tag}.client.{client}.fio'
                out = f'{self.dir}/fio/{tag}.client.{client}.json'
                remote.write_file(jobfile, job)
                proc = remote.run(
                    args=['timeout', '-k', '30', str(timeout), self.fio[name],
                          '--output-format=json+', f'--output={out}', jobfile],
                    wait=False, check_status=False)
                procs.append((name, client, proc, out))
        return procs

    def _fio_wait(self, tag, procs):
        """wait for the fio processes; returns their parsed outputs. Raises
        BenchBroken if one failed, or did no I/O."""
        outputs = []
        errors = []
        for name, client, proc, out in procs:
            proc.wait()
            remote = self.nodes[name]['remote']
            try:
                text = remote.read_file(out).decode(errors='replace')
                self._save(f'raw/{tag}.client.{client}.json', text)
                output = parse_fio_output(text)
            except Exception as e:
                errors.append(f'client.{client}: no fio output ({e!r}); '
                              f'exit status {proc.exitstatus}')
                continue
            if proc.exitstatus != 0:
                errors.append(f'client.{client}: fio exit status '
                              f'{proc.exitstatus}')
            if fio_errors(output):
                errors.append(f'client.{client}: fio reports '
                              f'{fio_errors(output)} errors')
            ios = sum(j.get('read', {}).get('total_ios', 0) +
                      j.get('write', {}).get('total_ios', 0)
                      for j in output.get('jobs', []))
            if not ios:
                errors.append(f'client.{client}: fio completed no I/O')
            outputs.append(output)
        if errors:
            raise BenchBroken(f'fio {tag}: ' + '; '.join(errors))
        return outputs

    def prefill(self):
        """write every client's objects, of every size, once: the read
        workloads need them"""
        c = self.config
        for size in c['sizes']:
            tag = f'prefill-{size}'
            log.info('core-hints: %s', tag)
            t0 = time.time()
            procs = self._fio(tag, size, 'write', c['modes'][0],
                              [f"iodepth={c['prefill_iodepth']}"], 1800)
            self._fio_wait(tag, procs)
            log.info('core-hints: %s took %.0fs', tag, time.time() - t0)

    #
    # measurements
    #

    def _snapshot(self, objecter=False, logs=False):
        def snap(name):
            node = self.nodes[name]
            arg = {'osds': node['osds']}
            if objecter and node['clients']:
                arg['client_glob'] = f'{self.dir}/asok/*.asok'
            if logs:
                arg['osd_logs'] = node['osd_logs']
            return self._collect(name, 'snap', arg)
        return self._all(snap, list(self.nodes))

    def _sleep_until(self, t):
        delay = t - time.time()
        if delay > 0:
            time.sleep(delay)

    def run_one(self, size, rw, mode, rep, fraction=None, target_iops=None):
        """one run: unlimited, or capped at <target_iops> (<fraction> of
        the unlimited IOPS). Returns its summary row."""
        c = self.config
        load = 'max' if fraction is None else f'{100 * fraction:g}%'
        tag = (f'{size}-{rw}-{mode}-{rep}' if fraction is None else
               f'{size}-{rw}-{mode}-r{100 * fraction:g}-{rep}')
        log.info('core-hints: run %s', tag)
        for name in self.client_hosts:
            self.nodes[name]['remote'].run(
                args=['rm', '-f', run.Raw(f'{self.dir}/asok/*.asok')])
        # the objects exist (prefill()): do not touch them again, as that is
        # not part of the workload
        extra = ['time_based=1', f"runtime={c['runtime']}",
                 f"ramp_time={c['ramp']}", 'touch_objects=0']
        if rw == 'randrw':
            extra.append(f"rwmixread={c['rwmixread']}")
        if target_iops is not None:
            fio_jobs = c['numjobs'] * sum(len(self.nodes[n]['clients'])
                                          for n in self.client_hosts)
            extra += rate_options(rw, target_iops, fio_jobs, c['rwmixread'],
                                  c['rate_process'])
        launched = time.time()
        procs = self._fio(tag, size, rw, mode, extra,
                          c['ramp'] + c['runtime'] + c['fio_grace'])
        try:
            # the measured window: from just after the ramp to just before
            # the end, while the fio clients still run (to be sampled too)
            self._sleep_until(launched + c['ramp'] + 2)
            s0 = self._snapshot(logs=True)
            self._sleep_until(launched + c['ramp'] + c['runtime'] - 3)
            s1 = self._snapshot(objecter=True, logs=True)
            ended = [f'client.{client}' for _, client, proc, _ in procs
                     if proc.finished]
        finally:
            outputs = self._fio_wait(tag, procs)
        self._save(f'raw/{tag}.s0.json', s0)
        self._save(f'raw/{tag}.s1.json', s1)
        if ended:
            log.warning('core-hints: %s: fio ended before the end of the '
                        'window on %s: the OSD and client figures are not '
                        'reliable', tag, ', '.join(ended))
        oom = {osd: n for osd, n in log_counts(s1, 'bad_alloc').items() if n}
        if oom:
            # the OSD's connections keep failing: no later run is meaningful
            self.broken = True
            raise BenchBroken(f'{tag}: OSDs out of memory (bad_alloc): {oom}')

        counters = sum_client_counters(s1)
        if counters['errors'] or not counters['clients']:
            log.warning('core-hints: %s: read the counters of %d fio '
                        'clients, failed on %d', tag, counters['clients'],
                        counters['errors'])
        meta = {'size': size, 'workload': rw, 'mode': mode, 'load': load,
                'rep': rep, 'target_iops': target_iops}
        row, cores = summarize(meta, merge_fio(outputs), s0, s1, counters,
                               self.osd_hosts, self.client_hosts)
        row['window_s'] = max(s1[h]['t'] - s0[h]['t'] for h in s1)
        row['window_skew_s'] = (max(s0[h]['t'] for h in s0) -
                                min(s0[h]['t'] for h in s0))
        reason = read_failures(row, s0, s1, parse_size(size))
        row['valid'] = 'no' if reason else 'yes'
        if reason:
            row['invalid'] = reason
            self.invalid.append(f'{tag}: {reason}')
            log.error('core-hints: %s: the reads failed: %s', tag, reason)
        self._save(f'raw/{tag}.cores.tsv',
                   'osd\tshard\tlocal\tremote\tbusy_%\tbusy_ms\tpolls\t'
                   'tasks\tbytes_sent\tbytes_received\n' +
                   ''.join('\t'.join(f'{v:.1f}' if isinstance(v, float)
                                     else str(v) for v in r) + '\n'
                           for r in cores))
        self.rows.append(row)
        text, _ = format_table([row], SUMMARY_COLUMNS)
        log.info('core-hints: %s:\n%s', tag, text)
        return row

    def run(self):
        c = self.config
        self.params['config'] = c
        self.params['sha1'] = self.ctx.config.get('sha1')
        self.params['started'] = time.strftime('%Y-%m-%d %H:%M:%S')
        self._save('params.json', self.params)
        self.prefill()
        for rep in range(1, c['repeat'] + 1):
            # alternate the order of the modes, so that neither benefits
            # systematically from running second
            modes = list(c['modes'])
            if rep % 2 == 0:
                modes.reverse()
            for size in c['sizes']:
                for rw in c['workloads']:
                    # unlimited, then capped at fractions of the IOPS of
                    # the slower mode
                    runs = [(mode, None) for mode in modes]
                    runs += [(mode, f) for f in c['rate_fractions']
                             for mode in modes]
                    unlimited = {}
                    for mode, fraction in runs:
                        target = None
                        if fraction is not None:
                            if len(unlimited) < len(modes):
                                continue   # an unlimited run failed
                            target = fraction * min(unlimited.values())
                        try:
                            row = self.run_one(size, rw, mode, rep,
                                               fraction, target)
                            if fraction is None and row['valid'] == 'yes':
                                unlimited[mode] = row['rd_iops'] + row['wr_iops']
                        except (BenchBroken, CommandFailedError) as e:
                            log.error('core-hints: %s-%s-%s-%s (%s) failed: %s',
                                      size, rw, mode, rep, fraction or 'max', e)
                            self.failures.append(
                                f'{size}-{rw}-{mode}-{rep} '
                                f'({fraction or "max"}): {e}')
                            if (self.broken or
                                    len(self.failures) >= c['max_failed_runs']):
                                return
                        self.report()

    def report(self):
        if not self.rows:
            return
        text, tsv = format_table(self.rows, SUMMARY_COLUMNS)
        self._save('summary.txt', text)
        self._save('summary.tsv', tsv)
        cmp_text, cmp_tsv = format_table(compare(self.rows), COMPARISON_COLUMNS)
        self._save('comparison.txt', cmp_text)
        self._save('comparison.tsv', cmp_tsv)
        self._save('results.json', {'params': self.params, 'rows': self.rows,
                                    'failures': self.failures,
                                    'invalid': self.invalid})
        return text, cmp_text

    def final_report(self):
        r = self.report()
        if r:
            log.info('core-hints: summary:\n%s', r[0])
            log.info('core-hints: hints on vs off:\n%s', r[1])
        log.info('core-hints: results in %s', self.out)

    def cleanup(self):
        logs = os.path.join(self.out, 'logs')
        os.makedirs(logs, exist_ok=True)

        def clean(name):
            remote = self.nodes[name]['remote']
            if self.nodes[name]['clients']:
                # the fio clients' logs
                try:
                    teuthology.pull_directory(remote, f'{self.dir}/logs',
                                              os.path.join(logs, name))
                except Exception as e:
                    log.warning('core-hints: %s: no client logs: %r', name, e)
            remote.run(args=['rm', '-rf', self.dir], check_status=False)
        self._all(clean, list(self.nodes))


@contextlib.contextmanager
def task(ctx, config):
    """
    Measure the effect of core hints on client I/O to Crimson OSDs. See the
    module documentation.
    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'crimson_core_hints_perf: config must be a dict'
    overrides = ctx.config.get('overrides', {})
    teuthology.deep_merge(config, overrides.get('crimson_core_hints_perf', {}))
    c = dict(DEFAULTS)
    c.update(config)
    unknown = set(config) - set(DEFAULTS)
    if unknown:
        raise ValueError(f'crimson_core_hints_perf: unknown options {unknown}')
    for size in c['sizes']:
        parse_size(size)
    log.info('core-hints: config %s', json.dumps(c, sort_keys=True))

    bench = CoreHintsBench(ctx, c)
    try:
        bench.setup()
        bench.run()
    finally:
        try:
            bench.final_report()
        finally:
            bench.cleanup()
    if bench.failures or bench.invalid:
        raise BenchBroken('core-hints: failed runs: ' +
                          '; '.join(bench.failures + bench.invalid))
    yield
