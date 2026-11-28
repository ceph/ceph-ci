"""
Unit tests of the analysis in crimson_core_hints_perf.py, on synthetic data.

    cd ceph/qa
    pytest tasks/tests/test_crimson_core_hints_perf.py
"""
import json
import subprocess
import sys

import pytest

from tasks.crimson_core_hints_perf import (
    COLLECTOR, COMPARISON_COLUMNS, SUMMARY_COLUMNS, compare, fio_job,
    format_table, merge_fio, osd_deltas, parse_fio_output, parse_size,
    percentile_from_bins, read_failures, sum_client_counters, summarize)


def test_parse_size():
    assert parse_size('1k') == 1024
    assert parse_size('256K') == 256 * 1024
    assert parse_size('4m') == 4 << 20
    assert parse_size('4096') == 4096
    with pytest.raises(ValueError):
        parse_size('lots')


def test_fio_job():
    job = fio_job('/c.conf', 'pool', 'ch_c0_4k', 'randread', 4096, 8, 16,
                  256, ['time_based=1'])
    lines = job.splitlines()
    assert lines[0] == '[global]'
    # a whole object per I/O: size = objects * object size
    assert 'size=1048576' in lines
    assert 'bs=4096' in lines
    assert 'nrfiles=256' in lines
    assert 'conf=/c.conf' in lines
    assert 'time_based=1' in lines
    # the job (named after the client and size) comes last
    assert lines[-2:] == ['[ch_c0_4k]', 'rw=randread']


def test_parse_fio_output():
    assert parse_fio_output('note: x\n{"jobs": []}') == {'jobs': []}
    with pytest.raises(ValueError):
        parse_fio_output('fio: engine rados not loadable')


def test_percentile_from_bins():
    bins = {100: 50, 200: 49, 1000: 1}
    assert percentile_from_bins(bins, 50) == 100
    assert percentile_from_bins(bins, 99) == 200
    assert percentile_from_bins(bins, 99.9) == 1000
    assert percentile_from_bins({}, 99) == 0


def _fio(rd_ios, rd_mean_ns, rd_bins, wr_ios=0, wr_mean_ns=0, wr_bins=None,
         secs=10):
    def ddir(ios, mean, bins):
        clat = {'mean': mean,
                'percentile': {'99.000000': max(bins) if bins else 0}}
        if bins is not None:
            clat['bins'] = {str(k): v for k, v in bins.items()}
        return {'total_ios': ios, 'iops': ios / secs, 'clat_ns': clat}
    return {'jobs': [{'error': 0,
                      'read': ddir(rd_ios, rd_mean_ns, rd_bins),
                      'write': ddir(wr_ios, wr_mean_ns, wr_bins or {})}]}


def test_merge_fio_exact():
    a = _fio(1000, 100_000, {100_000: 990, 2_000_000: 10})
    b = _fio(3000, 200_000, {200_000: 3000})
    m = merge_fio([a, b])
    rd = m['read']
    assert rd['iops'] == 400
    assert rd['ios'] == 4000
    # weighted by the ops: (1000 * 100 + 3000 * 200) / 4000 us
    assert rd['mean_us'] == pytest.approx(175)
    assert rd['exact']
    assert rd['p50_us'] == 200
    # 10 slow ops of 4000: above the 99th percentile, under the 99.9th
    assert rd['p99_us'] == 200
    assert rd['p999_us'] == 2000
    assert m['write']['iops'] == 0
    assert m['write']['p99_us'] == 0


def test_merge_fio_without_bins():
    a = _fio(1000, 100_000, None)
    a['jobs'][0]['read']['clat_ns']['percentile'] = {'99.000000': 300_000}
    b = _fio(1000, 100_000, None)
    b['jobs'][0]['read']['clat_ns']['percentile'] = {'99.000000': 500_000}
    rd = merge_fio([a, b])['read']
    assert not rd['exact']
    # the worst client's
    assert rd['p99_us'] == 500


def _host(t, osds=None, tcp=(0, 0), ticks=0, clients=None):
    return {'t': t, 'tcp': list(tcp), 'fio_ticks': ticks, 'clk_tck': 100,
            'osds': osds or {}, 'clients': clients or {}}


def _osd(ops, reactor):
    return {'ops': ops, 'reactor': reactor}


def _snapshots():
    s0 = {
        'osdhost': _host(100.0, {
            '0': _osd({'0': [0, 0], '1': [10, 10]},
                      {'0': [0, 0, 0, 0, 0], '1': [100, 10, 10, 0, 0]})},
            tcp=(0, 1000)),
        'clienthost': _host(100.5, ticks=0),
    }
    s1 = {
        'osdhost': _host(110.0, {
            '0': _osd({'0': [900, 100], '1': [10, 10]},
                      {'0': [5000, 3000, 10000, 1024000, 0],
                       '1': [1100, 10, 10, 0, 0]})},
            tcp=(0, 3000)),
        'clienthost': _host(110.5, ticks=2000, clients={
            '/a.asok': {'op_send': 600, 'op_send_core': 540,
                        'op_send_switch_pending': 6, 'core_switch_hold': 2,
                        'core_switch_hold_expired': 1, 'op_unanswered': 3,
                        'op_unanswered_forgotten': 0,
                        'core_switch_delay': {'avgcount': 2, 'sum': 0.01,
                                              'max_inc': 0.008}},
            '/b.asok': {'op_send': 400, 'op_send_core': 360},
            '/c.asok': {'error': 'ConnectionRefusedError()'},
        }),
    }
    return s0, s1


def test_osd_deltas():
    s0, s1 = _snapshots()
    rows = osd_deltas(s0, s1)
    assert len(rows) == 2
    r0, r1 = rows
    # osd, shard, local, remote, busy %, then the raw deltas
    assert r0[:4] == [0, 0, 900, 100]
    assert r0[4] == pytest.approx(50.0)     # 5000 ms busy over 10 s
    assert r0[5:] == [5000, 3000, 10000, 1024000, 0]
    assert r1[:4] == [0, 1, 0, 0]
    assert r1[4] == pytest.approx(10.0)


def test_sum_client_counters():
    _, s1 = _snapshots()
    c = sum_client_counters(s1)
    assert c['clients'] == 2
    assert c['errors'] == 1
    assert c['op_send'] == 1000
    assert c['op_send_core'] == 900
    assert c['delay_n'] == 2
    assert c['delay_max'] == pytest.approx(0.008)


def test_summarize():
    s0, s1 = _snapshots()
    fio = merge_fio([_fio(1000, 100_000, {100_000: 1000})])
    meta = {'size': '4k', 'workload': 'randread', 'mode': 'on', 'rep': 1}
    row, cores = summarize(meta, fio, s0, s1, sum_client_counters(s1),
                           ['osdhost'], ['clienthost'])
    assert row['size'] == '4k'
    assert row['osd_ops'] == 1000
    assert row['osd_hop_pct'] == pytest.approx(10.0)
    assert row['cpu_avg_pct'] == pytest.approx(30.0)
    assert row['cpu_max_pct'] == pytest.approx(50.0)
    # 6000 busy ms over 1000 ops
    assert row['cpu_us_op'] == pytest.approx(6000.0)
    assert row['tasks_op'] == pytest.approx(10.0)
    assert row['polls_op'] == pytest.approx(3.0)
    assert row['segs_op'] == pytest.approx(2.0)
    assert row['tx_kb_op'] == pytest.approx(1.0)
    # 2000 ticks at 100/s over 10 s
    assert row['clnt_cores'] == pytest.approx(2.0)
    assert row['core_send_pct'] == pytest.approx(90.0)
    assert row['pend_pct'] == pytest.approx(0.6)
    assert row['holds'] == '2/1'
    assert row['sw_delay_ms'] == '5.0/8.0'
    assert row['unans'] == '3/0'
    assert row['clients'] == 2
    text, tsv = format_table([row], SUMMARY_COLUMNS)
    assert len(text.splitlines()) == 2
    assert tsv.splitlines()[0].split('\t')[0] == 'size'


def test_compare():
    def row(mode, rep, iops, p99, cpu_op, hop, core):
        return {'size': '4k', 'workload': 'randread', 'mode': mode,
                'rep': rep, 'rd_iops': iops, 'wr_iops': 0,
                'rd_p50_us': 100, 'rd_p99_us': p99, 'wr_p50_us': 0,
                'wr_p99_us': 0, 'cpu_us_op': cpu_op, 'cpu_max_pct': 50,
                'osd_hop_pct': hop, 'core_send_pct': core}
    rows = [row('off', 1, 1000, 400, 20.0, 93.0, 0.0),
            row('on', 1, 1100, 300, 17.0, 1.0, 99.0),
            # no 'on' run for rep 2: not compared
            row('off', 2, 1000, 400, 20.0, 93.0, 0.0)]
    out = compare(rows)
    assert len(out) == 1
    c = out[0]
    assert c['iops_delta'] == pytest.approx(10.0)
    assert c['cpu_op_delta'] == pytest.approx(-15.0)
    assert c['rd_p99'] == '400->300'
    assert c['hop'] == '93.0->1.0'
    text, _ = format_table(out, COMPARISON_COLUMNS)
    assert '+10.0' in text


def test_collector(tmp_path):
    """the node-side collector, on this host: no OSDs, no clients"""
    script = tmp_path / 'collect.py'
    script.write_text(COLLECTOR)
    host = json.loads(subprocess.check_output(
        [sys.executable, str(script), 'host']))
    assert host['cpus'] >= 1
    assert host['cpu_siblings']
    log = tmp_path / 'osd.log'
    # the needle across the collector's read chunks is not tested here
    log.write_text('x std::bad_alloc\nbad_alloc\n'
                   'osd - full-object read crc 1 != expected 2 on x\n')
    snap = json.loads(subprocess.check_output(
        [sys.executable, str(script), 'snap',
         json.dumps({'osd_logs': {'0': str(log),
                                  '1': str(tmp_path / 'missing.log')},
                     'client_glob': str(tmp_path / '*.asok')})]))
    assert snap['logs'] == {'0': {'bad_alloc': 2, 'read_crc': 1},
                            '1': {'bad_alloc': -1, 'read_crc': -1}}
    assert snap['clients'] == {}
    assert len(snap['tcp']) == 2


def test_read_failures():
    def snap(crc):
        return {'osdhost': {'logs': {'0': {'read_crc': crc, 'bad_alloc': 0}}}}
    # randrw 70% reads of 4k objects: at least 2867 bytes sent per op
    row = {'rd_iops': 700, 'wr_iops': 300, 'osd_ops': 1000, 'tx_kb_op': 3.0}
    assert read_failures(dict(row), snap(5), snap(5), 4096) is None
    r = dict(row)
    assert 'digest' in read_failures(r, snap(5), snap(9), 4096)
    assert r['rd_crc_errs'] == 4
    # no read errors logged, but (almost) no data sent: the reads failed
    r = dict(row, tx_kb_op=0.3)
    assert 'under half' in read_failures(r, snap(0), snap(0), 4096)
    # writes only: nothing to check
    r = dict(row, rd_iops=0, wr_iops=1000, tx_kb_op=0.1)
    assert read_failures(r, snap(0), snap(0), 4096) is None


def test_collector_osds(tmp_path):
    """the collector's OSD info, for an OSD not running here"""
    script = tmp_path / 'collect.py'
    script.write_text(COLLECTOR)
    osds = json.loads(subprocess.check_output(
        [sys.executable, str(script), 'osds',
         json.dumps({'99': str(tmp_path / 'osd.99.asok')})]))
    assert osds['99']['pid'] is None
    assert osds['99']['reactors'] == {}
    assert osds['99']['crimson_cpu_set'].startswith('error')
