import ipaddress
import json
import os
import socket
import subprocess
import tempfile
import time
import urllib.request

import yaml

from cephutil import ceph_cmd


def nfs_config():
    return {
        'cluster_id': os.environ.get('NFS_CLUSTER_ID', 'foo'),
        'pseudo_path': os.environ.get('NFS_PSEUDO', '/fake'),
        'mount_point': os.environ.get('NFS_MNT', '/mnt/foo'),
        'data_port': int(os.environ.get('NFS_DATA_PORT', '2049')),
        'mon_port': int(os.environ.get('NFS_MON_PORT', '9587')),
        'qos_port': int(os.environ.get('NFS_QOS_PORT', '31311')),
        'colo_data_port': int(os.environ.get('NFS_COLO_DATA_PORT', '13049')),
        'colo_mon_port': int(os.environ.get('NFS_COLO_MON_PORT', '11587')),
        'colo_qos_port': int(os.environ.get('NFS_COLO_QOS_PORT', '31312')),
        'count_per_host': int(os.environ.get('NFS_COUNT_PER_HOST', '2')),
        'bw_limit': os.environ.get('NFS_BW_LIMIT', '2MiB'),
        'bw_cluster_default': os.environ.get('NFS_BW_CLUSTER_DEFAULT', '100MiB'),
        'iops_limit': int(os.environ.get('NFS_IOPS_LIMIT', '100')),
        'iops_cluster_default': int(
            os.environ.get('NFS_IOPS_CLUSTER_DEFAULT', '10000')
        ),
        'fio_runtime': int(os.environ.get('NFS_FIO_RUNTIME', '30')),
        'tolerance_pct': int(os.environ.get('NFS_TOLERANCE_PCT', '25')),
    }


def service_name(cluster_id):
    return f"nfs.{cluster_id}"


def wait_nfs_running(cluster_id, wait_for_restart=False, old_started_time=None):
    """Wait for NFS daemon to be running.

    Args:
        cluster_id: NFS cluster ID
        wait_for_restart: If True, wait for daemon restart to complete
        old_started_time: Previous daemon start time for restart detection
    """
    name = service_name(cluster_id)

    if wait_for_restart and old_started_time:
        # Wait for daemon to restart (started time changes)
        max_restart_wait = 60
        start_wait = time.time()
        while time.time() - start_wait < max_restart_wait:
            result = ceph_cmd(
                ['orch', 'ps', '--service_name', name, '--format', 'json'],
                json_output=True,
                check=False,
            )
            if result.obj:
                for daemon in result.obj:
                    new_started = daemon.get('started')
                    if new_started and new_started != old_started_time:
                        # Daemon has restarted, now wait for it to be running
                        break
            time.sleep(2)

    # Wait for daemon to be running
    while True:
        result = ceph_cmd(
            ['orch', 'ps', '--service_name', name, '--format', 'json'],
            json_output=True,
            check=False,
        )
        if result.obj:
            for daemon in result.obj:
                if daemon.get('status_desc') == 'running':
                    # Add brief stabilization delay
                    time.sleep(2)
                    return
        time.sleep(5)


def wait_nfs_running_count(cluster_id, expected):
    name = service_name(cluster_id)
    while True:
        result = ceph_cmd(
            ['orch', 'ps', '--service_name', name, '--format', 'json'],
            json_output=True,
            check=False,
        )
        running = sum(
            1 for d in (result.obj or [])
            if d.get('status_desc') == 'running'
        )
        if running >= expected:
            return
        time.sleep(5)


def get_running_daemons(cluster_id):
    result = ceph_cmd(
        ['orch', 'ps', '--service_name', service_name(cluster_id), '--format', 'json'],
        json_output=True,
    )
    return [
        d for d in (result.obj or [])
        if d.get('status_desc') == 'running'
    ]


def get_nfs_daemon_started_time(cluster_id):
    """Get the started timestamp of the first running NFS daemon."""
    daemons = get_running_daemons(cluster_id)
    if not daemons:
        return None
    return daemons[0].get('started')


def get_nfs_hostname(cluster_id):
    daemons = get_running_daemons(cluster_id)
    if not daemons:
        raise AssertionError('no running NFS daemon found')
    return daemons[0]['hostname']


def get_host_addr(hostname):
    result = ceph_cmd(['orch', 'host', 'ls', '--format', 'json'], json_output=True)
    for host in result.obj or []:
        if host['hostname'] == hostname:
            return host['addr']
    raise AssertionError(f'host {hostname!r} not found')


def cidr_for_ip(addr):
    network = ipaddress.ip_network(f'{addr}/24', strict=False)
    return str(network)


def get_nfs_daemon_name(cluster_id):
    daemons = get_running_daemons(cluster_id)
    if not daemons:
        raise AssertionError(f'no running NFS daemon found for cluster {cluster_id}')
    return daemons[0]['daemon_name']


def get_ganesha_conf(daemon_name):
    fsid = ceph_cmd(['fsid']).stdout.strip()
    path = f"/var/lib/ceph/{fsid}/{daemon_name}/etc/ganesha/ganesha.conf"
    with open(path, encoding='utf-8') as fh:
        return fh.read()


def assert_ganesha_conf_contains(conf, pattern):
    assert pattern in conf, (
        f'ganesha.conf missing expected setting: {pattern}\n{conf}'
    )


def assert_port_bound(addr, port):
    out = subprocess.check_output(
        ['ss', '-tln', f'( sport = :{port} )'],
        text=True,
    )
    target = f'{addr}:{port}'
    for line in out.splitlines()[1:]:
        if not line.strip():
            continue
        local = line.split()[3]
        if local.startswith(target):
            return
    raise AssertionError(f'no listener on {target}\n{out}')


def assert_port_listening(port):
    out = subprocess.check_output(
        ['ss', '-tln', f'( sport = :{port} )'],
        text=True,
    )
    suffix = f':{port}'
    for line in out.splitlines()[1:]:
        if not line.strip():
            continue
        local = line.split()[3]
        if local.endswith(suffix):
            return
    raise AssertionError(f'no listener on port {port}\n{out}')


def mount_and_write_test(mount_host, data_port, pseudo_path, mount_point, tag):
    if subprocess.run(['mountpoint', '-q', mount_point], check=False).returncode == 0:
        subprocess.run(['umount', mount_point], check=False)
    mount_opts = 'sync'
    if data_port:
        mount_opts = f'{mount_opts},port={data_port}'
    subprocess.run(
        ['mount', '-t', 'nfs', f'{mount_host}:{pseudo_path}', mount_point,
         '-o', mount_opts],
        check=True,
    )
    try:
        test_file = os.path.join(mount_point, f'{tag}.test')
        content = f'nfs {tag} test'
        with open(test_file, 'w', encoding='utf-8') as fh:
            fh.write(content)
        with open(test_file, encoding='utf-8') as fh:
            assert content in fh.read()
    finally:
        subprocess.run(['umount', mount_point], check=True)


def assert_monitoring_endpoint(addr, port):
    if ':' in addr and not addr.startswith('['):
        host = f'[{addr}]'
    else:
        host = addr
    url = f'http://{host}:{port}/metrics'
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            if resp.status != 200:
                raise AssertionError(
                    f'monitoring endpoint {url} returned HTTP {resp.status}'
                )
            body = resp.read(8192).decode('utf-8', errors='replace')
    except urllib.error.URLError as exc:
        raise AssertionError(
            f'failed to reach monitoring endpoint {url}: {exc}'
        ) from exc
    except OSError as exc:
        raise AssertionError(
            f'network error reaching monitoring endpoint {url}: {exc}'
        ) from exc
    if '# TYPE' not in body and '# HELP' not in body:
        raise AssertionError(
            f'monitoring endpoint {url} did not return prometheus metrics'
        )


def export_service_spec(cluster_id):
    result = ceph_cmd(
        ['orch', 'ls', '--service_name', service_name(cluster_id), '--export'],
    )
    return yaml.safe_load(result.stdout)


def apply_spec(spec):
    with tempfile.NamedTemporaryFile('w', suffix='.yaml', delete=False) as fh:
        yaml.safe_dump(spec, fh)
        path = fh.name
    try:
        ceph_cmd(['orch', 'apply', '-i', path])
    finally:
        os.unlink(path)


def apply_nfs_bind_spec(cluster_id, hostname, bind_addr, mon_addr,
                        data_port, mon_port):
    spec = {
        'service_type': 'nfs',
        'service_id': cluster_id,
        'placement': {'hosts': [hostname]},
        'ip_addrs': {hostname: bind_addr},
        'spec': {
            'port': data_port,
            'monitoring_port': mon_port,
            'monitoring_ip_addrs': {hostname: mon_addr},
        },
    }
    apply_spec(spec)
    wait_nfs_running(cluster_id)


def assert_nfs_bind_spec(cluster_id, hostname, bind_addr, mon_addr,
                         data_port, mon_port):
    spec = export_service_spec(cluster_id)
    assert spec['ip_addrs'][hostname] == bind_addr
    assert not spec.get('networks')
    assert spec['spec']['port'] == data_port
    assert spec['spec']['monitoring_port'] == mon_port
    assert spec['spec']['monitoring_ip_addrs'][hostname] == mon_addr
    assert not spec['spec'].get('monitoring_networks')


def apply_nfs_networks_spec(cluster_id, hostname, network_cidr,
                            data_port, mon_port):
    spec = {
        'service_type': 'nfs',
        'service_id': cluster_id,
        'placement': {'hosts': [hostname]},
        'networks': [network_cidr],
        'spec': {
            'port': data_port,
            'monitoring_port': mon_port,
            'monitoring_networks': [network_cidr],
        },
    }
    apply_spec(spec)
    wait_nfs_running(cluster_id)


def assert_nfs_networks_spec(cluster_id, hostname, network_cidr,
                             data_port, mon_port):
    spec = export_service_spec(cluster_id)
    assert spec['networks'] == [network_cidr]
    assert not spec.get('ip_addrs')
    assert spec['spec']['port'] == data_port
    assert spec['spec']['monitoring_port'] == mon_port
    assert spec['spec']['monitoring_networks'] == [network_cidr]
    assert not spec['spec'].get('monitoring_ip_addrs')


def verify_bind_monitoring_runtime(cluster_id, bind_addr, mon_addr, config):
    ganesha_conf = get_ganesha_conf(get_nfs_daemon_name(cluster_id))
    assert_ganesha_conf_contains(ganesha_conf, f"Bind_addr = {bind_addr}")
    assert_ganesha_conf_contains(
        ganesha_conf, f"Monitoring_Addr = {mon_addr}")
    assert_ganesha_conf_contains(
        ganesha_conf, f"NFS_Port = {config['data_port']}")
    assert_ganesha_conf_contains(
        ganesha_conf, f"Monitoring_Port = {config['mon_port']}")

    show_spec = export_service_spec(cluster_id)
    assert show_spec['spec']['port'] == config['data_port']
    assert show_spec['spec']['monitoring_port'] == config['mon_port']

    assert_port_bound(bind_addr, config['data_port'])
    assert_port_bound(mon_addr, config['mon_port'])

    mount_and_write_test(
        bind_addr, config['data_port'], config['pseudo_path'],
        config['mount_point'], 'bind',
    )
    assert_monitoring_endpoint(mon_addr, config['mon_port'])


def default_colo_ports(config, offset=1):
    return [
        config['data_port'] + offset,
        config['mon_port'] + offset,
        config['qos_port'] + offset,
    ]


def apply_nfs_colocation_spec(cluster_id, hostname, config,
                              *, custom_colocation_ports=True):
    service_spec = {
        'port': config['data_port'],
        'monitoring_port': config['mon_port'],
        'cluster_qos_port': config['qos_port'],
    }
    if custom_colocation_ports:
        service_spec['colocation_ports'] = [{
            'data_port': config['colo_data_port'],
            'monitoring_port': config['colo_mon_port'],
            'cluster_qos_port': config['colo_qos_port'],
        }]
    spec = {
        'service_type': 'nfs',
        'service_id': cluster_id,
        'placement': {
            'count_per_host': config['count_per_host'],
            'hosts': [hostname],
        },
        'spec': service_spec,
    }
    apply_spec(spec)
    wait_nfs_running_count(cluster_id, config['count_per_host'])


def assert_nfs_colocation_spec(cluster_id, hostname, config,
                               *, custom_colocation_ports=True):
    spec = export_service_spec(cluster_id)
    placement = spec['placement']
    assert placement.get('count_per_host') == config['count_per_host']
    assert hostname in placement.get('hosts', [])
    service_spec = spec['spec']
    assert service_spec['port'] == config['data_port']
    assert service_spec['monitoring_port'] == config['mon_port']
    assert service_spec['cluster_qos_port'] == config['qos_port']
    if custom_colocation_ports:
        colo_ports = service_spec['colocation_ports']
        assert len(colo_ports) == config['count_per_host'] - 1
        entry = colo_ports[0]
        assert entry['data_port'] == config['colo_data_port']
        assert entry['monitoring_port'] == config['colo_mon_port']
        assert entry['cluster_qos_port'] == config['colo_qos_port']
    else:
        assert not service_spec.get('colocation_ports')


def get_colocated_daemons(cluster_id, hostname, config, *, colo_ports=None):
    base_ports = [config['data_port'], config['mon_port'], config['qos_port']]
    if colo_ports is None:
        colo_ports = [
            config['colo_data_port'],
            config['colo_mon_port'],
            config['colo_qos_port'],
        ]
    daemons = [
        d for d in get_running_daemons(cluster_id)
        if d.get('hostname') == hostname
    ]
    assert len(daemons) == config['count_per_host'], (
        f'expected {config["count_per_host"]} running daemons on '
        f'{hostname!r}, got {len(daemons)}'
    )
    seen_ports = sorted(tuple(d.get('ports') or []) for d in daemons)
    expected_ports = sorted([base_ports, colo_ports])
    assert seen_ports == expected_ports, (
        f'daemon port sets mismatch: {seen_ports!r} != {expected_ports!r}'
    )
    by_ports = {tuple(d.get('ports') or []): d for d in daemons}
    return by_ports[tuple(base_ports)], by_ports[tuple(colo_ports)]


def verify_colocated_daemons(cluster_id, nfs_hostname, nfs_addr, config,
                             colo_ports):
    """Verify colocated daemons, configs, ports, mounts, and monitoring."""
    base_daemon, colo_daemon = get_colocated_daemons(
        cluster_id, nfs_hostname, config, colo_ports=colo_ports,
    )
    base_ports = base_daemon['ports']

    base_conf = get_ganesha_conf(base_daemon['daemon_name'])
    assert_ganesha_conf_contains(
        base_conf, f"NFS_Port = {config['data_port']}")
    assert_ganesha_conf_contains(
        base_conf, f"Monitoring_Port = {config['mon_port']}")
    assert_ganesha_conf_contains(
        base_conf, f"Cqos_Port = {config['qos_port']}")

    colo_conf = get_ganesha_conf(colo_daemon['daemon_name'])
    assert_ganesha_conf_contains(
        colo_conf, f"NFS_Port = {colo_ports[0]}")
    assert_ganesha_conf_contains(
        colo_conf, f"Monitoring_Port = {colo_ports[1]}")
    assert_ganesha_conf_contains(
        colo_conf, f"Cqos_Port = {colo_ports[2]}")

    assert_port_listening(config['data_port'])
    assert_port_listening(config['mon_port'])
    assert_port_listening(colo_ports[0])
    assert_port_listening(colo_ports[1])

    mount_and_write_test(
        nfs_hostname, base_ports[0], config['pseudo_path'],
        config['mount_point'], 'base',
    )
    mount_and_write_test(
        nfs_hostname, colo_ports[0], config['pseudo_path'],
        config['mount_point'], 'colo',
    )

    assert_monitoring_endpoint(nfs_addr, base_ports[1])
    assert_monitoring_endpoint(nfs_addr, colo_ports[1])


def get_cluster_qos(cluster_id):
    return ceph_cmd(
        ['nfs', 'cluster', 'qos', 'get', cluster_id],
        json_output=True,
    ).obj


def get_export_qos(cluster_id, pseudo_path):
    return ceph_cmd(
        ['nfs', 'export', 'qos', 'get', cluster_id, pseudo_path],
        json_output=True,
    ).obj


def assert_qos_fields(actual, expected):
    for key, value in expected.items():
        assert actual.get(key) == value, (
            f'QoS mismatch for {key}: {actual.get(key)!r} != {value!r}'
        )


def enable_cluster_qos_bw(cluster_id, write_bw, read_bw, qos_type='PerShare'):
    old_started = get_nfs_daemon_started_time(cluster_id)
    ceph_cmd([
        'nfs', 'cluster', 'qos', 'enable', 'bandwidth_control',
        cluster_id, qos_type,
        '--max_export_write_bw', write_bw,
        '--max_export_read_bw', read_bw,
    ])
    wait_nfs_running(cluster_id, wait_for_restart=True, old_started_time=old_started)
    return get_cluster_qos(cluster_id)


def disable_cluster_qos_bw(cluster_id):
    old_started = get_nfs_daemon_started_time(cluster_id)
    ceph_cmd(['nfs', 'cluster', 'qos', 'disable', 'bandwidth_control', cluster_id])
    wait_nfs_running(cluster_id, wait_for_restart=True, old_started_time=old_started)
    return get_cluster_qos(cluster_id)


def enable_cluster_qos_ops(cluster_id, max_export_iops, qos_type='PerShare'):
    old_started = get_nfs_daemon_started_time(cluster_id)
    ceph_cmd([
        'nfs', 'cluster', 'qos', 'enable', 'ops_control',
        cluster_id, qos_type,
        '--max_export_iops', str(max_export_iops),
    ])
    wait_nfs_running(cluster_id, wait_for_restart=True, old_started_time=old_started)
    return get_cluster_qos(cluster_id)


def disable_cluster_qos_ops(cluster_id):
    old_started = get_nfs_daemon_started_time(cluster_id)
    ceph_cmd(['nfs', 'cluster', 'qos', 'disable', 'ops_control', cluster_id])
    wait_nfs_running(cluster_id, wait_for_restart=True, old_started_time=old_started)
    return get_cluster_qos(cluster_id)


def set_cluster_qos_interval(cluster_id, msg_interval):
    old_started = get_nfs_daemon_started_time(cluster_id)
    ceph_cmd(['nfs', 'cluster', 'qos', 'set', cluster_id, str(msg_interval)])
    wait_nfs_running(cluster_id, wait_for_restart=True, old_started_time=old_started)
    return get_cluster_qos(cluster_id)


def enable_export_qos_bw(cluster_id, pseudo_path, write_bw, read_bw):
    ceph_cmd([
        'nfs', 'export', 'qos', 'enable', 'bandwidth_control',
        cluster_id, pseudo_path,
        '--max_export_write_bw', write_bw,
        '--max_export_read_bw', read_bw,
    ])
    max_wait = 30
    start_time = time.time()
    while time.time() - start_time < max_wait:
        qos = get_export_qos(cluster_id, pseudo_path)
        if qos.get('enable_bw_control'):
            return qos
        time.sleep(1)
    return get_export_qos(cluster_id, pseudo_path)


def disable_export_qos_bw(cluster_id, pseudo_path):
    ceph_cmd([
        'nfs', 'export', 'qos', 'disable', 'bandwidth_control',
        cluster_id, pseudo_path,
    ])
    return get_export_qos(cluster_id, pseudo_path)


def enable_export_qos_ops(cluster_id, pseudo_path, max_export_iops):
    ceph_cmd([
        'nfs', 'export', 'qos', 'enable', 'ops_control',
        cluster_id, pseudo_path,
        '--max_export_iops', str(max_export_iops),
    ])
    max_wait = 30
    start_time = time.time()
    while time.time() - start_time < max_wait:
        qos = get_export_qos(cluster_id, pseudo_path)
        if qos.get('enable_iops_control'):
            return qos
        time.sleep(1)
    return get_export_qos(cluster_id, pseudo_path)


def disable_export_qos_ops(cluster_id, pseudo_path):
    ceph_cmd([
        'nfs', 'export', 'qos', 'disable', 'ops_control',
        cluster_id, pseudo_path,
    ])
    return get_export_qos(cluster_id, pseudo_path)


def cleanup_qos(config):
    cluster_id = config['cluster_id']
    pseudo_path = config['pseudo_path']
    ceph_cmd([
        'nfs', 'export', 'qos', 'disable', 'bandwidth_control',
        cluster_id, pseudo_path,
    ], check=False)
    ceph_cmd([
        'nfs', 'export', 'qos', 'disable', 'ops_control',
        cluster_id, pseudo_path,
    ], check=False)
    ceph_cmd([
        'nfs', 'cluster', 'qos', 'disable', 'bandwidth_control',
        cluster_id,
    ], check=False)
    ceph_cmd([
        'nfs', 'cluster', 'qos', 'disable', 'ops_control',
        cluster_id,
    ], check=False)


def bw_limit_bytes(value):
    if value.endswith('MiB'):
        return int(float(value[:-3]) * 1024 * 1024)
    if value.endswith('KiB'):
        return int(float(value[:-3]) * 1024)
    raise AssertionError(f'unsupported bandwidth value {value}')


def _mount_export(pseudo_path, mount_point):
    if subprocess.run(['mountpoint', '-q', mount_point], check=False).returncode == 0:
        subprocess.run(['umount', mount_point], check=False)
    subprocess.run(
        ['mount', '-t', 'nfs', f'{socket.gethostname()}:{pseudo_path}',
         mount_point, '-o', 'sync'],
        check=True,
    )
    os.makedirs(os.path.join(mount_point, 'fio'), exist_ok=True)


def _umount_export(mount_point):
    fio_dir = os.path.join(mount_point, 'fio')
    if os.path.isdir(fio_dir):
        subprocess.run(['rm', '-rf', fio_dir], check=True)
    subprocess.run(['umount', mount_point], check=True)


def measure_write_bw(config):
    _mount_export(config['pseudo_path'], config['mount_point'])
    try:
        fio_path = os.path.join(config['mount_point'], 'fio', 'qos.test')
        proc = subprocess.run([
            'fio', '--name=qos-write', f'--filename={fio_path}',
            '--rw=write', '--bs=1M', '--size=256M', '--direct=1', '--ioengine=sync',
            '--time_based', f'--runtime={config["fio_runtime"]}',
            '--group_reporting', '--output-format=json',
        ], capture_output=True, text=True, check=True)
        job = json.loads(proc.stdout)['jobs'][0]['write']
        bw = job.get('bw_bytes', job.get('bw', 0) * 1024)
        return bw
    finally:
        _umount_export(config['mount_point'])


def measure_write_iops(config):
    _mount_export(config['pseudo_path'], config['mount_point'])
    try:
        fio_path = os.path.join(config['mount_point'], 'fio', 'qos.iops')
        proc = subprocess.run([
            'fio', '--name=qos-iops', f'--filename={fio_path}',
            '--rw=randwrite', '--bs=4k', '--size=512M', '--direct=1',
            '--ioengine=sync', '--time_based',
            f'--runtime={config["fio_runtime"]}',
            '--group_reporting', '--output-format=json',
        ], capture_output=True, text=True, check=True)
        iops = json.loads(proc.stdout)['jobs'][0]['write']['iops']
        return iops
    finally:
        _umount_export(config['mount_point'])


def assert_at_most(measured, limit, tolerance_pct):
    max_allowed = limit * (100 + tolerance_pct) / 100
    assert measured <= max_allowed, (
        f'measured {measured} exceeds limit {limit} (+{tolerance_pct}%)'
    )


def assert_less_than(left, right, msg):
    assert left < right, msg
