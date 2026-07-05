"""
Ceph teuthology task for managed NFS workunit tests.
"""
import copy
import logging
import shlex

log = logging.getLogger(__name__)

_ENV_KEYS = {
    'cluster_id': 'NFS_CLUSTER_ID',
    'fs_name': 'NFS_FS_NAME',
    'pseudo_path': 'NFS_PSEUDO',
    'mount_point': 'NFS_MNT',
    'data_port': 'NFS_DATA_PORT',
    'mon_port': 'NFS_MON_PORT',
    'qos_port': 'NFS_QOS_PORT',
    'colo_data_port': 'NFS_COLO_DATA_PORT',
    'colo_mon_port': 'NFS_COLO_MON_PORT',
    'colo_qos_port': 'NFS_COLO_QOS_PORT',
    'count_per_host': 'NFS_COUNT_PER_HOST',
    'bw_limit': 'NFS_BW_LIMIT',
    'bw_cluster_default': 'NFS_BW_CLUSTER_DEFAULT',
    'iops_limit': 'NFS_IOPS_LIMIT',
    'iops_cluster_default': 'NFS_IOPS_CLUSTER_DEFAULT',
    'fio_runtime': 'NFS_FIO_RUNTIME',
    'tolerance_pct': 'NFS_TOLERANCE_PCT',
    'bind_data_port': 'NFS_BIND_DATA_PORT',
    'bind_mon_port': 'NFS_BIND_MON_PORT',
}


def _marks(marks_value):
    if not marks_value:
        return ''
    if isinstance(marks_value, str):
        return marks_value
    if isinstance(marks_value, list):
        return ' or '.join(marks_value)
    raise ValueError(f'unexpected type: {marks_value!r}')


def _workunit_commands(
    key, values, *, default_script='nfs/nfs_tests.sh', default_target='tests'
):
    commands = []
    if isinstance(values, str):
        values = values.split()
    for value in values:
        script = default_script
        target = default_target
        custom_args = []
        if isinstance(value, str):
            marks = _marks(value)
        elif isinstance(value, list):
            marks = _marks(value)
        elif isinstance(value, dict):
            opts = value
            script = opts.get('script', script)
            target = opts.get('target', target)
            marks = _marks(opts.get('marks', []))
            custom_args = [str(v) for v in (opts.get('custom_args') or [])]
        else:
            raise ValueError(f'unexpected workunit value: {value!r}')

        cmd = [script]
        if marks:
            cmd.append('-m')
            cmd.append(marks)
        cmd += custom_args
        cmd.append(target)
        commands.append(shlex.join(cmd))
    return commands


def workunit(ctx, config):
    """Workunit wrapper with special behaviors for NFS tests."""
    from . import workunit

    _config = copy.deepcopy(config)
    clients = _config.get('clients') or {}
    env = _config.get('env') or {}

    for key, env_key in _ENV_KEYS.items():
        if key in config:
            env[env_key] = str(config[key])

    clients = {k: _workunit_commands(k, v) for k, v in clients.items()}
    _config['clients'] = clients
    _config['env'] = env
    _config['no_coverage_and_limits'] = config.get(
        'no_coverage_and_limits', True
    )
    log.info('Passing NFS workunit config: %r', _config)
    return workunit.task(ctx, _config)
