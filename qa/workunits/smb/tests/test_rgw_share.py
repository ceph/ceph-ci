import contextlib as _contextlib
import pathlib as _pathlib
import re
import time

import pytest
import smbclient as _smbclient
import smbprotocol

import cephutil
import smbutil

NtStatus = smbprotocol.header.NtStatus

SEED_NAME = 'seed.txt'
SEED_TEXT = 'hello from s3\n'

# Marker used on every test in this module
pytestmark = pytest.mark.rgw_share


def _rgw_params(smb_cfg):
    params = smb_cfg.params.get('rgw_share') or {}
    if not params:
        pytest.skip('no rgw_share test params')
    return params


def _shares(smb_cfg, readonly):
    """Return a list of (share_id, share_params) tuples for shares matching
    the readonly setting.
    """
    params = _rgw_params(smb_cfg)
    return [
        (sid, sp)
        for sid, sp in params['shares'].items()
        if bool(sp.get('readonly', False)) == readonly
    ]


def _bucket_arg(share_params):
    """Return the bucket name in the form radosgw-admin expects, including the
    tenant if there is one.
    """
    bucket = share_params['bucket']
    tenant = share_params.get('tenant')
    return f'{tenant}/{bucket}' if tenant else bucket


def _bucket_objects(smb_cfg, share_params):
    """Return the names of the objects in the rgw bucket backing a share."""
    jres = cephutil.cephadm_shell_cmd(
        smb_cfg,
        [
            'radosgw-admin',
            'bucket',
            'list',
            '--bucket',
            _bucket_arg(share_params),
        ],
        load_json=True,
    )
    return [o['name'] for o in jres.obj]


def _wait_for_object(smb_cfg, share_params, name, present, timeout=60):
    end = time.monotonic() + timeout
    while True:
        found = name in _bucket_objects(smb_cfg, share_params)
        if found == present:
            return
        if time.monotonic() > end:
            state = 'appear in' if present else 'be removed from'
            raise AssertionError(f'timed out waiting for {name} to {state} bucket')
        time.sleep(2)


def test_rgw_share_config(smb_cfg):
    cluster_id = _rgw_params(smb_cfg)['cluster_id']
    for sid, sp in _rgw_params(smb_cfg)['shares'].items():
        share = smbutil.get_share_by_id(smb_cfg, cluster_id, sid)
        assert share['rgw']['bucket'] == sp['bucket']
        assert 'cephfs' not in share
        # a credential is automatically created and linked to the share
        assert share['rgw'].get('credential_ref')
        assert bool(share.get('readonly', False)) == bool(
            sp.get('readonly', False)
        )


def test_rgw_share_lists_bucket_objects(smb_cfg):
    params = _rgw_params(smb_cfg)
    for sid, _ in params['shares'].items():
        with smbutil.connection(smb_cfg, sid) as sharep:
            assert SEED_NAME in sharep.listdir()
            assert (sharep / SEED_NAME).read_text() == SEED_TEXT


def test_rgw_share_write_visible_in_bucket(smb_cfg):
    shares = _shares(smb_cfg, readonly=False)
    assert shares
    for sid, sp in shares:
        fname = f'smb_written_{sid}.dat'
        with smbutil.connection(smb_cfg, sid) as sharep:
            fpath = sharep / fname
            fpath.write_text('HELLO RGW\n')
            try:
                assert fname in sharep.listdir()
                assert fpath.read_text() == 'HELLO RGW\n'
                # the object must have been written to the bucket itself
                _wait_for_object(smb_cfg, sp, fname, present=True)
            finally:
                fpath.unlink()
        _wait_for_object(smb_cfg, sp, fname, present=False)


def test_rgw_readonly_share_rejects_write(smb_cfg):
    shares = _shares(smb_cfg, readonly=True)
    assert shares
    for sid, sp in shares:
        fname = f'smb_denied_{sid}.dat'
        with smbutil.connection(smb_cfg, sid) as sharep:
            with smbutil.raises_nt_error(NtStatus.STATUS_ACCESS_DENIED):
                (sharep / fname).write_text('should not be written\n')
            # reads still work
            assert SEED_NAME in sharep.listdir()
        assert fname not in _bucket_objects(smb_cfg, sp)


def test_rgw_share_missing_bucket_rejected(smb_cfg):
    cluster_id = _rgw_params(smb_cfg)['cluster_id']
    share = {
        'resource_type': 'ceph.smb.share',
        'cluster_id': cluster_id,
        'share_id': 's3missing',
        'rgw': {'bucket': 'bucket-that-does-not-exist', 'user_id': 's3user1'},
    }
    cmd_res = smbutil.apply_resources_unchecked(
        smb_cfg, [share], immediate=True
    )
    assert cmd_res.returncode != 0
    jres = cmd_res.obj
    assert not jres['success']
    assert len(jres['results']) == 1
    assert 'does not exist' in jres['results'][0]['msg']


def _get_rgw_user_info(smb_cfg, user_id):
    """Fetch radosgw-admin user info for the given user_id.

    Handles tenant-aware user IDs in 'tenant$user' format by splitting the
    tenant and passing it as a separate --tenant argument, which is what
    radosgw-admin requires.
    """
    if '$' in user_id:
        tenant, uid = user_id.split('$', 1)
        cmd = ['radosgw-admin', 'user', 'info', '--uid', uid, '--tenant', tenant]
    else:
        cmd = ['radosgw-admin', 'user', 'info', '--uid', user_id]
    jres = cephutil.cephadm_shell_cmd(smb_cfg, cmd, load_json=True)
    return jres.obj


def test_rgw_credential_valid(smb_cfg):
    """Validate that the auto-created RGW credential for each share exists
    as a live radosgw key for the bucket owner.

    We resolve the credential via 'ceph smb show ceph.smb.rgw.credential.<id>'
    to get the user_id, then cross-check that the user exists and is not
    suspended in radosgw.  The actual key values are masked by 'ceph smb show'
    by design, so we validate presence via radosgw-admin rather than trying to
    read the secret from the CLI output.
    """
    params = _rgw_params(smb_cfg)
    cluster_id = params['cluster_id']
    for sid, _ in params['shares'].items():
        share = smbutil.get_share_by_id(smb_cfg, cluster_id, sid)
        assert share['rgw'].get('credential_ref'), (
            f'share {sid} has no credential_ref'
        )
        cred_id = share['rgw']['credential_ref']
        jres = cephutil.cephadm_shell_cmd(
            smb_cfg,
            [
                'ceph',
                'smb',
                'show',
                '--results=full',
                f'ceph.smb.rgw.credential.{cred_id}',
            ],
            load_json=True,
        )
        cred_resources = jres.obj.get('resources', [])
        assert cred_resources, f'no credential resource found for {cred_id}'
        cred = cred_resources[0]
        user_id = cred.get('user_id', '')
        assert user_id, f'credential {cred_id} has no user_id'
        # Verify the user exists and has at least one key in radosgw
        user_info = _get_rgw_user_info(smb_cfg, user_id)
        assert user_info.get('keys'), (
            f'radosgw user {user_id!r} (credential {cred_id}) has no keys'
        )


def test_rgw_credential_not_suspended(smb_cfg):
    """Validate that the RGW user backing each share is not suspended."""
    params = _rgw_params(smb_cfg)
    cluster_id = params['cluster_id']
    for sid in params['shares']:
        share = smbutil.get_share_by_id(smb_cfg, cluster_id, sid)
        cred_id = share['rgw'].get('credential_ref')
        if not cred_id:
            continue
        jres = cephutil.cephadm_shell_cmd(
            smb_cfg,
            [
                'ceph',
                'smb',
                'show',
                '--results=full',
                f'ceph.smb.rgw.credential.{cred_id}',
            ],
            load_json=True,
        )
        cred_resources = jres.obj.get('resources', [])
        assert cred_resources
        user_id = cred_resources[0].get('user_id', '')
        if not user_id:
            continue
        user_info = _get_rgw_user_info(smb_cfg, user_id)
        assert user_info.get('suspended', 0) == 0, (
            f'RGW user {user_id!r} for share {sid!r} is suspended'
        )


def test_rgw_tenant_share_config(smb_cfg):
    """Validate that a tenant-aware share (user_id 'tenant$user') is
    configured correctly and references the right bucket.
    """
    params = _rgw_params(smb_cfg)
    cluster_id = params['cluster_id']
    tenant_shares = [
        (sid, sp)
        for sid, sp in params['shares'].items()
        if sp.get('tenant')
    ]
    if not tenant_shares:
        pytest.skip('no tenant shares configured')
    for sid, sp in tenant_shares:
        share = smbutil.get_share_by_id(smb_cfg, cluster_id, sid)
        assert share['rgw']['bucket'] == sp['bucket']
        assert share['rgw'].get('credential_ref'), (
            f'tenant share {sid} has no credential_ref'
        )
        # Seed object must be readable through the tenant share
        with smbutil.connection(smb_cfg, sid) as sharep:
            assert SEED_NAME in sharep.listdir()
            assert (sharep / SEED_NAME).read_text() == SEED_TEXT


def test_rgw_share_delete_removes_bucket_object(smb_cfg):
    """Verify that deleting a file over SMB removes the corresponding
    object from the RGW bucket.
    """
    shares = _shares(smb_cfg, readonly=False)
    assert shares
    sid, sp = shares[0]
    fname = f'smb_delete_test_{sid}.dat'
    with smbutil.connection(smb_cfg, sid) as sharep:
        fpath = sharep / fname
        fpath.write_text('to be deleted\n')
        _wait_for_object(smb_cfg, sp, fname, present=True)
        fpath.unlink()
    _wait_for_object(smb_cfg, sp, fname, present=False)


def test_rgw_share_overwrite_updates_bucket_object(smb_cfg):
    """Verify that overwriting an existing file via SMB is reflected when
    reading back via SMB.
    """
    shares = _shares(smb_cfg, readonly=False)
    assert shares
    sid, sp = shares[0]
    fname = f'smb_overwrite_{sid}.dat'
    second_content = 'second version\n'
    with smbutil.connection(smb_cfg, sid) as sharep:
        fpath = sharep / fname
        fpath.write_text('first version\n')
        try:
            _wait_for_object(smb_cfg, sp, fname, present=True)
            fpath.write_text(second_content)
            assert fpath.read_text() == second_content
        finally:
            fpath.unlink()
    _wait_for_object(smb_cfg, sp, fname, present=False)


def test_rgw_share_large_file_roundtrip(smb_cfg):
    """Write a file larger than a typical SMB read-size to exercise
    multi-chunk I/O through the RGW VFS layer.
    """
    shares = _shares(smb_cfg, readonly=False)
    assert shares
    sid, sp = shares[0]
    fname = f'smb_large_{sid}.dat'
    # 4 MiB of repeated pattern
    chunk = b'A' * (4 * 1024 * 1024)
    with smbutil.connection(smb_cfg, sid) as sharep:
        fpath = sharep / fname
        fpath.write_bytes(chunk)
        try:
            _wait_for_object(smb_cfg, sp, fname, present=True)
            assert fpath.stat().st_size == len(chunk)
        finally:
            fpath.unlink()
    _wait_for_object(smb_cfg, sp, fname, present=False)


def test_rgw_share_subdir_create_and_list(smb_cfg):
    """Verify that creating a sub-directory and writing a file inside it
    works correctly (RGW VFS represents directories as key prefixes).
    """
    shares = _shares(smb_cfg, readonly=False)
    assert shares
    sid, _ = shares[0]
    dname = f'smb_subdir_{sid}'
    fname = 'inner.txt'
    with smbutil.connection(smb_cfg, sid) as sharep:
        subdir = sharep / dname
        subdir.mkdir(exist_ok=True)
        try:
            assert dname in sharep.listdir()
            inner = subdir / fname
            inner.write_text('inside subdir\n')
            try:
                assert fname in subdir.listdir()
                assert inner.read_text() == 'inside subdir\n'
            finally:
                inner.unlink()
        finally:
            subdir.rmdir()


# ---------------------------------------------------------------------------
# Multi-node / CTDB tests
# All tests below carry the rgw_share marker (via module-level pytestmark)
# and run only when the deploy YAML passes [rgw_share] in the clients list.
# ---------------------------------------------------------------------------


@_contextlib.contextmanager
def _connection_to(server_ip, share, conf, username=None, password=None):
    """Like smbutil.connection() but targets an explicit server IP so
    cross-node tests can connect to any CTDB node, not just nodes[0].
    """
    port = 445
    username = conf.username if username is None else username
    password = conf.password if password is None else password
    _smbclient.register_session(
        server=server_ip,
        port=port,
        username=username,
        password=password,
    )
    try:
        spath = _pathlib.PureWindowsPath(f'//{server_ip}/{share}')
        yield smbutil.PathWrapper(spath)
    finally:
        _smbclient.delete_session(server_ip, port)


def _skip_if_single_node(smb_cfg):
    """Skip the test when fewer than 2 CTDB nodes are present in the config."""
    if len(smb_cfg.servers) < 2:
        pytest.skip('multi-node test requires at least 2 smb_nodes')


def test_rgw_ctdb_cluster_healthy(smb_cfg):
    """After deploying RGW shares with placement count:3, CTDB reports
    all three nodes as OK.

    Reuses the same poll-and-match approach as test_ctdb_cluster_healthy
    in test_ctdb_clustering.py but drives it from the RGW task.
    """
    _skip_if_single_node(smb_cfg)
    cluster_id = _rgw_params(smb_cfg)['cluster_id']
    matchers = [
        re.compile(r'pnn:0 .*OK'),
        re.compile(r'pnn:1 .*OK'),
        re.compile(r'pnn:2 .*OK'),
        re.compile(r'Number of nodes:3'),
    ]
    end = time.monotonic() + 120  # up to 2 min after orch says service is up
    last_status = ''
    while True:
        result = cephutil.cephadm_enter_cmd(
            smb_cfg,
            cluster_id,
            ['ctdb', 'status'],
            capture_output=True,
            check=True,
        )
        last_status = result.stdout.decode()
        if all(m.search(last_status) for m in matchers):
            return
        if time.monotonic() > end:
            break
        time.sleep(10)
    raise AssertionError(
        f'CTDB cluster {cluster_id} did not converge with 3 OK nodes.\n'
        f'Last status:\n{last_status}'
    )


def test_rgw_share_credential_on_all_nodes(smb_cfg):
    """Verify that the private-store RGW credential stub was delivered to
    every CTDB node: each node can serve the share (i.e. list it), which
    requires having the correct access_key / secret_key in its running config.
    """
    _skip_if_single_node(smb_cfg)
    shares = _shares(smb_cfg, readonly=False)
    assert shares, 'no writable share in params'
    sid, _ = shares[0]

    for server in smb_cfg.servers:
        with _connection_to(server.ip_address, sid, smb_cfg) as sharep:
            listing = sharep.listdir()
            assert isinstance(listing, list), (
                f'listdir on node {server.ip_address} did not return a list'
            )
            assert SEED_NAME in listing, (
                f'seed object missing on node {server.ip_address}'
            )


def test_rgw_share_cross_node_read_after_write(smb_cfg):
    """Write a file via the first CTDB node, then read it back via a
    different node.  Both nodes must resolve to the same RGW bucket object,
    confirming that the credential propagated and the S3 namespace is shared.
    """
    _skip_if_single_node(smb_cfg)
    shares = _shares(smb_cfg, readonly=False)
    assert shares, 'no writable share in params'
    sid, sp = shares[0]

    servers = smb_cfg.servers
    writer_ip = servers[0].ip_address
    reader_ip = servers[-1].ip_address  # last node — different from writer

    fname = f'smb_xnode_{sid}.dat'
    content = 'cross-node RGW write\n'

    # write on writer node
    with _connection_to(writer_ip, sid, smb_cfg) as sharep:
        (sharep / fname).write_text(content)

    try:
        # object must appear in the bucket
        _wait_for_object(smb_cfg, sp, fname, present=True)

        # read back on reader node
        with _connection_to(reader_ip, sid, smb_cfg) as sharep:
            assert fname in sharep.listdir(), (
                f'{fname} not visible on reader node {reader_ip}'
            )
            assert (sharep / fname).read_text() == content, (
                f'content mismatch on reader node {reader_ip}'
            )
    finally:
        # clean up via writer node
        with _connection_to(writer_ip, sid, smb_cfg) as sharep:
            (sharep / fname).unlink()
        _wait_for_object(smb_cfg, sp, fname, present=False)


def test_rgw_share_cross_node_delete(smb_cfg):
    """Write a file via one CTDB node and delete it via another.
    The bucket object must disappear after the cross-node delete.
    """
    _skip_if_single_node(smb_cfg)
    shares = _shares(smb_cfg, readonly=False)
    assert shares, 'no writable share in params'
    sid, sp = shares[0]

    servers = smb_cfg.servers
    writer_ip = servers[0].ip_address
    deleter_ip = servers[-1].ip_address

    fname = f'smb_xnode_del_{sid}.dat'

    with _connection_to(writer_ip, sid, smb_cfg) as sharep:
        (sharep / fname).write_text('to be cross-deleted\n')

    _wait_for_object(smb_cfg, sp, fname, present=True)

    with _connection_to(deleter_ip, sid, smb_cfg) as sharep:
        (sharep / fname).unlink()

    _wait_for_object(smb_cfg, sp, fname, present=False)


def test_rgw_readonly_share_on_all_nodes(smb_cfg):
    """Every CTDB node must enforce read-only on the read-only share:
    writes must be denied and reads must succeed on every node.
    """
    _skip_if_single_node(smb_cfg)
    shares = _shares(smb_cfg, readonly=True)
    if not shares:
        pytest.skip('no read-only RGW share configured')
    sid, sp = shares[0]

    for server in smb_cfg.servers:
        fname = f'smb_ro_node_{server.ip_address}_{sid}.dat'
        with _connection_to(server.ip_address, sid, smb_cfg) as sharep:
            with smbutil.raises_nt_error(NtStatus.STATUS_ACCESS_DENIED):
                (sharep / fname).write_text('should not land\n')
            # reads must still work on this node
            assert SEED_NAME in sharep.listdir(), (
                f'seed object not readable on node {server.ip_address}'
            )
        assert fname not in _bucket_objects(smb_cfg, sp), (
            f'file unexpectedly written to bucket from node {server.ip_address}'
        )
