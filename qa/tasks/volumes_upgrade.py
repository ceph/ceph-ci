"""
mgr/volumes upgrade sub-tasks: verify that auto-upgrade of v2 subvolumes to
the v3 layout does not disrupt subvolume clients doing IO, that async clones
and purges started before an upgrade finish successfully after it, that v2
subvolumes keep working as such while only ceph-mgr is upgraded, and that
snapshots taken before and after auto-upgrade are all listed.

Run these in order (see qa/suites/fs/upgrade/volumes):

  - volumes_upgrade.setup             (on the old release)
  - volumes_upgrade.verify_mgr_only   (only after a mgr-only upgrade)
  - volumes_upgrade.check_io          (any time after setup)
  - volumes_upgrade.post_upgrade      (after the whole cluster is upgraded)
"""

import json
import logging
import re
import time
from configparser import ConfigParser
from textwrap import dedent

from teuthology import misc
from teuthology.contextutil import safe_while
from teuthology.orchestra import run

log = logging.getLogger(__name__)

VOLNAME = 'cephfs'
SNAPNAME = 'snap0'
SNAPNAME_2 = 'snap1'
# taken (on the clone source) by the new mgr while the MDS is still old
SNAP_MGR_ONLY = 'snap_mgr_only'
# taken on every subvolume after auto-upgrade
SNAP_V3 = 'snap_v3'
TRASH_DIR = 'volumes/_deleting'
STATE_KEY = 'volumes-upgrade-state'

# (name, group) of the subvolumes this task creates
IO_SUBVOLS = [('sv_io_0', None), ('sv_io_1', None), ('sv_io_2', 'grp_io')]
CLONE_SRC = ('sv_clone_src', 'grp_src')
CLONES = [('sv_clone_0', None), ('sv_clone_1', None), ('sv_clone_2', 'grp_clone')]
PURGES = [('sv_purge_0', None), ('sv_purge_1', None), ('sv_purge_2', 'grp_purge')]
# v2 snapshots taken on the IO subvolumes, while their clients are writing
IO_V2_SNAPS = [[], ['snap_v2_0'], ['snap_v2_0', 'snap_v2_1']]

# Runs on the client node as root, from within the subvolume mount. Writes
# files until the stop file appears, periodically re-verifying recent writes
# and doing namespace operations. Any failure (e.g., EACCES/ESTALE after the
# subvolume's data dir gets renamed during auto-upgrade) makes it exit
# non-zero. The checksum manifest is kept outside CephFS on purpose.
WRITER_SCRIPT = dedent('''\
    set -u
    mnt=$1
    state=$2
    manifest=$state/manifest
    fail() { echo "$(date -u +%FT%T) writer: $*" >&2; exit 1; }
    : > "$manifest" || fail "creating manifest"
    cd "$mnt" || fail "cd $mnt"
    mkdir -p io || fail "mkdir io"
    i=0
    while [ ! -e "$state/stop" ]; do
        i=$((i + 1))
        f=io/f.$i
        dd if=/dev/urandom of=$f.tmp bs=4k count=4 conv=fsync status=none || fail "write $f.tmp"
        mv $f.tmp $f || fail "rename $f.tmp"
        md5sum $f >> "$manifest" || fail "checksum $f"
        if [ $((i % 20)) -eq 0 ]; then
            tail -n 20 "$manifest" | md5sum -c --quiet || fail "verify at $i"
            mkdir io/d.$i && rmdir io/d.$i || fail "mkdir/rmdir io/d.$i"
            ls io > /dev/null || fail "readdir"
            stat . > /dev/null || fail "stat"
        fi
        echo $i > "$state/progress"
        sleep 0.5
    done
    md5sum -c --quiet "$manifest" || fail "final verify"
    n=$(find io -maxdepth 1 -name "f.*" ! -name "*.tmp" | wc -l)
    [ "$n" -eq "$i" ] || fail "found $n files, wrote $i"
    echo "writer: wrote and verified $i files"
''')


class Subvol:
    def __init__(self, name, group):
        self.name = name
        self.group = group
        self.base = f'volumes/{group or "_nogroup"}/{name}'
        # path to v2 data dir, relative to fs root
        self.v2_path = None

    @property
    def grp_arg(self):
        return f' --group_name {self.group}' if self.group else ''

    def __str__(self):
        return f'{self.group or "_nogroup"}/{self.name}'


class State:
    def __init__(self, ctx, config):
        self.manager = ctx.managers['ceph']
        self.admin = _get_mount(ctx, config.get('admin_client', 'client.3'))
        self.io_mounts = [_get_mount(ctx, c) for c in
                          config.get('io_clients',
                                     ['client.0', 'client.1', 'client.2'])]
        assert len(self.io_mounts) == len(IO_SUBVOLS)
        self.testdir = misc.get_testdir(ctx)

        self.io_svs = [Subvol(*s) for s in IO_SUBVOLS]
        self.src = Subvol(*CLONE_SRC)
        self.clones = [Subvol(*s) for s in CLONES]
        self.purges = [Subvol(*s) for s in PURGES]

        self.clone_src_files = config.get('clone_src_files', 5000)
        self.purge_files = config.get('purge_files', 5000)
        # subvolume name -> names of snapshots it is expected to have
        self.snaps = {}
        # (subvolume name, snapshot name) -> manifest of the snapshot's data
        self.snap_manifests = {}
        # clones issued after setup: (clone, source, snapshot)
        self.late_clones = []
        # one entry per io_svs: dict(mount, statedir, proc)
        self.writers = []


def _get_mount(ctx, role):
    _, _, id_ = misc.split_role(role)
    return ctx.mounts[id_]


def _state(ctx):
    return ctx[STATE_KEY]


def _ceph(st, cmd, check_status=True):
    p = st.manager.ceph(cmd, check_status=check_status)
    return p.exitstatus, p.stdout.getvalue()


def _admin_sh(st, payload, timeout=900):
    """
    Run payload as root via bash, from the root of the admin mount. The
    payload must not contain single quotes.
    """
    p = st.admin.run_shell_payload(payload, sudo=True, timeout=timeout)
    return p.stdout.getvalue()


def _wait_for(desc, pred, timeout, sleep=5):
    with safe_while(sleep=sleep, tries=max(1, timeout // sleep),
                    action=desc) as proceed:
        while proceed():
            if pred():
                return


def _set_paused(st, cloning=None, purging=None):
    if cloning is not None:
        _ceph(st, f'config set mgr mgr/volumes/pause_cloning {str(cloning).lower()}')
    if purging is not None:
        _ceph(st, f'config set mgr mgr/volumes/pause_purging {str(purging).lower()}')
    # config_notify() is asynchronous
    time.sleep(5)


def _count_files(st, path):
    out = _admin_sh(st, f'test -d {path} && find {path} -type f | wc -l || echo 0')
    return int(out.strip())


def _read_meta(st, sv, meta='.meta'):
    out = _admin_sh(st, f'cat {sv.base}/{meta}')
    cp = ConfigParser()
    cp.read_string(out)
    return cp


def _getpath(st, sv, check_status=True):
    rc, out = _ceph(st, f'fs subvolume getpath {VOLNAME} {sv.name}{sv.grp_arg}',
                    check_status=check_status)
    return rc, out.strip()


def _manifest(st, path):
    return _admin_sh(st, f'cd {path} && find . -type f -print0 | sort -z | '
                         'xargs -0 -r md5sum')


def _populate(st, path, nfiles, files_per_dir=100):
    ndirs = max(1, nfiles // files_per_dir)
    _admin_sh(st, dedent(f'''\
        set -e
        cd {path}
        for d in $(seq 1 {ndirs}); do
            mkdir -p d.$d
            for f in $(seq 1 {files_per_dir}); do
                head -c 4096 /dev/urandom > d.$d/f.$f
            done
        done
    '''), timeout=3600)


# ----- subvolume layout checks -----


def _assert_v2(st, sv):
    """
    The subvolume is in v2 layout and untouched by any (failed) auto-upgrade
    attempt.
    """
    ftype = _admin_sh(st, f'stat -c %F {sv.base}/.meta').strip()
    assert ftype == 'regular file', f'{sv}: .meta is a {ftype}'
    meta = _read_meta(st, sv)
    version = meta.get('GLOBAL', 'version')
    assert version == '2', f'{sv}: version = {version}'
    path = meta.get('GLOBAL', 'path')
    assert path == f'/{sv.v2_path}', f'{sv}: meta path = {path}'
    out = _admin_sh(st, f'test -d {sv.v2_path} && echo yes; '
                        f'test -e {sv.base}/roots && echo yes || echo no')
    assert out.split() == ['yes', 'no'], \
        f'{sv}: half-upgraded subvolume (v2 dir, roots dir present?: {out.split()})'


def _assert_v3(st, sv):
    """
    Trigger auto-upgrade (if not done already) through getpath and verify the
    subvolume is in v3 layout. A v2 subvolume, with or without snapshots, is
    upgraded in place: its data dir is moved under roots/ keeping its uuid
    (so that clients need not remount), while its v2 snapshots stay where
    they were (<subvolume>/.snap).
    """
    _, path = _getpath(st, sv)
    m = re.fullmatch(rf'/{re.escape(sv.base)}/roots/([0-9a-f-]{{36}})/mnt', path)
    assert m, f'{sv}: getpath returned {path}, expected a v3 path'
    uuid = m.group(1)

    link = _admin_sh(st, f'readlink {sv.base}/.meta').strip()
    assert link == f'.meta.{uuid}', f'{sv}: .meta -> {link}, uuid = {uuid}'

    meta = _read_meta(st, sv)
    glob = dict(meta.items('GLOBAL'))
    log.info(f'{sv}: v3 meta = {glob}')
    assert glob.get('version') == '3', f'{sv}: meta = {glob}'
    assert glob.get('path') == path, f'{sv}: meta = {glob}'
    assert glob.get('state') == 'complete', f'{sv}: meta = {glob}'

    if sv.v2_path:
        v2_uuid = sv.v2_path.rsplit('/', 1)[1]
        assert uuid == v2_uuid, f'{sv}: uuid changed {v2_uuid} -> {uuid}'
        v2_dir = _admin_sh(st, f'test -e {sv.v2_path} && echo yes || echo no').strip()
        assert v2_dir == 'no', f'{sv}: v2 data dir {sv.v2_path} still exists'
    return path.lstrip('/')


def _assert_v2_usable(st, sv):
    """
    The subvolume is still v2 and subvolume commands on it work as usual.
    """
    _, path = _getpath(st, sv)
    assert path == f'/{sv.v2_path}', f'{sv}: getpath returned {path}'
    _, out = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}')
    info = json.loads(out)
    assert info['path'] == f'/{sv.v2_path}', f'{sv}: info = {info}'
    _assert_snaps(st, sv)
    _assert_v2(st, sv)


# ----- snapshots -----


def _snap_create(st, sv, snap, v2=False):
    _ceph(st, f'fs subvolume snapshot create {VOLNAME} {sv.name} {snap}{sv.grp_arg}')
    st.snaps.setdefault(sv.name, set()).add(snap)
    if v2:
        # v2 snapshots are taken a level above the data (uuid) dir
        v2_uuid = sv.v2_path.rsplit('/', 1)[1]
        st.snap_manifests[(sv.name, snap)] = \
            _manifest(st, f'{sv.base}/.snap/{snap}/{v2_uuid}')


def _assert_snaps(st, sv):
    """
    Snapshot listing has exactly the snapshots taken so far (v2 as well as
    v3 ones), and each of them can be queried.
    """
    _, out = _ceph(st, f'fs subvolume snapshot ls {VOLNAME} {sv.name}{sv.grp_arg}')
    names = {s['name'] for s in json.loads(out)}
    expected = st.snaps.get(sv.name, set())
    assert names == expected, f'{sv}: snapshots = {names}, expected {expected}'
    for snap in names:
        _ceph(st, f'fs subvolume snapshot info {VOLNAME} {sv.name} {snap}{sv.grp_arg}')


# ----- IO writers -----


def _start_writer(st, mount, sv):
    statedir = f'{st.testdir}/volumes_upgrade.io.{sv.name}'
    script = f'{st.testdir}/volumes_upgrade.writer.sh'
    remote = mount.client_remote
    remote.run(args=['sudo', 'rm', '-rf', statedir])
    remote.run(args=['mkdir', '-p', statedir])
    remote.write_file(script, WRITER_SCRIPT)
    proc = remote.run(
        args=['sudo', 'stdin-killer', '--timeout=60', '--',
              'bash', script, mount.mountpoint, statedir],
        wait=False, stdin=run.PIPE, label=f'writer {sv}')
    return dict(mount=mount, sv=sv, statedir=statedir, proc=proc)


def _writer_progress(w):
    if w['proc'].finished:
        # surfaces the writer's exit status
        w['proc'].wait()
        raise RuntimeError(f'writer for {w["sv"]} exited prematurely')
    remote = w['mount'].client_remote
    out = remote.sh(f'cat {w["statedir"]}/progress 2>/dev/null || echo 0')
    return int(out.strip())


def _wait_for_writer_progress(w, nfiles, timeout=300):
    start = _writer_progress(w)
    _wait_for(f'writer for {w["sv"]} to write {nfiles} files past {start}',
              lambda: _writer_progress(w) >= start + nfiles, timeout)


def _remount_and_verify(w, path, client_id, **creds):
    """
    Remount the client at the subvolume's (new) v3 path with the given
    credentials (client_keyring or client_keyring_path), and verify that all
    the data written by the writer is accessible and intact, and that the
    subvolume is writable.
    """
    mount, sv = w['mount'], w['sv']
    log.info(f'{sv}: remounting as client.{client_id} at /{path}')
    mount.remount(client_id=client_id, cephfs_mntpt=f'/{path}', **creds)
    manifest = f'{w["statedir"]}/manifest'
    mount.run_shell(['sudo', 'md5sum', '-c', '--quiet', manifest], timeout=1800)
    nlines = int(mount.client_remote.sh(f'wc -l < {manifest}').strip())
    found = int(mount.get_shell_stdout(
        'sudo find io -maxdepth 1 -name "f.*" ! -name "*.tmp" | wc -l').strip())
    assert found == nlines, f'{sv}: {found} files after remount, expected {nlines}'
    mount.run_shell(['sudo', 'bash', '-c',
                     f'echo {client_id} > io/post-remount && '
                     f'grep -q {client_id} io/post-remount && '
                     'rm io/post-remount'])


def _stop_writer(w):
    remote = w['mount'].client_remote
    remote.run(args=['sudo', 'touch', f'{w["statedir"]}/stop'])
    _wait_for(f'writer for {w["sv"]} to stop',
              lambda: w['proc'].finished, timeout=900)
    w['proc'].stdin.close()
    w['proc'].wait()
    return int(remote.sh(f'cat {w["statedir"]}/progress').strip())


# ----- async jobs -----


def _clone_state(st, sv):
    """
    Read the clone state from the .meta file directly: "fs clone status"
    itself opens the subvolume, triggering auto-upgrade.
    """
    return _read_meta(st, sv).get('GLOBAL', 'state')


def _clone(st, src, snap, c):
    cmd = f'fs subvolume snapshot clone {VOLNAME} {src.name} {snap} {c.name}{src.grp_arg}'
    if c.group:
        _ceph(st, f'fs subvolumegroup create {VOLNAME} {c.group}')
        cmd += f' --target_group_name {c.group}'
    _ceph(st, cmd)


def _wait_for_clone(st, c):
    def done():
        _, out = _ceph(st, f'fs clone status {VOLNAME} {c.name}{c.grp_arg}')
        status = json.loads(out)['status']
        assert status['state'] in ('pending', 'in-progress', 'complete'), \
            f'clone {c}: status = {status}'
        return status['state'] == 'complete'
    _wait_for(f'clone {c} to complete', done, timeout=1800, sleep=10)


def _assert_clone_data(st, c, src, snap, path=None):
    if path is None:
        path = _getpath(st, c)[1].lstrip('/')
    assert _manifest(st, path) == st.snap_manifests[(src.name, snap)], \
        f'clone {c}: data does not match snapshot {snap} of {src}'


def _trash_entries(st):
    out = _admin_sh(st, f'test -d {TRASH_DIR} && '
                        f'find {TRASH_DIR} -mindepth 1 -maxdepth 1 | wc -l || echo 0')
    return int(out.strip())


def _setup_clones(st):
    sv = st.src
    _ceph(st, f'fs subvolumegroup create {VOLNAME} {sv.group}')
    _ceph(st, f'fs subvolume create {VOLNAME} {sv.name}{sv.grp_arg}')
    _, path = _getpath(st, sv)
    sv.v2_path = path.lstrip('/')
    _populate(st, sv.v2_path, st.clone_src_files)
    _snap_create(st, sv, SNAPNAME, v2=True)
    # a second v2 snapshot, with different contents
    _admin_sh(st, f'mkdir {sv.v2_path}/extra && touch {sv.v2_path}/extra/file')
    _snap_create(st, sv, SNAPNAME_2, v2=True)

    _set_paused(st, cloning=True)
    for c in st.clones:
        _clone(st, sv, SNAPNAME, c)
        c.v2_path = _read_meta(st, c).get('GLOBAL', 'path').lstrip('/')
        assert _clone_state(st, c) == 'pending'

    # let the clones copy some data, then pause them mid-way so that they're
    # guaranteed to be unfinished (and at least one partially copied) when
    # the upgrade begins.
    threshold = st.clone_src_files // 20
    _set_paused(st, cloning=False)
    _wait_for('clones to make progress',
              lambda: any(_count_files(st, c.v2_path) >= threshold
                          for c in st.clones),
              timeout=600, sleep=1)
    _set_paused(st, cloning=True)
    partial = 0
    for c in st.clones:
        state = _clone_state(st, c)
        copied = _count_files(st, c.v2_path)
        log.info(f'clone {c}: state = {state}, copied {copied}/{st.clone_src_files}')
        assert state in ('pending', 'in-progress'), \
            f'clone {c} was not caught mid-way (state = {state}), increase clone_src_files'
        if state == 'in-progress' and copied > 0:
            partial += 1
    assert partial > 0, 'no clone was caught partially copied'


def _setup_purges(st):
    for sv in st.purges:
        if sv.group:
            _ceph(st, f'fs subvolumegroup create {VOLNAME} {sv.group}')
        _ceph(st, f'fs subvolume create {VOLNAME} {sv.name}{sv.grp_arg}')
        _, path = _getpath(st, sv)
        sv.v2_path = path.lstrip('/')
        _populate(st, sv.v2_path, st.purge_files)

    _set_paused(st, purging=True)
    for sv in st.purges:
        _ceph(st, f'fs subvolume rm {VOLNAME} {sv.name}{sv.grp_arg}')
    total = _count_files(st, TRASH_DIR)
    assert _trash_entries(st) == len(st.purges)

    # as with clones, catch the purges mid-way
    _set_paused(st, purging=False)
    _wait_for('purges to make progress',
              lambda: _count_files(st, TRASH_DIR) <= total * 0.9,
              timeout=600, sleep=1)
    _set_paused(st, purging=True)
    left = _count_files(st, TRASH_DIR)
    log.info(f'purges paused with {left}/{total} files and '
             f'{_trash_entries(st)} trash entries left')
    assert left > 0, 'purges were not caught mid-way, increase purge_files'


def _authorize(st, sv, auth_id):
    _ceph(st, f'fs subvolume authorize {VOLNAME} {sv.name} {auth_id}'
              f'{sv.grp_arg} --access_level rw')
    # kernel clients may not support the default key type of newer
    # releases; same as the kclient task.
    _ceph(st, f'auth rotate client.{auth_id} --key-type=aes', check_status=False)
    _, keyring = _ceph(st, f'auth get client.{auth_id}')
    return keyring


def _setup_io(st):
    for i, (mount, sv) in enumerate(zip(st.io_mounts, st.io_svs)):
        if sv.group:
            _ceph(st, f'fs subvolumegroup create {VOLNAME} {sv.group}')
        _ceph(st, f'fs subvolume create {VOLNAME} {sv.name}{sv.grp_arg}')
        _, path = _getpath(st, sv)
        sv.v2_path = path.lstrip('/')

        # mount the subvolume using its own (path restricted) credentials
        auth_id = f'svio{i}'
        keyring = _authorize(st, sv, auth_id)
        mount.remount(client_id=auth_id, client_keyring=keyring,
                      cephfs_mntpt=f'/{sv.v2_path}')
        st.writers.append(_start_writer(st, mount, sv))

    for w, snaps in zip(st.writers, IO_V2_SNAPS):
        _wait_for_writer_progress(w, 20)
        for snap in snaps:
            _snap_create(st, w['sv'], snap, v2=True)
            _wait_for_writer_progress(w, 20)


# ----- sub-tasks -----


def setup(ctx, config):
    """
    On the old release, create v2 subvolumes with clients doing IO in them,
    and in-progress (paused) clones and purges.

    Example::

        tasks:
        - volumes_upgrade.setup:
            io_clients: [client.0, client.1, client.2]
            admin_client: client.3
    """
    config = config or {}
    st = State(ctx, config)
    ctx[STATE_KEY] = st

    _setup_io(st)
    _setup_clones(st)
    _setup_purges(st)

    for sv in st.io_svs + [st.src]:
        _assert_v2_usable(st, sv)
    for c in st.clones:
        _assert_v2(st, c)
    _ceph(st, f'fs subvolume ls {VOLNAME}')


def check_io(ctx, config):
    """
    Verify that the subvolume clients are still doing IO.
    """
    st = _state(ctx)
    for w in st.writers:
        _wait_for_writer_progress(w, 10)


def verify_mgr_only(ctx, config):
    """
    With only ceph-mgr upgraded, auto-upgrade can't set
    ceph.dir.subvolume.prevpath on the old MDS. The v2 subvolumes must keep
    working as v2: client IO, subvolume commands, clones and purges.
    """
    st = _state(ctx)
    check_io(ctx, config)

    for sv in st.io_svs + [st.src]:
        _assert_v2_usable(st, sv)

    # a v2 snapshot taken by the new mgr
    _snap_create(st, st.src, SNAP_MGR_ONLY)
    _assert_v2_usable(st, st.src)

    # the new mgr does not honor the pause options on startup, so the jobs
    # may already be running. resume them explicitly anyway.
    _set_paused(st, cloning=False, purging=False)

    _wait_for('purges to finish with old MDS',
              lambda: _trash_entries(st) == 0, timeout=1800, sleep=10)
    for sv in st.purges:
        rc, _ = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}',
                      check_status=False)
        assert rc != 0, f'purged subvolume {sv} still exists'

    for c in st.clones:
        _wait_for_clone(st, c)
        _assert_v2_usable(st, c)
        _assert_clone_data(st, c, st.src, SNAPNAME, path=c.v2_path)

    # a new clone of a v2 snapshot, by the new mgr
    c = Subvol('sv_clone_mgr_only', 'grp_clone')
    _clone(st, st.src, SNAPNAME_2, c)
    _wait_for_clone(st, c)
    _assert_clone_data(st, c, st.src, SNAPNAME_2)
    _assert_snaps(st, c)
    st.late_clones.append((c, st.src, SNAPNAME_2))

    check_io(ctx, config)


def post_upgrade(ctx, config):
    """
    With the whole cluster upgraded, verify auto-upgrade of the subvolumes
    while their clients do IO, that the clones and purges finish, and that
    snapshots taken before and after auto-upgrade are all listed.
    """
    st = _state(ctx)
    check_io(ctx, config)
    _set_paused(st, cloning=False, purging=False)

    # first subvolume command on each subvolume triggers its auto-upgrade;
    # the writers must carry on regardless.
    for w in st.writers:
        sv = w['sv']
        _assert_v3(st, sv)
        _wait_for_writer_progress(w, 20)

    for c in st.clones:
        _wait_for_clone(st, c)
        _assert_clone_data(st, c, st.src, SNAPNAME, path=_assert_v3(st, c))
    for c, src, snap in st.late_clones:
        _assert_clone_data(st, c, src, snap, path=_assert_v3(st, c))

    # v2 snapshots must still be listed after auto-upgrade, and listed along
    # with v3 snapshots once those are taken.
    _assert_v3(st, st.src)
    late = [c for c, _, _ in st.late_clones]
    for sv in st.io_svs + [st.src] + st.clones + late:
        _assert_snaps(st, sv)
        _snap_create(st, sv, SNAP_V3)
        _assert_snaps(st, sv)

    # clones of v2 snapshots (taken pre-upgrade) must still work
    io_src = st.io_svs[-1]
    for c, src, snap in [(Subvol('sv_clone_post_0', None), st.src, SNAPNAME_2),
                         (Subvol('sv_clone_post_1', 'grp_clone'), io_src,
                          IO_V2_SNAPS[-1][-1])]:
        _clone(st, src, snap, c)
        _wait_for_clone(st, c)
        _assert_clone_data(st, c, src, snap, path=_assert_v3(st, c))

    _wait_for('purges to finish', lambda: _trash_entries(st) == 0,
              timeout=1800, sleep=10)
    for sv in st.purges:
        rc, _ = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}',
                      check_status=False)
        assert rc != 0, f'purged subvolume {sv} still exists'

    for w in st.writers:
        sv = w['sv']
        written = _stop_writer(w)
        path = _getpath(st, sv)[1].lstrip('/')
        found = _count_files(st, f'{path}/io')
        log.info(f'{sv}: writer wrote {written} files, found {found} at {path}')
        assert found == written, f'{sv}: wrote {written} files, found {found}'

    # clients may choose to remount, in which case they mount the new v3
    # path, with either the credentials they had been using (authorized for
    # the v2 path) or with ones authorized after the upgrade.
    for i, w in enumerate(st.writers):
        sv, mount = w['sv'], w['mount']
        path = _getpath(st, sv)[1].lstrip('/')
        _remount_and_verify(w, path, mount.client_id,
                            client_keyring_path=mount.client_keyring_path)

        auth_id = f'svio{i}new'
        keyring = _authorize(st, sv, auth_id)
        _, out = _ceph(st, f'auth get client.{auth_id} --format=json')
        mds_caps = json.loads(out)[0]['caps']['mds']
        assert f'path=/{path}' in mds_caps, \
            f'{sv}: caps of client.{auth_id} not for the v3 path: {mds_caps}'
        _remount_and_verify(w, path, auth_id, client_keyring=keyring)

    _ceph(st, f'fs subvolume ls {VOLNAME}')
