#!/usr/bin/python3
"""
Remove directory trees with the MDS request (CEPH_MDS_OP_RMTREE) for a while,
then check what trash did with them.

Run it from a directory in a CephFS mount, while a thrasher fails MDS daemons
and changes max_mds. Each worker builds a tree through the mount, with
directories pinned to different ranks and hard links that cross the tree's
boundary, removes it through libcephfs and starts again under the same name.

At the end:

 - every rank's trash is empty,
 - no object of a removed file is left in a data pool,
 - a file that had a link outside its tree still has its data and one link.

Environment:

 RMTREE_DURATION       seconds to keep removing trees (default 900)
 RMTREE_WORKERS        trees built and removed side by side (default 3)
 RMTREE_DRAIN_TIMEOUT  seconds to wait for each final check (default 1800)
 RMTREE_SEED           seed for the tree shapes (default: the time)
"""

import errno
import hashlib
import json
import multiprocessing
import os
import random
import subprocess
import sys
import time

import cephfs

DURATION = int(os.environ.get('RMTREE_DURATION', '900'))
WORKERS = int(os.environ.get('RMTREE_WORKERS', '3'))
DRAIN_TIMEOUT = int(os.environ.get('RMTREE_DRAIN_TIMEOUT', '1800'))
SEED = int(os.environ.get('RMTREE_SEED', str(int(time.time()))))

# A rank that is stopping has let go of its trash and answers EAGAIN. The
# directory is exported to another rank soon after.
EAGAIN_TIMEOUT = 600


def log(msg):
    print(time.strftime('%H:%M:%S'), msg, flush=True)


def ceph_json(*args):
    out = subprocess.check_output(('ceph',) + args + ('--format=json',),
                                  timeout=120, stderr=subprocess.DEVNULL)
    return json.loads(out)


def libcephfs_mount():
    """
    The request is for root only and the client checks that itself, so act
    as root. The MDS still checks the caps of the key.
    """
    last = None
    for auth_id in (os.environ.get('CEPH_ID'), 'admin'):
        if not auth_id:
            continue
        fs = cephfs.LibCephFS(conffile='', auth_id=auth_id)
        try:
            fs.conf_set('client_mount_uid', '0')
            fs.conf_set('client_mount_gid', '0')
            fs.mount()
            return fs
        except cephfs.Error as e:
            last = e
            fs.shutdown()
    raise RuntimeError(f'cannot mount with libcephfs: {last}')


def cephfs_path_of(fs, local):
    """The path of a directory of the mount as libcephfs sees it."""
    mnt = os.environ.get('CEPH_MNT')
    if mnt:
        path = '/' + os.path.relpath(local, mnt)
    else:
        probe = local
        while probe != '/' and not os.path.ismount(probe):
            probe = os.path.dirname(probe)
        path = '/' + os.path.relpath(local, probe)
    path = os.path.normpath(path)
    if fs.stat(path).st_ino != os.stat(local).st_ino:
        raise RuntimeError(f'{local} is not {path} in the file system; '
                           'is a subdirectory mounted?')
    return path


def write_file(path, data):
    """Returns the file's inode number."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        os.write(fd, data)
        return os.fstat(fd).st_ino
    finally:
        os.close(fd)


def file_data(rng):
    roll = rng.random()
    if roll < 0.10:
        size = 0
    elif roll < 0.99:
        size = rng.randint(1, 8192)
    else:
        # more than one object
        size = rng.randint(4 << 20, 6 << 20)
    return rng.randbytes(size)


def build_tree(rng, tree, keep, tag, max_mds):
    """
    Returns the inode numbers of the files that go away with the tree and a
    list of (path, sha1) for the files that must outlive it.
    """
    doomed = set()
    survivors = []

    dirs = [tree]
    for i in range(rng.randint(2, 5)):
        d = os.path.join(tree, f'd{i}')
        dirs.append(d)
        for j in range(rng.randint(0, 2)):
            sub = os.path.join(d, f's{j}')
            dirs.append(sub)
            if rng.random() < 0.3:
                dirs.append(os.path.join(sub, 'deep'))
    for d in dirs:
        os.mkdir(d)

    # One directory in four is wide enough to be fragmented.
    wide = rng.choice(dirs) if rng.random() < 0.25 else None
    for d in dirs:
        count = rng.randint(300, 600) if d == wide else rng.randint(3, 40)
        for i in range(count):
            data = file_data(rng) if d != wide else b'w'
            doomed.add(write_file(os.path.join(d, f'f{i}'), data))

    # Directories of the tree on other ranks. A pin on a rank that is not
    # there has no effect until it is.
    for d in dirs[1:]:
        if rng.random() < 0.5:
            os.setxattr(d, 'ceph.dir.pin', str(rng.randrange(max_mds)).encode())
    if max_mds > 1 and rng.random() < 0.15:
        os.setxattr(tree, 'ceph.dir.pin.distributed', b'1')

    # A file of the tree with a link outside it. The tree goes, the file stays.
    inside = os.path.join(rng.choice(dirs), 'linked_out')
    data = rng.randbytes(rng.randint(1, 65536))
    ino = write_file(inside, data)
    outside = os.path.join(keep, f'{tag}_from_tree')
    os.link(inside, outside)
    survivors.append((outside, hashlib.sha1(data).hexdigest(), ino))

    # A file outside the tree with a link in it. Only the link goes.
    outside = os.path.join(keep, f'{tag}_own')
    data = rng.randbytes(rng.randint(1, 65536))
    ino = write_file(outside, data)
    os.link(outside, os.path.join(rng.choice(dirs), 'linked_in'))
    survivors.append((outside, hashlib.sha1(data).hexdigest(), ino))

    return doomed, survivors


def rmtree(fs, path):
    """Returns how many times the MDS said to try again."""
    retries = 0
    deadline = time.time() + EAGAIN_TIMEOUT
    while True:
        try:
            fs._rmtree_rpc(path)
            return retries
        except cephfs.Error as e:
            if getattr(e, 'errno', None) != errno.EAGAIN or time.time() > deadline:
                raise
        retries += 1
        time.sleep(1)


def worker(wid, base, max_mds, deadline, results):
    try:
        rng = random.Random(SEED * 1000 + wid)
        home = os.path.join(base, f'w{wid}')
        keep = os.path.join(home, 'keep')
        tree = os.path.join(home, 'tree')
        os.makedirs(keep)

        fs = libcephfs_mount()
        tree_in_fs = os.path.join(cephfs_path_of(fs, home), 'tree')

        doomed = set()
        survivors = []
        rounds = retries = 0
        while time.time() < deadline:
            # The same name every time: it is free as soon as the request
            # returns, whatever trash still holds.
            d, s = build_tree(rng, tree, keep, f'r{rounds}', max_mds)
            retries += rmtree(fs, tree_in_fs)
            if os.path.lexists(tree):
                raise RuntimeError(f'{tree} is still there after rmtree')
            doomed |= d
            survivors += s
            rounds += 1
            if rounds % 10 == 0:
                log(f'worker {wid}: {rounds} trees, {len(doomed)} files, '
                    f'{retries} EAGAIN')
        fs.shutdown()
        results.put((wid, None, rounds, retries, doomed, survivors))
    except BaseException as e:
        import traceback
        traceback.print_exc()
        results.put((wid, repr(e), 0, 0, set(), []))


def wait_for(what, check, timeout=None):
    """check() returns None when satisfied, else what is still wrong."""
    deadline = time.time() + (DRAIN_TIMEOUT if timeout is None else timeout)
    problem = None
    while time.time() < deadline:
        try:
            problem = check()
        except (subprocess.SubprocessError, ValueError, KeyError, OSError) as e:
            # a daemon in the middle of a failover
            problem = repr(e)
        if problem is None:
            log(f'ok: {what}')
            return
        time.sleep(5)
    raise RuntimeError(f'timed out waiting for: {what}: {problem}')


def fs_of_mount():
    """(name, mdsmap) of the file system. There must be one."""
    dump = ceph_json('fs', 'dump')
    want = os.environ.get('RMTREE_FS')
    fss = [f for f in dump['filesystems']
           if not want or f['mdsmap']['fs_name'] == want]
    if len(fss) != 1:
        raise RuntimeError('set RMTREE_FS to the name of the file system')
    return fss[0]['mdsmap']['fs_name'], fss[0]['mdsmap']


def trash_busy(fs_name):
    """None when the trash of every rank is empty."""
    _, mdsmap = fs_of_mount()
    for rank in sorted(mdsmap['in']):
        info = ceph_json('tell', f'mds.{fs_name}:{rank}', 'trash', 'ls')
        info = info.get('trash', info)
        if info.get('incomplete') or info['entries'] != 0:
            return f'rank {rank}: {info}'
        for root in info.get('roots', []):
            if root.get('state') in ('active', 'parked'):
                return f'rank {rank}: {info}'
    return None


def leaked_objects(mdsmap, inos):
    """None when no data pool holds an object of the inodes."""
    pools = {p['pool_id']: p['pool_name']
             for p in ceph_json('osd', 'pool', 'ls', 'detail')}
    for pool_id in mdsmap['data_pools']:
        out = subprocess.check_output(
            ['rados', '--pool', pools[pool_id], 'ls', '--all'], timeout=600,
            stderr=subprocess.DEVNULL).decode()
        left = set()
        for line in out.split():
            # <ino in hex>.<object number>; --all puts the namespace in front
            name = line.split('\t')[-1].split('.')[0]
            try:
                ino = int(name, 16)
            except ValueError:
                continue
            if ino in inos:
                left.add(ino)
        if left:
            some = ', '.join('%x' % i for i in sorted(left)[:5])
            return f'{len(left)} removed files have objects in {pools[pool_id]}: {some}'
    return None


def check_survivors(survivors):
    for path, digest, _ in survivors:
        with open(path, 'rb') as f:
            if hashlib.sha1(f.read()).hexdigest() != digest:
                raise RuntimeError(f'{path} lost its data')

    def one_link():
        for path, _, _ in survivors:
            nlink = os.stat(path).st_nlink
            if nlink != 1:
                return f'{path} has {nlink} links'
        return None
    wait_for('files linked across a tree have one link left', one_link)


def main():
    base = os.path.join(os.getcwd(), 'rmtree_stress')
    os.mkdir(base)
    fs_name, mdsmap = fs_of_mount()
    max_mds = mdsmap['max_mds']
    log(f'fs {fs_name} max_mds {max_mds} seed {SEED} duration {DURATION}s '
        f'workers {WORKERS}')

    results = multiprocessing.Queue()
    deadline = time.time() + DURATION
    procs = [multiprocessing.Process(target=worker,
                                     args=(w, base, max_mds, deadline, results))
             for w in range(WORKERS)]
    for p in procs:
        p.start()
    doomed = set()
    survivors = []
    failed = []
    rounds = retries = 0
    for _ in procs:
        wid, err, r, e, d, s = results.get()
        if err:
            failed.append(f'worker {wid}: {err}')
        rounds += r
        retries += e
        doomed |= d
        survivors += s
    for p in procs:
        p.join()
    if failed:
        raise RuntimeError('; '.join(failed))
    log(f'removed {rounds} trees with {len(doomed)} files, {retries} EAGAIN, '
        f'{len(survivors)} files must survive')
    if rounds < WORKERS:
        raise RuntimeError('not every worker removed a tree')

    wait_for('trash is empty on every rank', lambda: trash_busy(fs_name))
    wait_for('no object of a removed file is left',
             lambda: leaked_objects(mdsmap, doomed))
    check_survivors(survivors)

    # The survivors go the ordinary way. Their objects must go as well, or
    # trash left something behind that holds on to them.
    for path, _, _ in survivors:
        os.unlink(path)
    for w in range(WORKERS):
        os.rmdir(os.path.join(base, f'w{w}', 'keep'))
        os.rmdir(os.path.join(base, f'w{w}'))
    os.rmdir(base)
    wait_for('no object of a survivor is left once it is unlinked',
             lambda: leaked_objects(mdsmap, {ino for _, _, ino in survivors}))
    wait_for('trash is still empty on every rank', lambda: trash_busy(fs_name))
    log('OK')


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log(f'FAILED: {e}')
        sys.exit(1)
