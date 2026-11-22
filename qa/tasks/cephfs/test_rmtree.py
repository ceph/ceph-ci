"""
Tests for CEPH_MDS_OP_RMTREE.

The kernel and fuse clients still walk a tree themselves. These tests call
libcephfs so the new request is what the MDS sees.
"""

import errno
import logging
import time
from io import StringIO
from textwrap import dedent

from teuthology.exceptions import CommandFailedError

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class RmtreeTestCase(CephFSTestCase):
    """What the rmtree tests share: the request itself and a view of trash."""

    def _rmtree(self, path, sudo=True, direct=True):
        """Remove path through libcephfs. direct uses the MDS request."""
        op = "_rmtree_rpc" if direct else "rmtree"
        script = dedent(f"""
            import cephfs
            fs = cephfs.LibCephFS(conffile='')
            fs.mount()
            try:
                fs.{op}({path!r})
                print('ok')
            except Exception as e:
                print(type(e).__name__)
                print(getattr(e, 'errno', ''))
            finally:
                fs.shutdown()
            """)
        out = self.mount_a.run_python(script, sudo=sudo)
        lines = out.splitlines()
        self.assertTrue(lines, out)
        if lines[0] != 'ok':
            err = lines[0]
            eno = int(lines[1]) if len(lines) > 1 and lines[1] else None
            raise AssertionError(f"rmtree {path} failed: {err} errno={eno}")

    def _rmtree_fails(self, path, exc_name):
        script = dedent(f"""
            import cephfs
            fs = cephfs.LibCephFS(conffile='')
            fs.mount()
            try:
                fs._rmtree_rpc({path!r})
                print('ok')
            except Exception as e:
                print(type(e).__name__)
            finally:
                fs.shutdown()
            """)
        out = self.mount_a.run_python(script, sudo=True).splitlines()
        self.assertEqual(out[0], exc_name, out)

    def _rmtree_many(self, paths):
        """Remove every path with the MDS request, from one mount."""
        script = dedent(f"""
            import cephfs
            fs = cephfs.LibCephFS(conffile='')
            fs.mount()
            try:
                for path in {list(paths)!r}:
                    fs._rmtree_rpc(path)
                print('ok')
            finally:
                fs.shutdown()
            """)
        out = self.mount_a.run_python(script, sudo=True)
        self.assertEqual(out.splitlines()[-1:], ['ok'], out)

    def _trash(self, rank=0):
        try:
            info = self.fs.rank_asok(['trash', 'ls'], rank=rank)
        except CommandFailedError:
            return None
        if isinstance(info, dict) and 'entries' not in info and 'trash' in info:
            info = info['trash']
        return info

    def _wait_trash_idle(self, ranks=(0,), timeout=120):
        deadline = time.time() + timeout
        while time.time() < deadline:
            idle = True
            for rank in ranks:
                info = self._trash(rank)
                if not info:
                    idle = False
                    break
                if info.get('incomplete') or info.get('entries', 1) != 0:
                    idle = False
                    break
                for root in info.get('roots', []):
                    if root.get('state') in ('active', 'parked'):
                        idle = False
                        break
            if idle:
                return
            time.sleep(1)
        state = {rank: self._trash(rank) for rank in ranks}
        self.fail(f"trash did not drain on ranks {ranks}: {state}")

    def _stat_fails(self, mount, path):
        try:
            mount.run_shell(["stat", path])
        except CommandFailedError:
            return
        self.fail(f"{path} still exists")

    def _trash_counter(self, name, rank=0):
        dump = self.fs.rank_asok(['perf', 'dump', 'mds_cache', name], rank=rank)
        return dump['mds_cache'][name]

    def _trash_entries(self, rank=0):
        """Entries in the rank's trash directory, or None if not known yet."""
        info = self._trash(rank)
        if not info or info.get('incomplete'):
            return None
        return info['entries']


class TestRmtree(RmtreeTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 2

    def test_rmtree_and_reuse_name(self):
        """The name is gone when the request returns and can be reused."""
        self.mount_a.run_shell(["mkdir", "-p", "tree/sub"])
        self.mount_a.write_file("tree/sub/file", "data")
        self._rmtree("/tree")
        self._stat_fails(self.mount_a, "tree")
        self._stat_fails(self.mount_b, "tree")
        self.mount_a.run_shell(["mkdir", "tree"])
        self.mount_a.write_file("tree/new", "ok")
        self.mount_a.run_shell(["stat", "tree/new"])
        self._wait_trash_idle()

    def test_rstats_drop_at_rename(self):
        """Parent rstats drop when the tree is renamed, before the purge."""
        self.mount_a.run_shell(["mkdir", "-p", "parent/tree"])
        self.mount_a.write_file("parent/tree/file", "x" * 4096)
        self.mount_a.run_shell(["sync"])

        def rbytes():
            return int(self.mount_a.getfattr("parent", "ceph.dir.rbytes"))

        # The file reaches the rstats of "tree" at once and those of
        # "parent" lazily. Wait for it, or there is nothing to drop.
        self.wait_until_true(lambda: rbytes() >= 4096, timeout=60, period=1)
        before = rbytes()
        self.fs.rank_asok(['trash', 'pause'])
        try:
            self._rmtree("/parent/tree")
            # The purge is paused, so only the rename can lower this.
            self.wait_until_true(lambda: rbytes() < before, timeout=30,
                                 period=1)
            info = self._trash()
            self.assertGreaterEqual(info.get('entries', 0), 1)
        finally:
            self.fs.rank_asok(['trash', 'resume'])
        self._wait_trash_idle()

    def test_hardlink_outside_survives(self):
        self.mount_a.run_shell(["mkdir", "tree"])
        self.mount_a.write_file("outside", "data")
        self.mount_a.run_shell(["ln", "outside", "tree/inside"])
        self._rmtree("/tree")
        self._wait_trash_idle()
        self.assertEqual(self.mount_a.read_file("outside"), "data")
        nlink = self.mount_a.get_shell_stdout(["stat", "-c", "%h", "outside"]).strip()
        self.assertEqual(nlink, "1")

    def test_hardlink_from_inside_survives(self):
        self.mount_a.run_shell(["mkdir", "tree"])
        self.mount_a.write_file("tree/inside", "data")
        self.mount_a.run_shell(["ln", "tree/inside", "outside"])
        self._rmtree("/tree")
        self.assertEqual(self.mount_a.read_file("outside"), "data")
        self._wait_trash_idle()
        nlink = self.mount_a.get_shell_stdout(["stat", "-c", "%h", "outside"]).strip()
        self.assertEqual(nlink, "1")

    def test_ancestor_snapshot_keeps_tree(self):
        self.mount_a.run_shell(["mkdir", "-p", "tree"])
        self.mount_a.write_file("tree/file", "snapdata")
        self.mount_a.run_shell(["mkdir", ".snap/ancestor"])
        self._rmtree("/tree")
        self.assertEqual(self.mount_a.read_file(".snap/ancestor/tree/file"), "snapdata")
        self._stat_fails(self.mount_a, "tree")
        self.mount_a.run_shell(["rmdir", ".snap/ancestor"])
        self._wait_trash_idle()

    def test_own_snapshot_is_refused(self):
        self.mount_a.run_shell(["mkdir", "tree"])
        self.mount_a.write_file("tree/file", "x")
        self.mount_a.run_shell(["mkdir", "tree/.snap/mine"])
        self._rmtree_fails("/tree", "ObjectNotEmpty")
        self.mount_a.run_shell(["stat", "tree/file"])
        self.mount_a.run_shell(["rmdir", "tree/.snap/mine"])

    def test_cached_dentries_are_dropped(self):
        """A second client that has the tree cached loses it."""
        self.config_set('mds', 'mds_allow_async_dirops', 'true')
        try:
            self.mount_a.run_shell(["mkdir", "-p", "tree/sub"])
            self.mount_a.write_file("tree/sub/file", "data")
            self.mount_b.run_shell(["ls", "-R", "tree"])
            self.mount_b.run_shell(["stat", "tree/sub/file"])
            self._rmtree("/tree")
            self._stat_fails(self.mount_b, "tree")
            self._stat_fails(self.mount_b, "tree/sub/file")
        finally:
            self.config_set('mds', 'mds_allow_async_dirops', 'false')
        self._wait_trash_idle()

    def test_open_file_and_create_in_trashed_dir(self):
        """An open file stays readable. Creates under the tree fail."""
        script = dedent("""
            import os, cephfs
            os.makedirs('held/sub', exist_ok=True)
            with open('held/sub/file', 'w') as f:
                f.write('hello')
            fd = os.open('held/sub/file', os.O_RDONLY)
            dirfd = os.open('held/sub', os.O_RDONLY | os.O_DIRECTORY)
            fs = cephfs.LibCephFS(conffile='')
            fs.mount()
            fs._rmtree_rpc(b'/held')
            fs.shutdown()
            data = os.read(fd, 16)
            os.close(fd)
            print(data.decode())
            try:
                os.mkdir('child', dir_fd=dirfd)
                print('mkdir-ok')
            except OSError as e:
                print(e.errno)
            os.close(dirfd)
            """)
        proc = self.mount_a.run_shell(["sudo", "python3", "-c", script])
        out = proc.stdout.getvalue()
        self.assertIn("hello", out)
        self.assertIn(str(errno.ENOENT), out)
        self._stat_fails(self.mount_a, "held")
        self._wait_trash_idle()

    def test_failover_during_purge(self):
        self.config_set('mds', 'mds_trash_max_inflight', '1')
        try:
            self.mount_a.run_shell(["mkdir", "wide"])
            self.mount_a.run_shell(["python3", "-c",
                                    "import os\n"
                                    "[open(f'wide/f{i}','w').write('x') for i in range(300)]"])
            self._rmtree("/wide")
            self._stat_fails(self.mount_a, "wide")
            self.fs.rank_fail(rank=0)
            self.fs.wait_for_daemons()
            self._stat_fails(self.mount_a, "wide")
            self.mount_a.run_shell(["mkdir", "wide"])
        finally:
            self.config_set('mds', 'mds_trash_max_inflight', '8')
        self._wait_trash_idle()

    def test_fs_down_keeps_trash(self):
        """Rank 0 stops with entries in trash and purges them once it is back."""
        self.mount_a.run_shell(["mkdir", "-p", "kept/sub"])
        self.mount_a.write_file("kept/sub/file", "data")
        # The option survives the restart. "trash pause" would not.
        self.config_set('mds', 'mds_trash_pause', 'true')
        try:
            self._rmtree("/kept")
            self._stat_fails(self.mount_a, "kept")
            self.mount_a.umount_wait()
            self.mount_b.umount_wait()
            self.fs.set_down()
            self.fs.wait_for_daemons()
            self.fs.set_down(False)
            self.fs.wait_for_daemons()
        finally:
            self.config_rm('mds', 'mds_trash_pause')
        self._wait_trash_idle()
        # The counter starts at zero in the new rank 0, so the directories
        # were removed after the restart and not while the rank was stopping.
        self.assertGreaterEqual(self._trash_counter('trash_dirs'), 2)

    def test_wide_tree_is_purged_once(self):
        """Unlinks run side by side and each entry is removed exactly once."""
        self.mount_a.run_shell(["mkdir", "-p", "wide/d1", "wide/d2"])
        self.mount_a.run_shell(["python3", "-c",
                                "import os\n"
                                "for d in ('wide', 'wide/d1', 'wide/d2'):\n"
                                "    for i in range(100):\n"
                                "        open(os.path.join(d, 'f%d' % i), 'w').close()\n"])
        self._rmtree("/wide")
        self._stat_fails(self.mount_a, "wide")
        self._wait_trash_idle()
        # A request sent twice comes back with ENOENT and is counted again.
        self.assertEqual(self._trash_counter('trash_files'), 300)
        self.assertEqual(self._trash_counter('trash_dirs'), 3)

    def _journal_segments(self):
        return self.fs.rank_asok(['perf', 'dump', 'mds_log'])['mds_log']['seg']

    def test_purge_with_full_journal(self):
        """
        The walker waits for the journal to be trimmed. On a rank that does
        nothing else the journal stays above mds_log_max_segments: trimming
        stops there, and the expired segments stay until the next major one.
        That must not hold the walker for good.
        """
        max_segments = int(self.fs.rank_asok(
            ['config', 'get', 'mds_log_max_segments'])['mds_log_max_segments'])
        # Short segments, so that a few thousand files fill the journal.
        self.config_set('mds', 'mds_log_events_per_segment', '32')
        self.config_set('mds', 'mds_trash_pause', 'true')
        try:
            self.mount_a.run_shell(["mkdir", "-p", "full/sub", "fill"])
            self.mount_a.run_shell(["python3", "-c",
                                    "import os\n"
                                    "for d in ('full', 'full/sub'):\n"
                                    "    for i in range(100):\n"
                                    "        open(os.path.join(d, 'f%d' % i), 'w').close()\n"])
            fill = dedent("""
                import os, sys
                for i in range(int(sys.argv[2])):
                    open(os.path.join('fill', '%s_%d' % (sys.argv[1], i)), 'w').close()
                """)
            batch = 0
            while self._journal_segments() <= max_segments:
                self.assertLess(batch, 100, "the journal does not fill")
                self.mount_a.run_shell(["python3", "-c", fill, f"a{batch}", "500"])
                batch += 1
            self._rmtree("/full")
            self._stat_fails(self.mount_a, "full")
            # Let trimming finish. If the last segment it expired was the one
            # before a major segment, nothing is left over; add a segment or
            # two and look again.
            for attempt in range(20):
                time.sleep(10)
                if self._journal_segments() > max_segments:
                    break
                self.mount_a.run_shell(["python3", "-c", fill, f"b{attempt}", "30"])
            else:
                self.fail("the journal does not stay above mds_log_max_segments")
            self.assertEqual(self._trash_entries(), 1)
        finally:
            self.config_rm('mds', 'mds_trash_pause')
        self._wait_trash_idle()
        self.assertEqual(self._trash_counter('trash_files'), 200)
        self.assertEqual(self._trash_counter('trash_dirs'), 2)

    def _trash_root(self):
        roots = (self._trash() or {}).get('roots', [])
        return roots[0] if roots else {}

    def _park_tree_with_snapshot(self):
        """
        Trash a tree with a snapshot inside it and wait for the walker to
        park it. Returns the inode number of the trash entry.
        """
        self.mount_a.run_shell(["mkdir", "-p", "parked/a/b"])
        self.mount_a.write_file("parked/a/b/file", "x")
        self.mount_a.write_file("parked/other", "y")
        self.mount_a.run_shell(["mkdir", "parked/a/.snap/keep"])
        # The request refuses a tree with snapshots unless their count has
        # not reached the top yet. Skip that check to get here every time.
        opt = 'mds_inject_rmtree_skip_snap_check'
        self.fs.rank_asok(['config', 'set', opt, 'true'])
        try:
            self._rmtree("/parked")
        finally:
            self.fs.rank_asok(['config', 'set', opt, 'false'])
        self._stat_fails(self.mount_a, "parked")
        self.wait_until_true(
            lambda: self._trash_root().get('state') == 'parked',
            timeout=60, period=1)
        return self._trash_root()['ino']

    def test_parked_tree_rmsnaps(self):
        """A tree parked on a snapshot is purged once the admin drops it."""
        ino = self._park_tree_with_snapshot()
        self.fs.rank_asok(['trash', 'rmsnaps', str(ino)])
        self._wait_trash_idle()

    def test_rmsnaps_survives_failover(self):
        """The request to drop the snapshots is kept across a failover."""
        ino = self._park_tree_with_snapshot()
        # Hold the walker so the purge is left for the next rank 0. The
        # option keeps the new one paused until the check below is done.
        self.fs.rank_asok(['trash', 'pause'])
        self.config_set('mds', 'mds_trash_pause', 'true')
        try:
            self.fs.rank_asok(['trash', 'rmsnaps', str(ino)])
            self.wait_until_true(
                lambda: self._trash_root().get('rmsnaps') is True,
                timeout=30, period=1)
            self.assertEqual(self._trash_root().get('state'), 'parked')
            self.fs.rank_fail(rank=0)
            self.fs.wait_for_daemons()
        finally:
            self.config_rm('mds', 'mds_trash_pause')
        self._wait_trash_idle()

    def test_multimds_and_rank_removal(self):
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        self.mount_a.run_shell(["mkdir", "-p", "d0/d1/sub"])
        self.mount_a.write_file("d0/d1/sub/file", "data")
        self.mount_a.setfattr("d0", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d0/d1", "ceph.dir.pin", "1")
        # Without this the tree may still be on rank 0 alone.
        self._wait_subtrees([("/d0/d1", 1)], rank=1, path="/d0", timeout=60)
        self._rmtree("/d0")
        self._stat_fails(self.mount_a, "d0")
        self._wait_trash_idle(ranks=(0, 1), timeout=180)
        self.fs.set_max_mds(1)
        self.fs.wait_for_daemons()
        self._wait_trash_idle(ranks=(0,))

    def test_rank_removal_migrates_trash(self):
        """A rank that stops hands the trees in its trash to rank 0."""
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        self.mount_a.run_shell(["mkdir", "-p", "onrank1/tree/sub"])
        self.mount_a.write_file("onrank1/tree/sub/file", "data")
        # The rank that holds the name does the rename, so pinning the parent
        # puts the tree in rank 1's trash.
        self.mount_a.setfattr("onrank1", "ceph.dir.pin", "1")
        self._wait_subtrees([("/onrank1", 1)], rank=1, path="/onrank1")
        # Hold both walkers so the tree is still there when rank 1 stops, and
        # still there on rank 0 for the check below.
        self.config_set('mds', 'mds_trash_pause', 'true')
        try:
            self.wait_until_true(
                lambda: all((self._trash(rank) or {}).get('paused')
                            for rank in (0, 1)),
                timeout=30, period=1)
            self._rmtree("/onrank1/tree")
            self._stat_fails(self.mount_a, "onrank1/tree")
            self.wait_until_true(lambda: self._trash_entries(1) == 1,
                                 timeout=30, period=1)
            self.assertEqual(self._trash_entries(0), 0)

            self.fs.set_max_mds(1)
            self.fs.wait_for_daemons()
            self.wait_until_true(lambda: self._trash_entries(0) == 1,
                                 timeout=30, period=1)
        finally:
            self.config_rm('mds', 'mds_trash_pause')
        self._wait_trash_idle()
        # Rank 1 was paused, so rank 0 removed all of it.
        self.assertGreaterEqual(self._trash_counter('trash_dirs'), 2)
        self.assertGreaterEqual(self._trash_counter('trash_files'), 1)

    # A trash directory this small is split by a few dozen entries. The
    # merge size is below what a fragment gets from the split, or the
    # fragments would be merged again at once.
    TRASH_SPLIT_SIZE = 20
    TRASH_MERGE_SIZE = 2

    def _small_trash_fragments(self):
        self.config_set('mds', 'mds_bal_split_size', str(self.TRASH_SPLIT_SIZE))
        self.config_set('mds', 'mds_bal_merge_size', str(self.TRASH_MERGE_SIZE))

    def _trash_is_split(self, rank=0):
        """True if the rank's trash directory has more than one fragment."""
        tree = self.fs.rank_asok(
            ['dump', 'tree', f'~mds{rank}/trash', '0'], rank=rank)
        return len(tree[0]['dirfragtree']['splits']) > 0

    def _make_trees(self, top, count, files=3):
        """
        count directories, each with a few files. They are ten to a parent
        directory, so that the split size does not fragment the parents.
        """
        self.mount_a.run_shell(["mkdir", "-p", top])
        self.mount_a.run_shell(["python3", "-c",
                                "import os, sys\n"
                                "top, count, files = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])\n"
                                "for i in range(count):\n"
                                "    d = os.path.join(top, 'g%d' % (i // 10), 't%d' % i)\n"
                                "    os.makedirs(d)\n"
                                "    for j in range(files):\n"
                                "        open(os.path.join(d, 'f%d' % j), 'w').close()\n",
                                top, str(count), str(files)])
        return [f"/{top}/g{i // 10}/t{i}" for i in range(count)]

    def test_trash_directory_is_split(self):
        """
        The trash directory is fragmented like any other when it holds many
        trees. New trees go into the fragments, a new rank 0 finds the trees
        in all of them, and the walker removes each once. The fragments are
        merged again when they are empty.
        """
        self._small_trash_fragments()
        trees = self._make_trees("split", 52)
        first, rest = trees[:40], trees[40:]
        self.config_set('mds', 'mds_trash_pause', 'true')
        try:
            self.wait_until_true(lambda: (self._trash() or {}).get('paused'),
                                 timeout=30, period=1)
            self._rmtree_many(first)
            self.wait_until_true(self._trash_is_split, timeout=120, period=2)
            self.wait_until_true(
                lambda: self._trash_entries() == len(first),
                timeout=60, period=1)

            self._rmtree_many(rest)
            self._stat_fails(self.mount_a, "split/g5/t51")
            self.wait_until_true(
                lambda: self._trash_entries() == len(trees),
                timeout=60, period=1)

            self.fs.rank_fail()
            self.fs.wait_for_daemons()
            # The walker is paused, and the rank still has to read every
            # fragment to count the entries.
            self.wait_until_true(
                lambda: self._trash_entries() == len(trees),
                timeout=120, period=1)
            self.assertTrue(self._trash_is_split())
        finally:
            self.config_rm('mds', 'mds_trash_pause')
        self._wait_trash_idle(timeout=300)
        # The counters start at zero in the new rank 0.
        self.assertEqual(self._trash_counter('trash_dirs'), len(trees))
        self.assertEqual(self._trash_counter('trash_files'), len(trees) * 3)
        for group in range(6):
            self.assertEqual(self.mount_a.ls(f"split/g{group}"), [])
        self.wait_until_true(lambda: not self._trash_is_split(),
                             timeout=120, period=2)

    def test_rank_removal_migrates_split_trash(self):
        """A rank that stops hands over the trees in every fragment of its trash."""
        self._small_trash_fragments()
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        trees = self._make_trees("onrank1", 40)
        self.mount_a.setfattr("onrank1", "ceph.dir.pin", "1")
        self._wait_subtrees([("/onrank1", 1)], rank=1, path="/onrank1")
        self.config_set('mds', 'mds_trash_pause', 'true')
        try:
            self.wait_until_true(
                lambda: all((self._trash(rank) or {}).get('paused')
                            for rank in (0, 1)),
                timeout=30, period=1)
            self._rmtree_many(trees)
            self.wait_until_true(lambda: self._trash_is_split(1),
                                 timeout=120, period=2)
            self.wait_until_true(
                lambda: self._trash_entries(1) == len(trees),
                timeout=60, period=1)
            self.assertEqual(self._trash_entries(0), 0)

            self.fs.set_max_mds(1)
            self.fs.wait_for_daemons()
            self.wait_until_true(
                lambda: self._trash_entries(0) == len(trees),
                timeout=120, period=1)
        finally:
            self.config_rm('mds', 'mds_trash_pause')
        self._wait_trash_idle(timeout=300)
        # Rank 1 was paused, so rank 0 removed all of it.
        self.assertEqual(self._trash_counter('trash_dirs'), len(trees))
        self.assertEqual(self._trash_counter('trash_files'), len(trees) * 3)

    def test_path_restricted_cap(self):
        self.mount_a.run_shell(["mkdir", "-p", "allowed/tree", "secret/tree"])
        self.mount_a.write_file("allowed/tree/f", "a")
        self.mount_a.write_file("secret/tree/f", "s")
        # The client mounts the root, so it needs to read it. Without that
        # the MDS rejects the session.
        keyring = self.fs.authorize("rmtree", (("/", "r"), ("/allowed", "rw")))
        keyring_path = self.mount_a.client_remote.mktemp(data=keyring)
        script = dedent(f"""
            import cephfs
            fs = cephfs.LibCephFS(conffile='', auth_id='rmtree',
                                  conf={{'keyring': {keyring_path!r}}})
            fs.mount()
            try:
                fs._rmtree_rpc(b'/allowed/tree')
                print('allowed-ok')
            except Exception as e:
                print('allowed ' + type(e).__name__)
            try:
                fs._rmtree_rpc(b'/secret/tree')
                print('secret-ok')
            except Exception as e:
                print('secret ' + type(e).__name__)
            finally:
                fs.shutdown()
            """)
        out = self.mount_a.run_python(script, sudo=True)
        self.assertIn("allowed-ok", out)
        self.assertIn("secret PermissionDenied", out)
        self.mount_a.run_shell(["stat", "secret/tree/f"])
        self._stat_fails(self.mount_a, "allowed/tree")
        self._wait_trash_idle()

    def test_nonroot_falls_back_to_walk(self):
        """A non-root caller is refused by the client and the walk still deletes."""
        uid = self.mount_a.run_python("import os; print(os.getuid())", sudo=False).strip()
        if uid == "0":
            self.skipTest("client remote process is root; cannot observe the uid check")
        self.mount_a.run_shell(["mkdir", "-p", "walk/sub"])
        self.mount_a.write_file("walk/sub/f", "x")
        self.mount_a.run_shell(["chmod", "-R", "a+rwx", "walk"])
        script = dedent("""
            import cephfs
            fs = cephfs.LibCephFS(conffile='')
            fs.mount()
            try:
                fs._rmtree_rpc(b'/walk')
                print('rpc-ok')
            except cephfs.PermissionError:
                print('rpc-eperm')
            fs.rmtree(b'/walk')
            print('walk-ok')
            fs.shutdown()
            """)
        out = self.mount_a.run_python(script, sudo=False)
        self.assertIn("rpc-eperm", out)
        self.assertIn("walk-ok", out)
        self._stat_fails(self.mount_a, "walk")


class TestRmtreeMultiMDS(RmtreeTestCase):
    """
    Trees whose directories are spread over three active ranks. The rank that
    holds the name moves the tree into its trash. Each rank then removes the
    part of the tree it is auth for.
    """
    MDSS_REQUIRED = 3
    CLIENTS_REQUIRED = 1

    RANKS = (0, 1, 2)
    # files in each directory of a test tree
    FILES = 20

    def setUp(self):
        super().setUp()
        self.fs.set_max_mds(len(self.RANKS))
        self.fs.wait_for_daemons()

    def _populate(self, top, leaves, files=None):
        """
        Create the leaves under top and put files in every directory.
        Returns how many directories and files the tree holds.
        """
        files = self.FILES if files is None else files
        script = dedent(f"""
            import os
            for leaf in {leaves!r}:
                os.makedirs(os.path.join({top!r}, leaf))
            ndirs = nfiles = 0
            for path, _, _ in os.walk({top!r}):
                ndirs += 1
                for i in range({files}):
                    with open(os.path.join(path, 'f%d' % i), 'w') as f:
                        f.write('x')
                    nfiles += 1
            print(ndirs, nfiles)
            """)
        out = self.mount_a.run_shell(["python3", "-c", script]).stdout.getvalue()
        ndirs, nfiles = (int(n) for n in out.split())
        self.mount_a.run_shell(["sync"])
        return ndirs, nfiles

    def _pin(self, pins, top):
        """pins maps a path to a rank. Returns once every rank has its part."""
        for path, rank in pins.items():
            self.mount_a.setfattr(path, "ceph.dir.pin", str(rank))
        self._wait_subtrees([("/" + path, rank) for path, rank in pins.items()],
                            rank="all", path="/" + top, timeout=60)

    def _subtrees_under_trash(self):
        found = []
        for rank in self.RANKS:
            for s in self.fs.rank_asok(["get", "subtrees"], rank=rank):
                path = s['dir']['path']
                if path.startswith("~mds") and "/trash/" in path:
                    found.append((rank, path, s['auth_first']))
        return found

    def _data_objects(self):
        out = self.fs.rados(["ls"], pool=self.fs.get_data_pool_name(),
                            stdout=StringIO()).stdout.getvalue()
        return out.split()

    def _removed(self, name):
        return sum(self._trash_counter(name, rank) for rank in self.RANKS)

    def _check_purged(self, ndirs=None, nfiles=None):
        """
        Every rank is done, nothing of the tree is left in any rank's cache
        or in the data pool, and each entry was removed once.
        """
        self._wait_trash_idle(ranks=self.RANKS, timeout=300)
        # A request sent twice comes back with ENOENT and is counted again.
        if nfiles is not None:
            self.assertEqual(self._removed('trash_files'), nfiles)
        if ndirs is not None:
            self.assertEqual(self._removed('trash_dirs'), ndirs)
        self.wait_until_true(lambda: not self._subtrees_under_trash(),
                             timeout=60, period=2)
        # A rank that keeps a walk it has finished keeps the directory too.
        self.wait_until_true(
            lambda: not any((self._trash(rank) or {'roots': [1]})['roots']
                            for rank in self.RANKS),
            timeout=60, period=2)
        self.wait_until_true(lambda: not self._data_objects(),
                             timeout=120, period=2)

    def test_subdirs_pinned_to_each_rank(self):
        """
        The tree is in rank 0's trash. Ranks 1 and 2 hold directories of it,
        one of them inside the other's, and rank 0 holds one below rank 2.
        """
        ndirs, nfiles = self._populate(
            "spread", ["r0/a", "r1/a/b", "r1/nested/c", "r2/a/b"])
        self._pin({"spread/r1": 1, "spread/r1/nested": 2,
                   "spread/r2": 2, "spread/r2/a": 0}, "spread")
        self._rmtree("/spread")
        self._stat_fails(self.mount_a, "spread")
        self.mount_a.run_shell(["mkdir", "spread"])
        self._check_purged(ndirs, nfiles)
        for rank in self.RANKS:
            self.assertGreater(self._trash_counter('trash_files', rank), 0)

    def test_tree_in_trash_of_another_rank(self):
        """The name is on rank 2, so its trash takes the tree."""
        self.mount_a.run_shell(["mkdir", "home"])
        ndirs, nfiles = self._populate("home/tree", ["r0/a", "r1/a", "r2/a"])
        self._pin({"home": 2, "home/tree/r0": 0, "home/tree/r1": 1}, "home")
        self._rmtree("/home/tree")
        self._stat_fails(self.mount_a, "home/tree")
        self._check_purged(ndirs, nfiles)
        # The tree's own directory is removed by the rank whose trash it was in.
        self.assertGreater(self._trash_counter('trash_dirs', 2), 0)
        for rank in self.RANKS:
            self.assertGreater(self._trash_counter('trash_files', rank), 0)

    def test_distributed_pin(self):
        """The fragments of one directory are on different ranks."""
        self.mount_a.run_shell(["mkdir", "dist"])
        ndirs, nfiles = self._populate(
            "dist/tree", [f"d{i}" for i in range(40)], files=5)
        self.mount_a.setfattr("dist/tree", "ceph.dir.pin.distributed", "1")
        self._wait_distributed_subtrees(2 * len(self.RANKS), rank="all",
                                        path="/dist/tree")
        self._rmtree("/dist/tree")
        self._stat_fails(self.mount_a, "dist/tree")
        self._check_purged(ndirs, nfiles)
        for rank in self.RANKS:
            self.assertGreater(self._trash_counter('trash_files', rank), 0)

    def test_hardlinks_between_ranks(self):
        """A hard link on another rank keeps the file, whichever side is removed."""
        self.mount_a.run_shell(["mkdir", "-p", "links/tree/r1", "links/keep"])
        self.mount_a.write_file("links/tree/r1/inside", "in")
        self.mount_a.run_shell(["ln", "links/tree/r1/inside",
                                "links/keep/from_inside"])
        self.mount_a.write_file("links/keep/outside", "out")
        self.mount_a.run_shell(["ln", "links/keep/outside",
                                "links/tree/r1/to_outside"])
        # An empty directory is not exported, so pin them once they have files.
        self._pin({"links/tree/r1": 1, "links/keep": 2}, "links")
        self._rmtree("/links/tree")
        self._stat_fails(self.mount_a, "links/tree")
        self._wait_trash_idle(ranks=self.RANKS, timeout=300)
        for name, data in (("from_inside", "in"), ("outside", "out")):
            path = f"links/keep/{name}"
            self.assertEqual(self.mount_a.read_file(path), data)
            self.wait_until_true(
                lambda: self.mount_a.get_shell_stdout(
                    ["stat", "-c", "%h", path]).strip() == "1",
                timeout=60, period=2)

    def test_failover_while_tree_spans_ranks(self):
        """
        Ranks 1 and 0 fail while a tree is in trash. Their replacements find
        their parts of it again and each entry is still removed once.
        """
        ndirs, nfiles = self._populate("spread", ["r0/a", "r1/a/b", "r2/a/b"])
        self._pin({"spread/r1": 1, "spread/r2": 2}, "spread")
        # The option holds the new daemons as well, so nothing is removed
        # before both ranks have been replaced.
        self.config_set('mds', 'mds_trash_pause', 'true')
        try:
            self.wait_until_true(
                lambda: all((self._trash(rank) or {}).get('paused')
                            for rank in self.RANKS),
                timeout=30, period=1)
            self._rmtree("/spread")
            self._stat_fails(self.mount_a, "spread")
            for rank in (1, 0):
                self.fs.rank_fail(rank=rank)
                self.fs.wait_for_daemons()
            self._stat_fails(self.mount_a, "spread")
            # The new rank 0 has to read its trash directory first.
            self.wait_until_true(lambda: self._trash_entries(0) == 1,
                                 timeout=60, period=1)
        finally:
            self.config_rm('mds', 'mds_trash_pause')
        self._check_purged(ndirs, nfiles)

    def test_snapshot_in_subtree_of_another_rank(self):
        """
        The snapshot is in the part of the tree that rank 1 holds, so rank 1
        is the one that stops at it. The admin still names the trash entry,
        on the rank that has it.
        """
        self._populate("parked", ["r0/a", "r1/a/b"], files=5)
        self._pin({"parked/r1": 1}, "parked")
        self.mount_a.run_shell(["mkdir", "parked/r1/a/.snap/keep"])
        # The request refuses a tree with snapshots unless their count has
        # not reached the top yet. Skip that check to get here every time.
        opt = 'mds_inject_rmtree_skip_snap_check'
        self.fs.rank_asok(['config', 'set', opt, 'true'])
        try:
            self._rmtree("/parked")
        finally:
            self.fs.rank_asok(['config', 'set', opt, 'false'])
        self._stat_fails(self.mount_a, "parked")

        def parked_on_rank_1():
            return [root for root in (self._trash(1) or {}).get('roots', [])
                    if root['state'] == 'parked']
        self.wait_until_true(parked_on_rank_1, timeout=60, period=1)
        entry = parked_on_rank_1()[0]['entry']
        self.assertEqual(self._trash_entries(0), 1)
        self.assertEqual(self._trash_entries(1), 0)
        self.fs.rank_asok(['trash', 'rmsnaps', str(entry)])
        # Rank 1 looks at a parked tree again once a minute.
        self._check_purged()

    def test_ranks_removed_while_tree_spans_them(self):
        """
        Ranks 1 and 2 stop while they hold parts of a tree in rank 0's trash.
        They hand those back and rank 0 removes all of it.
        """
        ndirs, nfiles = self._populate("spread", ["r0/a", "r1/a/b", "r2/a/b"])
        self._pin({"spread/r1": 1, "spread/r2": 2}, "spread")
        # No walker removes anything until rank 0 is the only one left.
        self.config_set('mds', 'mds_trash_pause', 'true')
        try:
            self.wait_until_true(
                lambda: all((self._trash(rank) or {}).get('paused')
                            for rank in self.RANKS),
                timeout=30, period=1)
            self._rmtree("/spread")
            self._stat_fails(self.mount_a, "spread")
            self.wait_until_true(lambda: self._trash_entries(0) == 1,
                                 timeout=30, period=1)
            self.fs.set_max_mds(1)
            self.fs.wait_for_daemons()
            self.wait_until_true(lambda: self._trash_entries(0) == 1,
                                 timeout=30, period=1)
        finally:
            self.config_rm('mds', 'mds_trash_pause')
        self._wait_trash_idle(timeout=300)
        self.assertEqual(self._trash_counter('trash_files'), nfiles)
        self.assertEqual(self._trash_counter('trash_dirs'), ndirs)
        self.wait_until_true(lambda: not self._data_objects(),
                             timeout=120, period=2)
