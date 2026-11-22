"""
Tests for CEPH_MDS_OP_RMTREE.

The kernel and fuse clients still walk a tree themselves. These tests call
libcephfs so the new request is what the MDS sees.
"""

import errno
import logging
import time
from textwrap import dedent

from teuthology.exceptions import CommandFailedError

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestRmtree(CephFSTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 2

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
        self.fail(f"trash did not drain on ranks {ranks}: {self._trash(0)}")

    def _stat_fails(self, mount, path):
        try:
            mount.run_shell(["stat", path])
        except CommandFailedError:
            return
        self.fail(f"{path} still exists")

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
        before = int(self.mount_a.getfattr("parent", "ceph.dir.rbytes"))
        self.fs.rank_asok(['trash', 'pause'])
        try:
            self._rmtree("/parent/tree")
            after = int(self.mount_a.getfattr("parent", "ceph.dir.rbytes"))
            self.assertLess(after, before)
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
        # Snapshot a directory this client owns. The mount root is chmod
        # 1777 and owned by root, so rmdir of its .snap fails with EPERM
        # for the unprivileged test user.
        self.mount_a.run_shell(["mkdir", "-p", "parent/tree"])
        self.mount_a.write_file("parent/tree/file", "snapdata")
        self.mount_a.run_shell(["mkdir", "parent/.snap/ancestor"])
        self._rmtree("/parent/tree")
        self.assertEqual(
            self.mount_a.read_file("parent/.snap/ancestor/tree/file"),
            "snapdata")
        self._stat_fails(self.mount_a, "parent/tree")
        self.mount_a.run_shell(["rmdir", "parent/.snap/ancestor"])
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
        proc = self.mount_a.run_shell(["python3", "-c", script])
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

    def test_multimds_and_rank_removal(self):
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        self.mount_a.run_shell(["mkdir", "-p", "d0/d1/sub"])
        self.mount_a.write_file("d0/d1/sub/file", "data")
        self.mount_a.setfattr("d0", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d0/d1", "ceph.dir.pin", "1")
        time.sleep(5)
        self._rmtree("/d0")
        self._stat_fails(self.mount_a, "d0")
        self._wait_trash_idle(ranks=(0, 1), timeout=180)
        self.fs.set_max_mds(1)
        self.fs.wait_for_daemons()
        self._wait_trash_idle(ranks=(0,))

    def test_path_restricted_cap(self):
        self.mount_a.run_shell(["mkdir", "-p", "allowed/tree", "secret/tree"])
        self.mount_a.write_file("allowed/tree/f", "a")
        self.mount_a.write_file("secret/tree/f", "s")
        keyring = self.fs.authorize("rmtree", ("/allowed", "rw"))
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
            except PermissionError:
                print('rpc-eperm')
            fs.rmtree(b'/walk')
            print('walk-ok')
            fs.shutdown()
            """)
        out = self.mount_a.run_python(script, sudo=False)
        self.assertIn("rpc-eperm", out)
        self.assertIn("walk-ok", out)
        self._stat_fails(self.mount_a, "walk")
