"""
Roll back a subvolume to a snapshot with `scrub rollback`. The subvolume
and its snapshots are created with the `fs subvolume` commands.

The rollback is verified both through the head (contents and metadata
match the rollback snapshot) and through the snapshots (every snapshot
keeps showing what it captured), which exercises the [first, last]
ranges of the dentries/inodes that the rollback rewrites. The checks are
repeated after journal replay and after reloading the dirfrags from the
metadata pool.
"""

import json
import logging
import os
import time
from textwrap import dedent

from teuthology.exceptions import CommandFailedError

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

STATE_SCRIPT = dedent("""
    import hashlib, json, os, stat

    def state(root):
        out = {{}}
        for dirpath, dirnames, filenames in os.walk(root):
            for name in sorted(dirnames + filenames):
                p = os.path.join(dirpath, name)
                st = os.lstat(p)
                e = {{"mode": st.st_mode}}
                if stat.S_ISREG(st.st_mode):
                    with open(p, "rb") as f:
                        e["md5"] = hashlib.md5(f.read()).hexdigest()
                    e["size"] = st.st_size
                    e["mtime"] = int(st.st_mtime)
                elif stat.S_ISLNK(st.st_mode):
                    e["target"] = os.readlink(p)
                if not stat.S_ISLNK(st.st_mode):
                    e["xattrs"] = {{k: os.getxattr(p, k).decode()
                                   for k in sorted(os.listxattr(p))
                                   if k.startswith("user.")}}
                out[os.path.relpath(p, root)] = e
        return out

    print(json.dumps(state("{path}")))
""")


class TestSnapRollback(CephFSTestCase):
    MDSS_REQUIRED = 1
    CLIENTS_REQUIRED = 1

    SUBVOL = "rbsv"
    # the .meta file of the subvolume: it is in every snapshot and gets
    # rolled back along with everything else
    ROOT_FILES = 1

    def setUp(self):
        super().setUp()
        # Only a subvolume is rolled back, from its root. That has the .meta
        # file and the data directory of the subvolume, which is where the
        # tests make their changes.
        self._subvolume_cmd("create", self.fs.name, self.SUBVOL, "--mode=777")
        path = self.get_ceph_cmd_stdout("fs", "subvolume", "getpath",
                                        self.fs.name, self.SUBVOL).strip()
        self.dir = path.lstrip("/")
        self.root = os.path.dirname(self.dir)

    def tearDown(self):
        # a rollback that the test did not get to wait for
        try:
            self._fail_mgr()
        finally:
            super().tearDown()

    def _fail_mgr(self):
        # A rollback evicts and blocklists every client, including the
        # libcephfs handles of ceph-mgr modules, which the modules keep
        # using: no `fs subvolume` command works after it. Fail over the
        # mgr so that it comes up with clean handles.
        self.run_ceph_cmd("mgr", "fail")
        self.mon_manager.wait_for_mgr_available(timeout=120)

    def _subvolume_cmd(self, *args):
        self.run_ceph_cmd("fs", "subvolume", *args)

    def _mark_subvolume(self, val):
        self.mount_a.run_shell(["sudo", "setfattr", "-n", "ceph.dir.subvolume",
                                "-v", "1" if val else "0", self.root])

    def _state(self, path):
        abs_path = os.path.join(self.mount_a.hostfs_mntpt, path)
        return json.loads(self.mount_a.run_python(STATE_SCRIPT.format(path=abs_path)))

    def _snap_dir(self, snap):
        return os.path.join(self.root, ".snap", snap)

    def _snap_path(self, snap):
        # the data directory of the subvolume in the snapshot
        return os.path.join(self._snap_dir(snap), os.path.basename(self.dir))

    def _mksnap(self, snap):
        self._subvolume_cmd("snapshot", "create", self.fs.name, self.SUBVOL, snap)

    def _rmsnap(self, snap):
        self._subvolume_cmd("snapshot", "rm", self.fs.name, self.SUBVOL, snap)

    def _num_strays(self):
        return self.fs.rank_asok(['perf', 'dump', 'mds_cache', 'num_strays'])['mds_cache']['num_strays']

    def _rollback_start(self, path, snap):
        out = self.fs.run_scrub(["rollback", path, snap])
        self.assertEqual(out["return_code"], 0, f"rollback failed: {out}")
        return out["scrub_tag"]

    def _rollback(self, path, snap):
        return self._rollback_wait(self._rollback_start(path, snap))

    def _rollback_wait(self, tag):
        self.assertTrue(self.fs.wait_until_scrub_complete(tag=tag, sleep=5))
        # rollback counters are reaped into scrub_stats on tick
        def get_stats():
            stats = self.fs.get_scrub_status()['scrub_stats'].get(tag, {})
            return stats.get('rollback')
        self.wait_until_true(lambda: get_stats() is not None, timeout=60)
        stats = get_stats()
        log.info(f"rollback stats: {stats}")
        self._fail_mgr()
        return stats

    def _populate(self):
        """
        Build a directory with three snapshots (s1, s2, s3) and changes in
        between. The rollback target is s2.
        """
        d = self.dir
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            mkdir -p {d}/sub
            echo unchanged > {d}/unchanged
            echo v1 > {d}/modified
            echo deleted > {d}/deleted
            echo replaced-1 > {d}/replaced
            echo meta > {d}/meta
            chmod 644 {d}/meta
            setfattr -n user.a -v 1 {d}/meta
            ln -s target-1 {d}/symlink
            dd if=/dev/urandom of={d}/shrink bs=1M count=10
            dd if=/dev/urandom of={d}/grow bs=1M count=1
            echo inner-1 > {d}/sub/inner
        """))
        self._mksnap("s1")

        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            echo v2 > {d}/modified
        """))
        self._mksnap("s2")

        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            echo v3 > {d}/modified
            rm -f {d}/deleted
            rm -f {d}/replaced
            echo replaced-2 > {d}/replaced
            chmod 600 {d}/meta
            setfattr -n user.a -v 2 {d}/meta
            setfattr -n user.b -v 1 {d}/meta
            rm -f {d}/symlink
            ln -s target-2 {d}/symlink
            truncate -s 1M {d}/shrink
            dd if=/dev/urandom of={d}/grow bs=1M count=11 conv=notrunc oflag=append
            echo inner-2 > {d}/sub/inner
            echo created > {d}/created
        """))
        self._mksnap("s3")

        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            echo v4 > {d}/modified
            echo created-after-s3 > {d}/created2
        """))

    def _check_rolled_back(self, expected, head=None):
        if head is None:
            head = expected["s2"]
        self.assertEqual(self._state(self.dir), head, "head does not match rollback snapshot")
        for snap in ("s1", "s2", "s3"):
            self.assertEqual(self._state(self._snap_path(snap)), expected[snap],
                             f"snapshot {snap} changed after rollback")

    def _check_no_damage(self):
        # write out backtraces of the rewritten inodes
        self.fs.flush()
        out = self.fs.run_scrub(["start", f"/{self.root}", "recursive,force"])
        self.assertEqual(out["return_code"], 0)
        self.assertTrue(self.fs.wait_until_scrub_complete(tag=out["scrub_tag"], sleep=5))
        self.assertEqual(self.fs.get_damage(rank=0), [], "metadata damage after rollback")

    def _restart_mds(self, flush=False):
        if flush:
            self.fs.flush()
        self.mount_a.umount_wait()
        self.fs.rank_fail(rank=0)
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()

    def test_rollback_files(self):
        """
        Roll back regular files and symlinks: unchanged, modified, deleted,
        replaced, created after the snapshot, metadata-only changes, and
        files shrunk/grown across object boundaries.
        """
        self._populate()

        expected = {snap: self._state(self._snap_path(snap)) for snap in ("s1", "s2", "s3")}
        pre_rollback = self._state(self.dir)
        self.assertNotEqual(pre_rollback, expected["s2"])

        # no clients during rollback; let unlinked inodes get purged
        self.mount_a.umount_wait()
        self.wait_until_equal(self._num_strays, 0, timeout=60)

        stats = self._rollback(f"/{self.root}", "s2")
        # unchanged, modified, meta, shrink, grow, sub/inner
        self.assertEqual(stats["rolled_back"], 6 + self.ROOT_FILES)
        # replaced, symlink, created, created2
        self.assertEqual(stats["removed"], 4)
        # deleted, replaced, symlink
        self.assertEqual(stats["reinstated"], 3)
        # everything deleted has been purged
        self.assertEqual(stats["relinked"], 0)
        self.assertEqual(stats["skipped"], 0)
        self.assertEqual(stats["failed"], 0)

        self.mount_a.mount_wait()
        self._check_rolled_back(expected)
        self._check_no_damage()

        # writes after the rollback belong to head and to new snapshots
        # only (head first > newest snapshot).
        self.mount_a.run_shell_payload(f"echo v5 > {self.dir}/modified")
        self._mksnap("s4")
        self.mount_a.run_shell_payload(f"echo v6 > {self.dir}/modified")
        self.assertEqual(self.mount_a.read_file(f"{self._snap_path('s4')}/modified"), "v5")
        self.assertEqual(self.mount_a.read_file(f"{self._snap_path('s3')}/modified"), "v3")
        self.assertEqual(self.mount_a.read_file(f"{self._snap_path('s2')}/modified"), "v2")
        self.assertEqual(self.mount_a.read_file(f"{self.dir}/modified"), "v6")
        self._rmsnap("s4")
        head = self._state(self.dir)

        # journal replay
        self._restart_mds()
        self._check_rolled_back(expected, head)

        # reload from the metadata pool
        self._restart_mds(flush=True)
        self._check_rolled_back(expected, head)
        self._check_no_damage()

    def test_rollback_twice(self):
        """
        Roll back to s2, then to s1: the first rollback's [first, last]
        splits must not get in the way of the second.
        """
        self._populate()
        expected = {snap: self._state(self._snap_path(snap)) for snap in ("s1", "s2", "s3")}

        self.mount_a.umount_wait()
        self.wait_until_equal(self._num_strays, 0, timeout=60)
        self._rollback(f"/{self.root}", "s2")
        self.mount_a.mount_wait()
        self._check_rolled_back(expected)

        self.mount_a.umount_wait()
        self.wait_until_equal(self._num_strays, 0, timeout=60)
        stats = self._rollback(f"/{self.root}", "s1")
        self.assertEqual(stats["failed"], 0)
        self.mount_a.mount_wait()
        self.assertEqual(self._state(self.dir), expected["s1"])
        for snap in ("s1", "s2", "s3"):
            self.assertEqual(self._state(self._snap_path(snap)), expected[snap])
        self._check_no_damage()

    def test_rollback_renamed(self):
        """
        Files renamed after the snapshot get moved back, keeping their inode
        number: the old name sorted before and after the new name, swapped
        names, a move across directories, and a snapshot covering the new
        names (s3).
        """
        d = self.dir
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            mkdir -p {d}/sub
            echo rn1 > {d}/rn1
            echo rn4 > {d}/rn4
            echo sw1 > {d}/sw1
            echo sw2 > {d}/sw2
            echo x > {d}/sub/x
        """))
        names = ("rn1", "rn4", "sw1", "sw2", "sub/x")
        inos = {n: self.mount_a.path_to_ino(f"{d}/{n}") for n in names}
        self._mksnap("s1")
        self._mksnap("s2")
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            mv {d}/rn1 {d}/rn2
            mv {d}/rn4 {d}/rn3
            mv {d}/sw1 {d}/swtmp
            mv {d}/sw2 {d}/sw1
            mv {d}/swtmp {d}/sw2
            mv {d}/sub/x {d}/moved
            echo more >> {d}/rn2
        """))
        self._mksnap("s3")
        expected = {snap: self._state(self._snap_path(snap)) for snap in ("s1", "s2", "s3")}

        self.mount_a.umount_wait()
        stats = self._rollback(f"/{self.root}", "s2")
        # Nothing gets purged during the rollback: whatever it removes before
        # reaching the name it belongs to is relinked from stray, not recreated.
        self.assertEqual(stats["relinked"], 5)
        self.assertEqual(stats["rolled_back"], 5 + self.ROOT_FILES)
        # rn3, moved, and sw1's head inode
        self.assertEqual(stats["removed"], 3)
        self.assertEqual(stats["reinstated"], 0)
        self.assertEqual(stats["skipped"], 0)
        self.assertEqual(stats["failed"], 0)

        self.mount_a.mount_wait()
        self._check_rolled_back(expected)
        for name, ino in inos.items():
            self.assertEqual(self.mount_a.path_to_ino(f"{d}/{name}"), ino)
        self._check_no_damage()

        self._restart_mds(flush=True)
        self._check_rolled_back(expected)

    def test_rollback_stray_old_inodes(self):
        """
        A deleted file that was hardlinked when a snapshot was taken stays in
        stray (a newer snapshot needs its old_inodes). Rolling back to an
        older snapshot relinks it; the newer snapshot keeps its view.
        """
        d = self.dir
        self.mount_a.write_file(f"{d}/f", "v1\n")
        ino = self.mount_a.path_to_ino(f"{d}/f")
        self._mksnap("s1")
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            ln {d}/f {d}/g
            echo v2 > {d}/f
        """))
        self._mksnap("s2")
        self.mount_a.run_shell(["rm", "-f", f"{d}/g", f"{d}/f"])
        expected = {snap: self._state(self._snap_path(snap)) for snap in ("s1", "s2")}

        self.mount_a.umount_wait()
        self.wait_until_equal(self._num_strays, 1, timeout=60)
        stats = self._rollback(f"/{self.root}", "s1")
        self.assertEqual(stats["relinked"], 1)
        self.assertEqual(stats["rolled_back"], 1 + self.ROOT_FILES)
        self.assertEqual(stats["reinstated"], 0)
        self.assertEqual(stats["failed"], 0)
        self.assertEqual(self._num_strays(), 0)

        self.mount_a.mount_wait()
        self.assertEqual(self._state(d), expected["s1"])
        self.assertEqual(self.mount_a.path_to_ino(f"{d}/f"), ino)
        self.assertEqual(self.mount_a.read_file(f"{self._snap_path('s2')}/f"), "v2")
        self.assertEqual(self.mount_a.read_file(f"{self._snap_path('s2')}/g"), "v2")
        for snap in ("s1", "s2"):
            self.assertEqual(self._state(self._snap_path(snap)), expected[snap])

        # the link count comes back from the journal, too
        self._restart_mds()
        self.assertEqual(self.mount_a.stat(f"{d}/f")["st_nlink"], 1)
        self.assertEqual(self._state(d), expected["s1"])
        self._check_no_damage()

    def test_rollback_waits_for_queued_purge(self):
        """
        A purge queued before the rollback can't be called back. Reinstating
        the file waits for it, or the purge would delete the restored objects.
        """
        d = self.dir
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            mkdir -p {d}
            dd if=/dev/urandom of={d}/f bs=1M count=10
        """))
        self._mksnap("s1")
        expected = self._state(self._snap_path("s1"))

        self.config_set('mds', 'mds_max_purge_files', 0)
        self.mount_a.run_shell(["rm", "-f", f"{d}/f"])
        self.mount_a.umount_wait()
        # not in stray anymore, the item is durable in the purge queue
        self.wait_until_equal(self._num_strays, 0, timeout=60)

        tag = self._rollback_start(f"/{self.root}", "s1")
        time.sleep(15)
        status = self.fs.get_scrub_status()['scrubs'][tag]['rollback']
        self.assertEqual(status["reinstated"], 0)

        self.config_set('mds', 'mds_max_purge_files', 64)
        stats = self._rollback_wait(tag)
        self.assertEqual(stats["reinstated"], 1)
        self.assertEqual(stats["failed"], 0)

        self.mount_a.mount_wait()
        self.assertEqual(self._state(d), expected)
        # the finished purge isn't executed again by a restart
        self._restart_mds()
        self.assertEqual(self._state(d), expected)
        self._check_no_damage()

    def test_rollback_head_kept_by_other_link(self):
        """
        The snapshot inode is alive because of a hardlink outside the
        subvolume, made while the directory wasn't marked as one: it can't
        be moved, and the file now at the name must not get removed for it.
        """
        d = self.dir
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            mkdir -p other
            echo v1 > {d}/f
        """))
        self._mksnap("s1")
        # hardlinks do not cross a subvolume
        with self.assertRaises(CommandFailedError):
            self.mount_a.run_shell(["ln", f"{d}/f", "other/g"])
        self._mark_subvolume(False)
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            ln {d}/f other/g
            rm -f {d}/f
            echo new > {d}/f
        """))
        self._mark_subvolume(True)

        self.mount_a.umount_wait()
        stats = self._rollback(f"/{self.root}", "s1")
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(stats["removed"], 0)
        self.assertEqual(stats["failed"], 0)

        self.mount_a.mount_wait()
        self.assertEqual(self.mount_a.read_file(f"{d}/f"), "new")
        self.assertEqual(self.mount_a.read_file("other/g"), "v1")
        self._check_no_damage()

    def _check_links(self, links):
        """
        `links` maps an inode number to (names, link count).
        """
        d = self.dir
        for ino, (names, nlink) in links.items():
            for name in names:
                st = self.mount_a.stat(f"{d}/{name}")
                self.assertEqual(st["st_ino"], ino, f"{name} links another inode")
                self.assertEqual(st["st_nlink"], nlink, f"link count of {name}")

    def test_rollback_hardlinks(self):
        """
        Every name ends up linking the inode that it linked in the snapshot,
        wherever the inode is stored now (unlinking the primary link moves it
        to another link, or to stray), and a hardlinked inode is rolled back
        once.
        """
        d = self.dir
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            mkdir -p {d}/sub
            # both links stay
            echo keep-1 > {d}/keep
            ln {d}/keep {d}/sub/keep.link
            # gets a link after the snapshot
            echo added-1 > {d}/added
            # loses the remote link, then the primary link
            echo rmremote-1 > {d}/rmremote
            ln {d}/rmremote {d}/rmremote.link
            echo rmprimary-1 > {d}/rmprimary
            ln {d}/rmprimary {d}/rmprimary.link
            # loses both links
            dd if=/dev/urandom of={d}/gone bs=1M count=5
            ln {d}/gone {d}/gone.link
            # has a link replaced by another file
            echo repl-1 > {d}/repl
            ln {d}/repl {d}/repl.link
            # gets renamed and then linked
            echo mv-1 > {d}/mvold
        """))
        groups = {
            "keep": (["keep", "sub/keep.link"], 2),
            "added": (["added"], 1),
            "rmremote": (["rmremote", "rmremote.link"], 2),
            "rmprimary": (["rmprimary", "rmprimary.link"], 2),
            "gone": (["gone", "gone.link"], 2),
            "repl": (["repl", "repl.link"], 2),
            "mvold": (["mvold"], 1),
        }
        links = {self.mount_a.path_to_ino(f"{d}/{name}"): v
                 for name, v in groups.items()}
        self._mksnap("s1")

        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            echo keep-2 > {d}/sub/keep.link
            ln {d}/added {d}/added.link
            echo added-2 > {d}/added.link
            rm -f {d}/rmremote.link
            echo rmremote-2 > {d}/rmremote
            rm -f {d}/rmprimary
            echo rmprimary-2 > {d}/rmprimary.link
            rm -f {d}/gone {d}/gone.link
            rm -f {d}/repl
            echo repl-new > {d}/repl
            echo repl-2 > {d}/repl.link
            mv {d}/mvold {d}/mvnew
            ln {d}/mvnew {d}/mvnew.link
            echo mv-2 > {d}/mvnew.link
        """))
        self._mksnap("s2")
        self.mount_a.run_shell_payload(f"echo keep-3 > {d}/keep")
        expected = {snap: self._state(self._snap_path(snap)) for snap in ("s1", "s2")}

        def check():
            self.assertEqual(self._state(d), expected["s1"],
                             "head does not match rollback snapshot")
            for snap in ("s1", "s2"):
                self.assertEqual(self._state(self._snap_path(snap)), expected[snap],
                                 f"snapshot {snap} changed after rollback")
            self._check_links(links)

        self.mount_a.umount_wait()
        stats = self._rollback(f"/{self.root}", "s1")
        self.assertEqual(stats["skipped"], 0)
        self.assertEqual(stats["failed"], 0)
        # one rollback per inode, not per link
        self.assertEqual(stats["rolled_back"], len(links) + self.ROOT_FILES)
        # added.link, mvnew, mvnew.link, and the file that replaced repl
        self.assertEqual(stats["removed"], 4)
        # gone and mvold come back from stray; gone.link, rmremote.link,
        # rmprimary and repl get a link, unless the inode is still in stray
        # (not moved to its other link yet), from where it is relinked.
        self.assertEqual(stats["linked"] + stats["relinked"], 6)
        self.assertGreaterEqual(stats["linked"], 2)
        self.assertEqual(stats["reinstated"], 0)

        self.mount_a.mount_wait()
        check()
        self._check_no_damage()

        # journal replay
        self._restart_mds()
        check()

        # reload from the metadata pool; by now the strays that the rollback
        # left behind have been purged or moved to a link
        self._restart_mds(flush=True)
        check()
        self._check_no_damage()

    def test_rollback_hardlinks_twice(self):
        """
        Roll back to s2, then to s1, with the links differing in each.
        """
        d = self.dir
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            mkdir -p {d}
            echo a-1 > {d}/a
            echo b-1 > {d}/b
            ln {d}/b {d}/b.link
        """))
        ino_a = self.mount_a.path_to_ino(f"{d}/a")
        ino_b = self.mount_a.path_to_ino(f"{d}/b")
        self._mksnap("s1")
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            ln {d}/a {d}/a.link
            echo a-2 > {d}/a
            rm -f {d}/b
            echo b-2 > {d}/b.link
        """))
        self._mksnap("s2")
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            rm -f {d}/a {d}/a.link {d}/b.link
            echo new > {d}/a
        """))
        expected = {snap: self._state(self._snap_path(snap)) for snap in ("s1", "s2")}

        self.mount_a.umount_wait()
        stats = self._rollback(f"/{self.root}", "s2")
        self.assertEqual(stats["skipped"], 0)
        self.assertEqual(stats["failed"], 0)
        self.mount_a.mount_wait()
        self.assertEqual(self._state(d), expected["s2"])
        self._check_links({ino_a: (["a", "a.link"], 2), ino_b: (["b.link"], 1)})

        self.mount_a.umount_wait()
        stats = self._rollback(f"/{self.root}", "s1")
        self.assertEqual(stats["skipped"], 0)
        self.assertEqual(stats["failed"], 0)
        self.mount_a.mount_wait()
        self.assertEqual(self._state(d), expected["s1"])
        self._check_links({ino_a: (["a"], 1), ino_b: (["b", "b.link"], 2)})
        for snap in ("s1", "s2"):
            self.assertEqual(self._state(self._snap_path(snap)), expected[snap])
        self._check_no_damage()

        self._restart_mds(flush=True)
        self.assertEqual(self._state(d), expected["s1"])
        self._check_links({ino_a: (["a"], 1), ino_b: (["b", "b.link"], 2)})

    def test_rollback_reinstated_hardlink(self):
        """
        A reinstated inode knows about the snapshots it is in, including
        those older than the one it was reinstated from: hard linking it
        (the inode gets its own snaprealm) must not hide them from the
        next rollback.
        """
        d = self.dir
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            mkdir -p {d}
            echo v1 > {d}/f
        """))
        self._mksnap("s1")
        # a snapshot inode of its own for each of the snapshots
        self.mount_a.run_shell_payload(f"echo v2 > {d}/f")
        self._mksnap("s2")
        self.mount_a.run_shell_payload(f"rm -f {d}/f")
        expected = {snap: self._state(self._snap_path(snap)) for snap in ("s1", "s2")}

        # have the head inode purged
        self.mount_a.umount_wait()
        self.wait_until_equal(self._num_strays, 0, timeout=60)
        stats = self._rollback(f"/{self.root}", "s2")
        self.assertEqual(stats["reinstated"], 1)
        self.assertEqual(stats["skipped"], 0)
        self.assertEqual(stats["failed"], 0)
        self.mount_a.mount_wait()
        self.assertEqual(self._state(d), expected["s2"])

        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            ln {d}/f {d}/f.link
            echo v3 > {d}/f
        """))

        self.mount_a.umount_wait()
        stats = self._rollback(f"/{self.root}", "s1")
        self.assertEqual(stats["skipped"], 0)
        self.assertEqual(stats["failed"], 0)
        self.mount_a.mount_wait()
        self.assertEqual(self._state(d), expected["s1"])
        for snap in ("s1", "s2"):
            self.assertEqual(self._state(self._snap_path(snap)), expected[snap])
        self._check_no_damage()

        self._restart_mds(flush=True)
        self.assertEqual(self._state(d), expected["s1"])

    def test_rollback_evicts_clients(self):
        """
        Mounted clients are evicted before the rollback starts.
        """
        self._populate()
        expected = self._state(self._snap_path("s2"))
        client_id = self.mount_a.get_global_id()

        # An evicted kernel client that still has dirty pages cannot be
        # force unmounted: the pages are never written, the inodes stay
        # pinned and the unmount ends in "Busy inodes after unmount".
        self.mount_a.run_shell(["sync", "-f", "."])

        stats = self._rollback(f"/{self.root}", "s2")
        self.assertEqual(stats["failed"], 0)
        self.wait_until_evicted(client_id)

        self.mount_a.kill()
        self.mount_a.kill_cleanup()
        self.mount_a.mount_wait()
        self.assertEqual(self._state(self.dir), expected)

    def test_rollback_bad_args(self):
        self.mount_a.run_shell_payload(dedent(f"""
            set -ex
            mkdir -p {self.dir}/sub plain
            mkdir plain/.snap/s1
        """))
        self.mount_a.write_file(f"{self.dir}/file", "data")
        self._mksnap("s1")
        client_id = self.mount_a.get_global_id()

        out = self.fs.run_scrub(["rollback", f"/{self.root}", "nosuchsnap"])
        self.assertEqual(out["return_code"], -2)  # ENOENT
        out = self.fs.run_scrub(["rollback", f"/{self.dir}/file", "s1"])
        self.assertEqual(out["return_code"], -20)  # ENOTDIR
        out = self.fs.run_scrub(["rollback", "/nosuchdir", "s1"])
        self.assertEqual(out["return_code"], -2)
        # only a subvolume, from its root
        out = self.fs.run_scrub(["rollback", "/plain", "s1"])
        self.assertEqual(out["return_code"], -22)  # EINVAL
        # what `fs subvolume getpath` gives is the data directory
        out = self.fs.run_scrub(["rollback", f"/{self.dir}", "s1"])
        self.assertEqual(out["return_code"], -22)
        out = self.fs.run_scrub(["rollback", f"/{self.dir}/sub", "s1"])
        self.assertEqual(out["return_code"], -22)

        # failed requests do not evict anyone
        self.assertIn(client_id, [s['id'] for s in self._session_list()])
        self.mount_a.run_shell(["ls", self.dir])
