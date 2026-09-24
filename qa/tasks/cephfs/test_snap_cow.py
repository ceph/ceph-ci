import logging

from teuthology.exceptions import CommandFailedError
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

"""
End-to-end tests for snapshot copy-on-write.

The scenarios are chosen to cover the cases where the CoW decision and the
purge decision consult different snaprealms:

  * a rename inside one snaprealm, where the old name has to survive in the
    snapshot but the new name must not appear in it,
  * a rename across snaprealms, where the inode has to keep the source realm's
    snapshots and must _not_ retroactively join the destination realm's older
    ones.
  * a realm that goes empty because its snapshots were deleted, which is the
    state in which the MDS may advance an inode's @first without copying.

The subvolume shape - where a subvolume's snapids never enter its ancestors'
realm - is covered by the same oracle in
test_old_inodes.py::TestStaleOldInodeReclaim, which builds a real subvolume
through TestVolumesHelper rather than an ordinary snapshotted directory.
"""

class TestSnapCoW(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def setUp(self):
        super(TestSnapCoW, self).setUp()
        # rstat propagation up the tree is throttled by mds_dirstat_min_interval
        # (default 1s).  Turn it off so that one snapshot means one
        # propagation attempt, which keeps what these tests observe
        # deterministic rather than a race.
        self.config_set('mds', 'mds_dirstat_min_interval', '0')

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _write(self, path, content):
        self.mount_a.write_file(path, content)

    def _read(self, path):
        return self.mount_a.read_file(path)

    def _snap(self, dirpath, name):
        self.mount_a.run_shell(["mkdir", "-p", "%s/.snap/%s" % (dirpath, name)])

    def _rmsnap(self, dirpath, name):
        self.mount_a.run_shell(["rmdir", "%s/.snap/%s" % (dirpath, name)])

    def _snap_path(self, dirpath, snapname, rel):
        return "%s/.snap/%s/%s" % (dirpath, snapname, rel)

    def _read_snap(self, dirpath, snapname, rel):
        return self._read(self._snap_path(dirpath, snapname, rel))

    def _exists(self, path):
        try:
            self.mount_a.run_shell(["test", "-e", path])
            return True
        except CommandFailedError:
            return False

    def _assert_oracle(self, expected):
        """
        expected: list of (dirpath, snapname, relpath, content).

        The single assertion that matters: every snapshot still serves what was
        visible when it was taken.
        """
        for dirpath, snapname, rel, want in expected:
            got = self._read_snap(dirpath, snapname, rel)
            self.assertEqual(
                got, want,
                "%s should read %r but read %r - a snapshot lost data"
                % (self._snap_path(dirpath, snapname, rel), want, got))

    def test_rename_within_snaprealm(self):
        """
        Renaming inside a realm must leave the OLD name readable in snapshots
        that covered it (journal_cow_dentry leaves a snapped dentry behind) and
        must _not_ make the NEW name appear in them (the new dentry's @first is
        past the current seq).
        """
        d = "d"
        self.mount_a.run_shell(["mkdir", "-p", d])
        self._write("%s/a" % d, "payload-A")
        self._snap(d, "s1")

        self.mount_a.run_shell(["mv", "%s/a" % d, "%s/b" % d])

        # old name survives in the snapshot, with its content
        self._assert_oracle([(d, "s1", "a", "payload-A")])
        # new name must not have existed at s1
        self.assertFalse(
            self._exists(self._snap_path(d, "s1", "b")),
            "the post-rename name appeared in a snapshot taken before it")
        # and the live tree is as renamed
        self.assertEqual(self._read("%s/b" % d), "payload-A")
        self.assertFalse(self._exists("%s/a" % d))

    def test_rename_within_snaprealm_then_overwrite(self):
        """
        Same, but the inode is modified after the rename, so the snapshot has
        to be served from a CoW'd version rather than from the head.
        """
        d = "d"
        self.mount_a.run_shell(["mkdir", "-p", d])
        self._write("%s/a" % d, "before")
        self._snap(d, "s1")
        self.mount_a.run_shell(["mv", "%s/a" % d, "%s/b" % d])
        self._write("%s/b" % d, "after")

        self._assert_oracle([(d, "s1", "a", "before")])
        self.assertEqual(self._read("%s/b" % d), "after")

    def test_rename_across_snaprealms(self):
        """
        Moving an inode to a different realm must:
          a) keep it in the SOURCE realm's snapshots it was already part of -
             record_snaprealm_past_parent() copies them into past_parent_snaps.
          b) not put it into the destination realm's older snapshots -
             current_parent_since jumps past the current global seq.

        Both are asserted through real reads, so this does not depend on how
        the MDS chose to represent it.
        """
        x, y = "x", "y"
        self.mount_a.run_shell(["mkdir", "-p", x, y])

        # give x and y their own snaprealms
        self._snap(x, "x0")
        self._snap(y, "y0")

        self._write("%s/f" % x, "in-x")
        self._snap(x, "x1")          # covers x/f
        self._snap(y, "y1")          # must NOT come to cover f

        self.mount_a.run_shell(["mv", "%s/f" % x, "%s/f" % y])

        # (a) the source realm's snapshot still serves it
        self._assert_oracle([(x, "x1", "f", "in-x")])
        # (b) the destination realm's older snapshot must not
        self.assertFalse(
            self._exists(self._snap_path(y, "y1", "f")),
            "a renamed inode appeared in a destination-realm snapshot taken "
            "before the rename")
        self.assertEqual(self._read("%s/f" % y), "in-x")

        # and a destination snapshot taken AFTER the move does cover it
        self._snap(y, "y2")
        self._assert_oracle([(y, "y2", "f", "in-x")])

    def test_rename_across_snaprealms_then_overwrite(self):
        """
        As above, with a modification after the move so the source realm's
        snapshot must be served from a preserved version.
        """
        x, y = "x", "y"
        self.mount_a.run_shell(["mkdir", "-p", x, y])
        self._snap(x, "x0")
        self._snap(y, "y0")
        self._write("%s/f" % x, "original")
        self._snap(x, "x1")

        self.mount_a.run_shell(["mv", "%s/f" % x, "%s/f" % y])
        self._write("%s/f" % y, "rewritten")

        self._assert_oracle([(x, "x1", "f", "original")])
        self.assertEqual(self._read("%s/f" % y), "rewritten")

    def test_reads_survive_a_realm_going_empty(self):
        """
        Deleting every snapshot in a realm leaves it with an empty snap set,
        which is the state in which the MDS may advance an inode's @first
        past the current seq iwthout keeping a copy - there is nothing that
        could reference the old version.

        Churn writes in that state, then snapshot again, and assert the new
        snapshots are intact.  If advancing @first ever skips a copy that was
        needed, this is where it shows up.
        """
        d = "d/sub"
        self.mount_a.run_shell(["mkdir", "-p", d])

        self._write("%s/f" % d, "era1")
        self._snap("d", "g1")
        self._rmsnap("d", "g1")            # realm now has no snaps

        for i in range(10):                # churn with an empty realm
            self._write("%s/f" % d, "churn-%d" % i)
            self._write("%s/other" % d, "x%d" % i)

        expected = []
        for i in range(6):
            content = "era2-%d" % i
            self._write("%s/f" % d, content)
            name = "g2_%d" % i
            self._snap("d", name)
            expected.append(("d", name, "sub/f", content))

        self._assert_oracle(expected)

    def test_reads_survive_interleaved_create_and_delete(self):
        """
        Snapshot rotation: each removal allocates a fresh snapid and moves the
        filesystem-wide last_destroyed, so the sequence the MDS CoWs against
        keeps moving while the realm's own contents churn.  Surviving
        snapshots must still read back.
        """
        d = "d"
        self.mount_a.run_shell(["mkdir", "-p", d])

        live = []
        for i in range(14):
            content = "rot-%d" % i
            self._write("%s/f" % d, content)
            name = "r%d" % i
            self._snap(d, name)
            live.append((name, content))
            if len(live) > 4:
                gone, _ = live.pop(0)
                self._rmsnap(d, gone)
            # everything still live must read back, every round
            self._assert_oracle([(d, n, "f", c) for n, c in live])
