import os
import json
import logging
import collections
from io import StringIO
from pathlib import Path

from teuthology.exceptions import CommandFailedError

from tasks.cephfs.test_volumes import TestVolumesHelper

log = logging.getLogger(__name__)

# root inode 0x1 has a single dirfrag; /volumes' dentry lives in it, so
# this object's fnode carries the snap_purged_thru that gates the purge
# of /volumes' old_inodes.
# scratch path on remote for the raw fnode blob
FNODE_TMP_PATH = "/tmp/ceph_test_old_inodes_fnode.bin"


class TestOldInodeGrowth(TestVolumesHelper):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    # 90 snapshots on one subvolume, under mds_max_snaps_per_dir (default 100)
    SNAPS_PER_BATCH = 10
    BATCHES = 9

    def setUp(self):
        super(TestOldInodeGrowth, self).setUp()

        # rstat propagation up the tree is throttled by mds_dirstat_min_interval
        # (default 1s).  With the throttle on, whether a given ancestor gets
        # CoW'd for a given snapshot is a race; turn it off so one snapshot
        # means one propagation attempt.
        self.config_set('mds', 'mds_dirstat_min_interval', '0')

    def _set_global_seq_config(self, enabled):
        self.config_set('mds', 'mds_use_global_snaprealm_seq_for_subvol',
                        enabled)

    def _paths(self, group, subvol):
        """Ordered subvol -> root chain, as filesystem-absolute paths."""
        sv = Path(self._fs_cmd("subvolume", "getpath", self.volname,
                               subvol, group).strip())
        return collections.OrderedDict([
            ("subvol", sv.parent),
            ("group", sv.parent.parent),
            ("volumes", sv.parent.parent.parent),
            ("root", Path("/")),
        ])

    def _ino(self, path):
        rel = str(path).lstrip("/") or "."
        return self.mount_a.path_to_ino(rel)

    def _old_inode_counts(self, paths):
        """
        count of @old_inodes for each inode in the chain.

        Reads the MDS's in-memory stat.: purge_stale_snap_data() runs
        during the dirfrag commit so flushing the journal before measuring
        would hide the peak we are hunting.
        """
        counts = collections.OrderedDict()
        for label, path in paths.items():
            try:
                dump = self.fs.mds_asok(['dump', 'inode', hex(self._ino(path))])
            except CommandFailedError:
                counts[label] = None       # not in cache
                continue
            counts[label] = len(dump["old_inodes"]) if dump else None
        return counts

    def _snap_table(self):
        d = self.fs.mds_asok(['dump', 'snaps', '--server'])
        return int(d['last_created']), int(d['last_destroyed'])

    def _observe(self, label, paths, gate=False):
        counts = self._old_inode_counts(paths)
        last_created, last_destroyed = self._snap_table()
        suffix = ""
        if gate:
            bits = []
            for label, (obj, purged_thru, is_open) in \
                    self._gate_states(paths).items():
                bits.append("%s[%s]=%s/%s"
                            % (label, obj,
                               "?" if purged_thru is None else purged_thru,
                               "?" if is_open is None
                               else ("OPEN" if is_open else "SHUT")))
            suffix = " gates(purged_thru/state): " + " ".join(bits)
        log.debug("OLDINO %-28s old_inodes=%s last_created=%d "
                 "last_destroyed=%d%s",
                 label, json.dumps(counts), last_created, last_destroyed,
                 suffix)
        return counts

    def _dirfrag_object(self, ino):
        """
        RADOS object name for the (unfragmented) dirfrag of @ino.
        """
        return "%x.%08x" % (ino, 0)

    def _gates(self, paths):
        """
        label -> dirfrag object whose fnode gates that inode's purge.

        X's old_inodes are purged while X's dentry is written, i.e. during the
        commit of X's parent dirfrag -- so the gate for X is the parent
        dirfrag's fnode, not X's own. There is one watermark per dirfrag,
        so different ancestors can sit in different gate states at the same time.

        'subvol' is absent on purpose: it has its own snaprealm, so _parse_dentry
        takes the ungated branch and no fnode watermark applies. 'root' is absent
        too: a base inode commit has the purge unconditional.
        """
        return collections.OrderedDict([
            ("volumes", self._dirfrag_object(self._ino(paths["root"]))),
            ("group", self._dirfrag_object(self._ino(paths["volumes"]))),
        ])

    def _snap_purged_thru(self, dirfrag_obj):
        """
        snap_purged_thru out of `dirfrag_obj`'s on-disk fnode.

        CAREFUL: the gate tests the IN-MEMORY fnode.  _omap_fetched() sets
        snap_purged_thru in memory and only calls log_mark_dirty(), so the
        value does not reach RADOS until the next commit.  CDir::dump does not
        expose the field, so there is no asok route to the live value - which
        means this on-disk read is only meaningful AFTER a commit has flushed
        the fnode.  Read it any earlier and it reports the previous value.

        Returns None if the object or ceph-dencoder is unavailable; the gate
        reading is diagnostic, never load bearing for an assertion.
        """
        try:
            # Write the raw header to a file on the remote rather than piping
            # it back through stdout: the encoded fnode contains NUL bytes,
            # and anything binary that reaches teuthology.log makes grep treat
            # the WHOLE log as binary ("binary file matches"), which hides
            # every OLDINO line in the run.  Only JSON crosses the wire here.
            self.fs.radosm(["getomapheader", dirfrag_obj, FNODE_TMP_PATH])
            out = self.fs.tool_remote.run(
                args=[os.path.join(self.fs._prefix, "ceph-dencoder"),
                      "type", "fnode_t",
                      "import", FNODE_TMP_PATH,
                      "decode", "dump_json"],
                stdout=StringIO()).stdout.getvalue()
            return int(json.loads(out)['snap_purged_thru'])
        except Exception as e:
            log.warning("could not read snap_purged_thru from %s: %s",
                        dirfrag_obj, e)
            return None

    def _gate_states(self, paths):
        """label -> (dirfrag_obj, snap_purged_thru, is_open)"""
        _, last_destroyed = self._snap_table()
        states = collections.OrderedDict()
        for label, obj in self._gates(paths).items():
            purged_thru = self._snap_purged_thru(obj)
            states[label] = (
                obj, purged_thru,
                None if purged_thru is None else purged_thru < last_destroyed)
        return states

    def _commit_dirfrags(self):
        """
        Force the dirfrag commit that gives purge_stale_snap_data() a chance
        to run.

        Flushed twice on purpose: the first flush commits (and purges), but the
        flush itself drives predirty_journal_parents() and can immediately
        re-CoW.  Same reasoning as TestVolumesHelper._verify_old_inodes().
        """
        self.fs.mds_asok(["flush", "journal"])
        self.fs.mds_asok(["flush", "journal"])

    def _restart_mds_to_shut_gate(self, paths):
        """
        Force root's dirfrag to be re-fetched from RADOS, which is the only
        thing that advances snap_purged_thru.

        On a brand new file system, root's dirfrag is built in memory and is
        already complete. @snap_purged_thru therefore stays at its default 0
        while last_destroyed sits at its birth value 1 and the gate is OPEN
        (0 < 1) - which is definitely not the state a long-lived cluster is in.

        Flushing first matters: it commits and trims the log so that replay
        does not simply repopulate root's dirfrag from the journal, which
        would again skip the fetch.
        """
        self._commit_dirfrags()

        self.fs.fail()
        self.mount_a.umount_wait(force=True)
        self.fs.set_joinable()
        self.fs.wait_for_daemons()
        self.mount_a.mount_wait()

        # _omap_fetched() only dirtied the fnode in memory. Commit so the new
        # watermark actually lands on disk, otherwise @snap_purged_thru returns
        # the pre-restart value and we misread the gate as open while the MDS
        # is really treating it as shut.
        self._commit_dirfrags()
        for label, (obj, purged_thru, is_open) in \
                self._gate_states(paths).items():
            log.info("OLDINO post-restart %-8s dirfrag=%s "
                     "snap_purged_thru=%s -> %s", label, obj,
                     "?" if purged_thru is None else purged_thru,
                     "?" if is_open is None
                     else ("OPEN" if is_open else "SHUT"))

    def _assert_gate(self, paths, expected_open):
        """
        Assert _eveny_ gate this test depends on is in the state it needs,
        before drawing any conclusion from the run.  A test that silently
        runs in the wrong gate state produces a confidently wrong answer.

        There is one watermark per dirfrag, so /volumes and <group> are gated
        independently and both have to be checked - they only moved together
        in earlier runs because the restart re-fetched the whole chain.
        """
        _, last_destroyed = self._snap_table()
        states = self._gate_states(paths)
        want = "OPEN" if expected_open else "SHUT"

        for label, (obj, purged_thru, is_open) in states.items():
            log.info("OLDINO GATE %-8s dirfrag=%s snap_purged_thru=%s "
                     "last_destroyed=%d -> %s (wanted %s)",
                     label, obj,
                     "?" if purged_thru is None else purged_thru,
                     last_destroyed,
                     "?" if is_open is None
                     else ("OPEN" if is_open else "SHUT"), want)

        unreadable = [l for l, (_, pt, _o) in states.items() if pt is None]
        if unreadable:
            self.skipTest("could not read snap_purged_thru for %s; cannot "
                          "establish the gate state" % ", ".join(unreadable))

        wrong = {l: (obj, pt) for l, (obj, pt, io) in states.items()
                 if io != expected_open}
        self.assertFalse(
            wrong,
            "precondition not met: wanted every purge gate %s, but %s "
            "(last_destroyed=%d). If SHUT was wanted and a gate reads OPEN, "
            "either that dirfrag was never re-fetched on restart "
            "(MDCache::open_root() found it already complete, e.g. journal "
            "replay repopulated it), or the fetched watermark reached memory "
            "but not disk - CDir::_omap_fetched() only marks the fnode dirty."
            % (want,
               ", ".join("%s[%s] snap_purged_thru=%s" % (l, obj, pt)
                         for l, (obj, pt) in wrong.items()),
               last_destroyed))
        return states

    def _snapshot_and_commit_loop(self, group, subvol, snapnames):
        """
        Take snapshots in batches, forcing a dirfrag commit after each batch,
        and record /volumes before and after each commit.  Returns
        (pre_series, post_series, root_post_series).
        """
        paths = self._paths(group, subvol)
        pre_series, post_series, root_post_series = [], [], []

        for b in range(self.BATCHES):
            names = ["s_%d_%d" % (b, i) for i in range(self.SNAPS_PER_BATCH)]
            for name in names:
                self._fs_cmd("subvolume", "snapshot", "create", self.volname,
                             subvol, name, group)
            snapnames.extend(names)
            taken = (b + 1) * self.SNAPS_PER_BATCH

            pre = self._observe("%d snaps, pre-commit" % taken, paths)
            self._commit_dirfrags()
            post = self._observe("%d snaps, post-commit" % taken, paths,
                                 gate=True)

            pre_series.append(self._volumes_count(pre))
            post_series.append(self._volumes_count(post))
            root_post_series.append(post["root"])

        log.info("OLDINO SERIES /volumes pre-commit : %s", pre_series)
        log.info("OLDINO SERIES /volumes post-commit: %s", post_series)
        log.info("OLDINO SERIES root     post-commit: %s", root_post_series)
        return pre_series, post_series, root_post_series

    def _assert_root_was_reclaimed(self, root_post_series):
        """
        Control: root accumulates at the same rate as /volumes but reclaims by
        a different route.  A dirty base inode goes purges unconditionally.
        If root does not come back down, the commits are not happening and
        nothing else in the run is interpretable.
        """
        for i, n in enumerate(root_post_series):
            self.assertIsNotNone(n, "root not in cache at batch %d" % i)
            self.assertLessEqual(
                n, 2,
                "root still has %d old_inodes after a commit (series=%s). "
                "root inode purges unconditionally so either the flush did "
                "not commit anything or root's realm is not empty - the rest "
                "of this test is not interpretable until that is explained."
                % (n, root_post_series))

    def _volumes_count(self, counts):
        """old_inodes on /volumes, asserting the inode was actually cached."""
        self.assertIsNotNone(
            counts["volumes"],
            "/volumes was not in the MDS cache, cannot measure old_inodes "
            "(counts=%s)" % counts)
        return counts["volumes"]

    def _make_subvolume(self):
        group = self._gen_subvol_grp_name()
        subvol = self._gen_subvol_name()
        self._fs_cmd("subvolumegroup", "create", self.volname, group)
        self._fs_cmd("subvolume", "create", self.volname, subvol, group,
                     "--mode=777")
        return group, subvol

    def _cleanup(self, group, subvol, snapnames):
        for name in snapnames:
            try:
                self._fs_cmd("subvolume", "snapshot", "rm", self.volname,
                             subvol, name, group, "--force")
            except CommandFailedError:
                pass
        self._fs_cmd("subvolume", "rm", self.volname, subvol, group, "--force")
        self._fs_cmd("subvolumegroup", "rm", self.volname, group, "--force")
        self._wait_for_trash_empty()

    def test_growth_with_subvolume_snapshots_only(self):
        """
        Pure subvolume use case.  Subvolume snapshots only, nothing snapshotted
        outside /volumes/<group>/<subvol>, and nothing ever deleted.  Default
        config (mds_use_global_snaprealm_seq_for_subvol = true).

        Claim under test: old_inodes on /volumes grows roughly linearly with
        the number of subvolume snapshots, even though /volumes' snaprealm can
        never reference any of them.

        PASS => subvolume snapshots alone are sufficient to drive the buildup,
                with no snapshot anywhere outside the subvolume.
        FAIL => either nothing accumulates (the CoW does not reach /volumes),
                or something reclaims mid-run (the series dips).  Both are
                informative; read the OLDINO lines either way.
        """
        self._set_global_seq_config(True)
        # suppress trimming: we want the in-memory peak with no commit at all
        self.config_set('mds', 'mds_log_max_segments', '1024')

        group, subvol = self._make_subvolume()
        paths = self._paths(group, subvol)
        snapnames = []

        try:
            self._observe("baseline", paths, gate=True)

            series = []
            for b in range(self.BATCHES):
                names = ["s_%d_%d" % (b, i)
                         for i in range(self.SNAPS_PER_BATCH)]
                for name in names:
                    self._fs_cmd("subvolume", "snapshot", "create",
                                 self.volname, subvol, name, group)
                snapnames.extend(names)
                taken = (b + 1) * self.SNAPS_PER_BATCH
                counts = self._observe("after %d snapshots" % taken, paths)
                series.append(self._volumes_count(counts))

            total = self.BATCHES * self.SNAPS_PER_BATCH
            log.info("OLDINO SERIES /volumes old_inodes after each batch of "
                     "%d snapshots: %s", self.SNAPS_PER_BATCH, series)

            # Nothing should reclaim while we are only creating: the purge runs
            # from the dirfrag commit and is gated on
            #          snap_purged_thru < last_destroyed
            # and we have never deleted a snapshot.
            for prev, cur in zip(series, series[1:]):
                self.assertGreaterEqual(
                    cur, prev,
                    "old_inodes on /volumes went down during a create-only "
                    "phase (series=%s). Something committed the dirfrag and "
                    "purged; check whether a journal trim slipped through "
                    "despite mds_log_max_segments." % series)

            self.assertGreaterEqual(
                series[-1], total // 2,
                "expected /volumes to accumulate on the order of one old_inode "
                "per subvolume snapshot (%d taken), got %d (series=%s)"
                % (total, series[-1], series))
        finally:
            self._cleanup(group, subvol, snapnames)

    def _run_arm(self, shut_the_gate):
        self._set_global_seq_config(True)
        # trim as aggressively as the MDS allows (min is 8)
        self.config_set('mds', 'mds_log_max_segments', '8')

        group, subvol = self._make_subvolume()
        paths = self._paths(group, subvol)
        snapnames = []

        try:
            if shut_the_gate:
                self._restart_mds_to_shut_gate(paths)
            self._assert_gate(paths, expected_open=not shut_the_gate)

            self._observe("baseline", paths, gate=True)
            pre, post, root_post = self._snapshot_and_commit_loop(
                group, subvol, snapnames)
            self._assert_root_was_reclaimed(root_post)
            return pre, post
        finally:
            self._cleanup(group, subvol, snapnames)

    def test_gate_shut_growth_survives_commits(self):
        """
        Gate SHUT arm

        An MDS restart re-fetches root's dirfrag, so _omap_fetched() sets
        snap_purged_thru = last_destroyed. Nothing is ever deleted here,
        so @last_destroyed never moves past that and the gate stays shut for
        the whole run.

        Claim: with the gate shut, committing the dirfrag over and over
        reclaims nothing, and /volumes keeps climbing.  Trimming does not
        save you.

        PASS => the buildup is real on a live cluster; #67102 is reachable
                with subvolume snapshots alone.
        FAIL => /volumes was reclaimed even with the gate shut, so the gate is
                not what governs this and the model is wrong.
        """
        pre, post = self._run_arm(shut_the_gate=True)
        self.assertGreaterEqual(
            post[-1], self.BATCHES * self.SNAPS_PER_BATCH // 2,
            "/volumes old_inodes did not survive repeated dirfrag commits "
            "with the purge gate SHUT (post=%s, pre=%s). The gate is not what "
            "governs reclaim here." % (post, pre))

    def test_gate_open_growth_is_reclaimed(self):
        """
        Gate OPEN arm

        No MDS restart, so on this freshly created filesystem root's dirfrag
        was never fetched and snap_purged_thru is still 0 while last_destroyed
        is 1: the gate is open.

        Claim: with the gate open, every commit purges - so /volumes is driven
        back to zero on each commit and never accumulates.
        """
        pre, post = self._run_arm(shut_the_gate=False)
        self.assertLessEqual(
            max(post), 2,
            "/volumes retained old_inodes across commits even with the purge "
            "gate OPEN (post=%s, pre=%s). Either the purge is not running or "
            "root's realm snap set is not empty." % (post, pre))
        self.assertGreater(
            max(pre), 2,
            "/volumes never accumulated even before a commit (pre=%s); the "
            "CoW is not happening at all, so this arm proves nothing." % pre)
