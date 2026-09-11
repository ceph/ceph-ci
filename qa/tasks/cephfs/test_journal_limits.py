import logging
import time

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestJournalLimits(CephFSTestCase):
    """
    Journal hard limit: once the journal reaches mds_log_hard_limit_factor *
    mds_log_max_segments, new client mutations must block (not fail with
    ENOSPC), and they must complete once the backlog clears.

    Segments only expire when trimming runs. The test stops trimming with a
    long mds_log_trim_upkeep_interval, so segments grow from real journal
    commits. Restoring the interval lets trimming run again.
    """

    LIMIT_MARKER = "has reached the hard limit"
    HEALTH_MARKER = "MDS_JOURNAL_HARD_LIMIT"
    TRIM_STOP_MS = '600000'  # 10 minutes: effectively no trimming during the test
    BURST = 200

    def _cfg_get(self, opt):
        return self.ceph_cluster.mon_manager.raw_cluster_cmd(
            'config', 'get', 'mds', opt).strip()

    def _wait_for_health(self, present, timeout=120):
        deadline = time.time() + timeout
        while True:
            out = self.ceph_cluster.mon_manager.raw_cluster_cmd('health', 'detail')
            if (self.HEALTH_MARKER in out) == present:
                return
            if time.time() > deadline:
                self.fail("%s health check did not %s" % (
                    self.HEALTH_MARKER, "appear" if present else "clear"))
            time.sleep(2)

    def _limit_logged(self):
        out = self.ceph_cluster.mon_manager.raw_cluster_cmd('log', 'last', '500')
        return self.LIMIT_MARKER in out

    def test_hard_limit_parks_then_resumes(self):
        opts = ['mds_log_events_per_segment', 'mds_log_max_segments',
                'mds_log_warn_factor', 'mds_log_hard_limit_factor',
                'mds_log_trim_upkeep_interval']
        orig = {opt: self._cfg_get(opt) for opt in opts}

        try:
            # hard limit = 8 * 1.5 = 12 segments; warn = 8 segments
            self.config_set('mds', 'mds_log_events_per_segment', '10')
            self.config_set('mds', 'mds_log_max_segments', '8')
            self.config_set('mds', 'mds_log_warn_factor', '1.0')
            self.config_set('mds', 'mds_log_hard_limit_factor', '1.5')

            # Stop trimming. Journal commits continue, so segments accumulate
            # until the hard limit is reached.
            self.config_set('mds', 'mds_log_trim_upkeep_interval', self.TRIM_STOP_MS)

            burst_cmd = ("for i in $(seq 1 {n}); do timeout 900 touch burst_$i & done; "
                         "wait").format(n=self.BURST)
            burst = self.mount_a.run_shell(['sh', '-c', burst_cmd], wait=False)

            deadline = time.time() + 180
            while not self._limit_logged():
                if time.time() > deadline:
                    self.fail("MDS never reported reaching the journal hard limit")
                time.sleep(2)

            # With the limit reached, a new mutation must block. It must not fail.
            # timeout(1) exits 124 when it kills a command that is still running.
            probe = self.mount_a.run_shell(['timeout', '10', 'touch', 'probe'],
                                           check_status=False)
            self.assertEqual(probe.exitstatus, 124,
                             "new mutation returned while the hard limit was active")

            # The state must also surface as a health check.
            self._wait_for_health(present=True)

            # The mon must refuse to fail an MDS while the journal is over its hard
            # limit, since failover would have to replay the oversized journal.
            name = self.fs.get_active_names()[0]
            rc = self.ceph_cluster.mon_manager.raw_cluster_cmd_result('mds', 'fail', name)
            self.assertNotEqual(rc, 0, "mds fail was allowed while the hard limit was reached")

            # Restore trimming. Segments expire, the journal drops below the limit,
            # and parked requests resume.
            self.config_set('mds', 'mds_log_trim_upkeep_interval',
                            orig['mds_log_trim_upkeep_interval'] or '1000')
            # `wait` in the shell returns only the last job's status, so a failed
            # touch in the middle would not show here. The file count below is the
            # real check that every parked create completed.
            burst.wait()
            count = self.mount_a.run_shell(
                ['sh', '-c', 'ls burst_* | wc -l']).stdout.getvalue().strip()
            self.assertEqual(int(count), self.BURST,
                             "not all parked mutations completed after the limit cleared")

            # Once the journal is back under the limit, the health check clears.
            self._wait_for_health(present=False)
        finally:
            for opt in opts:
                if orig[opt]:
                    self.config_set('mds', opt, orig[opt])
