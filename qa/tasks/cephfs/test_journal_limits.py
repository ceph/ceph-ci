import logging
import time

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestJournalLimits(CephFSTestCase):
    """
    Journal hard limit: once the journal reaches mds_log_hard_limit_factor *
    mds_log_max_segments, new client mutations must block (not fail with
    ENOSPC), and they must complete once the backlog clears.
    """

    LIMIT_MARKER = "has reached the hard limit"
    HEALTH_MARKER = "MDS_JOURNAL_HARD_LIMIT"
    BURST = 100

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
                'mds_log_pause']
        orig = {opt: self._cfg_get(opt) for opt in opts}

        try:
            # hard limit = 4 * 1.5 = 6 segments; warn = 4 segments
            self.config_set('mds', 'mds_log_events_per_segment', '10')
            self.config_set('mds', 'mds_log_max_segments', '4')
            self.config_set('mds', 'mds_log_warn_factor', '1.0')
            self.config_set('mds', 'mds_log_hard_limit_factor', '1.5')

            # Stop journal flushes. Segments cannot become safe, so they cannot
            # expire, and the backlog grows deterministically past the hard limit.
            self.config_set('mds', 'mds_log_pause', 'true')

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

            # Clearing the stall lets the journal flush and trim. Parked requests
            # must then complete successfully.
            self.config_set('mds', 'mds_log_pause', 'false')
            burst.wait()
            self.assertEqual(burst.exitstatus, 0,
                             "parked mutations did not complete after the limit cleared")

            count = self.mount_a.run_shell(
                ['sh', '-c', 'ls burst_* | wc -l']).stdout.getvalue().strip()
            self.assertEqual(int(count), self.BURST)

            # Once the journal is back under the limit, the health check clears.
            self._wait_for_health(present=False)
        finally:
            self.config_set('mds', 'mds_log_pause', 'false')
            for opt in opts:
                if orig[opt]:
                    self.config_set('mds', opt, orig[opt])
