import logging
import time
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)

class TestJournalLimits(CephFSTestCase):
    def test_journal_hard_limit_enospc(self):
        """
        Verify that the MDS rejects mutating operations with ENOSPC when
        the journal size exceeds mds_log_hard_limit_segments, and
        resumes normally when the limit is lifted.
        """
        self.config_set('mds', 'mds_log_events_per_segment', '10')
        self.config_set('mds', 'mds_log_max_segments', '2')
        self.config_set('mds', 'mds_log_warn_factor', '1.0')
        self.config_set('mds', 'mds_log_hard_limit_segments', '5')

        self.fs.mds_restart()
        self.fs.wait_for_daemons()

        hit_enospc = False
        try:
            for i in range(200):
                self.mount_a.run_shell(['touch', f'test_limit_{i}'])
        except CommandFailedError as e:
            if "No space left on device" in str(e):
                hit_enospc = True
                log.info("Successfully hit the journal hard limit ENOSPC.")
            else:
                raise

        self.assertTrue(hit_enospc, "Failed to trigger the journal hard limit ENOSPC!")

        self.mount_a.run_shell(['ls', '-l', '/'])

        self.config_set('mds', 'mds_log_hard_limit_segments', '0')
        time.sleep(5)

        self.mount_a.run_shell(['touch', 'recovery_success'])
        self.mount_a.run_shell(['rm', '-f', 'recovery_success'])
