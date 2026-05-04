import logging
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

# Must match enum ino_prealloc_killpoint in src/mds/mdstypes.h
INO_PREALLOC_KILLPOINTS = [
    "NONE",                                 # 0
    "INO_PREALLOC_DELEGATE_BEFORE",         # 1
    "INO_PREALLOC_DELEGATE_AFTER",          # 2
    "INO_PREALLOC_PREPARE_NEW_INODE",       # 3
    "INO_PREALLOC_APPLY_ALLOCATED_BEFORE",  # 4
    "INO_PREALLOC_APPLY_ALLOCATED_AFTER",   # 5
    "INO_PREALLOC_SESSION_SAVE_BEFORE",     # 6
    "INO_PREALLOC_REPLAY_ERASE_BEFORE",     # 7
]

class TestInodePreallocationKillpoints(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 3

    def _run_workload(self, killpoint_val, killpoint_name):
        self.fs.set_max_mds(2)
        status = self.fs.wait_for_daemons()

        # Target Rank 1 for the killpoint test
        rinfo = self.fs.get_rank(rank=1, status=status)

        # Force aggressive inode delegation and preallocation churn
        self.fs.set_config("mds_client_prealloc_inos", "100", rank=1, status=status)
        self.fs.set_config("mds_client_delegate_inos_pct", "100", rank=1, status=status)
        self.fs.set_config("mds_allow_async_dirops", "true", rank=1, status=status)
        self.fs.set_config("mds_kill_ino_prealloc_at", str(killpoint_val), rank=1, status=status)

        # Setup directory pinned to Rank 1
        self.mount_a.run_shell_payload("mkdir -p top")
        self.mount_a.setfattr("top", "ceph.dir.pin", "1")
        self._wait_subtrees([('/top', 1)], status=status, rank=0)

        log.info(f"Triggering workload for killpoint {killpoint_name} ({killpoint_val}) on Rank 1...")

        # Workload: Trigger file creations, deletions, and flushes to exercise
        # delegate_inos, prepare_new_inode, apply_allocated_inos, and SessionMap::save
        try:
            self.mount_a.run_shell_payload(
                "for i in $(seq 1 500); do "
                "  touch top/file_$i && rm -f top/file_$i; "
                "done"
            )
        except Exception as e:
            log.info(f"Workload interrupted by expected MDS crash: {e}")

        # Wait for Rank 1 to crash at the specified killpoint
        log.info(f"Waiting for Rank 1 ({rinfo['name']}) to abort at killpoint {killpoint_name}...")
        self.fs.wait_for_death(timeout=120, status=status, rank=1)
        self.delete_mds_coredump(rinfo['name'])

        log.info(f"MDS Rank 1 killed successfully at {killpoint_name}. Restarting daemon...")

        # Reset killpoint before restarting to prevent crash-loops during recovery
        self.fs.set_config("mds_kill_ino_prealloc_at", "0", rank=1, status=status)
        self.fs.mds_restart(rinfo['name'])

        # Verify active cluster recovery and journal/sessionmap replay
        status = self.fs.wait_for_daemons()
        log.info("Cluster successfully recovered and replayed preallocation state.")

        # Verification check: Ensure client can perform I/O post-recovery
        self.mount_a.run_shell_payload("touch top/recovery_check_file")
        self.mount_a.run_shell_payload("rm -f top/recovery_check_file")

    @staticmethod
    def make_test_killpoint(killpoint_val, killpoint_name):
        def test(self):
            log.info(f"=== Starting test for {killpoint_name} ({killpoint_val}) ===")
            self._run_workload(killpoint_val, killpoint_name)
            log.info(f"=== Completed test for {killpoint_name} ===")
        return test


# Dynamically attach test_ino_prealloc_killpoint_<NAME> for each enum value (1..7)
for val, name in enumerate(INO_PREALLOC_KILLPOINTS):
    if val == 0:  # Skip NONE
        continue
    test_func = TestInodePreallocationKillpoints.make_test_killpoint(val, name)
    setattr(TestInodePreallocationKillpoints, f"test_ino_prealloc_killpoint_{name}", test_func)
