import logging
import time
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class TestStandbyAffinity(CephFSTestCase):
    MDSS_REQUIRED = 3

    def test_standby_affinity_promotion(self):
        """
        Verify that the MDSMonitor honors the mds_join_fs configuration
        during a failover event.
        1. The test configures mds_join_fs for a dynamically discovered standby.
        2. The standby daemon restarts, reads the config, and reports it to the Monitor.
        3. The active Rank 0 MDS is forcefully stopped.
        4. The Monitor invokes get_available_standby() to evaluate candidates.
        5. The Monitor successfully promotes the preferred standby over vanilla alternatives.
        """
        fs_name = self.fs.name
        mds_ids = list(self.mds_cluster.mds_ids)

        # Guarantee we target the true Active Rank 0 MDS by querying the runtime map
        active_mds_names = self.fs.get_active_names()
        if not active_mds_names:
            self.fail("No active MDS found in the filesystem map.")

        active_mds = active_mds_names[0]

        preferred_standby = None
        vanilla_standby = None

        # Safely assign standby roles from the remaining IDs (skipping active primary)
        for mds_id in mds_ids:
            if mds_id == active_mds:
                continue
            if not preferred_standby:
                preferred_standby = mds_id
            elif not vanilla_standby:
                vanilla_standby = mds_id

        log.info(f"Roles assigned - Active Rank 0: {active_mds}, "
                 f"Preferred Standby: {preferred_standby}, "
                 f"Vanilla Standby: {vanilla_standby}")

        # Explicitly assign one standby to prefer the filesystem.
        # When a daemon starts up, it reads this mds_join_fs string and
        # sends it to the Monitor. The Monitor converts this into the
        # info.join_fscid integer inside the MDSMap::mds_info_t struct.
        # This feeds data into the scoring matrix.
        self.config_set(f'mds.{preferred_standby}', 'mds_join_fs', fs_name)
        # Ensure the other is strictly vanilla
        self.config_set(f'mds.{vanilla_standby}', 'mds_join_fs', "")

        # Restart daemons to apply configuration changes safely
        self.mds_cluster.mds_restart(preferred_standby)
        self.mds_cluster.mds_restart(vanilla_standby)
        self.fs.wait_for_daemons()

        # Trigger the Failover on the verified active primary
        log.info(f"Killing active MDS {active_mds} to force failover...")
        self.mds_cluster.mds_stop(active_mds)
        self.mds_cluster.mds_fail(active_mds)

        # The Monitor evaluates both standbys. Because preferred_standby has
        # a matching mds_join_fs configuration, it receives Score SCORE_PREF_MATCH (6)
        # and vanilla_standby receives Score SCORE_PREF_VANILLA (5).
        # Because 6 > 5, the loop returns the preferred_standby pointer to the Monitor,
        # which then promotes it.
        time.sleep(10)
        self.fs.wait_for_daemons()

        # Verify the outcome
        new_status = self.fs.status()
        new_active_name = new_status.get_rank(self.fs.id, 0)['name']

        log.info(f"Monitor promoted MDS {new_active_name} to Active.")

        # Assertion: The monitor MUST have picked the preferred standby over the vanilla one.
        self.assertEqual(
            new_active_name,
            preferred_standby,
            f"Failover failure! MDSMonitor promoted {new_active_name} "
            f"instead of {preferred_standby}."
        )

    def test_standby_affinity_multi_host_avoidance(self):
        """
        Verify that multi-host anti-affinity scanning properly downgrades a
        preferred standby if it shares a host with ANY active daemon.

        1. Dynamically discovers the host topology of the active Rank 0 MDS.
        2. Identifies a standby sharing the host (co-located) and an isolated
           standby (clean).
        3. The co-located standby is given a target mds_join_fs configuration.
        4. The active Rank 0 MDS is forcefully stopped, triggering an election.
        5. The Monitor invokes get_available_standby() and uses the full address vector
           scan to penalize the co-located preferred standby.
        6. The Monitor successfully bypasses the preferred candidate on the bad host
           and promotes the clean vanilla standby instead.
        """
        fs_name = self.fs.name
        mds_ids = list(self.mds_cluster.mds_ids)

        # Guarantee we target the true Active Rank 0 MDS by querying the live map
        active_mds_names = self.fs.get_active_names()
        if not active_mds_names:
            self.fail("No active MDS found in the filesystem map.")

        active_mds = active_mds_names[0]

        # Query the runtime map to find the physical host string of the true active daemon
        active_host = self.mds_cluster.mon_manager.find_remote('mds', active_mds).hostname

        co_located_preferred_standby = None
        clean_vanilla_standby = None

        # Dynamically discover standby roles from the remaining IDs (skipping active)
        for mds_id in mds_ids:
            if mds_id == active_mds:
                continue
            current_host = self.mds_cluster.mon_manager.find_remote('mds', mds_id).hostname
            if current_host == active_host and not co_located_preferred_standby:
                co_located_preferred_standby = mds_id
            if current_host != active_host and not clean_vanilla_standby:
                clean_vanilla_standby = mds_id

        # Guard clause: Skip if the environment topology does not have a host mismatch
        if not co_located_preferred_standby or not clean_vanilla_standby:
            self.fail("QA cluster topology lacks a mix of co-located and isolated nodes.")

        log.info(f"Topology discovered - Active Rank 0: {active_mds} (Host: {active_host}), "
                 f"Co-located Preferred Standby: {co_located_preferred_standby}, "
                 f"Clean Vanilla Standby: {clean_vanilla_standby}")

        # Explicitly assign targets to create the affinity/anti-affinity conflict.
        # When the co-located standby starts up, it registers its target fs_name,
        # which the Monitor parses into info.join_fscid inside the MDSMap::mds_info_t struct.
        # This gives it an initial high priority tier, while the vanilla standby remains
        # at a baseline tier. This setup explicitly tests whether the host anti-affinity
        # scan loop can successfully degrade a preferred tier down to a fallback tier.
        self.config_set(f'mds.{co_located_preferred_standby}', 'mds_join_fs', fs_name)
        self.config_set(f'mds.{clean_vanilla_standby}', 'mds_join_fs', "")

        # Apply and stabilize configurations
        self.mds_cluster.mds_restart(co_located_preferred_standby)
        self.mds_cluster.mds_restart(clean_vanilla_standby)
        self.fs.wait_for_daemons()

        # Kill the true active daemon to force election evaluation
        log.info(f"Triggering failover of active MDS {active_mds}...")
        self.mds_cluster.mds_stop(active_mds)
        self.mds_cluster.mds_fail(active_mds)

        # Allow time for the Monitor election loop to process and stabilize.
        # The Monitor evaluates both standbys. The preferred standby's score
        # drops from SCORE_PREF_MATCH (6) to SCORE_FALLBACK_MATCH (4) because
        # it shares a physical host with the daemon that just failed.
        # The clean vanilla standby avoids this penalty and receives
        # SCORE_PREF_VANILLA (5). Because 5 > 4, the Monitor must bypass the
        # preferred candidate and promote the vanilla one.
        time.sleep(10)
        self.fs.wait_for_daemons()

        # Verify the outcome
        new_status = self.fs.status()
        new_active_name = new_status.get_rank(self.fs.id, 0)['name']

        log.info(f"Monitor finalized election. Promoted MDS: {new_active_name}")

        # Assertion: The preferred standby's score drops to SCORE_FALLBACK_MATCH
        # because it shares a host with the dead rank. The clean vanilla standby
        # gets SCORE_PREF_VANILLA. Since SCORE_PREF_VANILLA > SCORE_FALLBACK_MATCH,
        # the vanilla daemon MUST win!
        self.assertEqual(
            new_active_name,
            clean_vanilla_standby,
            f"Anti-affinity bug regression! Monitor promoted co-located daemon {new_active_name} "
            f"instead of shifting to clean host daemon {clean_vanilla_standby}."
        )
