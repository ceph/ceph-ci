#include "gtest/gtest.h"
#include "mds/FSMap.h"
#include "mds/mdstypes.h"

class StandbyAffinityTest : public ::testing::Test {
protected:
  FSMap fsmap;

  // Create a default filesystem object.
  // By default, fs.get_fscid() is FS_CLUSTER_ID_NONE.
  Filesystem fs;
  fs_cluster_id_t test_fscid;
  entity_addrvec_t active_addrs;
  entity_addrvec_t same_host_addrs;
  entity_addrvec_t diff_host_addrs;

  void SetUp() override {
    test_fscid = fs.get_fscid();

    // Define our target addresses to avoid (Active Rank Host)
    entity_addr_t active_addr;
    active_addr.parse("10.0.0.1:6800", nullptr); // Host A
    active_addrs.v.push_back(active_addr);

    // Define addresses for our candidates
    entity_addr_t same_host_addr;
    same_host_addr.parse("10.0.0.1:6801", nullptr); // Host A (Same Host!)
    same_host_addrs.v.push_back(same_host_addr);

    entity_addr_t diff_host_addr;
    diff_host_addr.parse("10.0.0.2:6800", nullptr); // Host B (Different Host!)
    diff_host_addrs.v.push_back(diff_host_addr);
  }
};

/* Scenario A: Cross-Tier Prioritization (Same-Host Match vs. Different-Host Other FS).
 * Even though Standby 1 perfectly matches the fscid, it shares a host.
 * Standby 2 is affiliated with a different FS (Last Resort), but sits on a
 * different host, so it must still win. (SCORE_PREF_OTHER_FS > SCORE_FALLBACK_MATCH)
 */
TEST_F(StandbyAffinityTest, ScenarioA_Cross_Tier_Prioritization) {
  FSMap local_fsmap = fsmap;

  // Standby 1
  MDSMap::mds_info_t s1;
  s1.global_id = 101;
  s1.name = "standby_same_host_match";
  s1.state = MDSMap::STATE_STANDBY;
  s1.addrs = same_host_addrs;
  s1.join_fscid = test_fscid; // Matches the target fscid
  local_fsmap.insert(s1);

  // Standby 2
  MDSMap::mds_info_t s2;
  s2.global_id = 102;
  s2.name = "standby_diff_host_other_fs";
  s2.state = MDSMap::STATE_STANDBY;
  s2.addrs = diff_host_addrs;
  s2.join_fscid = 2; // Different, non-matching FSCID
  local_fsmap.insert(s2);

  const auto* selected = local_fsmap.get_available_standby(fs, &active_addrs);
  ASSERT_NE(selected, nullptr);
  // Must pick the remote 'Other FS' standby
  ASSERT_EQ(static_cast<uint64_t>(selected->global_id), 102ULL);
}

/* Scenario B: Short-Circuit Optimization (Different-Host Match).
 * Hits SCORE_PREF_MATCH for 201 and then short circuit.
 */
TEST_F(StandbyAffinityTest, ScenarioB_Short_Circuit_Optimization) {
  FSMap local_fsmap = fsmap;

  // Standby 1
  MDSMap::mds_info_t s1;
  s1.global_id = 201;
  s1.name = "standby_diff_host_match";
  s1.state = MDSMap::STATE_STANDBY;
  s1.addrs = diff_host_addrs;
  s1.join_fscid = test_fscid; // Perfect match (Matches fscid & different host)
  local_fsmap.insert(s1);

  // Standby 2
  MDSMap::mds_info_t s2;
  s2.global_id = 202;
  s2.name = "standby_diff_host_vanilla";
  s2.state = MDSMap::STATE_STANDBY;
  s2.addrs = diff_host_addrs;
  s2.join_fscid = 2;
  local_fsmap.insert(s2);

  const auto* selected = local_fsmap.get_available_standby(fs, &active_addrs);
  ASSERT_NE(selected, nullptr);
  // Must pick the perfect match
  ASSERT_EQ(static_cast<uint64_t>(selected->global_id), 201ULL);
}

/* Scenario C: Fallback Graceful Degradation (Fallback behavior when
 * all standbys are on the same host).
 * (CORE_FALLBACK_MATCH > SCORE_FALLBACK_OTHER_FS)
 */
TEST_F(StandbyAffinityTest, ScenarioC_Fallback_Graceful_Degradation) {
  FSMap local_fsmap = fsmap;

  // Standby 1
  MDSMap::mds_info_t s1;
  s1.global_id = 301;
  s1.name = "standby_same_host_vanilla";
  s1.state = MDSMap::STATE_STANDBY;
  s1.addrs = same_host_addrs;
  s1.join_fscid = 2; // SCORE_FALLBACK_OTHER_FS fallback
  local_fsmap.insert(s1);

  // Standby 2
  MDSMap::mds_info_t s2;
  s2.global_id = 302;
  s2.name = "standby_same_host_match";
  s2.state = MDSMap::STATE_STANDBY;
  s2.addrs = same_host_addrs;
  s2.join_fscid = test_fscid; // SCORE_FALLBACK_MATCH
  local_fsmap.insert(s2);

  const auto* selected = local_fsmap.get_available_standby(fs, &active_addrs);
  ASSERT_NE(selected, nullptr);
  // Falls back to same-host match cleanly
  ASSERT_EQ(static_cast<uint64_t>(selected->global_id), 302ULL);
}

/* Scenario D: Multi-Active Avoidance (Avoiding Multiple Active Hosts).
 * If the cluster has active daemons on Host A and Host C, a standby
 * on Host B must be chosen over a standby on Host C.
 * (SCORE_PREF_MATCH > SCORE_FALLBACK_MATCH).
 */
TEST_F(StandbyAffinityTest, ScenarioD_Multi_Active_Avoidance) {
  FSMap local_fsmap = fsmap;

  // Expand our avoid list to include Host C
  entity_addr_t active_addr_c;
  active_addr_c.parse("10.0.0.3:6800", nullptr);
  entity_addrvec_t multi_active_addrs = active_addrs; // Contains Host A
  multi_active_addrs.v.push_back(active_addr_c);      // Now contains Host A and Host C

  entity_addr_t host_c_addr;
  host_c_addr.parse("10.0.0.3:6801", nullptr);
  entity_addrvec_t host_c_addrs;
  host_c_addrs.v.push_back(host_c_addr);

  // Standby 1
  MDSMap::mds_info_t s1;
  s1.global_id = 401;
  s1.name = "standby_host_c";
  s1.state = MDSMap::STATE_STANDBY;
  s1.addrs = host_c_addrs;    // Host C
  s1.join_fscid = test_fscid; // Matches, but shares Host C with an active daemon
  local_fsmap.insert(s1);

  // Standby 2
  MDSMap::mds_info_t s2;
  s2.global_id = 402;
  s2.name = "standby_host_b";
  s2.state = MDSMap::STATE_STANDBY;
  s2.addrs = diff_host_addrs; // Host B
  s2.join_fscid = test_fscid; // Matches, and is on a completely free host
  local_fsmap.insert(s2);

  const auto* selected = local_fsmap.get_available_standby(fs, &multi_active_addrs);
  ASSERT_NE(selected, nullptr);
  // Must safely pick Host B
  ASSERT_EQ(static_cast<uint64_t>(selected->global_id), 402ULL);
}

/* Scenario E: Deterministic Tie-Breaker.
 * If two remote daemons have the exact same highest score, the algorithm
 * should cleanly select one without failing.
 * (Hits SCORE_PREF_MATCH for 501 and then short circuit).
 */
TEST_F(StandbyAffinityTest, ScenarioE_Deterministic_Tie_Breaker) {
  FSMap local_fsmap = fsmap;

  MDSMap::mds_info_t s1;
  s1.global_id = 501;
  s1.name = "standby_host_b";
  s1.state = MDSMap::STATE_STANDBY;
  s1.addrs = diff_host_addrs; // Host B
  s1.join_fscid = test_fscid; // Perfect match - SCORE_PREF_MATCH
  local_fsmap.insert(s1);

  entity_addr_t host_c_addr;
  host_c_addr.parse("10.0.0.3:6801", nullptr);
  entity_addrvec_t host_c_addrs;
  host_c_addrs.v.push_back(host_c_addr);

  MDSMap::mds_info_t s2;
  s2.global_id = 502;
  s2.name = "standby_host_c";
  s2.state = MDSMap::STATE_STANDBY;
  s2.addrs = host_c_addrs; // Host C
  s2.join_fscid = test_fscid; // Perfect match - SCORE_PREF_MATCH
  local_fsmap.insert(s2);

  const auto* selected = local_fsmap.get_available_standby(fs, &active_addrs);
  ASSERT_NE(selected, nullptr);

  // It should pick 501 because it evaluates it first and the score doesn't *exceed* 501's score.
  ASSERT_EQ(static_cast<uint64_t>(selected->global_id), 501ULL);
}
