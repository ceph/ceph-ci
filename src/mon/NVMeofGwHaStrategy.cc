/*
 * NVMeofGwHaStrategy.cc
 *
 *  Created on: Aug 31, 2026
 *      Author: LEONIDCHERNIN
 */

#include "NVMeofGwTypes.h"
#include "NVMeofGwHaStrategy.h"
#include "NVMeofGwMap.h" // Included here where NVMeofGwMap member calls are used

// Factory instantiation
std::shared_ptr<NVMeofHaStrategy> create_ha_strategy(HaMode mode) {
    if (mode == HaMode::ACTIVE_ACTIVE) {
        return std::make_shared<ActiveActiveHaStrategy>();
    }
    return std::make_shared<ActivePassiveHaStrategy>();
}

// ============================================================================
// Active-Passive Strategy Implementation (Legacy)
// ============================================================================
int ActivePassiveHaStrategy::on_create_gw(NVMeofGwMap& map, const NvmeGwId& gw_id,
                            const NvmeGroupKey& group_key, uint64_t features) {
    // Delegate to legacy map logic or implement legacy creation rules
	return map.cfg_add_gw(gw_id, group_key, features);
}

int ActivePassiveHaStrategy::on_delete_gw(NVMeofGwMap& map, const NvmeGwId& gw_id,
                            const NvmeGroupKey& group_key) {
    // Legacy deletion rules
	return map.cfg_delete_gw(gw_id, group_key);
}

int ActivePassiveHaStrategy::on_update_gw_location(NVMeofGwMap& map, const NvmeGwId& gw_id,
                        const NvmeGroupKey& group_key,
                          std::string& NvmeLocation, bool &propose_pending) {
	return map.cfg_set_location(gw_id, group_key, NvmeLocation, propose_pending);
}

int ActivePassiveHaStrategy::on_disaster_clear(NVMeofGwMap& map, const NvmeGroupKey& group_key,
                      std::string& NvmeLocation, bool &propose_pending) {
	return map.cfg_location_disaster_clear(group_key, NvmeLocation, propose_pending);
}

void ActivePassiveHaStrategy::gw_down(NVMeofGwMap& map, const NvmeGwId& gw_id,
                            const NvmeGroupKey& group_key, bool &propose_pending) {
    // Legacy FSM failover triggers
    map.process_gw_map_gw_down(gw_id, group_key, propose_pending);
}

void ActivePassiveHaStrategy::gw_alive(NVMeofGwMap& map, const NvmeGwId& gw_id,
                            const NvmeGroupKey& group_key, epoch_t& last_osd_epoch, bool &propose_pending) {
    // Legacy recovery / failback rules
    map.process_gw_map_ka(gw_id, group_key, last_osd_epoch, propose_pending);
}
void ActivePassiveHaStrategy::periodic_ha(NVMeofGwMap& map, bool &propose_pending) {
    // Legacy recovery / failback rules
    map.handle_abandoned_ana_groups(propose_pending);
}

// ============================================================================
// Active-Active Strategy Implementation (Isolated New Logic)
// ============================================================================
int ActiveActiveHaStrategy::on_create_gw(NVMeofGwMap& map, const NvmeGwId& gw_id,
                  const NvmeGroupKey& group_key, uint64_t features) {
   // update_ana_states(map);
	return 0;
}

int ActiveActiveHaStrategy::on_delete_gw(NVMeofGwMap& map, const NvmeGwId& gw_id,
                 const NvmeGroupKey& group_key) {
    //update_ana_states(map);
	return 0;
}

int ActiveActiveHaStrategy::on_update_gw_location(NVMeofGwMap& map, const NvmeGwId& gw_id,
        const NvmeGroupKey& group_key,
          std::string& NvmeLocation, bool &propose_pending) {
    //update_ana_states(map);
	/*return map.cfg_set_location(gw_id, group_key, NvmeLocation, propose_pending);
	 *  need to update the maps of all gws and incremen gw epoch for the group key
	 *  update_ana_states_location_modified
	 *
	 * */
	return 0;
}

int ActiveActiveHaStrategy::on_disaster_clear(NVMeofGwMap& map, const NvmeGroupKey& group_key,
                      std::string& NvmeLocation, bool &propose_pending) {
  // return map.cfg_location_disaster_clear(group_key, NvmeLocation, propose_pending);
  // but no need to put  disaster intermediate state with "failbacks_in_process = true"
  return 0;
}

void ActiveActiveHaStrategy::gw_down(NVMeofGwMap& map, const NvmeGwId& gw_id,
                 const NvmeGroupKey& group_key, bool &propose_pending) {
    // 1. Transient hold active during blocklisting
    //map.set_transient_hold(true);

    // 2. Issue blocklist for dead GW
    //map.issue_osd_blocklist(gw_id);

    // 3. Recalculate static location-based ANA states
    //update_ana_states(map);

    // 4. Release transient hold
    //map.set_transient_hold(false);
}

void ActiveActiveHaStrategy::gw_alive(NVMeofGwMap& map, const NvmeGwId& gw_id,
                 const NvmeGroupKey& group_key, epoch_t& last_osd_epoch, bool &propose_pending) {
    // Directly restore states based on location map (no waiting FSM)
    //update_ana_states(map);
}

void ActiveActiveHaStrategy::periodic_ha(NVMeofGwMap& map, bool &propose_pending) {
    // Legacy recovery / failback rules
   // update_ana_states(map);
}
