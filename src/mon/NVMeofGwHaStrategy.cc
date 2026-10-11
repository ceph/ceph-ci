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

int ActivePassiveHaStrategy::gw_created(NVMeofGwMap& map, const NvmeGwId& gw_id,
                            const NvmeGroupKey& group_key, bool &propose_pending) {
   return map.process_gw_map_gw_pass_to_created(gw_id, group_key, propose_pending);
}

void ActivePassiveHaStrategy::periodic_ha(NVMeofGwMap& map,
                            const NvmeGroupKey& group_key, bool &propose_pending) {
    // Legacy recovery / failback rules
    map.handle_abandoned_ana_groups(group_key, propose_pending);
}

// ============================================================================
// Active-Active Strategy Implementation (Isolated New Logic)
// ============================================================================
int ActiveActiveHaStrategy::on_create_gw(NVMeofGwMap& map, const NvmeGwId& gw_id,
                  const NvmeGroupKey& group_key, uint64_t features) {
  return map.cfg_add_gw(gw_id, group_key, features);
}

int ActiveActiveHaStrategy::on_delete_gw(NVMeofGwMap& map, const NvmeGwId& gw_id,
                 const NvmeGroupKey& group_key) {
    //update_ana_states(map);
	return map.cfg_delete_gw(gw_id, group_key);
}

int ActiveActiveHaStrategy::on_update_gw_location(NVMeofGwMap& map, const NvmeGwId& gw_id,
        const NvmeGroupKey& group_key,
          std::string& NvmeLocation, bool &propose_pending) {
	return map.cfg_set_location_active_active(gw_id, group_key, NvmeLocation, propose_pending);
}

int ActiveActiveHaStrategy::on_disaster_clear(NVMeofGwMap& map, const NvmeGroupKey& group_key,
                      std::string& NvmeLocation, bool &propose_pending) {
  return map.cfg_location_disaster_clear_active_active(group_key, NvmeLocation, propose_pending);
}

void ActiveActiveHaStrategy::gw_down(NVMeofGwMap& map, const NvmeGwId& gw_id,
                 const NvmeGroupKey& group_key, bool &propose_pending) {
  map.process_gw_map_gw_down_active_active(gw_id, group_key, propose_pending);
}

void ActiveActiveHaStrategy::gw_alive(NVMeofGwMap& map, const NvmeGwId& gw_id,
                 const NvmeGroupKey& group_key, epoch_t& last_osd_epoch, bool &propose_pending) {
   map.process_gw_map_ka_active_active(gw_id, group_key, last_osd_epoch, propose_pending);
}

int ActiveActiveHaStrategy::gw_created(NVMeofGwMap& map, const NvmeGwId& gw_id,
                            const NvmeGroupKey& group_key, bool &propose_pending) {
  return map.process_gw_map_gw_pass_to_created_active_active(gw_id, group_key, propose_pending);
}

void ActiveActiveHaStrategy::periodic_ha(NVMeofGwMap& map,
                            const NvmeGroupKey& group_key, bool &propose_pending) {
    // Legacy recovery / failback rules
  map.periodic_ha_handle_active_active(group_key, propose_pending);
}
