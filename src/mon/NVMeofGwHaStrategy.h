/*
 * NVMeGwHaStrategy.h
 *
 *  Created on: Aug 31, 2026
 *      Author: LEONIDCHERNIN
 */

#ifndef MON_NVMEOFGWHASTRATEGY_H_
#define MON_NVMEOFGWHASTRATEGY_H_

//#include "NVMeofGwMap.h"

class NVMeofGwMap;

class NVMeofHaStrategy {
public:
    virtual ~NVMeofHaStrategy() = default;

    // Gateway Lifecycle Events
    virtual int on_create_gw(NVMeofGwMap& map, const NvmeGwId& gw_id, const NvmeGroupKey& group_key, uint64_t features) = 0;
    virtual int on_delete_gw(NVMeofGwMap& map, const NvmeGwId& gw_id, const NvmeGroupKey& group_key) = 0;

    // Location Map Modifications
    virtual int on_update_gw_location(NVMeofGwMap& map, const NvmeGwId& gw_id, const NvmeGroupKey& group_key,
                      std::string& NvmeLocation, bool &propose_pending) = 0;
    virtual int on_disaster_clear(NVMeofGwMap& map, const NvmeGroupKey& group_key,
                      std::string& NvmeLocation, bool &propose_pending) = 0;
    // Failover & Failback Operations
    virtual void gw_down(NVMeofGwMap& map, const NvmeGwId& gw_id,
                   const NvmeGroupKey& group_key, bool &propose_pending) = 0;
    virtual void gw_alive(NVMeofGwMap& map, const NvmeGwId& gw_id,
       const NvmeGroupKey& group_key, epoch_t& last_osd_epoch, bool &propose_pending) = 0;
    virtual void periodic_ha(NVMeofGwMap& map, bool &propose_pending) = 0;
    //virtual void update_ana_states(NVMeofGwMap& map) = 0;
};

// Legacy Active-Passive Strategy
class ActivePassiveHaStrategy : public NVMeofHaStrategy {
public:
    int on_create_gw(NVMeofGwMap& map, const NvmeGwId& gw_id, const NvmeGroupKey& group_key, uint64_t features) override;
    int on_delete_gw(NVMeofGwMap& map, const NvmeGwId& gw_id, const NvmeGroupKey& group_key) override;
    int on_update_gw_location(NVMeofGwMap& map, const NvmeGwId& gw_id, const NvmeGroupKey& group_key,
                  std::string& NvmeLocation, bool &propose_pending) override;
    virtual int on_disaster_clear(NVMeofGwMap& map, const NvmeGroupKey& group_key,
                          std::string& NvmeLocation, bool &propose_pending) override;
    void gw_down(NVMeofGwMap& map, const NvmeGwId& gw_id,
                   const NvmeGroupKey& group_key, bool &propose_pending) override;
    void gw_alive(NVMeofGwMap& map, const NvmeGwId& gw_id, const NvmeGroupKey& group_key,
                   epoch_t& last_osd_epoch, bool &propose_pending) override;
    void periodic_ha(NVMeofGwMap& map, bool &propose_pending) override;
   // void update_ana_states(NVMeofGwMap& map) override;
};

// New Active-Active Strategy
class ActiveActiveHaStrategy : public NVMeofHaStrategy {
public:
    int on_create_gw(NVMeofGwMap& map, const NvmeGwId& gw_id, const NvmeGroupKey& group_key, uint64_t features) override;
    int on_delete_gw(NVMeofGwMap& map, const NvmeGwId& gw_id, const NvmeGroupKey& group_key) override;
    int on_update_gw_location(NVMeofGwMap& map, const NvmeGwId& gw_id, const NvmeGroupKey& group_key,
                     std::string& NvmeLocation, bool &propose_pending) override;
    virtual int on_disaster_clear(NVMeofGwMap& map, const NvmeGroupKey& group_key,
                          std::string& NvmeLocation, bool &propose_pending) override;
    void gw_down(NVMeofGwMap& map, const NvmeGwId& gw_id,
                   const NvmeGroupKey& group_key, bool &propose_pending) override;
    void gw_alive(NVMeofGwMap& map, const NvmeGwId& gw_id, const NvmeGroupKey& group_key,
                   epoch_t& last_osd_epoch, bool &propose_pending) override;
    void periodic_ha(NVMeofGwMap& map, bool &propose_pending) override;
   // void update_ana_states(NVMeofGwMap& map) override;
};

// Factory Helper
std::shared_ptr<NVMeofHaStrategy> create_ha_strategy(HaMode mode);


#endif /* MON_NVMEOFGWHASTRATEGY_H_ */
