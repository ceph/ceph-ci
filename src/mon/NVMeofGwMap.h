/*
 * NVMeofGwMap.h
 *
 *  Created on: Oct 17, 2023
 *      Author: 227870756
 */

#ifndef MON_NVMEOFGWMAP_H_
#define MON_NVMEOFGWMAP_H_
#include <map>
#include <iostream>
#include "include/encoding.h"
#include "include/utime.h"
#include "common/Formatter.h"
#include "common/ceph_releases.h"
#include "common/version.h"
#include "common/options.h"
#include "common/Clock.h"
#include "PaxosService.h"
#include "msg/Message.h"
#include "common/ceph_time.h"
#include "NVMeofGwTypes.h"

using ceph::coarse_mono_clock;

/*-------------------*/
class NVMeofGwMap
{
public:
    Monitor*                            mon           = NULL;   // just for logs in the mon module file
    epoch_t                             epoch         = 0;      // epoch is for Paxos synchronization  mechanizm
    bool                                delay_propose = false;

    // State: GMAP and Created_gws are sent to the clients, while Gmetadata is not
    std::map<GROUP_KEY, GWMAP>          Gmap;
    std::map<GROUP_KEY, GW_CREATED_MAP> Created_gws;
    std::map<GROUP_KEY, GWMETADATA>     Gmetadata;

    int   cfg_add_gw                    (const GW_ID_T &gw_id, const GROUP_KEY& group_key);
    int   cfg_delete_gw                 (const GW_ID_T &gw_id, const GROUP_KEY& group_key);
    void  process_gw_map_ka             (const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, bool &propose_pending);
    int   process_gw_map_gw_down        (const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, bool &propose_pending);
    void  update_active_timers          (bool &propose_pending);
    void  handle_abandoned_ana_groups   (bool &propose_pending);
    void  handle_removed_subsystems     (const std::vector<NQN_ID_T> &current_subsystems, const GROUP_KEY& group_key, bool &propose_pending);

private:
    void fsm_handle_gw_down    (const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, GW_STATES_PER_AGROUP_E state, ANA_GRP_ID_T grpid,  bool &map_modified);
    void fsm_handle_gw_delete  (const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, GW_STATES_PER_AGROUP_E state, ANA_GRP_ID_T grpid,  bool &map_modified);
    void fsm_handle_to_expired (const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, ANA_GRP_ID_T grpid,  bool &map_modified);

    void find_failover_candidate(const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, ANA_GRP_ID_T grpid, bool &propose_pending);
    void find_failback_gw       (const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, bool &found);
    void set_failover_gw_for_ANA_group (const GW_ID_T &failed_gw_id, const GROUP_KEY& group_key, const GW_ID_T &gw_id, const NQN_ID_T& nqn,
		   ANA_GRP_ID_T groupid);

    void start_timer(const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, ANA_GRP_ID_T anagrpid);
    int  get_timer(const GW_ID_T &gw_id, GROUP_KEY& group_key, const NQN_ID_T& nqn, ANA_GRP_ID_T anagrpid);
    void cancel_timer(const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, ANA_GRP_ID_T anagrpid);

public:
    void encode(ceph::buffer::list &bl, bool full_encode = true) const {
        using ceph::encode;
        __u8 struct_v = 0;
        encode(struct_v, bl); // version
        encode(epoch, bl);// global map epoch

        encode(Created_gws, bl); //Encode created GWs
        encode(Gmap, bl);
        if (full_encode) {
            encode(Gmetadata, bl);
        }
    }

    void decode(ceph::buffer::list::const_iterator &bl, bool full_decode = true) {
        using ceph::decode;
        __u8 struct_v;
        decode(struct_v, bl);
        ceph_assert(struct_v == 0);
        decode(epoch, bl);

        decode(Created_gws, bl);
        decode(Gmap, bl);
        if (full_decode) {
            decode(Gmetadata, bl);
        }
    }
};

#include "NVMeofGwSerialize.h"

#endif /* SRC_MON_NVMEOFGWMAP_H_ */
