/*
 * NVMeofGwTypes.h
 *
 *  Created on: Dec 29, 2023
 */

#ifndef MON_NVMEOFGWTYPES_H_
#define MON_NVMEOFGWTYPES_H_
#include <string>
#include <iomanip>
#include <map>
#include <iostream>

using GW_ID_T      = std::string;
using GROUP_KEY    = std::pair<std::string, std::string>;
using NQN_ID_T     = std::string;
using ANA_GRP_ID_T = uint32_t;


enum class GW_STATES_PER_AGROUP_E {
    GW_IDLE_STATE = 0, //invalid state
    GW_STANDBY_STATE,
    GW_ACTIVE_STATE,
    GW_BLOCKED_AGROUP_OWNER,
    GW_WAIT_FAILBACK_PREPARED
};

enum class GW_AVAILABILITY_E {
    GW_CREATED = 0,
    GW_AVAILABLE,
    GW_UNAVAILABLE,
    GW_DELETED
};

#define MAX_SUPPORTED_ANA_GROUPS 16
#define INVALID_GW_TIMER     0xffff
#define REDUNDANT_GW_ANA_GROUP_ID 0xFF

typedef GW_STATES_PER_AGROUP_E SM_STATE[MAX_SUPPORTED_ANA_GROUPS];

struct NqnState {
    std::string nqn;          // subsystem NQN
    SM_STATE    sm_state;     // susbsystem's state machine state
    uint16_t    opt_ana_gid;  // optimized ANA group index

    // Default constructor
    NqnState(const std::string& _nqn) : nqn(_nqn), opt_ana_gid(0) {
        for (int i=0; i < MAX_SUPPORTED_ANA_GROUPS; i++)
            sm_state[i] = GW_STATES_PER_AGROUP_E::GW_IDLE_STATE;
    }
};

typedef std::vector<NqnState> GwSubsystems;

struct GW_STATE_T {
    SM_STATE                sm_state;                      // state machine states per ANA group
    GW_ID_T                 failover_peer[MAX_SUPPORTED_ANA_GROUPS];
    ANA_GRP_ID_T            optimized_ana_group_id;        // optimized ANA group index as configured by Conf upon network entry, note for redundant GW it is FF
    GW_AVAILABILITY_E       availability;                  // in absence of  beacon  heartbeat messages it becomes inavailable
    uint64_t                version;                                    // version per all GWs of the same subsystem. subsystem version

    GW_STATE_T(ANA_GRP_ID_T id):
        optimized_ana_group_id(id),
        availability(GW_AVAILABILITY_E::GW_CREATED),
        version(0)
    {
        for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++)
            sm_state[i] = GW_STATES_PER_AGROUP_E::GW_IDLE_STATE;
    };

    GW_STATE_T() : GW_STATE_T(REDUNDANT_GW_ANA_GROUP_ID) {};

    void standby_state(ANA_GRP_ID_T grpid) {
        sm_state[grpid]       = GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE;
        failover_peer[grpid]  = "";
    };
};

struct GW_METADATA_T {
   int  anagrp_sm_tstamps[MAX_SUPPORTED_ANA_GROUPS]; // statemachine timer(timestamp) set in some state

    GW_METADATA_T() {
        for (int i=0; i<MAX_SUPPORTED_ANA_GROUPS; i++){
            anagrp_sm_tstamps[i] = INVALID_GW_TIMER;
        }
    };
};

using GWMAP               = std::map <NQN_ID_T, std::map<GW_ID_T, GW_STATE_T> >;
using GWMETADATA          = std::map <NQN_ID_T, std::map<GW_ID_T, GW_METADATA_T> >;
using SUBSYST_GWMAP       = std::map<GW_ID_T, GW_STATE_T>;
using SUBSYST_GWMETA      = std::map<GW_ID_T, GW_METADATA_T>;

using NONCE_VECTOR_T      = std::vector<std::string>;
using GW_ANA_NONCE_MAP    = std::map <ANA_GRP_ID_T, NONCE_VECTOR_T>;


struct GW_CREATED_T {
    ANA_GRP_ID_T     ana_grp_id; // ana-group-id allocated for this GW, GW owns this group-id
    GW_ANA_NONCE_MAP nonce_map;

    GW_CREATED_T(): ana_grp_id(REDUNDANT_GW_ANA_GROUP_ID) {};
    GW_CREATED_T(ANA_GRP_ID_T id): ana_grp_id(id) {};
};

using GW_CREATED_MAP      = std::map<GW_ID_T, GW_CREATED_T>;

#endif /* SRC_MON_NVMEOFGWTYPES_H_ */
