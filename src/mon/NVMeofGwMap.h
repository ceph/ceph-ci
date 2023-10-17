/*
 * NVMeofGwMap.h
 *
 *  Created on: Oct 17, 2023
 *      Author: 227870756
 */

#ifndef MON_NVMEOFGWMAP_H_
#define MON_NVMEOFGWMAP_H_
#include "string"
#include <iomanip>
#include "map"
#include <iostream>
#include <sstream>

#include "msg/Message.h"

typedef enum {
  GW_IDLE_STATE = 0, //invalid state
  GW_STANDBY_STATE,
  GW_ACTIVE_STATE,
  GW_BLOCKED_AGROUP_OWNER,
  GW_WAIT_FAILBACK_PREPARED
}GW_STATES_PER_AGROUP_E;

  enum class GW_AVAILABILITY_E {
	GW_CREATED = 0,
	GW_AVAILABLE,
	GW_UNAVAILABLE
};

#define MAX_SUPPORTED_ANA_GROUPS 5
#define REDUNDANT_GW_ANA_GROUP_ID 0xFF
typedef struct GW_STATE_T {
	//bool                    ana_state[MAX_SUPPORTED_ANA_GROUPS]; // real ana states per ANA group for this GW :1- optimized, 0- inaccessible
	GW_STATES_PER_AGROUP_E   sm_state [MAX_SUPPORTED_ANA_GROUPS];  // state machine states per ANA group
	uint16_t  optimized_ana_group_id;                     // optimized ANA group index as configured by Conf upon network entry, note for redundant GW it is FF
	GW_AVAILABILITY_E     availability;                  // in absence of  beacon  heartbeat messages it becomes inavailable
	uint16_t gw_id;
	uint64_t epoch;                                      // epoch per GW
}GW_STATE_T;

typedef struct GW_METADATA_T {
	uint32_t  anagrp_sm_tstamps[MAX_SUPPORTED_ANA_GROUPS]; // statemachine timer(timestamp) set in some state
}GW_METADATA_T;

using GWMAP       = std::map <std::string, std::map<uint16_t, GW_STATE_T> >;
using GWMETADATA  = std::map <std::string, std::map<uint16_t, GW_METADATA_T> >;

class NVMeofGwMap
{
	public:
	GWMAP      Gmap;
	GWMETADATA Gmetadata;
	std::map <std::string, uint64_t> subsyst_epoch;
	bool     listen_mode{ false };     // "listen" mode. started when detected invalid maps from some GW in the beacon messages. "Listen" mode Designed as Synchronisation mode
	uint32_t listen_mode_start_tick{0};

	NVMeofGwMap() = default;

};



#endif /* SRC_MON_NVMEOFGWMAP_H_ */
