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
#include <set>
#include "include/encoding.h"
#include "include/utime.h"
#include "common/Formatter.h"
#include "common/ceph_releases.h"
#include "common/version.h"
#include "common/options.h"
#include "common/Clock.h"
#include "PaxosService.h"
#include "msg/Message.h"
/*#include "NVMeofGwMon.h"

using std::ostream;

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, this)
using namespace TOPNSPC::common;

class NVMeofGwMap;

inline ostream& _prefix(std::ostream *_dout, const Monitor &mon,
                        const NVMeofGwMap *map) {
  return *_dout << "mon." << mon.name << "@" << mon.rank;
}
*/


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



inline void encode(const GW_STATE_T& state, ceph::bufferlist &bl) {
   for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
	  encode((int)(state.sm_state[i]), bl);
   }
   encode(state.optimized_ana_group_id, bl);
   encode((int)state.availability, bl);
   encode(state.gw_id, bl);
   encode(state.epoch, bl);
}

inline void decode(GW_STATE_T& state,  ceph::bufferlist::const_iterator& bl) {
	int sm_state;
	for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
		  decode(sm_state, bl);
		  state.sm_state[i] = (GW_STATES_PER_AGROUP_E)  sm_state;
	}
	decode(state.optimized_ana_group_id, bl);
	int avail;
	decode(avail, bl);
	state.availability = (GW_AVAILABILITY_E)avail;
	decode(state.gw_id, bl);
	decode(state.epoch, bl);
}

class NVMeofGwMap
{
	public:
	//Monitor &mon;// just for logs in the mon module file
	GWMAP      Gmap;
	GWMETADATA Gmetadata;
	std::map <std::string, uint64_t> subsyst_epoch;
	bool     listen_mode{ false };     // "listen" mode. started when detected invalid maps from some GW in the beacon messages. "Listen" mode Designed as Synchronisation mode
	uint32_t listen_mode_start_tick{0};


	//std::map<std::string,ModuleOption> module_options;
	void encode(ceph::buffer::list &bl) const {
		ENCODE_START(2, 1, bl); 	//encode(name, bl);	encode(can_run, bl);encode(error_string, bl);encode(module_options, bl);
		encode ((int)Gmap.size(),bl); // number nqn
		for (auto& itr : Gmap) {
			encode((const std::string &)itr.first, bl);// nqn
			encode( itr.second, bl);// encode the full map of this nqn : map<uint16_t, GW_STATE_T>
		}
		ENCODE_FINISH(bl);
	}

	void decode(ceph::buffer::list::const_iterator &bl) {
		DECODE_START(1, bl);
	//    decode(name, bl);//  decode(can_run, bl);//  decode(error_string, bl);//  decode(module_options, bl);
        int num_subsystems;
        std::string nqn;
        decode(num_subsystems, bl);
        std::map<uint16_t, GW_STATE_T> gw_map;
        Gmap.clear();
        for(int i = 0; i < num_subsystems; i++){
        	decode(nqn, bl);
        	Gmap.insert(make_pair(nqn, std::map<uint16_t, GW_STATE_T>()));
           //decode the map
        	gw_map.clear();
        	decode(gw_map, bl);
           //insert the qw_map to Gmap
        	for(auto &itr: gw_map ){
        		Gmap[nqn].insert({itr.first, itr.second});
        	}
        }
	  DECODE_FINISH(bl);
	}

	NVMeofGwMap() = default;

   GW_STATE_T * find_gw_map(uint16_t gw_id, const std::string& nqn )
    {
	auto it = Gmap.find(nqn);
	if (it != Gmap.end() /* && it->first == nqn*/) {
	  auto it2 = it->second.find(gw_id);
	  if (it2 != it->second.end() /* && it2->first == gw_id*/ ){ // cout << "AAAA " << gw_id << " " << it2->first << endl;
		  return  &it2->second;
	  }
   }
   return NULL;
  }

   int  cfg_add_gw (uint16_t gw_id, const std::string & nqn, uint16_t ana_grpid) {
	  GW_STATE_T state{ {GW_IDLE_STATE,} , ana_grpid, GW_AVAILABILITY_E::GW_CREATED, gw_id, 0 };

	   if (find_gw_map(gw_id, nqn)) {
		 //dout(4) << __func__ << " ERROR :GW already exists in map " << gw_id << dendl;
		 return 1;
	  }
	  if (ana_grpid >= MAX_SUPPORTED_ANA_GROUPS && ana_grpid != REDUNDANT_GW_ANA_GROUP_ID)
	  {
		//dout(4) << __func__ << " ERROR :GW " << gw_id << " bad ANA group " <<(int)ana_grpid << dendl;
		return 1;
	  }

    	//TODO check that all MAX_SUPPORTED_ANA_GROUPS are occupied in the subsystem - assert

	  if(Gmap[nqn].size() ==0 )
	     Gmap.insert(make_pair(nqn, std::map<uint16_t, GW_STATE_T>()));
	       Gmap[nqn].insert({gw_id, state});
      return 0;
  }

   int _dump_gws(  GWMAP & Gmap)const ;

};

#endif /* SRC_MON_NVMEOFGWMAP_H_ */
