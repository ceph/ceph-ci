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
#include "common/ceph_time.h"
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

using ceph::coarse_mono_clock;


using GW_ID_T   = std::string;

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
#define INVALID_GW_TIMER     0xffff
#define REDUNDANT_GW_ANA_GROUP_ID 0xFF
typedef struct GW_STATE_T {
    GW_STATES_PER_AGROUP_E   sm_state [MAX_SUPPORTED_ANA_GROUPS];  // state machine states per ANA group
    uint16_t  optimized_ana_group_id;                     // optimized ANA group index as configured by Conf upon network entry, note for redundant GW it is FF
    GW_AVAILABILITY_E     availability;                  // in absence of  beacon  heartbeat messages it becomes inavailable
    uint64_t version;                                      // version per all GWs of the same subsystem. subsystem version
}GW_STATE_T;

typedef struct GW_METADATA_T {
   int  anagrp_sm_tstamps[MAX_SUPPORTED_ANA_GROUPS]; // statemachine timer(timestamp) set in some state
}GW_METADATA_T;


using GWMAP               = std::map <std::string, std::map<GW_ID_T, GW_STATE_T> >;
using GWMETADATA          = std::map <std::string, std::map<GW_ID_T, GW_METADATA_T> >;
using SUBSYST_GWMAP       = std::map<GW_ID_T, GW_STATE_T>;
using SUBSYST_GWMETA      = std::map<GW_ID_T, GW_METADATA_T>;

inline void encode(const GW_STATE_T& state, ceph::bufferlist &bl) {
    for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
        encode((int)(state.sm_state[i]), bl);
    }
    encode(state.optimized_ana_group_id, bl);
    encode((int)state.availability, bl);
   // encode(state.gw_id, bl);
    encode(state.version, bl);
}

inline  void decode(GW_STATE_T& state,  ceph::bufferlist::const_iterator& bl) {
    int sm_state;
    for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
        decode(sm_state, bl);
        state.sm_state[i] = (GW_STATES_PER_AGROUP_E)  sm_state;
    }
    decode(state.optimized_ana_group_id, bl);
    int avail;
    decode(avail, bl);
    state.availability = (GW_AVAILABILITY_E)avail;
    //decode(state.gw_id, bl);
    decode(state.version, bl);
}


inline  void encode(const GW_METADATA_T& state, ceph::bufferlist &bl) {
    for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
        int tick = state.anagrp_sm_tstamps[i];
        encode( tick, bl);
     }
}

inline  void decode(GW_METADATA_T& state,  ceph::bufferlist::const_iterator& bl) {
    for(int i = 0; i <MAX_SUPPORTED_ANA_GROUPS; i ++){
        int tick;
        decode(tick, bl);
        state.anagrp_sm_tstamps[i] = tick;
    }
}


class NVMeofGwMap
{
public:
    Monitor *mon= NULL;// just for logs in the mon module file
    GWMAP      Gmap;
    GWMETADATA Gmetadata;//TODO !!! this map is used in the processing of Gmap - so it should be add to the encode/decode
    epoch_t epoch = 0;  // epoch is for Paxos synchronization  mechanizm

    bool     listen_mode{ false };     // "listen" mode. started when detected invalid maps from some GW in the beacon messages. "Listen" mode Designed as Synchronisation mode
    uint32_t listen_mode_start_tick{0};


    //std::map<std::string,ModuleOption> module_options;
    void encode(ceph::buffer::list &bl) const {
        ENCODE_START(2, 1, bl); 	//encode(name, bl);	encode(can_run, bl);encode(error_string, bl);encode(module_options, bl);
        encode((int) epoch, bl);// global map epoch
        encode ((int)Gmap.size(),bl); // number nqn
        for (auto& itr : Gmap) {
            encode((const std::string &)itr.first, bl);// nqn
            encode( itr.second, bl);// encode the full map of this nqn : map<uint16_t, GW_STATE_T>
        }
        // Encode Gmetadata
        encode ((int)Gmetadata.size(),bl);
        for (auto& itr : Gmetadata) {
            encode((const std::string &)itr.first, bl);// nqn
            encode( itr.second, bl);// encode the full map of this nqn :
        }

        ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator &bl) {
        DECODE_START(1, bl);
        int num_subsystems;
        std::string nqn;
        decode(epoch, bl);
        decode(num_subsystems, bl);
        SUBSYST_GWMAP    gw_map;
        Gmap.clear();
        //_dump_gwmap(Gmap);
        for(int i = 0; i < num_subsystems; i++){
            decode(nqn, bl);
            Gmap.insert(make_pair(nqn, std::map<std::string, GW_STATE_T>()));
            //decode the map
            gw_map.clear();
            decode(gw_map, bl);
            //insert the qw_map to Gmap
            for(auto &itr: gw_map ){
                Gmap[nqn].insert({itr.first, itr.second});
            }
        }
        // decode Gmetadata
        decode(num_subsystems, bl);
        SUBSYST_GWMETA    gw_meta;
        Gmetadata.clear();
        //_dump_gwmap(Gmap);
        for(int i = 0; i < num_subsystems; i++){
            decode(nqn, bl);
            Gmetadata.insert(make_pair(nqn, std::map<std::string, GW_METADATA_T>()));
            //decode the map
            gw_meta.clear();
            decode(gw_meta, bl);
            //insert the gw_meta to Gmap
            for(auto &itr: gw_meta ){
                Gmetadata[nqn].insert({itr.first, itr.second});
            }
        }
        DECODE_FINISH(bl);
    }

    //NVMeofGwMap( )  {}

    GW_STATE_T * find_gw_map(const GW_ID_T &gw_id, const std::string& nqn )
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
    int   update_active_timers();
    epoch_t get_epoch() const { return epoch; }
    int   _dump_gwmap(GWMAP & Gmap)const;
    int   _dump_active_timers( )const ;
    int   cfg_add_gw                    (const GW_ID_T &gw_id, const std::string & nqn, uint16_t ana_grpid);
    int   process_gw_map_ka             (const GW_ID_T &gw_id, const std::string& nqn ,  bool &propose_pending);
    int   process_gw_map_gw_down        (const GW_ID_T &gw_id, const std::string& nqn, bool &propose_pending);
    int   handle_homeless_ana_groups(bool &propose_pending);

    void debug_encode_decode(){
        ceph::buffer::list bl;
        encode(bl);
        auto p = bl.cbegin();
        decode(p);
    }
private:
    int  find_failover_candidate(const GW_ID_T &gw_id, const std::string& nqn,  GW_STATE_T* gw_state,  bool &propose_pending);
    int  set_failover_gw_for_ANA_group (const GW_ID_T &gw_id, const std::string& nqn, uint8_t ANA_groupid);
    void publish_map_to_gws(const std::string& nqn){
    }

    SUBSYST_GWMAP *  find_subsystem_map(const std::string& nqn)
    {
        auto it = Gmap.find(nqn);
        if (it != Gmap.end() ){
            return &it->second;
        }
        return  NULL;
    }

    int create_metadata(const GW_ID_T& gw_id, const std::string & nqn)
    {

        if(Gmetadata[nqn].size() == 0)
              Gmetadata.insert(make_pair(nqn, std::map<GW_ID_T, GW_METADATA_T>()));
        //Gmetadata[nqn].insert({ gw_id, new_metadata });
        return 0;
    }


    int  start_timer(const GW_ID_T &gw_id, const std::string& nqn, uint16_t anagrpid)
    {
        GW_METADATA_T* metadata;
        //const auto now = ceph::coarse_mono_clock::now();
        if ((metadata = find_gw_metadata(gw_id, nqn)) != NULL) {
            metadata->anagrp_sm_tstamps[anagrpid] = 0;// set timer
        }
        else {
            GW_METADATA_T  new_metadata = {INVALID_GW_TIMER,};
            for (int i=0; i<MAX_SUPPORTED_ANA_GROUPS; i++){
                new_metadata. anagrp_sm_tstamps[i] = INVALID_GW_TIMER;
            }
            new_metadata.anagrp_sm_tstamps[anagrpid] = 0;
            Gmetadata[nqn].insert({ gw_id, new_metadata });
        }
        return 0;
    }

    int  get_timer(const GW_ID_T &gw_id, const std::string& nqn, uint16_t anagrpid)
    {
        GW_METADATA_T* metadata;
        if ((metadata = find_gw_metadata(gw_id, nqn)) != NULL) {
            ceph_assert(metadata->anagrp_sm_tstamps[anagrpid] != INVALID_GW_TIMER);
            return metadata->anagrp_sm_tstamps[anagrpid];
        }
        else{
            ceph_assert(false);
        }
    }

    int  cancel_timer(const GW_ID_T &gw_id, const std::string& nqn, uint16_t anagrpid)
        {
            GW_METADATA_T* metadata;
            int i;
            if ((metadata = find_gw_metadata(gw_id, nqn)) != NULL) {
                metadata->anagrp_sm_tstamps[anagrpid] = INVALID_GW_TIMER;
                for(i=0; i<MAX_SUPPORTED_ANA_GROUPS; i++)
                    if(metadata->anagrp_sm_tstamps[i] != INVALID_GW_TIMER)
                        break;
                if(i==MAX_SUPPORTED_ANA_GROUPS){
                    Gmetadata[nqn].clear(); // remove all  gw_id timers from the map
                }
            }
            else {
              ceph_assert(false);
            }
            return 0;
        }

    GW_METADATA_T* find_gw_metadata(const GW_ID_T &gw_id, const std::string& nqn);
};

#endif /* SRC_MON_NVMEOFGWMAP_H_ */
