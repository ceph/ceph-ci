
#include <boost/tokenizer.hpp>
#include "include/stringify.h"
#include "NVMeofGwMon.h"

using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;


#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, this, this)
using namespace TOPNSPC::common;

static ostream& _prefix(std::ostream *_dout, const NVMeofGwMap *h,//const Monitor &mon,
        const NVMeofGwMap *map) {
    return *_dout << "gw-mon." << map->mon->name << "@" << map->mon->rank;
}



int  NVMeofGwMap::cfg_add_gw (const GW_ID_T &gw_id, const std::string & nqn, uint16_t ana_grpid) {
    GW_STATE_T state{ {GW_IDLE_STATE,} , ana_grpid, GW_AVAILABILITY_E::GW_CREATED,   0 };

    if (find_gw_map(gw_id, nqn)) {
        dout(4) << __func__ << " ERROR :GW already exists in map " << gw_id << dendl;
        return 1;
    }
    if (ana_grpid >= MAX_SUPPORTED_ANA_GROUPS && ana_grpid != REDUNDANT_GW_ANA_GROUP_ID)
    {
        dout(4) << __func__ << " ERROR :GW " << gw_id << " bad ANA group " <<(int)ana_grpid << dendl;
        return 1;
    }

    //TODO check that all MAX_SUPPORTED_ANA_GROUPS are occupied in the subsystem - assert

    if(Gmap[nqn].size() ==0 )
        Gmap.insert(make_pair(nqn, SUBSYST_GWMAP()));
    Gmap[nqn].insert({gw_id, state});

    create_metadata(gw_id, nqn);
    dout(4) << " Add GW :"<< gw_id << "nqn " << nqn << " ANA grpid: " << ana_grpid << dendl;
    return 0;
}


GW_METADATA_T* NVMeofGwMap::find_gw_metadata(const GW_ID_T &gw_id, const std::string& nqn)
{
    auto it = Gmetadata.find(nqn);
    if (it != Gmetadata.end() )   {
        auto it2 = it->second.find(gw_id);
        if (it2 != it->second.end() ) {
            return  &it2->second;
        }
        else{
            dout(4) << __func__ << " not found by gw id " << gw_id << dendl;
        }
    }
    else{
        dout(4) << __func__ << " not found by nqn " << nqn << dendl;
    }
    return NULL;
}


int NVMeofGwMap::_dump_gwmap(GWMAP & Gmap)const  {

    dout(0) << __func__  <<  " called  " << mon << dendl;
    std::ostringstream ss;
    ss  << std::endl;
    for (auto& itr : Gmap) {
        for (auto& ptr : itr.second) {

            ss	<< " NQN " << itr.first << " GW_ID " << ptr.first << " ANA gr " << std::setw(5) << (int)ptr.second.optimized_ana_group_id << " available " << (int)ptr.second.availability << " States: ";
            for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
                ss << (int)ptr.second.sm_state[i] << " " ;
            }
            ss  << std::endl;
        }
    }
    dout(0) << ss.str() <<dendl;
    return 0;
}

int NVMeofGwMap::_dump_gwmap(std::stringstream &ss)const  {
    ss << __func__  <<  " called  " << mon << std::endl;   
    for (auto& itr : Gmap) {
        for (auto& ptr : itr.second) {
            ss	<< " NQN " << itr.first << " GW_ID " << ptr.first << " ANA gr " << std::setw(5) 
            << (int)ptr.second.optimized_ana_group_id << " available " << (int)ptr.second.availability << " States: ";
            for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
                ss << (int)ptr.second.sm_state[i] << " " ;
            }
            ss  << std::endl;
        }
    }
    //dout(0) << ss.str() <<dendl;
    return 0;
}


int NVMeofGwMap:: update_active_timers( bool &propose_pending ){

    dout(4) << __func__  <<  " called  " << mon << dendl;
    for (auto& itr : Gmetadata) {
        for (auto& ptr : itr.second) {
            GW_METADATA_T *metadata = &ptr.second;
            for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
                if (metadata->anagrp_sm_tstamps[i]  != INVALID_GW_TIMER){
                    metadata->anagrp_sm_tstamps[i] ++;
                    dout(4) << "timer for GW " << ptr.first << " ANA GRP " << i<<" :" << metadata->anagrp_sm_tstamps[i] <<dendl;
                    if(metadata->anagrp_sm_tstamps[i] >= 2){//TODO define
                        fsm_handle_to_expired (ptr.first, itr.first, i, propose_pending);
                    }
                }
            }
        }
    }
    return 0;
}


int NVMeofGwMap::_dump_active_timers( )const  {

    dout(4) << __func__  <<  " called  " << mon << dendl;
    std::ostringstream ss;
    ss  << std::endl;
    for (auto& itr : Gmetadata) {
        for (auto& ptr : itr.second) {
            ss  << " NQN " << itr.first << " GW_ID " << ptr.first << std::endl;
            for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
               if (ptr.second.anagrp_sm_tstamps[i]  != INVALID_GW_TIMER){
                   ss << "timer for GW " << ptr.first << " ANA GRP " << i <<" :"<< ptr.second.anagrp_sm_tstamps[i];
               }
               ss << std::endl;
            }
        }
    }
    dout(4) << ss.str() <<dendl;
    return 0;
}


int NVMeofGwMap::process_gw_map_gw_down(const GW_ID_T &gw_id, const std::string& nqn,  bool &propose_pending)
{
    int rc = 0;
    int i;
    GW_STATE_T* gw_state = find_gw_map(gw_id, nqn);
    if (gw_state) {
        dout(4) << "GW down " << gw_id << dendl;
        gw_state->availability = GW_AVAILABILITY_E::GW_UNAVAILABLE;
        for (i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
            bool map_modified;
            fsm_handle_gw_down (gw_id, nqn, gw_state->sm_state[i], i, map_modified);
            if(map_modified) propose_pending = true;
            gw_state->sm_state[i] = GW_STANDBY_STATE;
        }
    }
    else {
        dout(4)  << __FUNCTION__ << "ERROR GW-id was not found in the map " << gw_id << dendl;
        rc = 1;
    }
    return rc;
}


int NVMeofGwMap::process_gw_map_ka(const GW_ID_T &gw_id, const std::string& nqn , bool &propose_pending)
{
    int rc = 0;
#define     FAILBACK_PERSISTENCY_INT_SEC 8
    GW_STATE_T* gw_state = find_gw_map(gw_id, nqn);
    if (gw_state) {
        dout(4)  << "KA beacon from the GW " << gw_id << " in state " << (int)gw_state->availability << dendl;
        propose_pending = false;
        if (gw_state->availability == GW_AVAILABILITY_E::GW_CREATED) {
            // first time appears - allow IO traffic for this GW
            gw_state->availability = GW_AVAILABILITY_E::GW_AVAILABLE;
            for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) gw_state->sm_state[i] = GW_STANDBY_STATE;
            if (gw_state->optimized_ana_group_id != REDUNDANT_GW_ANA_GROUP_ID) { // not a redundand GW
                gw_state->sm_state[gw_state->optimized_ana_group_id] = GW_ACTIVE_STATE;
            }
            propose_pending = true;
        }

        else if (gw_state->availability == GW_AVAILABILITY_E::GW_UNAVAILABLE) {
            gw_state->availability = GW_AVAILABILITY_E::GW_AVAILABLE;
            if (gw_state->optimized_ana_group_id == REDUNDANT_GW_ANA_GROUP_ID) {
                for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) gw_state->sm_state[i] = GW_STANDBY_STATE;
                propose_pending = true; //TODO  try to find the 1st GW overloaded by ANA groups and start  failback for ANA group that it is not an owner of
            }
            else {
                //========= prepare to Failback to this GW =========
                // find the GW that took over on the group gw_state->optimized_ana_group_id
                bool some_found = false;
                propose_pending = true;
                find_failback_gw(gw_id, nqn,  gw_state,  some_found);
                if (!some_found ) { // There is start of single GW so immediately turn its group to GW_ACTIVE_STATE
                    dout(4)  << "Warning - not found the GW responsible for" << gw_state->optimized_ana_group_id << " that took over the GW " << gw_id << "when it was fallen" << dendl;
                    gw_state->sm_state[gw_state->optimized_ana_group_id]  = GW_ACTIVE_STATE;
                }
            }
        }
        // if GW remains  AVAILABLE need to handle failback Timers , this is handled separately
    }
    else{
        dout(4)  <<  __func__ << "ERROR GW-id was not found in the map " << gw_id << dendl;
        rc = 1;
    }
    return rc;
}


int  NVMeofGwMap::handle_abandoned_ana_groups(bool & propose)
{
    propose = false;
    for (auto& nqn_itr : Gmap) {
        dout(4) << "NQN " << nqn_itr.first << dendl;

        for (auto& ptr : nqn_itr.second) { // loop for GWs inside nqn group
            auto gw_id = ptr.first;
            GW_STATE_T* state = &ptr.second;

            //1. Failover missed : is there is a GW in unavailable state? if yes, is its ANA group handled by some other GW?
            if (state->availability == GW_AVAILABILITY_E::GW_UNAVAILABLE && state->optimized_ana_group_id != REDUNDANT_GW_ANA_GROUP_ID) {
                auto found_gw_for_ana_group = false;
                for (auto& ptr2 : nqn_itr.second) {
                    if (ptr2.second.availability == GW_AVAILABILITY_E::GW_AVAILABLE && ptr2.second.sm_state[state->optimized_ana_group_id] == GW_ACTIVE_STATE) {
                        found_gw_for_ana_group = true; // dout(4) << "Found GW " << ptr2.first << " that handles ANA grp " << (int)state->optimized_ana_group_id << dendl;
                        break;
                    }
                }
                if (found_gw_for_ana_group == false) { //choose the GW for handle ana group
                    dout(4)<< "Was not found the GW " << " that handles ANA grp " << (int)state->optimized_ana_group_id << " find candidate "<< dendl;

                    GW_STATE_T* gw_state = find_gw_map(gw_id, nqn_itr.first);
                    for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++)
                        find_failover_candidate( gw_id,  nqn_itr.first , gw_state, i, propose );
                }
            }

            //2. Failback missed: Check this GW is Available and Standby and no other GW is doing Failback to it
            else if (state->availability == GW_AVAILABILITY_E::GW_AVAILABLE && state->optimized_ana_group_id != REDUNDANT_GW_ANA_GROUP_ID &&
                      state->sm_state[state->optimized_ana_group_id] == GW_STANDBY_STATE
                    )
            {
                bool found = false;
                for (auto& ptr2 : nqn_itr.second) {
                      if (  ptr2.second.sm_state[state->optimized_ana_group_id] == GW_WAIT_FAILBACK_PREPARED){
                          found = true;
                          break;
                      }
                }
                if(!found){
                    dout(4) << __func__ << " GW " <<gw_id   << " turns to be Active for ANA group " << state->optimized_ana_group_id << dendl;
                    state->sm_state[state->optimized_ana_group_id] = GW_ACTIVE_STATE;
                    propose = true;
                }
            }
        }
    }
    return 0;
}

int  NVMeofGwMap::set_failover_gw_for_ANA_group(const GW_ID_T &gw_id, const std::string& nqn, uint8_t ANA_groupid)
{
    GW_STATE_T* gw_state = find_gw_map(gw_id, nqn);
    gw_state->sm_state[ANA_groupid] = GW_ACTIVE_STATE;
    //publish_map_to_gws(nqn);
    dout(4) << "Set failower GW " << gw_id << " for ANA group " << (int)ANA_groupid << dendl;
    return 0;
}


int  NVMeofGwMap::find_failback_gw(const GW_ID_T &gw_id, const std::string& nqn, GW_STATE_T* gw_state,  bool &some_found)
{
   auto subsyst_it = find_subsystem_map(nqn);
    bool  found_some_gw = false;
    bool  found_candidate = false;
    for (auto& itr : *subsyst_it) {
        //cout << "Found GW " << itr.second.gw_id << endl;
        if (itr.second.sm_state[gw_state->optimized_ana_group_id] == GW_ACTIVE_STATE) {
            dout(4)  << "Found GW " << itr.first <<  ", nqn " << nqn << " that took over the ANAGRP " << (int)gw_state->optimized_ana_group_id << " of the available GW " << gw_id << dendl;
            itr.second.sm_state[gw_state->optimized_ana_group_id] = GW_WAIT_FAILBACK_PREPARED;
            start_timer(itr.first, nqn, gw_state->optimized_ana_group_id);// Add timestamp of start Failback preparation 
            gw_state->sm_state[gw_state->optimized_ana_group_id]  = GW_BLOCKED_AGROUP_OWNER;
            found_candidate = true;
            break;
        }
        else found_some_gw = true;
    }
    some_found =  found_candidate |found_some_gw;
    return 0;
}


// TODO When decision to change ANA state of group is prepared, need to consider that last seen FSM state is "approved" - means it was returned in beacon alone with map version
int  NVMeofGwMap::find_failover_candidate(const GW_ID_T &gw_id, const std::string& nqn,  GW_STATE_T* gw_state, int grpid,  bool &propose_pending)
{
   dout(4) <<__func__<< " process GW down " << gw_id << dendl;
#define ILLEGAL_GW_ID " "
#define MIN_NUM_ANA_GROUPS 0xFFF
   int min_num_ana_groups_in_gw = 0;
   int current_ana_groups_in_gw = 0;
   GW_ID_T min_loaded_gw_id = ILLEGAL_GW_ID;
   auto subsyst_it = find_subsystem_map(nqn);

       // this GW may handle several ANA groups and  for each of them need to found the candidate GW
        if (gw_state->sm_state[grpid] == GW_ACTIVE_STATE || gw_state->optimized_ana_group_id == grpid) {
            // Find a GW that takes over the ANA group(s)
            min_num_ana_groups_in_gw = MIN_NUM_ANA_GROUPS;
            min_loaded_gw_id = ILLEGAL_GW_ID;
            for (auto& itr : *subsyst_it) { // for all the gateways of the subsystem
                if (itr.second.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {

                    current_ana_groups_in_gw = 0;
                    for (int j = 0; j < MAX_SUPPORTED_ANA_GROUPS; j++) {
                        if (itr.second.sm_state[j] == GW_BLOCKED_AGROUP_OWNER || itr.second.sm_state[j] == GW_WAIT_FAILBACK_PREPARED) {
                            current_ana_groups_in_gw = 0xFFFF;
                            break; // dont take into account   GWs in the transitive state
                        }
                        else if (itr.second.sm_state[j] == GW_ACTIVE_STATE)
                            //dout(4) << " process GW down " << current_ana_groups_in_gw << dendl;
                            current_ana_groups_in_gw++; // how many ANA groups are handled by this GW
                    }

                    if (min_num_ana_groups_in_gw > current_ana_groups_in_gw) {
                        min_num_ana_groups_in_gw = current_ana_groups_in_gw;
                        min_loaded_gw_id = itr.first;
                        dout(4) << "choose: gw-id  min_ana_groups " << itr.first << current_ana_groups_in_gw << " min " << min_num_ana_groups_in_gw << dendl;
                    }
                }
            }
            if (min_loaded_gw_id != ILLEGAL_GW_ID) {
                propose_pending = true;
                set_failover_gw_for_ANA_group(min_loaded_gw_id, nqn, grpid);
            }
            else  propose_pending = false;
            gw_state->sm_state[grpid] = GW_STANDBY_STATE;
        }
    return 0;
}


 int NVMeofGwMap::fsm_handle_gw_down    (const GW_ID_T &gw_id, const std::string& nqn, GW_STATES_PER_AGROUP_E state , int grpid, bool &map_modified)
 {
    switch (state)
    {
        case GW_STANDBY_STATE:
        case GW_IDLE_STATE:
         // nothing to do
        break;

        case GW_WAIT_FAILBACK_PREPARED:
        {
           cancel_timer(gw_id, nqn, grpid);
           auto subsyst_it = find_subsystem_map(nqn);
           for (auto& itr : *subsyst_it){
              if (itr.second.sm_state[grpid] == GW_BLOCKED_AGROUP_OWNER) // found GW   that was intended for  Failback for this ana grp
              {
                 dout(4) << "Warning: Outgoing Failback when GW is down back - to rollback it" << nqn <<" GW "  <<gw_id << "for ANA Group " << grpid << dendl;
                itr.second.sm_state[grpid] = GW_STANDBY_STATE;
                map_modified = true;
                break;
              }
          }
        }
        break;    

        case GW_BLOCKED_AGROUP_OWNER:
        // nothing to do - let failback timer expire 
        break;

        case GW_ACTIVE_STATE:
        {
            GW_STATE_T* gw_state = find_gw_map(gw_id, nqn);
            find_failover_candidate( gw_id,  nqn, gw_state, grpid, map_modified);
        }
        break;

        default:{
            ceph_assert(false);
        }

    }
    return 0;
 }


int NVMeofGwMap::fsm_handle_to_expired (const GW_ID_T &gw_id, const std::string& nqn, int grpid, bool &map_modified)
{
    GW_STATE_T* gw_state = find_gw_map(gw_id, nqn);
    auto subsyst_it      = find_subsystem_map(nqn);
    if (gw_state->sm_state[grpid] == GW_WAIT_FAILBACK_PREPARED) {

        dout(4)  << "Expired Failback timer from GW " << gw_id << " ANA groupId "<< grpid <<  dendl;

        cancel_timer(gw_id, nqn, grpid);
        for (auto& itr : *subsyst_it) {
            if (itr.second.sm_state[grpid] == GW_BLOCKED_AGROUP_OWNER && itr.second.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {
                gw_state->sm_state[grpid]  = GW_STANDBY_STATE;
                itr.second.sm_state[grpid] = GW_ACTIVE_STATE;
                dout(4)  << "Failback from GW " << gw_id << " to " << itr.first << dendl;
                map_modified = true;
                break;
            }
            else if (itr.second.optimized_ana_group_id == grpid ){
                if(itr.second.sm_state[grpid] == GW_STANDBY_STATE  &&  itr.second.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {
                    itr.second.sm_state[grpid] = GW_ACTIVE_STATE; // GW failed and started during the persistency interval
                    dout(4)  << "Failback unsuccessfull. GW: " << itr.first << "becomes Active for the ana group " << grpid  << dendl;
                }
                gw_state->sm_state[grpid] = GW_STANDBY_STATE;
                dout(4)  << "Failback unsuccessfull GW: " << gw_id << "becomes standby for the ana group " << grpid  << dendl;
                map_modified = true;
                break;
            }
        }
    }
    return 0;
}
