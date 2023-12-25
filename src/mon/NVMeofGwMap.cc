
#include <boost/tokenizer.hpp>
#include "include/stringify.h"
#include "NVMeofGwMon.h"

using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;
using std::string;

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, this, this)
using namespace TOPNSPC::common;

static ostream& _prefix(std::ostream *_dout, const NVMeofGwMap *h,//const Monitor &mon,
        const NVMeofGwMap *map) {
    return *_dout << "gw-mon." << map->mon->name << "@" << map->mon->rank;
}

static std::string G_gw_avail[] = {
                            "GW_CREATED", 
                            "GW_AVAILAB", 
                            "GW_UNAVAIL"};

static std::string G_gw_ana_states[] = {
                            "IDLE_STATE     ",
                            "STANDBY_STATE  ",
                            "ACTIVE_STATE   ",
                            "BLOCKED_OWNER  ",
                            "WAIT_FLBACK_RDY"
};

int  NVMeofGwMap::cfg_add_gw (const GW_ID_T &gw_id, const std::string& pool, const std::string& group) {
   GW_CREATED_T  gw_created;
   bool allocated[MAX_SUPPORTED_ANA_GROUPS] = {false};
   gw_created.ana_grp_id = 0xff;
   std::string  gw_name;
   std::string gw_preffix;
   NVMeofGwMap::gw_preffix_from_id_pool_group (gw_preffix,  pool, group );
   NVMeofGwMap::gw_name_from_id_pool_group    (gw_name,  gw_id , pool, group );

   for (auto& itr : Created_gws){
       // Allocate ana_grp_ids  per pool + group pair
       if((itr.first.find(gw_preffix) != std::string::npos)) // gw_name contains ".pool.group" string
          allocated[itr.second.ana_grp_id ]  = true;
     if(itr.first == gw_name){
           dout(4) << __func__ << " ERROR create GW: already exists in map " << gw_name << dendl;
           return -EEXIST ;
     }
   }

   for(int i=0; i<=MAX_SUPPORTED_ANA_GROUPS; i++){
      if (allocated[i] == false){
          gw_created.ana_grp_id = i;
          break;
      }
   }
   if(gw_created.ana_grp_id == 0xff){
        dout(4) << __func__ << " ERROR create GW: " << gw_name << "   ANA groupId was not allocated "   << dendl;
        return -EINVAL;
   }

   Created_gws.insert({gw_name, gw_created});
   dout(4) << __func__ << "Created GW:  " << gw_name << " grpid " <<  gw_created.ana_grp_id  <<  dendl;
   std::stringstream  ss;
   _dump_created_gws(ss);
   dout(4) << ss.str() <<  dendl;
   return 0;
}


int   NVMeofGwMap::cfg_delete_gw (const GW_ID_T &gw_id, const std::string& pool, const std::string& group, bool & map_modified){

    GW_STATE_T * state;
    int  ana_grp_id = 0;
    std::string  gw_name;

    NVMeofGwMap::gw_name_from_id_pool_group   (gw_name,  gw_id , pool, group );

    if(find_created_gw(gw_name, ana_grp_id) != 0)
    {
       dout(4) << __func__ << " ERROR :GW was not created " << gw_name << dendl;
        return -ENODEV ;
    }
    // traverse the GMap , find  gw in the map for all  nqns

    map_modified  = false;
    for (auto& itr : Gmap)
        for (auto& ptr : itr.second) {
            GW_ID_T found_gw_id = ptr.first;
            const std::string& nqn = itr.first;
            state = &ptr.second;
            if (gw_name == found_gw_id) { // GW was created
                bool modified = false;
                for(int i=0; i<MAX_SUPPORTED_ANA_GROUPS; i++){
                    fsm_handle_gw_delete (gw_name, nqn,  state->sm_state[i], i, modified);
                    map_modified |= modified;
                }
                dout(4) << " Delete GW :"<< gw_name << "nqn " << nqn << " ANA grpid: " << state->optimized_ana_group_id  << dendl;
                Gmap[itr.first].erase(gw_name);
                delete_metadata(gw_name, nqn);
            }
        }
    Created_gws.erase(gw_name);//TODO check whether ana map with nonce vector is destroyed properly - probably not. to handle!

    return 0;
}


GW_METADATA_T* NVMeofGwMap::find_gw_metadata(const GW_ID_T &gw_name, const std::string& nqn)
{
    auto it = Gmetadata.find(nqn);
    if (it != Gmetadata.end() )   {
        auto it2 = it->second.find(gw_name);
        if (it2 != it->second.end() ) {
            return  &it2->second;
        }
        else{
            dout(4) << __func__ << " not found by gw id " << gw_name << dendl;
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

            ss	<< "(gw-mon) NQN " << itr.first << " GW_ID " << ptr.first << " ANA gr " << std::setw(5) << (int)ptr.second.optimized_ana_group_id+1 <<
                                       " available :" << G_gw_avail[(int)ptr.second.availability] << " States: ";
            int num_groups = Created_gws.size();
            for (int i = 0; i < num_groups; i++) {
                ss << G_gw_ana_states[(int)ptr.second.sm_state[i]] << " " ;
            }
            ss  << " Failover peers: " << std::endl << "  ";

            for (int i = 0; i < num_groups; i++) {
                ss <<  ptr.second.failover_peer[i]  << " " ;
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
            ss	<< "(gw-mon) NQN " << itr.first << " GW_ID " << ptr.first << " ANA gr " << std::setw(5)
            << (int)ptr.second.optimized_ana_group_id+1 << " available :" << G_gw_avail[(int)ptr.second.availability] << " States: ";
            int num_groups = Created_gws.size();
            for (int i = 0; i < num_groups; i++) {
                ss << G_gw_ana_states[(int)ptr.second.sm_state[i]] << " " ;
            }
            ss  << "(gw-mon)  Failover peers: " << std::endl << "  ";

            for (int i = 0; i < num_groups; i++) {
                ss <<  ptr.second.failover_peer[i]  << " " ;
            }
            ss  << std::endl;
        }
    }
    return 0;
}

int   NVMeofGwMap::_dump_created_gws(std::stringstream &ss)const  {
    ss << __func__  <<  " called  " << std::endl;
    ss << "(gw-mon) Created GWs:" << std::endl;
    for (auto& itr : Created_gws) {
       ss << "(gw-mon) gw :" << itr.first << ", Ana-grp: " << itr.second.ana_grp_id << std::endl;
       ss << "(gw-mon) nonces map :" << std::endl;
       const GW_ANA_NONCE_MAP &nonce_map = itr.second.nonce_map;
       for(auto &nonce_it : nonce_map ){
           ss << "(gw-mon)   ana-grp: " << nonce_it.first << " :" ;
           for(auto &nonce_vectr_it :nonce_it.second){
               ss << " " << nonce_vectr_it ;
           }
           ss << std::endl;
       }
    }
    ss  << std::endl;
    return 0;
}


int NVMeofGwMap:: update_active_timers( bool &propose_pending ){

    //dout(4) << __func__  <<  " called,  p_monitor: " << mon << dendl;
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


int NVMeofGwMap::process_gw_map_gw_down(const GW_ID_T &gw_name, const std::string& nqn,  bool &propose_pending)
{
    int rc = 0;
    int i;
    GW_STATE_T* gw_state = find_gw_map(gw_name, nqn);
    if (gw_state) {
        dout(4) << "GW down " << gw_name << " nqn " <<nqn<< dendl;
        gw_state->availability = GW_AVAILABILITY_E::GW_UNAVAILABLE;
        for (i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i ++) {
            bool map_modified;
            fsm_handle_gw_down (gw_name, nqn, gw_state->sm_state[i], i, map_modified);
            if(map_modified) propose_pending = true;
            set_gw_standby_state(gw_state, i);
        }
    }
    else {
        dout(4)  << __FUNCTION__ << "ERROR GW-id was not found in the map " << gw_name << dendl;
        rc = 1;
    }
    return rc;
}


int NVMeofGwMap::process_gw_map_ka(const GW_ID_T &gw_name, const std::string& nqn , bool &propose_pending)
{
    int rc = 0;
#define     FAILBACK_PERSISTENCY_INT_SEC 8
    GW_STATE_T* gw_state = find_gw_map(gw_name, nqn);
    if (gw_state) {
        dout(4)  << "KA beacon from the GW " << gw_name << " in state " << (int)gw_state->availability << dendl;

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
                find_failback_gw(gw_name, nqn,  gw_state,  some_found);
                if (!some_found ) { // There is start of single GW so immediately turn its group to GW_ACTIVE_STATE
                    dout(4)  << "Warning - not found the GW responsible for" << gw_state->optimized_ana_group_id << " that took over the GW " << gw_name << "when it was fallen" << dendl;
                    gw_state->sm_state[gw_state->optimized_ana_group_id]  = GW_ACTIVE_STATE;
                }
            }
        }
        // if GW remains  AVAILABLE need to handle failback Timers , this is handled separately
    }
    else{
        dout(4)  <<  __func__ << "ERROR GW-id was not found in the map " << gw_name << dendl;
        rc = 1;
        ceph_assert(false);
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


int  NVMeofGwMap::handle_removed_subsystems (const std::vector<std::string> &created_subsystems, bool &propose_pending)
{
    bool found = false;;
    for (auto& m_itr : Gmap) {
        //if not found in the vector of configured subsystems, need to remove  the nqn from the map
        found = false;
        for(auto v_itr : created_subsystems){
            if (m_itr.first == v_itr){
               found = true;
               break;
            }
        }
        if(!found){
           // remove   m_itr.first  from the map
            dout(4) << "seems subsystem nqn was removed - to remove nqn from the map " << m_itr.first <<dendl;
            Gmap.erase(m_itr.first);
            propose_pending = true;
            break;
        }
    }
    return 0;
}

int  NVMeofGwMap::set_failover_gw_for_ANA_group(const GW_ID_T &failed_gw_id, const GW_ID_T &gw_name, const std::string& nqn, uint8_t ANA_groupid)
{
    GW_STATE_T* gw_state = find_gw_map(gw_name, nqn);
    gw_state->sm_state[ANA_groupid] = GW_ACTIVE_STATE;
    gw_state->failover_peer[ANA_groupid] = failed_gw_id;
    //publish_map_to_gws(nqn);
    dout(4) << "Set failower GW " << gw_name << " for ANA group " << (int)ANA_groupid << dendl;
    return 0;
}


int  NVMeofGwMap::find_failback_gw(const GW_ID_T &gw_name, const std::string& nqn, GW_STATE_T* gw_state,  bool &some_found)
{
   auto subsyst_it = find_subsystem_map(nqn);
    bool  found_some_gw = false;
    bool  found_candidate = false;
    for (auto& itr : *subsyst_it) {
        //cout << "Found GW " << itr.second.gw_id << endl;
        if (itr.second.sm_state[gw_state->optimized_ana_group_id] == GW_ACTIVE_STATE) {
            ceph_assert(itr.second.failover_peer[gw_state->optimized_ana_group_id] == gw_name);

            dout(4)  << "Found GW " << itr.first <<  ", nqn " << nqn << " that took over the ANAGRP " << (int)gw_state->optimized_ana_group_id << " of the available GW " << gw_name << dendl;
            itr.second.sm_state[gw_state->optimized_ana_group_id] = GW_WAIT_FAILBACK_PREPARED;
            start_timer(itr.first, nqn, gw_state->optimized_ana_group_id);// Add timestamp of start Failback preparation 
            gw_state->sm_state[gw_state->optimized_ana_group_id]  = GW_BLOCKED_AGROUP_OWNER;
            found_candidate = true;

            break;
        }
        else found_some_gw = true;
    }
    some_found =  found_candidate |found_some_gw;
    //TODO cleanup myself (gw_id) from the Block-List
    return 0;
}


// TODO When decision to change ANA state of group is prepared, need to consider that last seen FSM state is "approved" - means it was returned in beacon alone with map version
int  NVMeofGwMap::find_failover_candidate(const GW_ID_T &gw_id, const std::string& nqn,  GW_STATE_T* gw_state, int grpid,  bool &propose_pending)
{
  // dout(4) <<__func__<< " process GW down " << gw_id << dendl;
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
                set_failover_gw_for_ANA_group(gw_id, min_loaded_gw_id, nqn, grpid);
            }
            else {
                if (gw_state->sm_state[grpid] == GW_ACTIVE_STATE){// not found candidate but map changed.
                    propose_pending = true;
                    dout(4) << "gw down no candidate found " << dendl;
                   _dump_gwmap(Gmap);
                }
            }
            gw_state->sm_state[grpid] = GW_STANDBY_STATE;
        }
    return 0;
}


 int NVMeofGwMap::fsm_handle_gw_down    (const GW_ID_T &gw_name, const std::string& nqn, GW_STATES_PER_AGROUP_E state , int grpid, bool &map_modified)
 {
    switch (state)
    {
        case GW_STANDBY_STATE:
        case GW_IDLE_STATE:
         // nothing to do
        break;

        case GW_WAIT_FAILBACK_PREPARED:
        {
           cancel_timer(gw_name, nqn, grpid);
           auto subsyst_it = find_subsystem_map(nqn);
           for (auto& itr : *subsyst_it){
              if (itr.second.sm_state[grpid] == GW_BLOCKED_AGROUP_OWNER) // found GW   that was intended for  Failback for this ana grp
              {
                 dout(4) << "Warning: Outgoing Failback when GW is down back - to rollback it" << nqn <<" GW "  <<gw_name << "for ANA Group " << grpid << dendl;
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
            GW_STATE_T* gw_state = find_gw_map(gw_name, nqn);
            //TODO Start Block-List on this GW context
            find_failover_candidate( gw_name,  nqn, gw_state, grpid, map_modified);
        }
        break;

        default:{
            ceph_assert(false);
        }

    }
    return 0;
 }


 int NVMeofGwMap::fsm_handle_gw_delete (const GW_ID_T &gw_name, const std::string& nqn, GW_STATES_PER_AGROUP_E state , int grpid, bool &map_modified)
  {
     switch (state)
     {
         case GW_STANDBY_STATE:
         case GW_IDLE_STATE:
         case GW_BLOCKED_AGROUP_OWNER:
         {
           GW_STATE_T* gw_state = find_gw_map(gw_name, nqn);
            if(grpid == gw_state->optimized_ana_group_id) {// Try to find GW that temporary owns my group - if found, this GW should pass to standby for  this group
               auto subsyst_it = find_subsystem_map(nqn);
               for (auto& itr : *subsyst_it){
                  if (itr.second.sm_state[grpid] == GW_ACTIVE_STATE  || itr.second.sm_state[grpid] == GW_WAIT_FAILBACK_PREPARED){
                      set_gw_standby_state(&itr.second, grpid);
                      map_modified = true;
                      if (itr.second.sm_state[grpid] == GW_WAIT_FAILBACK_PREPARED)
                           cancel_timer(itr.first, nqn, grpid);
                      break;
                  }
               }
            }
         }
         break;

         case GW_WAIT_FAILBACK_PREPARED:
         {
            cancel_timer(gw_name, nqn, grpid);
            auto subsyst_it = find_subsystem_map(nqn);
            for (auto& itr : *subsyst_it){
               if (itr.second.sm_state[grpid] == GW_BLOCKED_AGROUP_OWNER) // found GW   that was intended for  Failback for this ana grp
               {
                  dout(4) << "Warning: Outgoing Failback when GW is deleted - to rollback it" << nqn <<" GW "  <<gw_name << "for ANA Group " << grpid << dendl;
                 //itr.second.sm_state[grpid] = GW_STANDBY_STATE;
                 set_gw_standby_state(&itr.second, grpid);
                 map_modified = true;
                 break;
               }
           }
         }
         break;

         case GW_ACTIVE_STATE:
         {
             GW_STATE_T* gw_state = find_gw_map(gw_name, nqn);
             map_modified = true;
             set_gw_standby_state(gw_state, grpid);
         }
         break;

         default:{
             ceph_assert(false);
         }
     }
     return 0;
  }


int NVMeofGwMap::fsm_handle_to_expired (const GW_ID_T &gw_name, const std::string& nqn, int grpid, bool &map_modified)
{
    GW_STATE_T* gw_state = find_gw_map(gw_name, nqn);
    auto subsyst_it      = find_subsystem_map(nqn);
    if (gw_state->sm_state[grpid] == GW_WAIT_FAILBACK_PREPARED) {

        dout(4)  << "Expired Failback timer from GW " << gw_name << " ANA groupId "<< grpid <<  dendl;

        cancel_timer(gw_name, nqn, grpid);
        for (auto& itr : *subsyst_it) {
            if (itr.second.sm_state[grpid] == GW_BLOCKED_AGROUP_OWNER && itr.second.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {
                set_gw_standby_state(gw_state, grpid);
                itr.second.sm_state[grpid] = GW_ACTIVE_STATE;
                dout(4)  << "Failback from GW " << gw_name << " to " << itr.first << dendl;
                map_modified = true;
                break;
            }
            else if (itr.second.optimized_ana_group_id == grpid ){
                if(itr.second.sm_state[grpid] == GW_STANDBY_STATE  &&  itr.second.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {
                    itr.second.sm_state[grpid] = GW_ACTIVE_STATE; // GW failed and started during the persistency interval
                    dout(4)  << "Failback unsuccessfull. GW: " << itr.first << "becomes Active for the ana group " << grpid  << dendl;
                }
                set_gw_standby_state(gw_state, grpid);
                dout(4)  << "Failback unsuccessfull GW: " << gw_name << "becomes standby for the ana group " << grpid  << dendl;
                map_modified = true;
                break;
            }
        }
    }
    return 0;
}


int  NVMeofGwMap::set_gw_standby_state(GW_STATE_T* gw_state, uint8_t ANA_groupid)
{
   gw_state->sm_state[ANA_groupid]       = GW_STANDBY_STATE;
   gw_state->failover_peer[ANA_groupid]  = "NULL";
   return 0;
}
