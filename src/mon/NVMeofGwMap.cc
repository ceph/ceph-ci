#include <boost/tokenizer.hpp>
#include "include/stringify.h"
#include "NVMeofGwMon.h"
#include "NVMeofGwMap.h"

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

int  NVMeofGwMap::cfg_add_gw(const GW_ID_T &gw_id, const GROUP_KEY& group_key) {
    // Calculate allocated group bitmask
    bool allocated[MAX_SUPPORTED_ANA_GROUPS] = {false};
    for (auto& itr: Created_gws[group_key]) {
        allocated[itr.second.ana_grp_id] = true;
        if(itr.first == gw_id) {
            dout(4) << __func__ << " ERROR create GW: already exists in map " << gw_id << dendl;
            return -EEXIST ;
        }
    }

    // Allocate the new group id
    for(int i=0; i<=MAX_SUPPORTED_ANA_GROUPS; i++) {
        if (allocated[i] == false) {
            GW_CREATED_T gw_created(i);
            Created_gws[group_key][gw_id] = gw_created;

            dout(4) << __func__ << "Created GW:  " << gw_id << " pool " << group_key.first << "group" << group_key.second
                    << " grpid " <<  gw_created.ana_grp_id  <<  dendl;
            return 0;
        }
    }

    dout(4) << __func__ << " ERROR create GW: " << gw_id << "   ANA groupId was not allocated "   << dendl;
    return -EINVAL;
}

int NVMeofGwMap::cfg_delete_gw(const GW_ID_T &gw_id, const GROUP_KEY& group_key) {
    int rc = 0;
    for (auto& nqn_gws_states: Gmap[group_key]) {
        auto& nqn = nqn_gws_states.first;
        auto& gw_states = nqn_gws_states.second;
        auto  gw_id_state_it = gw_states.find(gw_id);
        if (gw_id_state_it != gw_states.end()) {
            auto& state = gw_id_state_it->second;
            for(int i=0; i<MAX_SUPPORTED_ANA_GROUPS; i++){
                bool modified;
                fsm_handle_gw_delete (gw_id, group_key, nqn,  state.sm_state[i], i, modified);
            }
            dout(4) << " Delete GW :"<< gw_id << "nqn " << nqn << " ANA grpid: " << state.optimized_ana_group_id  << dendl;
            Gmap[group_key][nqn].erase(gw_id);
            Gmetadata[group_key][nqn].erase(gw_id);
        } else {
            rc = -EINVAL;
        }
    }
    Created_gws[group_key].erase(gw_id);
    return rc;
}

void NVMeofGwMap::update_active_timers( bool &propose_pending ){

    //dout(4) << __func__  <<  " called,  p_monitor: " << mon << dendl;
    for (auto& group_md: Gmetadata) {
        auto& group_key = group_md.first;
        auto& pool = group_key.first;
        auto& group = group_key.second;

        for (auto& nqn_md: group_md.second) {
            auto& nqn = nqn_md.first;
            for (auto& gw_md: nqn_md.second) {
                auto& gw_id = gw_md.first;
                auto& md = gw_md.second;
                for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
                    if (md.anagrp_sm_tstamps[i] == INVALID_GW_TIMER) continue;

                    md.anagrp_sm_tstamps[i]++;
                    dout(4) << "timer for GW " << gw_id << " ANA GRP " << i<<" :" << md.anagrp_sm_tstamps[i] <<dendl;
                    if(md.anagrp_sm_tstamps[i] >= 2){//TODO define
                        fsm_handle_to_expired (gw_id, std::make_pair(pool, group), nqn, i, propose_pending);
                    }
                }
            }
        }
    }
}


int NVMeofGwMap::process_gw_map_gw_down(const GW_ID_T &gw_id, const GROUP_KEY& group_key,
                                            const NQN_ID_T& nqn, bool &propose_pending) {
    int rc = 0;
    auto& nqn_gws_states = Gmap[group_key][nqn];
    auto  gw_state = nqn_gws_states.find(gw_id);
    if (gw_state != nqn_gws_states.end()) {
        dout(4) << "GW down " << gw_id << " nqn " <<nqn<< dendl;
        auto& st = gw_state->second;
        st.availability = GW_AVAILABILITY_E::GW_UNAVAILABLE;
        for (ANA_GRP_ID_T i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i ++) {
            fsm_handle_gw_down (gw_id, group_key, nqn, st.sm_state[i], i, propose_pending);
            st.standby_state(i);
        }
    }
    else {
        dout(4)  << __FUNCTION__ << "ERROR GW-id was not found in the map " << gw_id << dendl;
        rc = -EINVAL;
    }
    return rc;
}


void NVMeofGwMap::process_gw_map_ka(const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn , bool &propose_pending)
{

#define     FAILBACK_PERSISTENCY_INT_SEC 8
    auto& nqn_gws_states = Gmap[group_key][nqn];
    auto  gw_state = nqn_gws_states.find(gw_id);
    ceph_assert (gw_state != nqn_gws_states.end());
    auto& st = gw_state->second;
    dout(4)  << "KA beacon from the GW " << gw_id << " in state " << (int)st.availability << dendl;

    if (st.availability == GW_AVAILABILITY_E::GW_CREATED) {
        // first time appears - allow IO traffic for this GW
        st.availability = GW_AVAILABILITY_E::GW_AVAILABLE;
        for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) st.sm_state[i] = GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE;
        if (st.optimized_ana_group_id != REDUNDANT_GW_ANA_GROUP_ID) { // not a redundand GW
            st.sm_state[st.optimized_ana_group_id] = GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE;
        }
        propose_pending = true;
    }
    else if (st.availability == GW_AVAILABILITY_E::GW_UNAVAILABLE) {
        st.availability = GW_AVAILABILITY_E::GW_AVAILABLE;
        if (st.optimized_ana_group_id == REDUNDANT_GW_ANA_GROUP_ID) {
            for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) st.sm_state[i] = GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE;
            propose_pending = true; //TODO  try to find the 1st GW overloaded by ANA groups and start  failback for ANA group that it is not an owner of
        }
        else {
            //========= prepare to Failback to this GW =========
            // find the GW that took over on the group st.optimized_ana_group_id
            bool some_found = false;
            propose_pending = true;
            find_failback_gw(gw_id, group_key, nqn, some_found);
            if (!some_found ) { // There is start of single GW so immediately turn its group to GW_ACTIVE_STATE
                dout(4)  << "Warning - not found the GW responsible for" << st.optimized_ana_group_id << " that took over the GW " << gw_id << "when it was fallen" << dendl;
                st.sm_state[st.optimized_ana_group_id]  = GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE;
            }
        }
    }
}


void NVMeofGwMap::handle_abandoned_ana_groups(bool& propose)
{
    propose = false;
    for (auto& group_state: Gmap) {
        auto& group_key = group_state.first;
        auto& nqn_gws_states = group_state.second;

        for (auto& nqn_gws_state: nqn_gws_states) {
            auto& nqn = nqn_gws_state.first;
            auto& gws_states = nqn_gws_state.second;
            dout(4) << "NQN " << nqn << dendl;

            for (auto& gw_state : gws_states) { // loop for GWs inside nqn group
                auto& gw_id = gw_state.first;
                GW_STATE_T& state = gw_state.second;

                //1. Failover missed : is there is a GW in unavailable state? if yes, is its ANA group handled by some other GW?
                if (state.availability == GW_AVAILABILITY_E::GW_UNAVAILABLE && state.optimized_ana_group_id != REDUNDANT_GW_ANA_GROUP_ID) {
                    auto found_gw_for_ana_group = false;
                    for (auto& gw_state2 : gws_states) {
                        GW_STATE_T& state2 = gw_state2.second;
                        if (state2.availability == GW_AVAILABILITY_E::GW_AVAILABLE && state2.sm_state[state.optimized_ana_group_id] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE) {
                            found_gw_for_ana_group = true; // dout(4) << "Found GW " << ptr2.first << " that handles ANA grp " << (int)state->optimized_ana_group_id << dendl;
                            break;
                        }
                    }
                    if (found_gw_for_ana_group == false) { //choose the GW for handle ana group
                        dout(4)<< "Was not found the GW " << " that handles ANA grp " << (int)state.optimized_ana_group_id << " find candidate "<< dendl;

                        for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++)
                            find_failover_candidate( gw_id, group_key, nqn, i, propose );
                    }
                }

                //2. Failback missed: Check this GW is Available and Standby and no other GW is doing Failback to it
                else if (state.availability == GW_AVAILABILITY_E::GW_AVAILABLE
                            && state.optimized_ana_group_id != REDUNDANT_GW_ANA_GROUP_ID &&
                            state.sm_state[state.optimized_ana_group_id] == GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE)
                {
                    bool found = false;
                    for (auto& gw_state2 : gws_states) {
                        auto& state2 = gw_state2.second;
                        if (state2.sm_state[state.optimized_ana_group_id] == GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED){
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        dout(4) << __func__ << " GW " << gw_id   << " turns to be Active for ANA group " << state.optimized_ana_group_id << dendl;
                        state.sm_state[state.optimized_ana_group_id] = GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE;
                        propose = true;
                    }
                }
            }
        }
    }
}

/*
    sync our sybsystems from the beacon. systems subsystems not in beacon are removed.
*/
void  NVMeofGwMap::handle_removed_subsystems (const std::vector<NQN_ID_T> &current_subsystems, const GROUP_KEY& group_key, bool &propose_pending)
{
    auto& nqn_gws_states = Gmap[group_key];
    for (auto it = nqn_gws_states.begin(); it != nqn_gws_states.end(); ) {
        if (std::find(current_subsystems.begin(), current_subsystems.end(), it->first) == current_subsystems.end()) {
            // Erase the susbsystem nqn if the nqn is not in the current subsystems
            it = nqn_gws_states.erase(it);
        } else {
            // Move to the next pair
            ++it;
        }
    }
}

void  NVMeofGwMap::set_failover_gw_for_ANA_group(const GW_ID_T &failed_gw_id, const GROUP_KEY& group_key, const GW_ID_T &gw_id, const NQN_ID_T& nqn, ANA_GRP_ID_T ANA_groupid)
{
    GW_STATE_T& gw_state = Gmap[group_key][nqn][gw_id];
    gw_state.sm_state[ANA_groupid] = GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE;
    gw_state.failover_peer[ANA_groupid] = failed_gw_id;

    dout(4) << "Set failower GW " << gw_id << " for ANA group " << (int)ANA_groupid << dendl;
}


void  NVMeofGwMap::find_failback_gw(const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, bool &some_found)
{
    bool  found_some_gw = false;
    bool  found_candidate = false;
    auto& nqn_gws_states = Gmap[group_key][nqn];
    auto& gw_state = Gmap[group_key][nqn][gw_id];
    for (auto& nqn_gw_state: nqn_gws_states) {
        auto& found_gw_id = nqn_gw_state.first;
        auto& st = nqn_gw_state.second;
        if (st.sm_state[gw_state.optimized_ana_group_id] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE) {
            ceph_assert(st.failover_peer[gw_state.optimized_ana_group_id] == gw_id);

            dout(4)  << "Found GW " << found_gw_id <<  ", nqn " << nqn << " that took over the ANAGRP " << gw_state.optimized_ana_group_id << " of the available GW " << gw_id << dendl;
            st.sm_state[gw_state.optimized_ana_group_id] = GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED;
            start_timer(found_gw_id, group_key, nqn, gw_state.optimized_ana_group_id);// Add timestamp of start Failback preparation 
            gw_state.sm_state[gw_state.optimized_ana_group_id] = GW_STATES_PER_AGROUP_E::GW_BLOCKED_AGROUP_OWNER;
            found_candidate = true;

            break;
        }
        else found_some_gw = true;
    }
    some_found =  found_candidate |found_some_gw;
    //TODO cleanup myself (gw_id) from the Block-List
}


// TODO When decision to change ANA state of group is prepared, need to consider that last seen FSM state is "approved" - means it was returned in beacon alone with map version
void  NVMeofGwMap::find_failover_candidate(const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, ANA_GRP_ID_T grpid, bool &propose_pending)
{
    // dout(4) <<__func__<< " process GW down " << gw_id << dendl;
    #define ILLEGAL_GW_ID " "
    #define MIN_NUM_ANA_GROUPS 0xFFF
    int min_num_ana_groups_in_gw = 0;
    int current_ana_groups_in_gw = 0;
    GW_ID_T min_loaded_gw_id = ILLEGAL_GW_ID;

    auto& nqn_gws_states = Gmap[group_key][nqn];

    auto gw_state = nqn_gws_states.find(gw_id);
    ceph_assert(gw_state != nqn_gws_states.end());

    // this GW may handle several ANA groups and  for each of them need to found the candidate GW
    if (gw_state->second.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE || gw_state->second.optimized_ana_group_id == grpid) {
        // Find a GW that takes over the ANA group(s)
        min_num_ana_groups_in_gw = MIN_NUM_ANA_GROUPS;
        min_loaded_gw_id = ILLEGAL_GW_ID;
        for (auto& found_gw_state: nqn_gws_states) { // for all the gateways of the subsystem
            auto st = found_gw_state.second;
            if (st.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {
                current_ana_groups_in_gw = 0;
                for (int j = 0; j < MAX_SUPPORTED_ANA_GROUPS; j++) {
                    if (st.sm_state[j] == GW_STATES_PER_AGROUP_E::GW_BLOCKED_AGROUP_OWNER || st.sm_state[j] == GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED) {
                        current_ana_groups_in_gw = 0xFFFF;
                        break; // dont take into account   GWs in the transitive state
                    }
                    else if (st.sm_state[j] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE)
                        //dout(4) << " process GW down " << current_ana_groups_in_gw << dendl;
                        current_ana_groups_in_gw++; // how many ANA groups are handled by this GW
                    }

                    if (min_num_ana_groups_in_gw > current_ana_groups_in_gw) {
                        min_num_ana_groups_in_gw = current_ana_groups_in_gw;
                        min_loaded_gw_id = found_gw_state.first;
                        dout(4) << "choose: gw-id  min_ana_groups " << min_loaded_gw_id << current_ana_groups_in_gw << " min " << min_num_ana_groups_in_gw << dendl;
                    }
                }
            }
            if (min_loaded_gw_id != ILLEGAL_GW_ID) {
                propose_pending = true;
                set_failover_gw_for_ANA_group(gw_id, group_key, min_loaded_gw_id, nqn, grpid);
            }
            else {
                if (gw_state->second.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE){// not found candidate but map changed.
                    propose_pending = true;
                    dout(4) << "gw down no candidate found " << dendl;
                }
            }
            gw_state->second.sm_state[grpid] = GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE;
        }
}


 void NVMeofGwMap::fsm_handle_gw_down(const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, GW_STATES_PER_AGROUP_E state, ANA_GRP_ID_T grpid,  bool &map_modified)
 {
    switch (state)
    {
        case GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE:
        case GW_STATES_PER_AGROUP_E::GW_IDLE_STATE:
            // nothing to do
            break;

        case GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED:
            cancel_timer(gw_id, group_key, nqn, grpid);

            for (auto& gw_st: Gmap[group_key][nqn]) {
                auto& st = gw_st.second;
                if (st.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_BLOCKED_AGROUP_OWNER) { // found GW   that was intended for  Failback for this ana grp
                    dout(4) << "Warning: Outgoing Failback when GW is down back - to rollback it" << nqn <<" GW "  <<gw_id << "for ANA Group " << grpid << dendl;
                    st.sm_state[grpid] = GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE;
                    map_modified = true;
                    break;
                }
            }
            break;

        case GW_STATES_PER_AGROUP_E::GW_BLOCKED_AGROUP_OWNER:
            // nothing to do - let failback timer expire
            break;

        case GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE:
        {
            find_failover_candidate( gw_id, group_key, nqn, grpid, map_modified);
        }
        break;

        default:{
            ceph_assert(false);
        }

    }
 }


void NVMeofGwMap::fsm_handle_gw_delete (const GW_ID_T &gw_id, const GROUP_KEY& group_key,
    const NQN_ID_T& nqn, GW_STATES_PER_AGROUP_E state , ANA_GRP_ID_T grpid, bool &map_modified) {
    switch (state)
    {
        case GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE:
        case GW_STATES_PER_AGROUP_E::GW_IDLE_STATE:
        case GW_STATES_PER_AGROUP_E::GW_BLOCKED_AGROUP_OWNER:
        {
            GW_STATE_T& gw_state = Gmap[group_key][nqn][gw_id];

            if (grpid == gw_state.optimized_ana_group_id) {// Try to find GW that temporary owns my group - if found, this GW should pass to standby for  this group
                auto& gateway_states = Gmap[group_key][nqn];
                for (auto& gs: gateway_states) {
                    if (gs.second.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE  || gs.second.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED){
                        gs.second.standby_state(grpid);
                        if (gs.second.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED)
                            cancel_timer(gs.first, group_key, nqn, grpid);
                        break;
                    }
                }
            }
        }
        break;

        case GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED:
        {
            cancel_timer(gw_id, group_key, nqn, grpid);
            for (auto& nqn_gws_state: Gmap[group_key][nqn]) {
                auto& st = nqn_gws_state.second;

                if (st.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_BLOCKED_AGROUP_OWNER) { // found GW   that was intended for  Failback for this ana grp
                    dout(4) << "Warning: Outgoing Failback when GW is deleted - to rollback it" << nqn <<" GW "  <<gw_id << "for ANA Group " << grpid << dendl;
                    st.standby_state(grpid);
                    map_modified = true;
                    break;
                }
            }
        }
        break;

        case GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE:
        {
            GW_STATE_T& gw_state = Gmap[group_key][nqn][gw_id];
            map_modified = true;
            gw_state.standby_state(grpid);
        }
        break;

        default: {
            ceph_assert(false);
        }
    }
}


void NVMeofGwMap::fsm_handle_to_expired(const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, ANA_GRP_ID_T grpid,  bool &map_modified)
{
    auto& gw_state = Gmap[group_key][nqn][gw_id];

    if (gw_state.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED) {

        dout(4)  << "Expired Failback timer from GW " << gw_id << " ANA groupId "<< grpid <<  dendl;

        cancel_timer(gw_id, group_key, nqn, grpid);
        for (auto& gw_state: Gmap[group_key][nqn]) {
            auto& st = gw_state.second;
            if (st.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_BLOCKED_AGROUP_OWNER &&
                    st.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {
                st.standby_state(grpid);
                st.sm_state[grpid] = GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE;
                dout(4)  << "Failback from GW " << gw_id << " to " << gw_state.first << dendl;
                map_modified = true;
                break;
            }
            else if (st.optimized_ana_group_id == grpid ){
                if(st.sm_state[grpid] == GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE  &&  st.availability == GW_AVAILABILITY_E::GW_AVAILABLE) {
                    st.sm_state[grpid] = GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE; // GW failed and started during the persistency interval
                    dout(4)  << "Failback unsuccessfull. GW: " << gw_state.first << "becomes Active for the ana group " << grpid  << dendl;
                }
                st.standby_state(grpid);
                dout(4)  << "Failback unsuccessfull GW: " << gw_id << "becomes standby for the ana group " << grpid  << dendl;
                map_modified = true;
                break;
            }
        }
    }
}

void NVMeofGwMap::start_timer(const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, ANA_GRP_ID_T anagrpid) {
    Gmetadata[group_key][nqn][gw_id].anagrp_sm_tstamps[anagrpid] = 0;
}

int  NVMeofGwMap::get_timer(const GW_ID_T &gw_id, GROUP_KEY& group_key, const NQN_ID_T& nqn, ANA_GRP_ID_T anagrpid) {
    auto timer = Gmetadata[group_key][nqn][gw_id].anagrp_sm_tstamps[anagrpid];
    ceph_assert(timer != INVALID_GW_TIMER);
    return timer;
}

void NVMeofGwMap::cancel_timer(const GW_ID_T &gw_id, const GROUP_KEY& group_key, const NQN_ID_T& nqn, ANA_GRP_ID_T anagrpid) {
    Gmetadata[group_key][nqn][gw_id].anagrp_sm_tstamps[anagrpid] = INVALID_GW_TIMER;
}
