/*
 * NVMeGWMonitor.cc
 *
 *  Created on: Oct 17, 2023
 *      Author:
 */


#include <boost/tokenizer.hpp>
#include "include/stringify.h"
#include "NVMeofGwMon.h"
#include "messages/MNVMeofGwBeacon.h"
#include "messages/MNVMeofGwMap.h"

using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;
using std::string;
using std::vector;

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, this, this)
using namespace TOPNSPC::common;

static ostream& _prefix(std::ostream *_dout, const NVMeofGwMon *h,//const Monitor &mon,
        const NVMeofGwMon *hmon) {
    return *_dout << "gw-mon." << hmon->mon.name << "@" << hmon->mon.rank;
}
#define MY_MON_PREFFIX " NVMeGW "


void NVMeofGwMon::init(){
    dout(4) << MY_MON_PREFFIX << __func__  <<  "called " << dendl;
}

void NVMeofGwMon::on_restart(){
    dout(4) << MY_MON_PREFFIX << __func__  <<  "called " << dendl;
    last_beacon.clear();
    last_tick = ceph::coarse_mono_clock::now();
}


void NVMeofGwMon::on_shutdown() {

}

static int cnt ;
#define start_cnt 6
void NVMeofGwMon::inject1(){
    //bool propose = false;
    if( ++cnt  == 4  ){// simulation that new configuration was added
        pending_map.cfg_add_gw("GW1" );
        pending_map.cfg_add_gw("GW2" );
        pending_map.cfg_add_gw("GW3" );

        pending_map._dump_gwmap(pending_map.Gmap);
        pending_map.debug_encode_decode();
        dout(4) << "Dump map after decode encode:" <<dendl;
        pending_map._dump_gwmap(pending_map.Gmap);
    }
   /* else if( cnt  == start_cnt  ){  // simulate - add another GW - to check that new map would be synchronized with peons
        pending_map.cfg_add_gw("gw2"  );
        pending_map._dump_gwmap(pending_map.Gmap);

        //Simulate KA beacon from the gws

        pending_map.process_gw_map_ka( "gw1", "nqn2008.node1", propose);
        if(propose)
            propose_pending();

        pending_map._dump_gwmap(pending_map.Gmap);

    }
    else if( cnt  == start_cnt+2  ){  // simulate - add another GW - to check that new map would be synchronized with peons
        pending_map.process_gw_map_ka( "gw2", "nqn2008.node1", propose);
        if(propose)
             propose_pending();
    }
    else if( cnt  == start_cnt+3 ){  // simulate - gw1 down
        pending_map.process_gw_map_gw_down( "gw1", "nqn2008.node1", propose);
        if(propose)
            propose_pending();

    }


    else if( cnt  == start_cnt+5 ){  // simulate - gw1 is back

        pending_map.process_gw_map_ka( "gw1", "nqn2008.node1", propose);
        if(propose)
            propose_pending();
    }

    else if( cnt  == start_cnt+6 ){  // simulate - gw1 is down   Simulate gw election by polling function handle_homeless_ana_groups

            pending_map.process_gw_map_gw_down( "gw1", "nqn2008.node1", propose);
            if(propose)
                propose_pending();
    }

    else if( cnt  == start_cnt+7 ){  // simulate - gw1 is UP
        pending_map.process_gw_map_ka( "gw1", "nqn2008.node1", propose);
        if(propose)
            propose_pending();
    }
    else if( cnt  == start_cnt+9 ){  // simulate - gw2 still OK - checks the persistency timer in the state
        pending_map.process_gw_map_ka( "gw2", "nqn2008.node1", propose);
        if(propose)
            propose_pending();
    }
    */
}


void NVMeofGwMon::tick(){
   // static int cnt=0;
    if(map.delay_propose){
       check_subs(false);  // to send map to clients
       map.delay_propose = false;
    }

    if (!is_active() || !mon.is_leader()){
        dout(4) << __func__  <<  " NVMeofGwMon leader : " << mon.is_leader() << "active : " << is_active()  << dendl;
        return;
    }
    bool _propose_pending = false;
  
    //inject1();
    const auto now = ceph::coarse_mono_clock::now();
    const auto nvmegw_beacon_grace = g_conf().get_val<std::chrono::seconds>("mon_nvmeofgw_beacon_grace"); 
    dout(4) << MY_MON_PREFFIX << __func__  <<  "NVMeofGwMon leader got a real tick, pending epoch "<< pending_map.epoch     << dendl;

    const auto mgr_tick_period = g_conf().get_val<std::chrono::seconds>("mgr_tick_period");

    if (last_tick != ceph::coarse_mono_clock::zero()
          && (now - last_tick > (nvmegw_beacon_grace - mgr_tick_period))) {
        // This case handles either local slowness (calls being delayed
        // for whatever reason) or cluster election slowness (a long gap
        // between calls while an election happened)
        dout(4) << __func__ << ": resetting beacon timeouts due to mon delay "
                "(slow election?) of " << now - last_tick << " seconds" << dendl;
        for (auto &i : last_beacon) {
          i.second = now;
        }
    }

    last_tick = now;
    bool propose = false;

    pending_map.update_active_timers(propose);  // Periodic: check active FSM timers
    _propose_pending |= propose;


    //TODO handle exception of tick overdued in order to avoid false detection of overdued beacons , see MgrMonitor::tick

    const auto cutoff = now - nvmegw_beacon_grace;
    for(auto &itr : last_beacon){// Pass over all the stored beacons
        auto last_beacon_time = itr.second;
        GW_ID_T      gw_id;
        std::string  nqn;
        if(last_beacon_time < cutoff){
            get_gw_and_nqn_from_key(itr.first, gw_id,  nqn);
            dout(4) << "beacon timeout for GW " << gw_id << " nqn " << nqn << dendl;
            pending_map.process_gw_map_gw_down( gw_id, nqn, propose);
            _propose_pending |= propose;
            last_beacon.erase(itr.first);
        }
        else{
           dout(4) << "beacon live for GW key: " << itr.first << dendl;
        }
    }

    pending_map.handle_abandoned_ana_groups(propose); // Periodic: take care of not handled ANA groups
    _propose_pending |= propose;

    if(_propose_pending){
       //pending_map.delay_propose = true; // not to send map to clients immediately in "update_from_paxos"
       dout(4) << "decision to delayed_map" <<dendl;
       propose_pending();
    }

    // if propose_pending returned true , call propose_pending method of the paxosService
    // todo understand the logic of paxos.plugged for sending several propose_pending see MgrMonitor::tick
}


void NVMeofGwMon::create_pending(){

    pending_map = map;// deep copy of the object
    // TODO  since "pending_map"  can be reset  each time during paxos re-election even in the middle of the changes ...
    pending_map.epoch++;
    //map.epoch ++;
    dout(4) <<  MY_MON_PREFFIX << __func__ << " pending epoch " << pending_map.epoch  << dendl;

    dout(5) <<  MY_MON_PREFFIX << "dump_pending"  << dendl;

    pending_map._dump_gwmap(pending_map.Gmap);
}

void NVMeofGwMon::encode_pending(MonitorDBStore::TransactionRef t){

    dout(4) <<  MY_MON_PREFFIX << __func__  << dendl;
    bufferlist bl;
    pending_map.encode(bl);
    put_version(t, pending_map.epoch, bl);
    put_last_committed(t, pending_map.epoch);
}

void NVMeofGwMon::update_from_paxos(bool *need_bootstrap){
    version_t version = get_last_committed();

    //dout(4) <<  MY_MON_PREFFIX << __func__ << " version "  << version  << " map.epoch " << map.epoch << dendl;

    if (version != map.epoch) {
        dout(4) << " NVMeGW loading version " << version  << " " << map.epoch << dendl;

        bufferlist bl;
        int err = get_version(version, bl);
        ceph_assert(err == 0);

        auto p = bl.cbegin();
        map.decode(p);
        if(!mon.is_leader()) {
            std::stringstream  ss;
            map._dump_created_gws(ss);
            dout(4) << ss.str() <<  dendl;
            map._dump_gwmap(map.Gmap);
        }
        check_subs(true);

    }
}

void NVMeofGwMon::check_sub(Subscription *sub)
{
   /* MgrMonitor::check_sub*/
    //if (sub->type == "NVMeofGw") {
    dout(4) << "sub->next , map-epoch " << sub->next << " " << map.get_epoch() << dendl;
    if (sub->next <= map.get_epoch())
    {
      dout(4) << "Sending map to subscriber " << sub->session->con << " " << sub->session->con->get_peer_addr() << dendl;
      sub->session->con->send_message2(make_message<MNVMeofGwMap>(map));

      if (sub->onetime) {
        mon.session_map.remove_sub(sub);
      } else {
        sub->next = map.get_epoch() + 1;
      }
    }
}

void NVMeofGwMon::check_subs(bool t)
{
  const std::string type = "NVMeofGw";
  dout(4) <<  MY_MON_PREFFIX << __func__ << " count " << mon.session_map.subs.count(type) << dendl;

  if (mon.session_map.subs.count(type) == 0){
      return;
  }
  for (auto sub : *(mon.session_map.subs[type])) {
    dout(4) << "sub-type "<< sub->type <<  " delay_propose until next tick" << t << dendl;
    if (t) map.delay_propose = true;
    else  check_sub(sub);
  }
}

bool NVMeofGwMon::preprocess_query(MonOpRequestRef op){
    dout(4) <<  MY_MON_PREFFIX <<__func__  << dendl;

    auto m = op->get_req<PaxosServiceMessage>();
      switch (m->get_type()) {
        case MSG_MNVMEOF_GW_BEACON:
          return preprocess_beacon(op);

        case MSG_MON_COMMAND:
          try {
        return preprocess_command(op);
          } catch (const bad_cmd_get& e) {
          bufferlist bl;
          mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
          return true;
        }

        default:
          mon.no_reply(op);
          derr << "Unhandled message type " << m->get_type() << dendl;
          return true;
      }
    return false;
}

bool NVMeofGwMon::prepare_update(MonOpRequestRef op){
    dout(4) <<  MY_MON_PREFFIX <<__func__  << dendl;
    auto m = op->get_req<PaxosServiceMessage>();
      switch (m->get_type()) {
        case MSG_MNVMEOF_GW_BEACON:
          return prepare_beacon(op);

        case MSG_MON_COMMAND:
          try {
        return prepare_command(op);
          } catch (const bad_cmd_get& e) {
        bufferlist bl;
        mon.reply_command(op, -EINVAL, e.what(), bl, get_last_committed());
        return false; /* nothing to propose! */
          }

        default:
          mon.no_reply(op);
          derr << "Unhandled message type " << m->get_type() << dendl;
          return false; /* nothing to propose! */
      }
    return true;
}

bool NVMeofGwMon::preprocess_command(MonOpRequestRef op)
{
    dout(4) << MY_MON_PREFFIX << __func__ << dendl;
    auto m = op->get_req<MMonCommand>();
    std::stringstream ss;
    bufferlist rdata;

    cmdmap_t cmdmap;
    if (!cmdmap_from_json(m->cmd, &cmdmap, ss))
    {
        string rs = ss.str();
        mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
        return true;
    }
    MonSession *session = op->get_session();
    if (!session)
    {
        mon.reply_command(op, -EACCES, "access denied", rdata,
                          get_last_committed());
        return true;
    }
    string format = cmd_getval_or<string>(cmdmap, "format", "plain");
    boost::scoped_ptr<Formatter> f(Formatter::create(format));

    string prefix;
    cmd_getval(cmdmap, "prefix", prefix);
    dout(4) << "MonCommand : "<< prefix <<  dendl;
    // TODO   need to check formatter per preffix  - if f is NULL

    return false;
}

bool NVMeofGwMon::prepare_command(MonOpRequestRef op)
{
    dout(4) << MY_MON_PREFFIX << __func__ << dendl;
    auto m = op->get_req<MMonCommand>();
    int rc;
    std::stringstream ss;
    bufferlist rdata;
    string rs;
    int err = 0;
    cmdmap_t cmdmap;

    if (!cmdmap_from_json(m->cmd, &cmdmap, ss))
    {
        string rs = ss.str();
        mon.reply_command(op, -EINVAL, rs, rdata, get_last_committed());
        return true;
    }

    MonSession *session = op->get_session();
    if (!session)
    {
        mon.reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
        return true;
    }

    string format = cmd_getval_or<string>(cmdmap, "format", "plain");
    boost::scoped_ptr<Formatter> f(Formatter::create(format));

    const auto prefix = cmd_getval_or<string>(cmdmap, "prefix", string{});

    dout(4) << "MonCommand : "<< prefix <<  dendl;
    bool map_modified = false;
    if( prefix == "nvme-gw create" || prefix == "nvme-gw delete" ) {
        std::string id, pool, group;

        cmd_getval(cmdmap, "id", id);
        cmd_getval(cmdmap, "pool", pool);
        cmd_getval(cmdmap, "group", group);

        if(prefix == "nvme-gw create"){
            rc = pending_map.cfg_add_gw( id );
            ceph_assert(rc!= -EINVAL);
        }
        else{
            rc = pending_map.cfg_delete_gw( id, map_modified);
            ceph_assert(rc!= -EINVAL);
        }
        if(map_modified){
            propose_pending();
            goto update;
        }
        else {
            goto reply_no_propose;
        }
    }

  reply_no_propose:
    getline(ss, rs);
    if (err < 0 && rs.length() == 0)
    {
        rs = cpp_strerror(err);
        dout(4) << "Error command  err : "<< err  << " rs-len: " << rs.length() <<  dendl;
    }
    mon.reply_command(op, err, rs, rdata, get_last_committed());
    return false; /* nothing to propose */

  update:
    getline(ss, rs);
    wait_for_commit(op, new Monitor::C_Command(mon, op, 0, rs,
                            get_last_committed() + 1));
    return true;
}


bool NVMeofGwMon::preprocess_beacon(MonOpRequestRef op){
    dout(4) <<  MY_MON_PREFFIX <<__func__  << dendl;
    auto m = op->get_req<MNVMeofGwBeacon>();
     mon.no_reply(op); // we never reply to beacons
     dout(4) << "beacon from " << m->get_type() << dendl;
     MonSession *session = op->get_session();
     if (!session){
         dout(4) << "beacon no session "  << dendl;
         return true;
     }

    return false; // allways  return false to call leader's prepare beacon
}


#define GW_DELIM ','

void NVMeofGwMon::get_gw_and_nqn_from_key(std::string  key, GW_ID_T &gw_id , std::string& nqn)
{
    std::stringstream s1(key);

    std::getline(s1, gw_id, GW_DELIM);
    std::getline(s1, nqn,   GW_DELIM);
}

//#define BYPASS_GW_CREATE_CLI

bool NVMeofGwMon::prepare_beacon(MonOpRequestRef op){
    dout(4) <<  MY_MON_PREFFIX <<__func__  << dendl;
    GW_STATE_T* gw_state = NULL;
    auto m = op->get_req<MNVMeofGwBeacon>();

     //    dout(4) << "availability " <<  m->get_availability() << " GW : " <<m->get_gw_id() << " subsystems " << m->get_subsystems() <<  " epoch " << m->get_version() << dendl;
    std::stringstream    out;
    m->print(out);
    dout(4) << out.str() <<dendl;

    GW_ID_T gw_id = m->get_gw_id();
    GW_AVAILABILITY_E  avail = m->get_availability();
    const GwSubsystems& subsystems =  m->get_subsystems();
    bool propose = false;
    int ana_grp_id = 0;
    std::vector <std::string> configured_subsystems;

    if (avail == GW_AVAILABILITY_E::GW_CREATED){
        // in this special state GWs receive map with just "created_gws" vector
        if(pending_map.find_created_gw(gw_id, ana_grp_id) == 0) {// GW is created administratively
           dout(4) << "GW " << gw_id << " sent beacon being in state GW_WAIT_INITIAL_MAP" << dendl;
           propose = true;
        }
        else{
           dout(4) << "GW " << gw_id << " sent beacon being in state GW_WAIT_INITIAL_MAP but it is not created yet!!! "<< dendl; 
#ifdef BYPASS_GW_CREATE_CLI
           pending_map.cfg_add_gw(gw_id);
           dout(4) << "GW " << gw_id << " created since mode is bypass-create-cli "<< dendl;
           propose= true;
#endif
        }
        goto set_propose;
    }

    // Validation gw is in the database
    for (const NqnState &st : subsystems)
    {
        gw_state = pending_map.find_gw_map( gw_id, st.nqn );
        if (gw_state == NULL)
        {
            dout(4) <<  "GW + NQN pair is not in the  database: " << gw_id << " " << st.nqn << dendl;
            // if GW is created   
            if(pending_map.find_created_gw(gw_id, ana_grp_id) == 0) {// GW is created administratively
              pending_map.insert_gw_to_map(gw_id, st.nqn, ana_grp_id);
              dout(4) << "GW + NQN pair  " << gw_id << " " << st.nqn << " inserted to map, ANA grp-id " << ana_grp_id << dendl;
            }
            else {
                //drop beacon on the floor silently discard
                return 0;
            }
        }
        configured_subsystems.push_back(st.nqn);
    }
    pending_map.handle_removed_subsystems( configured_subsystems, propose );

    if(avail == GW_AVAILABILITY_E::GW_AVAILABLE)
    {
        auto now = ceph::coarse_mono_clock::now();
        // check pending_map.epoch vs m->get_version() - if different - drop the beacon

        for (const NqnState& st: subsystems) {
            last_beacon[(gw_id + GW_DELIM + st.nqn)] = now;
            pending_map.process_gw_map_ka( gw_id, st.nqn, propose );
        }
    }
    else if(avail == GW_AVAILABILITY_E::GW_UNAVAILABLE){ // state set by GW client application
        //  TODO: remove from last_beacon if found . if gw was found in last_beacon call process_gw_map_gw_down

        for (const NqnState& st: subsystems) {

            auto it = last_beacon.find(gw_id + GW_DELIM + st.nqn);
            if (it != last_beacon.end()){
                last_beacon.erase(gw_id + GW_DELIM + st.nqn);
                pending_map.process_gw_map_gw_down( gw_id, st.nqn, propose );
            }
        }
    }
set_propose:
    if (propose){
      dout(4) << "decision to delayed_map in prepare_beacon" <<dendl;
      return true;
    }
    else 
     return false; // if no changes are need in the map
}
