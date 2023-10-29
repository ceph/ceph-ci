/*
 * NVMeGWMonitor.cc
 *
 *  Created on: Oct 17, 2023
 *      Author:
 */


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
    bool propose = false;
    if( ++cnt  == 4  ){// simulation that new configuration was added
        pending_map.cfg_add_gw("gw1", "nqn2008.node1", 1);
        pending_map.cfg_add_gw("gw2", "nqn2008.node1", 2);
        pending_map.cfg_add_gw("gw3", "nqn2008.node1", 3);
        pending_map.cfg_add_gw("gw1", "nqn2008.node2", 2);
        pending_map._dump_gwmap(pending_map.Gmap);
        pending_map._dump_active_timers();
        pending_map.debug_encode_decode();
        dout(4) << "Dump map after decode encode:" <<dendl;
        pending_map._dump_gwmap(pending_map.Gmap);
    }
    else if( cnt  == start_cnt  ){  // simulate - add another GW - to check that new map would be synchronized with peons
        pending_map.cfg_add_gw("gw2", "nqn2008.node2", 3);
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
        pending_map._dump_active_timers();
    }


    else if( cnt  == start_cnt+5 ){  // simulate - gw1 is back
        pending_map._dump_active_timers();
        pending_map.process_gw_map_ka( "gw1", "nqn2008.node1", propose);
        if(propose)
            propose_pending();
    }

    else if( cnt  == start_cnt+6 ){  // simulate - gw1 is down
            pending_map._dump_active_timers();
            pending_map.process_gw_map_gw_down( "gw1", "nqn2008.node1", propose);
            if(propose)
                propose_pending();
    }

  /*  else if( cnt  == start_cnt+7 ){  // simulate - gw2 still OK
        pending_map.process_gw_map_ka( "gw2", "nqn2008.node1", propose);
        if(propose)
            propose_pending();
    }*/
    else if( cnt  == start_cnt+8 ){  // simulate - gw2 still OK - checks the persistency timer in the state
        pending_map.process_gw_map_ka( "gw2", "nqn2008.node1", propose);
        if(propose)
            propose_pending();
    }


}

void NVMeofGwMon::tick(){
    static int cnt=0;
    if (!is_active() || !mon.is_leader()){
        dout(4) << __func__  <<  " NVMeofGwMon leader : " << mon.is_leader() << "active : " << is_active()  << dendl;
        return;
    }

    inject1();
    const auto now = ceph::coarse_mono_clock::now();
    dout(4) << MY_MON_PREFFIX << __func__  <<  "NVMeofGwMon leader got a real tick, pending epoch "<< pending_map.epoch  << dendl;
    last_tick = now;

    pending_map.update_active_timers( );
    bool propose = false;
    if((cnt++ %2) == 0) {
        pending_map.handle_homeless_ana_groups(propose);
        if(propose){
           propose_pending();
        }
    }
    //TODO pass over the last_beacon map to detect the overdue beacons indicating the GW died
    //if found the one - convert the last_beacon key to  gw_id and nqn and call the function pending_map_process_gw_map_gw_down
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

    dout(4) <<  MY_MON_PREFFIX << __func__ << " version "  << version  << " map.epoch " << map.epoch << dendl;

    if (version != map.epoch) {
        dout(4) << " NVMeGW loading version " << version  << " " << map.epoch << dendl;

        bufferlist bl;
        int err = get_version(version, bl);
        ceph_assert(err == 0);

        auto p = bl.cbegin();
        map.decode(p);
        map._dump_gwmap(map.Gmap);
        check_subs();
    }
}


void NVMeofGwMon::check_sub(Subscription *sub)
{
   /* MgrMonitor::check_sub
    if (sub->type == "mgrmap") {
    if (sub->next <= map.get_epoch()) {
      dout(20) << "Sending map to subscriber " << sub->session->con
           << " " << sub->session->con->get_peer_addr() << dendl;
      sub->session->con->send_message2(make_message<MMgrMap>(map));
      if (sub->onetime) {
        mon.session_map.remove_sub(sub);
      } else {
        sub->next = map.get_epoch() + 1;
      }
    }
  }
    */
}


void NVMeofGwMon::check_subs()
{
  const std::string type = "nvmegwmap";//"mgrmap";
  dout(4) <<  MY_MON_PREFFIX << __func__ << " count " << mon.session_map.subs.count(type) << dendl;

  if (mon.session_map.subs.count(type) == 0)
    return;
  for (auto sub : *(mon.session_map.subs[type])) {
    check_sub(sub);
  }
}



bool NVMeofGwMon::preprocess_query(MonOpRequestRef op){
    dout(4) <<  MY_MON_PREFFIX <<__func__  << dendl;
    return false;
}

bool NVMeofGwMon::prepare_update(MonOpRequestRef op){
    dout(4) <<  MY_MON_PREFFIX <<__func__  << dendl;
    return true;
}

bool NVMeofGwMon::preprocess_command(MonOpRequestRef op){
    dout(4) <<  MY_MON_PREFFIX <<__func__  << dendl;
    return false;
}

bool NVMeofGwMon::prepare_command(MonOpRequestRef op){
    dout(4) <<  MY_MON_PREFFIX <<__func__  << dendl;
    return true;
}


bool NVMeofGwMon::preprocess_beacon(MonOpRequestRef op){
    dout(4) <<  MY_MON_PREFFIX <<__func__  << dendl;
    return false; // allways  return false to call leader's prepare beacon
}

bool NVMeofGwMon::prepare_beacon(MonOpRequestRef op){
    dout(4) <<  MY_MON_PREFFIX <<__func__  << dendl;
    //auto m = op->get_req<MMgrBeacon>();
    //last_beacon[m->get_gid()] = ceph::coarse_mono_clock::now();
    return true;
}
