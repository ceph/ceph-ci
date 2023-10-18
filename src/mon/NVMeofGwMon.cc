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
#define dout_prefix _prefix(_dout, mon, this)
using namespace TOPNSPC::common;

static ostream& _prefix(std::ostream *_dout, const Monitor &mon,
                        const NVMeofGwMon *hmon) {
  return *_dout << "mon." << mon.name << "@" << mon.rank;
}



  void NVMeofGwMon::init(){
	  dout(4) << __func__  <<  "called " << dendl;
  }

  void NVMeofGwMon::on_shutdown() {

  }
static int cnt ;
  void NVMeofGwMon::tick(){

	if (!is_active() || !mon.is_leader()){
		dout(4) << __func__  <<  " NVMeofGwMon leader : " << mon.is_leader() << "active : " << is_active()  << dendl;
		 if(mon.is_leader() && ++cnt  == 4){
			 Gmap.cfg_add_gw(1, "nqn2008.node1", 1);
			 Gmap.cfg_add_gw(2, "nqn2008.node1", 2);
			 Gmap.cfg_add_gw(3, "nqn2008.node1", 3);
			 Gmap.cfg_add_gw(1, "nqn2008.node2", 2);
			// map._dump_gws(map.Gmap);
		 }

		return;
	}

	const auto now = ceph::coarse_mono_clock::now();
	dout(4) << __func__  <<  "NVMeofGwMon leader got a real tick " << dendl;
	last_tick = now;
  }

  void NVMeofGwMon::update_from_paxos(bool *need_bootstrap){
	  dout(4) << __func__  << dendl;
  }


  void NVMeofGwMon::encode_pending(MonitorDBStore::TransactionRef t){
	  dout(4) << __func__  << dendl;
  }



  bool NVMeofGwMon::preprocess_query(MonOpRequestRef op){
	dout(4) << __func__  << dendl;
	return true;
  }

  bool NVMeofGwMon::prepare_update(MonOpRequestRef op){
	  dout(4) << __func__  << dendl;
	  return true;
  }

  bool NVMeofGwMon::preprocess_command(MonOpRequestRef op){
	  dout(4) << __func__  << dendl;
	  return true;
  }

  bool NVMeofGwMon::prepare_command(MonOpRequestRef op){
	  dout(4) << __func__  << dendl;
	  return true;
  }


  bool NVMeofGwMon::preprocess_beacon(MonOpRequestRef op){
	  dout(4) << __func__  << dendl;
	  return true;
  }

  bool NVMeofGwMon::prepare_beacon(MonOpRequestRef op){
	  dout(4) << __func__  << dendl;
	  return true;
  }
