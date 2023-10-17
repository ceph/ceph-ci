/*
 * NVMeGWMonitor.cc
 *
 *  Created on: Oct 17, 2023
 *      Author:
 */



#include "common/TextTable.h"
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

  void NVMeofGwMon::tick(){

	if (!is_active() || !mon.is_leader())
		return;

	const auto now = ceph::coarse_mono_clock::now();
	dout(4) << __func__  <<  "NVMeofGwMon leader got a tick " << dendl;
	last_tick = now;
  }

  void NVMeofGwMon::update_from_paxos(bool *need_bootstrap){
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
