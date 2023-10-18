
#include <boost/tokenizer.hpp>
#include "include/stringify.h"
#include "NVMeofGwMon.h"

using std::map;
using std::make_pair;
using std::ostream;
using std::ostringstream;

/*
#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, this)
using namespace TOPNSPC::common;

static ostream& _prefix(std::ostream *_dout, const Monitor &mon,
                        const NVMeofGwMap *hmon) {
  return *_dout << "mon." << mon.name << "@" << mon.rank;
}
*/


int NVMeofGwMap::_dump_gws(  GWMAP & Gmap)const  {

   	for (auto& itr : Gmap) {
   		for (auto& ptr : itr.second) {
   			std::cout << "NQN " << itr.first
   				<< " GW_ID " << ptr.first << " ANA gr " << std::setw(5) << (int)ptr.second.optimized_ana_group_id << " available " << (int)ptr.second.availability << " States: ";
   			for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
   				std::cout << ptr.second.sm_state[i] << " ";
   			}
   			std::cout << endl;
   		}
   	}
   	return 0;
   }
