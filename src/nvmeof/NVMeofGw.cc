// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <boost/algorithm/string/replace.hpp>

#include "common/errno.h"
#include "common/signal.h"
#include "common/ceph_argparse.h"
#include "include/compat.h"

#include "include/stringify.h"
#include "global/global_context.h"
#include "global/signal_handler.h"


#include "messages/MNVMeofGwBeacon.h"
#include "messages/MNVMeofGwMap.h"
#include "NVMeofGw.h"
#include "NVMeofGwClient.h"
#include "NVMeofGwMonitorGroupClient.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "nvmeofgw " << __PRETTY_FUNCTION__ << " "

using std::map;
using std::string;
using std::stringstream;
using std::vector;

NVMeofGw::NVMeofGw(int argc, const char **argv) :
  Dispatcher(g_ceph_context),
  monc{g_ceph_context, poolctx},
  client_messenger(Messenger::create(g_ceph_context, "async", entity_name_t::CLIENT(-1), "client", getpid())),
  objecter{g_ceph_context, client_messenger.get(), &monc, poolctx},
  client{client_messenger.get(), &monc, &objecter},
  finisher(g_ceph_context, "Nvmeof", "nvme-fin"),
  timer(g_ceph_context, lock),
  orig_argc(argc),
  orig_argv(argv)
{
}

NVMeofGw::~NVMeofGw() = default;

const char** NVMeofGw::get_tracked_conf_keys() const
{
  static const char* KEYS[] = {
    NULL
  };
  return KEYS;
}

int NVMeofGw::init()
{
  dout(0) << dendl;
  std::string val;
  auto args = argv_to_vec(orig_argc, orig_argv);

  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--gateway-name", (char*)NULL)) {
      name = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--gateway-address", (char*)NULL)) {
      gateway_address = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--monitor-address", (char*)NULL)) {
      monitor_address = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--server-key", (char*)NULL)) {
      server_key = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--server-cert", (char*)NULL)) {
      server_cert = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--client-cert", (char*)NULL)) {
      client_cert = val;
    } else {
      ++i;
    }
  }

  dout(0) << "gateway name: " << name <<  " address: " << gateway_address << dendl;
  ceph_assert(name != "" && gateway_address != "" && monitor_address != "");

  // todo
  ceph_assert(server_key == "" && server_cert == "" && client_cert == "");

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);

  std::lock_guard l(lock);

  // Start finisher
  finisher.start();

  // Initialize Messenger
  client_messenger->add_dispatcher_tail(this);
  client_messenger->add_dispatcher_head(&objecter);
  client_messenger->add_dispatcher_tail(&client);
  client_messenger->start();

  poolctx.start(2);

  // Initialize MonClient
  if (monc.build_initial_monmap() < 0) {
    client_messenger->shutdown();
    client_messenger->wait();
    return -1;
  }

  monc.sub_want("NVMeofGw", 0, 0);

  monc.set_want_keys(CEPH_ENTITY_TYPE_MON|CEPH_ENTITY_TYPE_OSD
      |CEPH_ENTITY_TYPE_MDS|CEPH_ENTITY_TYPE_MGR);
  monc.set_messenger(client_messenger.get());

  // We must register our config callback before calling init(), so
  // that we see the initial configuration message
  monc.register_config_callback([this](const std::string &k, const std::string &v){
      // removing value to hide sensitive data going into mgr logs
      // leaving this for debugging purposes
      dout(10) << "nvmeof config_callback: " << k << " : " << v << dendl;
      
      return false;
    });
  monc.register_config_notify_callback([this]() {
      dout(4) << "nvmeof monc config notify callback" << dendl;
    });
  dout(4) << "nvmeof Registered monc callback" << dendl;

  int r = monc.init();
  if (r < 0) {
    monc.shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }
  dout(0) << "nvmeof Registered monc callback" << dendl;

  r = monc.authenticate();
  if (r < 0) {
    derr << "Authentication failed, did you specify an ID with a valid keyring?" << dendl;
    monc.shutdown();
    client_messenger->shutdown();
    client_messenger->wait();
    return r;
  }
  dout(0) << "monc.authentication done" << dendl;
  // only forward monmap updates after authentication finishes, otherwise
  // monc.authenticate() will be waiting for MgrStandy::ms_dispatch()
  // to acquire the lock forever, as it is already locked in the beginning of
  // this method.
  monc.set_passthrough_monmap();

  client_t whoami = monc.get_global_id();
  client_messenger->set_myname(entity_name_t::MGR(whoami.v));
  objecter.set_client_incarnation(0);
  objecter.init();
  objecter.start();
  client.init();
  timer.init();

  tick();

  dout(0) << "Complete." << dendl;
  return 0;
}

void NVMeofGw::send_beacon()
{
  ceph_assert(ceph_mutex_is_locked_by_me(lock));
  //dout(0) << "sending beacon as gid " << monc.get_global_id() << dendl;
  GW_AVAILABILITY_E gw_availability = GW_AVAILABILITY_E::GW_CREATED;
  GwSubsystems subs;
  if (map.epoch > 0) { // handled map already
    NVMeofGwClient gw_client(
       grpc::CreateChannel(gateway_address, grpc::InsecureChannelCredentials()));
    subsystems_info gw_subsystems;
    bool ok = gw_client.get_subsystems(gw_subsystems);
    if (ok) {
      for (int i = 0; i < gw_subsystems.subsystems_size(); i++) {
        const subsystem& sub = gw_subsystems.subsystems(i);
        struct NqnState nqn_state(sub.nqn());
        GW_STATE_T* gw_state = map.find_gw_map(name, nqn_state.nqn);
        if (gw_state) {
          nqn_state.opt_ana_gid = gw_state->optimized_ana_group_id;
          for (int i=0; i < MAX_SUPPORTED_ANA_GROUPS; i++)
            nqn_state.sm_state[i] = gw_state->sm_state[i];
        }
        subs.push_back(nqn_state);
      }
    }
    gw_availability = ok ? GW_AVAILABILITY_E::GW_AVAILABLE : GW_AVAILABILITY_E::GW_UNAVAILABLE;
  }
  dout(0) << "sending beacon as gid " << monc.get_global_id() << " availability " << (int)gw_availability << dendl;
  auto m = ceph::make_message<MNVMeofGwBeacon>(
      name,
      subs,
      gw_availability,
      map.epoch);
  monc.send_mon_message(std::move(m));
}

void NVMeofGw::tick()
{
  dout(0) << dendl;
  send_beacon();

  timer.add_event_after(
      g_conf().get_val<std::chrono::seconds>("mgr_tick_period").count(),
      new LambdaContext([this](int r){
          tick();
      }
  ));
}

void NVMeofGw::shutdown()
{
  finisher.queue(new LambdaContext([&](int) {
    std::lock_guard l(lock);

    dout(4) << "nvmeof Shutting down" << dendl;


    // stop sending beacon first, I use monc to talk with monitors
    timer.shutdown();
    // client uses monc and objecter
    client.shutdown();
    // Stop asio threads, so leftover events won't call into shut down
    // monclient/objecter.
    poolctx.finish();
    // stop monc, so mon won't be able to instruct me to shutdown/activate after
    // the active_mgr is stopped
    monc.shutdown();

    // objecter is used by monc and active_mgr
    objecter.shutdown();
    // client_messenger is used by all of them, so stop it in the end
    client_messenger->shutdown();
  }));

  // Then stop the finisher to ensure its enqueued contexts aren't going
  // to touch references to the things we're about to tear down
  finisher.wait_for_empty();
  finisher.stop();
}

void NVMeofGw::handle_nvmeof_gw_map(ceph::ref_t<MNVMeofGwMap> mmap)
{
  dout(0) << "handle nvmeof gw map" << dendl;
  auto &mp = mmap->get_map();
  dout(0) << "received map epoch " << mp.get_epoch() << dendl;
  std::stringstream  ss;
  mp._dump_gwmap(ss);
  dout(0) << ss.str() <<  dendl;
  ana_info ai;
  if (map.epoch == 0){ // initial map
    int ana_grp_id = -1;
    if(mp.find_created_gw(name ,ana_grp_id) !=0 )
    {
      dout(0) << "Failed to find created gw for " << name << dendl;
      return;
    }
    std::stringstream  ss1;
    mp._dump_created_gws(ss1);
    dout(0) << ss1.str() <<  dendl;
    bool set_group_id = false;
    while (!set_group_id) {
      NVMeofGwMonitorGroupClient monitor_group_client(
          grpc::CreateChannel(monitor_address, grpc::InsecureChannelCredentials()));
      dout(0) << "GRPC set_group_id: " <<  ana_grp_id << dendl;
      set_group_id = monitor_group_client.set_group_id( ana_grp_id);
      if (!set_group_id) {
	      dout(0) << "GRPC set_group_id failed" << dendl;
	      usleep(1000); // TODO: conf options
      }
    }
  }

  // Interate over NQNs
  for (const auto& subsystemPair : mp.Gmap) {
    const std::string& nqn = subsystemPair.first;
    const auto& idStateMap = subsystemPair.second;
    nqn_ana_states nas;
    nas.set_nqn(nqn);

    // This gateway state for the current subsystem / nqn
    const auto& new_gateway_state = idStateMap.find(name);

    // There is no subsystem update for this gateway
    if (new_gateway_state == idStateMap.end()) continue;

    // Previously monitor distributed state
    GW_STATE_T* old_gw_state = map.find_gw_map(name, nqn);

    // Iterate over possible ANA Groups
    for (uint32_t  ana_grp_index = 0; ana_grp_index < MAX_SUPPORTED_ANA_GROUPS; ana_grp_index++) {
      ana_group_state gs;
      gs.set_grp_id(ana_grp_index + 1); // offset by 1, index 0 is ANAGRP1

      // There is no state change for this ANA Group
      auto old_state = old_gw_state ? old_gw_state->sm_state[ana_grp_index] : GW_STATES_PER_AGROUP_E::GW_IDLE_STATE;
      if (old_state == new_gateway_state->second.sm_state[ana_grp_index]) continue;

      // detect was active, but not any more transition
      if ((old_state == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE || old_state == GW_STATES_PER_AGROUP_E::GW_IDLE_STATE ) &&
          new_gateway_state->second.sm_state[ana_grp_index] != GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE) {
        gs.set_state(INACCESSIBLE); // Set the ANA state
        nas.mutable_states()->Add(std::move(gs));
        dout(0) << "nqn: " <<  nqn << " grpid " << (ana_grp_index + 1) << " INACCESSIBLE" <<dendl;
      // detect was not active, but becaome one transition
      } else if (old_state != GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE &&
          new_gateway_state->second.sm_state[ana_grp_index] == GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE) {
        gs.set_state(OPTIMIZED); // Set the ANA state
        nas.mutable_states()->Add(std::move(gs));
        dout(0) << "nqn: " <<  nqn << " grpid " << (ana_grp_index + 1) << " OPTIMIZED" <<dendl;
      } else continue; // Avoid dealing with intermediate states.
    }
    if (nas.states_size()) ai.mutable_states()->Add(std::move(nas));
  }
  if (ai.states_size()) {
    bool set_ana_state = false;
    while (!set_ana_state) {
      NVMeofGwClient gw_client(
          grpc::CreateChannel(gateway_address, grpc::InsecureChannelCredentials()));
      set_ana_state = gw_client.set_ana_state(ai);
      if (!set_ana_state) {
	dout(0) << "GRPC set_ana_state failed" << dendl;
	usleep(1000); // TODO conf option
      }
    }
  }
  map = mp;
}

bool NVMeofGw::ms_dispatch2(const ref_t<Message>& m)
{
  std::lock_guard l(lock);
  dout(0) << "got map type " << m->get_type() << dendl;

  if (m->get_type() == MSG_MNVMEOF_GW_MAP) {
    handle_nvmeof_gw_map(ref_cast<MNVMeofGwMap>(m));
  }
  bool handled = false;
  return handled;
}

int NVMeofGw::main(vector<const char *> args)
{
  client_messenger->wait();

  // Disable signal handlers
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  shutdown_async_signal_handler();

  return 0;
}
