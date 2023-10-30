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
#include "include/compat.h"

#include "include/stringify.h"
#include "global/global_context.h"
#include "global/signal_handler.h"


#include "messages/MNVMeofGwBeacon.h"
#include "messages/MNVMeofGwMap.h"
#include "NVMeofGw.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "nvmeofgw " << __PRETTY_FUNCTION__ << " "

using std::map;
using std::string;
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
  dout(0) << "sending beacon as gid " << monc.get_global_id() << dendl;

  auto m = ceph::make_message<MNVMeofGwBeacon>();

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
 // NVMeofGwMap
    auto &map = mmap->get_map();
    dout(0) << "received map epoch " << map.get_epoch() << dendl;
   // map._dump_gwmap(map.Gmap);


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
