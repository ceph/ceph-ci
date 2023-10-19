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


#ifndef NVMEOFGW_H_
#define NVMEOFGW_H_

#include "auth/Auth.h"
#include "common/async/context_pool.h"
#include "common/Finisher.h"
#include "common/Timer.h"
#include "common/LogClient.h"

#include "client/Client.h"
#include "mon/MonClient.h"
#include "osdc/Objecter.h"
#include "messages/MNVMeofGwMap.h"

class NVMeofGw : public Dispatcher,
		   public md_config_obs_t {
protected:
  ceph::async::io_context_pool poolctx;
  MonClient monc;
  std::unique_ptr<Messenger> client_messenger;
  Objecter objecter;
  Client client;

  ceph::mutex lock = ceph::make_mutex("NVMeofGw::lock");
  Finisher finisher;
  SafeTimer timer;

  int orig_argc;
  const char **orig_argv;

  void send_beacon();

public:
  NVMeofGw(int argc, const char **argv);
  ~NVMeofGw() override;

  // Dispatcher interface
  bool ms_dispatch2(const ceph::ref_t<Message>& m) override;
  bool ms_handle_reset(Connection *con) override { return false; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; };

  // config observer bits
  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
			  const std::set <std::string> &changed) override {};

  int init();
  void shutdown();
  int main(std::vector<const char *> args);
  void tick();

  void handle_nvmeof_gw_map(ceph::ref_t<MNVMeofGwMap> m);
};

#endif

