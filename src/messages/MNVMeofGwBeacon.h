// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_NVMEOFGWBEACON_H
#define CEPH_NVMEOFGWBEACON_H

#include <cstddef>
#include <vector>
#include "messages/PaxosServiceMessage.h"
#include "mon/MonCommand.h"
#include "mon/NVMeofGwMap.h"

#include "include/types.h"
class MNVMeofGwBeacon final : public PaxosServiceMessage {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

protected:
    std::string              gw_id;
    std::string              gw_pool;
    std::string              gw_group;
    GwSubsystems             subsystems;                           // gateway susbsystem and their state machine states
    GW_AVAILABILITY_E        availability;                         // in absence of  beacon  heartbeat messages it becomes inavailable
    uint32_t                 version;

public:
  MNVMeofGwBeacon()
    : PaxosServiceMessage{MSG_MNVMEOF_GW_BEACON, 0, HEAD_VERSION, COMPAT_VERSION}
  {}

  MNVMeofGwBeacon(const std::string &gw_id_,
        const std::string& gw_pool_,
        const std::string& gw_group_,
        const GwSubsystems& subsystems_,
        const GW_AVAILABILITY_E&  availability_,
        const uint32_t& version_
  )
    : PaxosServiceMessage{MSG_MNVMEOF_GW_BEACON, 0, HEAD_VERSION, COMPAT_VERSION},
      gw_id(gw_id_), gw_pool(gw_pool_), gw_group(gw_group_), subsystems(subsystems_),
      availability(availability_), version(version_)
  {}

  const std::string& get_gw_id() const { return gw_id; }
  const std::string& get_gw_pool() const { return gw_pool; }
  const std::string& get_gw_group() const { return gw_group; }
  const GW_AVAILABILITY_E& get_availability() const { return availability; }
  const uint32_t& get_version() const { return version; }
  const GwSubsystems& get_subsystems() const { return subsystems; };

private:
  ~MNVMeofGwBeacon() final {}

public:

  std::string_view get_type_name() const override { return "nvmeofgwbeacon"; }

  void encode_payload(uint64_t features) override {
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    using ceph::encode;
    paxos_encode();
    encode(gw_id, payload);
    encode(gw_pool, payload);
    encode(gw_group, payload);
    encode((int)subsystems.size(), payload);
    for (const NqnState& st: subsystems) {
      encode(st.nqn, payload);
      for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++)
        encode((int)st.sm_state[i], payload);
      encode(st.opt_ana_gid, payload);
    }
    encode((int)availability, payload);
    encode(version, payload); 
  }

  void decode_payload() override {
    using ceph::decode;
    auto p = payload.cbegin();
    
    paxos_decode(p);
    decode(gw_id, p);
    decode(gw_pool, p);
    decode(gw_group, p);
    int n;
    int tmp;
    decode(n, p);
    // Reserve memory for the vector to avoid reallocations
    subsystems.clear();
    subsystems.reserve(n);
    for (int i = 0; i < n; i++) {
      std::string nqn;
      decode(nqn, p);
      NqnState st(nqn);
      for (int j = 0; j < MAX_SUPPORTED_ANA_GROUPS; j++) {
        decode(tmp, p);
        st.sm_state[j] = static_cast<GW_STATES_PER_AGROUP_E>(tmp);
      }
      decode(st.opt_ana_gid, p);
      subsystems.push_back(st);
    }
    decode(tmp, p);
    availability = static_cast<GW_AVAILABILITY_E>(tmp);
    decode(version, p);  
  }

private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};


#endif
