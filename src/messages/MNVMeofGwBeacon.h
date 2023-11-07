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

typedef GW_STATES_PER_AGROUP_E SM_STATE[MAX_SUPPORTED_ANA_GROUPS];
struct NqnState {
  std::string nqn;          // subsystem NQN
  SM_STATE    sm_state;     // susbsystem's state machine state
  uint16_t    opt_ana_gid;  // optimized ANA group index
};

typedef std::vector<NqnState> GwSubsystems;

std::ostream& operator<<(std::ostream& os, const SM_STATE value) {
    os << "SM_STATE [ ";
    for (int i = 0; i < MAX_SUPPORTED_ANA_GROUPS; i++) {
      switch (value[i]) {
          case GW_STATES_PER_AGROUP_E::GW_IDLE_STATE: os << "IDLE "; break;
          case GW_STATES_PER_AGROUP_E::GW_STANDBY_STATE: os << "STANDBY "; break;
          case GW_STATES_PER_AGROUP_E::GW_ACTIVE_STATE: os << "ACTIVE "; break;
          case GW_STATES_PER_AGROUP_E::GW_BLOCKED_AGROUP_OWNER: os << "BLOCKED_AGROUP_OWNER "; break;
          case GW_STATES_PER_AGROUP_E::GW_WAIT_FAILBACK_PREPARED: os << "WAIT_FAILBACK_PREPARED "; break;
          default: os << "Invalid " << (int)value[i] << " ";
      }
    }
    os << "]";
    return os;
}

std::ostream& operator<<(std::ostream& os, const NqnState value) {
    os << "Subsystem( nqn: " << value.nqn << ", ANAGrpId: " << value.opt_ana_gid << ", " << value.sm_state << " )";
    return os;
}

std::ostream& operator<<(std::ostream& os, const GW_AVAILABILITY_E value) {
  switch (value) {

        case GW_AVAILABILITY_E::GW_CREATED: os << "CREATED"; break;
        case GW_AVAILABILITY_E::GW_AVAILABLE: os << "AVAILABLE"; break;
        case GW_AVAILABILITY_E::GW_UNAVAILABLE: os << "UNAVAILABLE"; break;

        default: os << "Invalid " << (int)value << " ";
    }
    return os;
}

class MNVMeofGwBeacon final : public PaxosServiceMessage {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;

protected:
    std::string              gw_id;
    GwSubsystems             subsystems;                           // gateway susbsystem and their state machine states
    GW_AVAILABILITY_E        availability;                         // in absence of  beacon  heartbeat messages it becomes inavailable
    uint32_t                 version;

public:
  MNVMeofGwBeacon()
    : PaxosServiceMessage{MSG_MNVMEOF_GW_BEACON, 0, HEAD_VERSION, COMPAT_VERSION}
  {}

  MNVMeofGwBeacon(const std::string &gw_id_, 
        const GwSubsystems& subsystems_,
        const GW_AVAILABILITY_E&  availability_,
        const uint32_t& version_
  )
    : PaxosServiceMessage{MSG_MNVMEOF_GW_BEACON, 0, HEAD_VERSION, COMPAT_VERSION},
      gw_id(gw_id_), subsystems(subsystems_),
      availability(availability_), version(version_)
  {}

  const std::string& get_gw_id() const { return gw_id; }
  const GW_AVAILABILITY_E& get_availability() const { return availability; }
  const uint32_t& get_version() const { return version; }
  const GwSubsystems& get_subsystems() const { return subsystems; };

private:
  ~MNVMeofGwBeacon() final {}

public:

  std::string_view get_type_name() const override { return "nvmeofgwbeacon"; }

  void print(std::ostream& out) const override {
    out << get_type_name() << " nvmeofgw " << "(" << gw_id <<  ", susbsystems: [ ";
    for (const NqnState& st: subsystems) {
      out << st << " ";
    }
    out << "], " << "availability: " << availability << ", version:" << version;
  }

  void encode_payload(uint64_t features) override {
    header.version = HEAD_VERSION;
    header.compat_version = COMPAT_VERSION;
    using ceph::encode;
    paxos_encode();
    encode(gw_id, payload);
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
    int n;
    int tmp;
    decode(n, p);
    // Reserve memory for the vector to avoid reallocations
    subsystems.clear();
    //subsystems.reserve(n);
    for (int i = 0; i < n; i++) {
      NqnState st;
      decode(st.nqn, p);
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
