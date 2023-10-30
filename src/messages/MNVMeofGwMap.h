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

#ifndef CEPH_MNVMEOFGWMAP_H
#define CEPH_MNVMEOFGWMAP_H

#include "msg/Message.h"
#include "mon/NVMeofGwMap.h"

class MNVMeofGwMap final : public Message {
protected:
  NVMeofGwMap map;

public:
  const NVMeofGwMap& get_map() {return map;}

private:
  MNVMeofGwMap() :
    Message{MSG_MNVMEOF_GW_MAP} {}
  MNVMeofGwMap(const NVMeofGwMap &map_) :
    Message{MSG_MNVMEOF_GW_MAP}, map(map_)
  {}
  ~MNVMeofGwMap() final {}

public:
  std::string_view get_type_name() const override { return "nvmeofgwmap"; }
  void print(std::ostream& out) const override {
    // ../src/messages/MNVMeofGwMap.h:40:39: error: no match for ‘operator<<’ (operand types are ‘std::basic_ostream<char>’ and ‘const NVMeofGwMap’)
    out << get_type_name() << "(map " << "should be map instance here" << ")";
  }

  void decode_payload() override {
    // ../src/messages/MNVMeofGwMap.h:46:11: error: no matching function for call to ‘decode(NVMeofGwMap&, ceph::buffer::v15_2_0::list::iterator_impl<true>&)’
    auto p = payload.cbegin();
    map.decode( p);
  }
  void encode_payload(uint64_t features) override {
    //../src/messages/MNVMeofGwMap.h:51:11: error: no matching function for call to ‘encode(NVMeofGwMap&, ceph::buffer::v15_2_0::list&, uint64_t&)’
    //using ceph::encode;
    //encode(map, payload, features);
    map.encode(payload);
  }
private:
  using RefCountedObject::put;
  using RefCountedObject::get;
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
  template<class T, typename... Args>
  friend MURef<T> crimson::make_message(Args&&... args);
};

#endif
