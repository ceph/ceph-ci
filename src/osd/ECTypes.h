// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "include/types.h"
#include "common/bitset_set.h"
#include "common/mini_flat_map.h"

struct ec_align_t {
  uint64_t offset;
  uint64_t size;
  uint32_t flags;
  friend std::ostream &operator<<(std::ostream &lhs, const ec_align_t &rhs) {
    return lhs << rhs.offset << ","
               << rhs.size << ","
               << rhs.flags;
  }
  ec_align_t(std::pair<uint64_t, uint64_t> p, uint32_t flags)
    : offset(p.first), size(p.second), flags(flags) {}
  ec_align_t(uint64_t offset, uint64_t size, uint32_t flags)
    : offset(offset), size(size), flags(flags) {}
  bool operator==(const ec_align_t &other) const;
};

using shard_id_set = bitset_set<128, shard_id_t>;

template <typename T>
using shard_id_map = mini_flat_map<shard_id_t, T>;
