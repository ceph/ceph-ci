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

#ifndef ECTRANSACTION_H
#define ECTRANSACTION_H

// Set to 1 to turn on parity delta writes
// Set to 0 to always use conventional writes instead
#define PARTIY_DELTA_WRITES 1

#include <ostream>

#include "ECUtil.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "os/Transaction.h"
#include "PGTransaction.h"

namespace ECTransaction {
  class WritePlanObj
  {
  public:

    const hobject_t hoid;
    std::optional<ECUtil::shard_extent_set_t> to_read;
    ECUtil::shard_extent_set_t will_write;
    const ECUtil::HashInfoRef hinfo;
    const ECUtil::HashInfoRef shinfo;
    const shard_id_set available_shards;
    const shard_id_set backfill_shards;
    const bool object_in_cache;
    const uint64_t orig_size;
    uint64_t projected_size;
    bool invalidates_cache;
    bool do_parity_delta_write;

    WritePlanObj(
      const hobject_t &hoid,
      const PGTransaction::ObjectOperation &op,
      const ECUtil::stripe_info_t &sinfo,
      const shard_id_set available_shards,
      const shard_id_set backfill_shards,
      const bool object_in_cache,
      uint64_t orig_size,
      const std::optional<object_info_t> &oi,
      const std::optional<object_info_t> &soi,
      const ECUtil::HashInfoRef &&hinfo,
      const ECUtil::HashInfoRef &&shinfo);

    friend std::ostream& operator<<(std::ostream& lhs, const WritePlanObj& rhs);
  };

  struct WritePlan {
    bool want_read;
    std::list<WritePlanObj> plans;
  };
  std::ostream& operator<<(std::ostream& lhs, const ECTransaction::WritePlan& rhs);
  std::ostream& operator<<(std::ostream& lhs, const WritePlanObj& rhs);

  void generate_transactions(
    PGTransaction* _t,
    WritePlan &plan,
    ceph::ErasureCodeInterfaceRef &ec_impl,
    pg_t pgid,
    const ECUtil::stripe_info_t &sinfo,
    const std::map<hobject_t, ECUtil::shard_extent_map_t> &partial_extents,
    std::vector<pg_log_entry_t> &entries,
    std::map<hobject_t, ECUtil::shard_extent_map_t>* written_map,
    shard_id_map<ceph::os::Transaction> *transactions,
    std::set<hobject_t> *temp_added,
    std::set<hobject_t> *temp_removed,
    DoutPrefixProvider *dpp,
    const OSDMapRef& osdmap);
};

#endif
