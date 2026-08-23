// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <map>
#include <set>
#include <string_view>

#include "include/encoding.h"
#include "include/utime.h"
#include "osd/osd_types.h"

/**
 * Mgr-owned PG rebuild history and in-progress latch.
 *
 * Completed count/sum is persisted; the in-progress latch is RAM-only and
 * is seeded from the OSD latch only when this mgr's latch is empty (failover
 * / restart). Once set, the mgr latch is authoritative.
 */
class PgRebuildStats {
public:
  static constexpr std::string_view STORE_KEY = "mgr/pg_rebuild/stats";

  struct Entry {
    utime_t start_time;
    int64_t base_recovered = 0;
    bool had_redundancy_loss = false;
    uint64_t count = 0;
    utime_t duration_sum;
  };

  void update(pg_t pgid, const pg_stat_t& st,
              const pg_rebuild_latch_t* osd_latch);

  /// Drop PGs whose pool is gone, or that were removed from the PG map.
  void prune_absent_pools(const std::map<int64_t, unsigned>& existing_pools);
  template<typename Set>
  void prune_removed(const Set& removed)
  {
    for (auto pgid : removed) {
      auto it = entries.find(pgid);
      if (it == entries.end()) {
        continue;
      }
      if (it->second.count > 0) {
        dirty = true;
      }
      entries.erase(it);
    }
  }

  pg_rebuild_dump_overlay dump_overlay() const;

  bool is_dirty() const { return dirty; }

  /// Encode completed count/sum only. Returns false if nothing to write.
  bool consume_dirty(ceph::buffer::list *bl);

  void encode_completed(ceph::buffer::list& bl) const;
  void decode_completed(ceph::buffer::list::const_iterator& p);

  const std::map<pg_t, Entry>& get_entries() const { return entries; }

private:
  std::map<pg_t, Entry> entries;
  bool dirty = false;
};
