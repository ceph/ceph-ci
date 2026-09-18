// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Temporary memory-leak instrumentation: live-object counters.
 *
 * Embed a CEPH_LIVE_COUNT(ID) member in a class and every construction
 * (default, copy or move) increments counters[ID].live and .total, and every
 * destruction decrements counters[ID].live.  The member is empty and marked
 * [[no_unique_address]] so it does not change the size of the class.
 *
 * The counters are inline variables so this header is self-contained: no
 * source file needs to be added to any library.  With default symbol
 * visibility the dynamic linker unifies the definitions across shared
 * objects, so libceph-common and ceph-osd see the same counters.
 *
 * Dumped once per second by OSD::tick_without_osd_lock() at debug_osd >= 5
 * with a "MEMDBG" prefix.  A counter whose live value grows without bound
 * under a steady workload identifies the leaked type.
 *
 * Counters use relaxed atomics, so the reported values are approximate when
 * read concurrently; that is fine for spotting unbounded growth.
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <ostream>

namespace ceph::live_count {

enum id_t : unsigned {
  MESSAGE = 0,        // every Message, any type
  MSG_OSD_OP,         // MOSDOp
  MSG_OSD_OP_REPLY,   // MOSDOpReply
  MSG_EC_WRITE,       // MOSDECSubOpWrite
  MSG_EC_WRITE_REPLY, // MOSDECSubOpWriteReply
  MSG_EC_READ,        // MOSDECSubOpRead
  MSG_EC_READ_REPLY,  // MOSDECSubOpReadReply
  CONTEXT,            // every Context subclass
  OP_REQUEST,         // OpRequest
  OS_TRANSACTION,     // ceph::os::Transaction (incl. copies)
  PG_TRANSACTION,     // PGTransaction
  EC_SUB_WRITE,       // ECSubWrite (incl. copies)
  EC_RMW_OP,          // ECCommon::RMWPipeline::Op (classical + dummy)
  EC_READ_OP,         // ECCommon::ReadOp
  EC_CLIENT_READ,     // ECCommon::ClientAsyncReadStatus
  OBJECT_CONTEXT,     // ObjectContext
  SHARD_EXTENT_MAP,   // ECUtil::shard_extent_map_t (incl. copies)
  EC_CACHE_OBJECT,    // ECExtentCache::Object
  EC_CACHE_LINE,      // ECExtentCache::Line
  EC_CACHE_OP,        // ECExtentCache::Op
  NUM
};

inline constexpr const char *names[NUM] = {
  "Message",
  "MOSDOp",
  "MOSDOpReply",
  "MOSDECSubOpWrite",
  "MOSDECSubOpWriteReply",
  "MOSDECSubOpRead",
  "MOSDECSubOpReadReply",
  "Context",
  "OpRequest",
  "os::Transaction",
  "PGTransaction",
  "ECSubWrite",
  "EC::RMWPipeline::Op",
  "EC::ReadOp",
  "EC::ClientAsyncReadStatus",
  "ObjectContext",
  "shard_extent_map_t",
  "ECExtentCache::Object",
  "ECExtentCache::Line",
  "ECExtentCache::Op",
};

struct counter_t {
  alignas(64) std::atomic<int64_t> live{0};
  alignas(64) std::atomic<int64_t> total{0};
};

inline counter_t counters[NUM];

template <id_t ID>
struct tracker {
  tracker() noexcept { inc(); }
  tracker(const tracker &) noexcept { inc(); }
  tracker(tracker &&) noexcept { inc(); }
  tracker &operator=(const tracker &) noexcept { return *this; }
  tracker &operator=(tracker &&) noexcept { return *this; }
  ~tracker() {
    counters[ID].live.fetch_sub(1, std::memory_order_relaxed);
  }

private:
  static void inc() noexcept {
    counters[ID].live.fetch_add(1, std::memory_order_relaxed);
    counters[ID].total.fetch_add(1, std::memory_order_relaxed);
  }
};

/// Print "name=live/total" for every counter.
inline void dump(std::ostream &os)
{
  for (unsigned i = 0; i < NUM; ++i) {
    if (i) {
      os << " ";
    }
    os << names[i] << "="
       << counters[i].live.load(std::memory_order_relaxed) << "/"
       << counters[i].total.load(std::memory_order_relaxed);
  }
}

} // namespace ceph::live_count

/// Declare a live-object tracking member for counter ID (an id_t enumerator).
#define CEPH_LIVE_COUNT(ID) \
  [[no_unique_address]] ::ceph::live_count::tracker< ::ceph::live_count::ID > \
    _live_count_##ID {}
