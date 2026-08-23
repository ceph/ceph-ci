// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "include/encoding.h"
#include "mgr/PgRebuildStats.h"
#include "osd/osd_types.h"

using ceph::decode;
using ceph::encode;

namespace {

pg_stat_t make_stat(uint64_t state, utime_t last_change, utime_t last_clean,
                    int64_t degraded = 0, int64_t recovered = 0,
                    utime_t last_fresh = utime_t())
{
  pg_stat_t st;
  st.state = state;
  st.last_change = last_change;
  st.last_clean = last_clean;
  st.last_fresh = last_fresh != utime_t() ? last_fresh : last_change;
  st.stats.sum.num_objects_degraded = degraded;
  st.stats.sum.num_objects_recovered = recovered;
  return st;
}

pg_rebuild_latch_t make_osd_latch(utime_t start, int64_t base = 0,
                                  bool redundancy = true)
{
  pg_rebuild_latch_t l;
  l.start_time = start;
  l.base_recovered = base;
  l.had_redundancy_loss = redundancy;
  return l;
}

} // namespace

TEST(PgRebuildStats, OwnLatchThenIgnoreLaterLastChange)
{
  PgRebuildStats stats;
  pg_t pgid(0, 1);
  const utime_t t0(100, 0);
  const utime_t t1(150, 0);
  const utime_t t2(200, 0);

  stats.update(pgid,
               make_stat(PG_STATE_ACTIVE | PG_STATE_DEGRADED, t0, utime_t(1, 0), 5),
               nullptr);
  ASSERT_EQ(stats.get_entries().at(pgid).start_time, t0);

  stats.update(pgid,
               make_stat(PG_STATE_ACTIVE | PG_STATE_DEGRADED, t1, utime_t(1, 0), 3),
               nullptr);
  ASSERT_EQ(stats.get_entries().at(pgid).start_time, t0);

  stats.update(pgid,
               make_stat(PG_STATE_ACTIVE | PG_STATE_CLEAN, t2, t2, 0, 2, t2),
               nullptr);
  const auto& e = stats.get_entries().at(pgid);
  ASSERT_EQ(e.start_time, utime_t());
  ASSERT_EQ(e.count, 1u);
  ASSERT_EQ(e.duration_sum, t2 - t0);
}

TEST(PgRebuildStats, CopyOsdLatchWhenMgrEmpty)
{
  PgRebuildStats stats;
  pg_t pgid(0, 1);
  const utime_t osd_start(50, 0);
  const utime_t last_change(200, 0);
  auto osd_latch = make_osd_latch(osd_start, 10, true);

  stats.update(pgid,
               make_stat(PG_STATE_ACTIVE | PG_STATE_DEGRADED, last_change,
                         utime_t(1, 0), 4, 12),
               &osd_latch);
  ASSERT_EQ(stats.get_entries().at(pgid).start_time, osd_start);
  ASSERT_EQ(stats.get_entries().at(pgid).base_recovered, 10);
  ASSERT_TRUE(stats.get_entries().at(pgid).had_redundancy_loss);
}

TEST(PgRebuildStats, IgnoreOsdLatchOnceMgrHasOne)
{
  PgRebuildStats stats;
  pg_t pgid(0, 1);
  const utime_t mgr_start(100, 0);
  stats.update(pgid,
               make_stat(PG_STATE_ACTIVE | PG_STATE_DEGRADED, mgr_start,
                         utime_t(1, 0), 2),
               nullptr);
  ASSERT_EQ(stats.get_entries().at(pgid).start_time, mgr_start);

  auto osd_latch = make_osd_latch(utime_t(999, 0));
  stats.update(pgid,
               make_stat(PG_STATE_ACTIVE | PG_STATE_DEGRADED, utime_t(400, 0),
                         utime_t(1, 0), 1),
               &osd_latch);
  ASSERT_EQ(stats.get_entries().at(pgid).start_time, mgr_start);
}

TEST(PgRebuildStats, EmptyOsdLatchStartsOwn)
{
  PgRebuildStats stats;
  pg_t pgid(0, 1);
  const utime_t t0(80, 0);
  pg_rebuild_latch_t empty;
  stats.update(pgid,
               make_stat(PG_STATE_ACTIVE | PG_STATE_DEGRADED, t0,
                         utime_t(1, 0), 1),
               &empty);
  ASSERT_EQ(stats.get_entries().at(pgid).start_time, t0);
}

TEST(PgRebuildStats, CompletedDuringGapNotRecorded)
{
  PgRebuildStats stats;
  pg_t pgid(0, 1);
  // Clean PG, no latch: an episode that finished while mgr was down.
  stats.update(pgid,
               make_stat(PG_STATE_ACTIVE | PG_STATE_CLEAN, utime_t(200, 0),
                         utime_t(200, 0)),
               nullptr);
  auto it = stats.get_entries().find(pgid);
  ASSERT_TRUE(it == stats.get_entries().end() || it->second.count == 0);
}

TEST(PgRebuildStats, StaleDoesNotLatchOrRecord)
{
  PgRebuildStats stats;
  pg_t pgid(0, 1);
  stats.update(pgid,
               make_stat(PG_STATE_STALE | PG_STATE_DEGRADED, utime_t(10, 0),
                         utime_t(1, 0), 3),
               nullptr);
  auto it = stats.get_entries().find(pgid);
  ASSERT_TRUE(it == stats.get_entries().end() ||
              it->second.start_time == utime_t());
}

TEST(PgRebuildStats, PersistReloadKeepsCountNotLatch)
{
  PgRebuildStats src;
  pg_t pgid(1, 2);
  src.update(pgid,
             make_stat(PG_STATE_ACTIVE | PG_STATE_DEGRADED, utime_t(10, 0),
                       utime_t(1, 0), 2, 0),
             nullptr);
  src.update(pgid,
             make_stat(PG_STATE_ACTIVE | PG_STATE_CLEAN, utime_t(40, 0),
                       utime_t(40, 0), 0, 2, utime_t(40, 0)),
             nullptr);
  ASSERT_EQ(src.get_entries().at(pgid).count, 1u);
  ASSERT_TRUE(src.is_dirty());

  bufferlist bl;
  src.encode_completed(bl);

  PgRebuildStats dst;
  auto p = bl.cbegin();
  dst.decode_completed(p);
  ASSERT_EQ(dst.get_entries().at(pgid).count, 1u);
  ASSERT_EQ(dst.get_entries().at(pgid).duration_sum,
            src.get_entries().at(pgid).duration_sum);
  ASSERT_EQ(dst.get_entries().at(pgid).start_time, utime_t());
  ASSERT_FALSE(dst.is_dirty());
}

TEST(PgRebuildStats, DumpOverlayWeightedPoolAvg)
{
  PgRebuildStats stats;
  pg_t a(0, 1);
  pg_t b(1, 1);
  pg_t c(0, 2);

  stats.update(a, make_stat(PG_STATE_ACTIVE | PG_STATE_DEGRADED,
                            utime_t(10, 0), utime_t(1, 0), 1), nullptr);
  stats.update(a, make_stat(PG_STATE_ACTIVE | PG_STATE_CLEAN,
                            utime_t(20, 0), utime_t(20, 0), 0, 1, utime_t(20, 0)),
               nullptr);
  stats.update(b, make_stat(PG_STATE_ACTIVE | PG_STATE_DEGRADED,
                            utime_t(10, 0), utime_t(1, 0), 1), nullptr);
  stats.update(b, make_stat(PG_STATE_ACTIVE | PG_STATE_CLEAN,
                            utime_t(40, 0), utime_t(40, 0), 0, 1, utime_t(40, 0)),
               nullptr);
  stats.update(c, make_stat(PG_STATE_ACTIVE | PG_STATE_DEGRADED,
                            utime_t(10, 0), utime_t(1, 0), 1), nullptr);
  stats.update(c, make_stat(PG_STATE_ACTIVE | PG_STATE_CLEAN,
                            utime_t(15, 0), utime_t(15, 0), 0, 1, utime_t(15, 0)),
               nullptr);

  auto ov = stats.dump_overlay();
  ASSERT_EQ(ov.lookup_pg(a).rebuilds, 1u);
  ASSERT_DOUBLE_EQ(ov.lookup_pg(a).avg_rebuild_time, 10.0);
  ASSERT_DOUBLE_EQ(ov.lookup_pg(b).avg_rebuild_time, 30.0);
  // pool 1: (10+30)/2 = 20
  ASSERT_EQ(ov.lookup_pool(1).rebuilds, 2u);
  ASSERT_DOUBLE_EQ(ov.lookup_pool(1).avg_rebuild_time, 20.0);
  ASSERT_EQ(ov.cluster.rebuilds, 3u);
  ASSERT_DOUBLE_EQ(ov.cluster.avg_rebuild_time, (10.0 + 30.0 + 5.0) / 3.0);
}

TEST(PgRebuildStats, ConsumeDirtyClearsFlag)
{
  PgRebuildStats stats;
  pg_t pgid(0, 1);
  stats.update(pgid,
               make_stat(PG_STATE_ACTIVE | PG_STATE_DEGRADED, utime_t(10, 0),
                         utime_t(1, 0), 1), nullptr);
  stats.update(pgid,
               make_stat(PG_STATE_ACTIVE | PG_STATE_CLEAN, utime_t(12, 0),
                         utime_t(12, 0), 0, 1, utime_t(12, 0)), nullptr);
  ASSERT_TRUE(stats.is_dirty());
  bufferlist bl;
  ASSERT_TRUE(stats.consume_dirty(&bl));
  ASSERT_FALSE(stats.is_dirty());
  ASSERT_GT(bl.length(), 0u);
  ASSERT_FALSE(stats.consume_dirty(&bl));
}

TEST(PgRebuildLatch, EncodeDecodeRoundtrip)
{
  pg_rebuild_latch_t in;
  in.start_time = utime_t(42, 7);
  in.base_recovered = 9;
  in.had_redundancy_loss = true;
  bufferlist bl;
  encode(in, bl);
  pg_rebuild_latch_t out;
  auto p = bl.cbegin();
  decode(out, p);
  ASSERT_EQ(out.start_time, in.start_time);
  ASSERT_EQ(out.base_recovered, in.base_recovered);
  ASSERT_EQ(out.had_redundancy_loss, in.had_redundancy_loss);
}
