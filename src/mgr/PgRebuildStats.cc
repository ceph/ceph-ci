// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "mgr/PgRebuildStats.h"

#include "osd/osd_types.h"

namespace {

bool is_vulnerable(const pg_stat_t& st)
{
  return (st.state & (PG_STATE_DEGRADED | PG_STATE_UNDERSIZED)) ||
         st.stats.sum.num_objects_degraded > 0 ||
         st.stats.sum.num_objects_misplaced > 0;
}

double avg_seconds(uint64_t count, const utime_t& sum)
{
  if (count == 0) {
    return 0.0;
  }
  return static_cast<double>(sum) / static_cast<double>(count);
}

} // namespace

void PgRebuildStats::update(pg_t pgid, const pg_stat_t& st,
                            const pg_rebuild_latch_t* osd_latch)
{
  const bool stale = st.state & PG_STATE_STALE;
  const bool active = st.state & PG_STATE_ACTIVE;
  const int64_t num_degraded = st.stats.sum.num_objects_degraded;
  const int64_t num_misplaced = st.stats.sum.num_objects_misplaced;
  const int64_t num_recovered = st.stats.sum.num_objects_recovered;
  const bool vulnerable = is_vulnerable(st);

  auto& e = entries[pgid];

  if (stale) {
    return;
  }

  if (e.start_time == utime_t()) {
    if (osd_latch && !osd_latch->empty()) {
      e.start_time = osd_latch->start_time;
      e.base_recovered = osd_latch->base_recovered;
      e.had_redundancy_loss = osd_latch->had_redundancy_loss;
    } else if (vulnerable) {
      const bool new_failure =
        st.last_clean == utime_t() || st.last_change > st.last_clean;
      if (new_failure) {
        e.start_time = st.last_change;
        e.base_recovered = num_recovered;
        e.had_redundancy_loss = (num_degraded > 0 || num_misplaced > 0);
      }
    }
  }

  if (!vulnerable && e.start_time != utime_t() && active) {
    const int64_t delta_recovered = num_recovered - e.base_recovered;
    utime_t end = st.last_fresh;
    if (end == utime_t() || end < e.start_time) {
      end = st.last_change;
    }
    if (end < e.start_time) {
      end = st.last_clean;
    }
    const utime_t dur = (end >= e.start_time) ? (end - e.start_time)
                                              : utime_t();
    if (dur.to_msec() > 0 &&
        (delta_recovered > 0 || e.had_redundancy_loss)) {
      e.count++;
      e.duration_sum += dur;
      dirty = true;
    }
    e.start_time = utime_t();
    e.base_recovered = 0;
    e.had_redundancy_loss = false;
  }
}

void PgRebuildStats::prune_absent_pools(
    const std::map<int64_t, unsigned>& existing_pools)
{
  for (auto it = entries.begin(); it != entries.end(); ) {
    if (!existing_pools.count(it->first.pool())) {
      if (it->second.count > 0) {
        dirty = true;
      }
      it = entries.erase(it);
    } else {
      ++it;
    }
  }
}

pg_rebuild_dump_overlay PgRebuildStats::dump_overlay() const
{
  pg_rebuild_dump_overlay o;
  std::map<int64_t, uint64_t> pool_count;
  std::map<int64_t, utime_t> pool_sum;
  uint64_t cluster_count = 0;
  utime_t cluster_sum;

  for (const auto& [pgid, e] : entries) {
    if (e.count == 0) {
      continue;
    }
    pg_rebuild_dump_t d;
    d.rebuilds = e.count;
    d.avg_rebuild_time = avg_seconds(e.count, e.duration_sum);
    o.by_pg[pgid] = d;
    pool_count[pgid.pool()] += e.count;
    pool_sum[pgid.pool()] += e.duration_sum;
    cluster_count += e.count;
    cluster_sum += e.duration_sum;
  }
  for (const auto& [pool, c] : pool_count) {
    pg_rebuild_dump_t d;
    d.rebuilds = c;
    d.avg_rebuild_time = avg_seconds(c, pool_sum[pool]);
    o.by_pool[pool] = d;
  }
  o.cluster.rebuilds = cluster_count;
  o.cluster.avg_rebuild_time = avg_seconds(cluster_count, cluster_sum);
  return o;
}

bool PgRebuildStats::consume_dirty(ceph::buffer::list *bl)
{
  if (!dirty) {
    return false;
  }
  encode_completed(*bl);
  dirty = false;
  return true;
}

void PgRebuildStats::encode_completed(ceph::buffer::list& bl) const
{
  using ceph::encode;
  std::map<pg_t, std::pair<uint64_t, utime_t>> completed;
  for (const auto& [pgid, e] : entries) {
    if (e.count > 0) {
      completed[pgid] = {e.count, e.duration_sum};
    }
  }
  ENCODE_START(1, 1, bl);
  encode(completed, bl);
  ENCODE_FINISH(bl);
}

void PgRebuildStats::decode_completed(ceph::buffer::list::const_iterator& p)
{
  using ceph::decode;
  std::map<pg_t, std::pair<uint64_t, utime_t>> completed;
  DECODE_START(1, p);
  decode(completed, p);
  DECODE_FINISH(p);
  for (const auto& [pgid, cs] : completed) {
    auto& e = entries[pgid];
    e.count = cs.first;
    e.duration_sum = cs.second;
  }
  dirty = false;
}
