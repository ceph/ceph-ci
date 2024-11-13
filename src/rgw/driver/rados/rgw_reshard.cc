// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <deque>
#include <limits>
#include <sstream>
#include <chrono>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>

#include "rgw_zone.h"
#include "driver/rados/rgw_bucket.h"
#include "driver/rados/reshard_writer.h"
#include "rgw_asio_thread.h"
#include "rgw_reshard.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "rgw_tracer.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/lock/cls_lock_client.h"
#include "common/async/lease_rados.h"
#include "common/async/spawn_throttle.h"
#include "common/errno.h"
#include "common/error_code.h"
#include "common/ceph_json.h"

#include "common/dout.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"
#include "services/svc_tier_rados.h"
#include "services/svc_bilog_rados.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

using namespace std;

const string reshard_oid_prefix = "reshard.";
const string reshard_lock_name = "reshard_process";

// key reduction values; NB maybe expose some in options
constexpr uint64_t default_min_objs_per_shard = 10000;
constexpr uint32_t min_dynamic_shards = 11;

/* All primes up to 2000 used to attempt to make dynamic sharding use
 * a prime numbers of shards. Note: this list also includes 1 for when
 * 1 shard is the most appropriate, even though 1 is not prime.
 */
const std::initializer_list<uint16_t> RGWBucketReshard::reshard_primes = {
  1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61,
  67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137,
  139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211,
  223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283,
  293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379,
  383, 389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461,
  463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563,
  569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643,
  647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739,
  743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829,
  839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937,
  941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021,
  1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069, 1087, 1091, 1093,
  1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163, 1171, 1181,
  1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259,
  1277, 1279, 1283, 1289, 1291, 1297, 1301, 1303, 1307, 1319, 1321,
  1327, 1361, 1367, 1373, 1381, 1399, 1409, 1423, 1427, 1429, 1433,
  1439, 1447, 1451, 1453, 1459, 1471, 1481, 1483, 1487, 1489, 1493,
  1499, 1511, 1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571, 1579,
  1583, 1597, 1601, 1607, 1609, 1613, 1619, 1621, 1627, 1637, 1657,
  1663, 1667, 1669, 1693, 1697, 1699, 1709, 1721, 1723, 1733, 1741,
  1747, 1753, 1759, 1777, 1783, 1787, 1789, 1801, 1811, 1823, 1831,
  1847, 1861, 1867, 1871, 1873, 1877, 1879, 1889, 1901, 1907, 1913,
  1931, 1933, 1949, 1951, 1973, 1979, 1987, 1993, 1997, 1999
};


uint32_t RGWBucketReshard::get_prime_shard_count(
  uint32_t shard_count,
  uint32_t max_dynamic_shards,
  uint32_t min_dynamic_shards)
{
  uint32_t prime_shard_count =
    get_prime_shards_greater_or_equal(shard_count);

  // if we cannot find a larger prime number, then just use what was
  // passed in
  if (! prime_shard_count) {
    prime_shard_count = shard_count;
  }

  // keep within min/max bounds
  return std::min(max_dynamic_shards,
		  std::max(min_dynamic_shards, prime_shard_count));
}


// Given the current number of shards and objects (entries), we
// calculate whether resharding is called for and if so, how many
// shards we should have given a variety of considerations to be used
// as part of the dynamic resharding capability.
void RGWBucketReshard::calculate_preferred_shards(
  const DoutPrefixProvider* dpp,
  const uint32_t max_dynamic_shards,
  const uint64_t max_objs_per_shard,
  const bool is_multisite,
  const uint64_t num_objs,
  const uint32_t current_num_shards,
  bool& need_resharding,
  uint32_t* suggested_num_shards,
  bool prefer_prime)
{
  constexpr uint32_t regular_multiplier = 2;
  // to reduce number of reshards in multisite, increase number of shards more aggressively
  constexpr uint32_t multisite_multiplier = 8;
  const char* verb = "n/a";

  // in case admin lowers max_objs_per_shard, we need to avoid thrashing
  const uint64_t min_objs_per_shard =
    std::min(default_min_objs_per_shard,
	     (uint64_t) std::ceil(max_objs_per_shard / 100.0));

  if (current_num_shards < max_dynamic_shards &&
      num_objs > current_num_shards * max_objs_per_shard) {
    need_resharding = true;
    verb = "expansion";
  } else if (current_num_shards > min_dynamic_shards &&
	     num_objs < current_num_shards * min_objs_per_shard) {
    need_resharding = true;
    verb = "reduction";
  } else {
    need_resharding = false;
    return;
  }

  const uint32_t multiplier =
    is_multisite ? multisite_multiplier : regular_multiplier;
  uint32_t calculated_num_shards =
    std::max(min_dynamic_shards,
	     std::min(max_dynamic_shards,
		      (uint32_t) (num_objs * multiplier / max_objs_per_shard)));
  if (calculated_num_shards == current_num_shards) {
    need_resharding = false;
    return;
  }

  if (prefer_prime) {
    calculated_num_shards = get_prime_shard_count(
      calculated_num_shards, max_dynamic_shards, min_dynamic_shards);
  }

  ldpp_dout(dpp, 20) << __func__ << ": reshard " << verb <<
    " suggested; current average (objects/shard) is " <<
    float(num_objs) / current_num_shards << ", which is not within " <<
    min_objs_per_shard << " and " << max_objs_per_shard <<
    "; suggesting " << calculated_num_shards << " shards" << dendl;

  if (suggested_num_shards) {
    *suggested_num_shards = calculated_num_shards;
  }
} // RGWBucketReshard::check_bucket_shards

// implements the Batch concept for reshard::Writer
class ShardBatch {
 public:
  ShardBatch(const boost::asio::any_io_executor& ex,
             librados::IoCtx& ioctx,
             const std::string& object,
             size_t batch_size,
             bool can_put_entries,
             bool check_existing)
    : ex(ex),
      ioctx(ioctx),
      object(object),
      batch_size(batch_size),
      can_put_entries(can_put_entries),
      check_existing(check_existing)
  {}

  [[nodiscard]] bool empty() const {
    return entries.empty();
  }

  [[nodiscard]] bool add(rgw_cls_bi_entry entry,
                         std::optional<RGWObjCategory> category,
                         rgw_bucket_category_stats entry_stats) {
    entries.push_back(std::move(entry));
    if (category && !can_put_entries) {
      stats[*category] += entry_stats;
    }
    // flush needed?
    return entries.size() >= batch_size;
  }

  void flush(rgwrados::reshard::Completion completion) {
    librados::ObjectWriteOperation op;

    if (can_put_entries) {
      // bi_put_entries() handles stats on the server side
      cls_rgw_bi_put_entries(op, std::move(entries), check_existing);
    } else {
      // issue a separate bi_put() call for each entry
      for (auto& entry : entries) {
        cls_rgw_bi_put(op, std::move(entry));
      }
      constexpr bool absolute = false; // add to existing stats
      cls_rgw_bucket_update_stats(op, absolute, stats);
    }
    entries.clear();
    stats.clear();

    constexpr int flags = 0;
    constexpr jspan_context* trace = nullptr;
    librados::async_operate(ex, ioctx, object, &op, flags,
                            trace, std::move(completion));
  }

 private:
  boost::asio::any_io_executor ex;
  librados::IoCtx& ioctx;
  const std::string& object;
  const size_t batch_size;
  const bool can_put_entries;
  const bool check_existing;
  std::vector<rgw_cls_bi_entry> entries;
  std::map<RGWObjCategory, rgw_bucket_category_stats> stats;
};

class BucketReshardManager {
  using Writer = rgwrados::reshard::Writer<ShardBatch>;
  std::deque<Writer> writers;
 public:
  BucketReshardManager(const boost::asio::any_io_executor& ex,
                       uint64_t max_aio,
                       librados::IoCtx& ioctx,
                       const std::map<int, std::string>& oids,
                       size_t batch_size,
                       bool can_put_entries,
                       bool check_existing)
  {
    int expected = 0;
    for (auto& [shard, oid] : oids) {
      assert(shard == expected); // shard ids must be sequential from 0
      expected++;

      writers.emplace_back(ex, max_aio, ex, ioctx, oid, batch_size,
                           can_put_entries, check_existing);
    }
  }

  void add_entry(int shard_index, rgw_cls_bi_entry entry,
                 bool account, RGWObjCategory category,
                 const rgw_bucket_category_stats& stats,
                 boost::asio::yield_context yield)
  {
    auto& writer = writers.at(shard_index); // may throw
    std::optional<RGWObjCategory> cat;
    if (account) {
      cat = category;
    }
    writer.write(std::move(entry), cat, stats, yield);
  }

  void flush()
  {
    for (auto& writer : writers) {
      writer.flush();
    }
  }

  void drain(boost::asio::yield_context yield)
  {
    std::exception_ptr eptr;
    for (auto& writer : writers) {
      try {
        writer.drain(yield);
      } catch (const std::exception&) {
        if (!eptr) {
          eptr = std::current_exception();
        }
      }
    }
    if (eptr) {
      // drain all writers before rethrowing
      std::rethrow_exception(eptr);
    }
  }
}; // class BucketReshardManager

RGWBucketReshard::RGWBucketReshard(rgw::sal::RadosStore* _store,
				   const RGWBucketInfo& _bucket_info,
				   const std::map<std::string, bufferlist>& _bucket_attrs) :
  store(_store), bucket_info(_bucket_info), bucket_attrs(_bucket_attrs)
{ }

// sets reshard status of bucket index shards for the current index layout
static int set_resharding_status(const DoutPrefixProvider *dpp,
				 rgw::sal::RadosStore* store,
				 const RGWBucketInfo& bucket_info,
                                 cls_rgw_reshard_status status)
{
  cls_rgw_bucket_instance_entry instance_entry;
  instance_entry.set_status(status);

  int ret = store->getRados()->bucket_set_reshard(dpp, bucket_info, instance_entry);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "RGWReshard::" << __func__ << " ERROR: error setting bucket resharding flag on bucket index: "
		  << cpp_strerror(-ret) << dendl;
    return ret;
  }
  return 0;
}

static int remove_old_reshard_instance(rgw::sal::RadosStore* store,
                                       const rgw_bucket& bucket,
                                       const DoutPrefixProvider* dpp, optional_yield y)
{
  RGWBucketInfo info;
  int r = store->getRados()->get_bucket_instance_info(bucket, info, nullptr,
                                                      nullptr, y, dpp);
  if (r < 0) {
    return r;
  }

  // delete its shard objects (ignore errors)
  store->svc()->bi->clean_index(dpp, info, info.layout.current_index);
  // delete the bucket instance metadata
  return store->ctl()->bucket->remove_bucket_instance_info(bucket, info, y, dpp);
}

// initialize the new bucket index shard objects
static int init_target_index(rgw::sal::RadosStore* store,
                             RGWBucketInfo& bucket_info,
                             const rgw::bucket_index_layout_generation& index,
                             ReshardFaultInjector& fault,
                             bool& support_logrecord,
                             const DoutPrefixProvider* dpp)
{

  int ret = 0;
  if (ret = fault.check("init_index");
      ret == 0) { // no fault injected, initialize index
    ret = store->svc()->bi->init_index(dpp, bucket_info, index, true);
  }
  if (ret == -EOPNOTSUPP) {
    ldpp_dout(dpp, 0) << "WARNING: " << "init_index() does not supported logrecord, "
                      << "falling back to block reshard mode." << dendl;
    support_logrecord = false;
    ret = store->svc()->bi->init_index(dpp, bucket_info, index, false);
  } else if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to initialize "
       "target index shard objects: " << cpp_strerror(ret) << dendl;
    return ret;
  }

  if (!bucket_info.datasync_flag_enabled()) {
    // if bucket sync is disabled, disable it on each of the new shards too
    auto log = rgw::log_layout_from_index(0, index);
    ret = store->svc()->bilog_rados->log_stop(dpp, bucket_info, log, -1);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to disable "
          "bucket sync on the target index shard objects: "
          << cpp_strerror(ret) << dendl;
      store->svc()->bi->clean_index(dpp, bucket_info, index);
      return ret;
    }
  }

  return ret;
}

// initialize a target index layout, create its bucket index shard objects, and
// write the target layout to the bucket instance metadata
static int init_target_layout(rgw::sal::RadosStore* store,
                              RGWBucketInfo& bucket_info,
			      std::map<std::string, bufferlist>& bucket_attrs,
                              ReshardFaultInjector& fault,
                              const uint32_t new_num_shards,
                              bool& support_logrecord,
                              const DoutPrefixProvider* dpp, optional_yield y)
{
  auto prev = bucket_info.layout; // make a copy for cleanup
  const auto current = prev.current_index;

  // initialize a new normal target index layout generation
  rgw::bucket_index_layout_generation target;
  target.layout.type = rgw::BucketIndexType::Normal;
  target.layout.normal.num_shards = new_num_shards;
  target.gen = current.gen + 1;

  if (bucket_info.reshard_status == cls_rgw_reshard_status::IN_PROGRESS) {
    // backward-compatible cleanup of old reshards, where the target was in a
    // different bucket instance
    if (!bucket_info.new_bucket_instance_id.empty()) {
      rgw_bucket new_bucket = bucket_info.bucket;
      new_bucket.bucket_id = bucket_info.new_bucket_instance_id;
      ldout(store->ctx(), 10) << __func__ << " removing target bucket instance "
          "from a previous reshard attempt" << dendl;
      // ignore errors
      remove_old_reshard_instance(store, new_bucket, dpp, y);
    }
    bucket_info.reshard_status = cls_rgw_reshard_status::NOT_RESHARDING;
  }

  if (bucket_info.layout.target_index) {
    // a previous reshard failed or stalled, and its reshard lock dropped
    ldpp_dout(dpp, 10) << __func__ << " removing existing target index "
        "objects from a previous reshard attempt" << dendl;
    // delete its existing shard objects (ignore errors)
    store->svc()->bi->clean_index(dpp, bucket_info, *bucket_info.layout.target_index);
    // don't reuse this same generation in the new target layout, in case
    // something is still trying to operate on its shard objects
    target.gen = bucket_info.layout.target_index->gen + 1;
  }

  // create the index shard objects
  int ret = init_target_index(store, bucket_info, target, fault, support_logrecord, dpp);
  if (ret < 0) {
    return ret;
  }

  // retry in case of racing writes to the bucket instance metadata
  static constexpr auto max_retries = 10;
  int tries = 0;

  do {

    // update resharding state
    bucket_info.layout.target_index = target;
    if (support_logrecord) {
      bucket_info.layout.resharding = rgw::BucketReshardState::InLogrecord;
    } else {
      bucket_info.layout.resharding = rgw::BucketReshardState::InProgress;
    }

    // update the judge time meanwhile
    bucket_info.layout.judge_reshard_lock_time = ceph::real_clock::now();
    if (ret = fault.check("set_target_layout");
        ret == 0) { // no fault injected, write the bucket instance metadata
      ret = store->getRados()->put_bucket_instance_info(bucket_info, false,
                                                        real_time(), &bucket_attrs, dpp, y);
    } else if (ret == -ECANCELED) {
      fault.clear(); // clear the fault so a retry can succeed
    }

    if (ret == -ECANCELED) {
      if (y.cancelled() != boost::asio::cancellation_type::none) {
        break;
      }

      // racing write detected, read the latest bucket info and try again
      int ret2 = store->getRados()->get_bucket_instance_info(
          bucket_info.bucket, bucket_info,
          nullptr, &bucket_attrs, y, dpp);
      if (ret2 < 0) {
        ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to read "
            "bucket info: " << cpp_strerror(ret2) << dendl;
        ret = ret2;
        break;
      }

      // check that we're still in the reshard state we started in
      if (bucket_info.layout.resharding != rgw::BucketReshardState::None ||
          bucket_info.layout.current_index != current) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " raced with "
            "another reshard" << dendl;
        break;
      }

      prev = bucket_info.layout; // update the copy
    }
    ++tries;
  } while (ret == -ECANCELED && tries < max_retries);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to write "
        "target index layout to bucket info: " << cpp_strerror(ret) << dendl;

    bucket_info.layout = std::move(prev);  // restore in-memory layout

    // delete the target shard objects (ignore errors)
    store->svc()->bi->clean_index(dpp, bucket_info, target);
    return ret;
  }

  return 0;
} // init_target_layout

// delete the bucket index shards associated with the target layout and remove
// it from the bucket instance metadata
static int revert_target_layout(rgw::sal::RadosStore* store,
                                RGWBucketInfo& bucket_info,
				std::map<std::string, bufferlist>& bucket_attrs,
                                ReshardFaultInjector& fault,
                                const DoutPrefixProvider* dpp, optional_yield y)
{
  auto prev = bucket_info.layout; // make a copy for cleanup

  // remove target index shard objects
  int ret = store->svc()->bi->clean_index(dpp, bucket_info, *prev.target_index);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " failed to remove "
        "target index with: " << cpp_strerror(ret) << dendl;
    ret = 0; // non-fatal error
  }
  // trim the reshard log entries written in logrecord state
  ret = store->getRados()->trim_reshard_log_entries(dpp, bucket_info, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " failed to trim "
        "reshard log entries: " << cpp_strerror(ret) << dendl;
    ret = 0; // non-fatal error
  }

  // retry in case of racing writes to the bucket instance metadata
  static constexpr auto max_retries = 10;
  int tries = 0;
  do {
    // clear target_index and resharding state
    bucket_info.layout.target_index = std::nullopt;
    bucket_info.layout.resharding = rgw::BucketReshardState::None;

    if (ret = fault.check("revert_target_layout");
        ret == 0) { // no fault injected, revert the bucket instance metadata
      ret = store->getRados()->put_bucket_instance_info(bucket_info, false,
                                                        real_time(),
                                                        &bucket_attrs, dpp, y);
    } else if (ret == -ECANCELED) {
      fault.clear(); // clear the fault so a retry can succeed
    }

    if (ret == -ECANCELED) {
      if (y.cancelled() != boost::asio::cancellation_type::none) {
        break;
      }

      // racing write detected, read the latest bucket info and try again
      int ret2 = store->getRados()->get_bucket_instance_info(
          bucket_info.bucket, bucket_info,
          nullptr, &bucket_attrs, y, dpp);
      if (ret2 < 0) {
        ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to read "
            "bucket info: " << cpp_strerror(ret2) << dendl;
        ret = ret2;
        break;
      }

      // check that we're still in the reshard state we started in
      if (bucket_info.layout.resharding == rgw::BucketReshardState::None) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " raced with "
            "reshard cancel" << dendl;
        return -ECANCELED;
      }
      if (bucket_info.layout.current_index != prev.current_index ||
          bucket_info.layout.target_index != prev.target_index) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " raced with "
            "another reshard" << dendl;
        return -ECANCELED;
      }

      prev = bucket_info.layout; // update the copy
    }
    ++tries;
  } while (ret == -ECANCELED && tries < max_retries);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to clear "
        "target index layout in bucket info: " << cpp_strerror(ret) << dendl;

    bucket_info.layout = std::move(prev);  // restore in-memory layout
    return ret;
  }
  return 0;
} // remove_target_layout

static int init_reshard(rgw::sal::RadosStore* store,
                        RGWBucketInfo& bucket_info,
			std::map<std::string, bufferlist>& bucket_attrs,
                        ReshardFaultInjector& fault,
                        const uint32_t new_num_shards,
                        bool& support_logrecord,
                        const DoutPrefixProvider *dpp, optional_yield y)
{
  if (new_num_shards == 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " got invalid new_num_shards=0" << dendl;
    return -EINVAL;
  }

  int ret = init_target_layout(store, bucket_info, bucket_attrs, fault, new_num_shards,
                               support_logrecord, dpp, y);
  if (ret < 0) {
    return ret;
  }

  // trim the reshard log entries to guarantee that any existing log entries are cleared,
  // if there are no reshard log entries, this is a no-op that costs little time
  if (support_logrecord) {
    if (ret = fault.check("trim_reshard_log_entries");
        ret == 0) { // no fault injected, trim reshard log entries
      ret = store->getRados()->trim_reshard_log_entries(dpp, bucket_info, y);
    }
    if (ret == -EOPNOTSUPP) {
      // not an error, logrecord is not supported, change to block reshard
      ldpp_dout(dpp, 0) << "WARNING: " << "trim_reshard_log_entries() does not supported"
                        << " logrecord, falling back to block reshard mode." << dendl;
      bucket_info.layout.resharding = rgw::BucketReshardState::InProgress;
      support_logrecord = false;
    } else if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to trim reshard log entries: "
                        << cpp_strerror(ret) << dendl;
      return ret;
    }
  }

  if (support_logrecord) {
    if (ret = fault.check("logrecord_writes");
        ret == 0) { // no fault injected, record log with writing to the current index shards
      ret = set_resharding_status(dpp, store, bucket_info,
                                  cls_rgw_reshard_status::IN_LOGRECORD);
    }
  } else {
    ret = set_resharding_status(dpp, store, bucket_info,
                                cls_rgw_reshard_status::IN_PROGRESS);
  }
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to pause "
        "writes to the current index: " << cpp_strerror(ret) << dendl;
    // clean up the target layout (ignore errors)
    revert_target_layout(store, bucket_info, bucket_attrs, fault, dpp, y);
    return ret;
  }
  return 0;
} // init_reshard

static int change_reshard_state(rgw::sal::RadosStore* store,
                                RGWBucketInfo& bucket_info,
                                std::map<std::string, bufferlist>& bucket_attrs,
                                ReshardFaultInjector& fault,
                                const DoutPrefixProvider *dpp, optional_yield y)
{
  auto prev = bucket_info.layout; // make a copy for cleanup
  const auto current = prev.current_index;

  // retry in case of racing writes to the bucket instance metadata
  static constexpr auto max_retries = 10;
  int tries = 0;
  int ret = 0;
  do {
    // update resharding state
    bucket_info.layout.resharding = rgw::BucketReshardState::InProgress;

    if (ret = fault.check("change_reshard_state");
        ret == 0) { // no fault injected, write the bucket instance metadata
      ret = store->getRados()->put_bucket_instance_info(bucket_info, false,
                                                        real_time(), &bucket_attrs, dpp, y);
    } else if (ret == -ECANCELED) {
      fault.clear(); // clear the fault so a retry can succeed
    }

    if (ret == -ECANCELED) {
      if (y.cancelled() != boost::asio::cancellation_type::none) {
        break;
      }

      // racing write detected, read the latest bucket info and try again
      int ret2 = store->getRados()->get_bucket_instance_info(
          bucket_info.bucket, bucket_info,
          nullptr, &bucket_attrs, y, dpp);
      if (ret2 < 0) {
        ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to read "
            "bucket info: " << cpp_strerror(ret2) << dendl;
        ret = ret2;
        break;
      }

      // check that we're still in the reshard state we started in
      if (bucket_info.layout.resharding != rgw::BucketReshardState::InLogrecord ||
          bucket_info.layout.current_index != current) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " raced with "
            "another reshard" << dendl;
        break;
      }
    }
    ++tries;
  } while (ret == -ECANCELED && tries < max_retries);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to commit "
        "target index layout: " << cpp_strerror(ret) << dendl;

    bucket_info.layout = std::move(prev); // restore in-memory layout

    // unblock writes to the current index shard objects
    int ret2 = set_resharding_status(dpp, store, bucket_info,
                                     cls_rgw_reshard_status::NOT_RESHARDING);
    if (ret2 < 0) {
      ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " failed to unblock "
          "writes to current index objects: " << cpp_strerror(ret2) << dendl;
      // non-fatal error
    }
    return ret;
  }

  if (ret = fault.check("block_writes");
      ret == 0) { // no fault injected, block writes to the current index shards
    ret = set_resharding_status(dpp, store, bucket_info,
                                cls_rgw_reshard_status::IN_PROGRESS);
  }

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to pause "
        "writes to the current index: " << cpp_strerror(ret) << dendl;
    // clean up the target layout (ignore errors)
    revert_target_layout(store, bucket_info, bucket_attrs, fault, dpp, y);
    return ret;
  }

  return 0;
} // change_reshard_state

static int cancel_reshard(rgw::sal::RadosStore* store,
                          RGWBucketInfo& bucket_info,
			  std::map<std::string, bufferlist>& bucket_attrs,
                          ReshardFaultInjector& fault,
                          const DoutPrefixProvider *dpp, optional_yield y)
{
  // unblock writes to the current index shard objects
  int ret = set_resharding_status(dpp, store, bucket_info,
                                  cls_rgw_reshard_status::NOT_RESHARDING);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " failed to unblock "
        "writes to current index objects: " << cpp_strerror(ret) << dendl;
    ret = 0; // non-fatal error
  }

  if (bucket_info.layout.target_index) {
    return revert_target_layout(store, bucket_info, bucket_attrs, fault, dpp, y);
  }
  // there is nothing to revert
  return 0;
} // cancel_reshard

static int commit_target_layout(rgw::sal::RadosStore* store,
                                RGWBucketInfo& bucket_info,
                                std::map<std::string, bufferlist>& bucket_attrs,
                                ReshardFaultInjector& fault,
                                const DoutPrefixProvider *dpp, optional_yield y)
{
  auto& layout = bucket_info.layout;
  const auto next_log_gen = layout.logs.empty() ? 1 :
      layout.logs.back().gen + 1;

  if (!store->svc()->zone->need_to_log_data()) {
    // if we're not syncing data, we can drop any existing logs
    layout.logs.clear();
  }

  // use the new index layout as current
  ceph_assert(layout.target_index);
  layout.current_index = std::move(*layout.target_index);
  layout.target_index = std::nullopt;
  layout.resharding = rgw::BucketReshardState::None;
  // add the in-index log layout
  layout.logs.push_back(log_layout_from_index(next_log_gen, layout.current_index));

  int ret = fault.check("commit_target_layout");
  if (ret == 0) { // no fault injected, write the bucket instance metadata
    ret = store->getRados()->put_bucket_instance_info(
        bucket_info, false, real_time(), &bucket_attrs, dpp, y);
  } else if (ret == -ECANCELED) {
    fault.clear(); // clear the fault so a retry can succeed
  }
  return ret;
} // commit_target_layout

static int commit_reshard(rgw::sal::RadosStore* store,
                          RGWBucketInfo& bucket_info,
			  std::map<std::string, bufferlist>& bucket_attrs,
                          ReshardFaultInjector& fault,
                          const DoutPrefixProvider *dpp, optional_yield y)
{
  auto prev = bucket_info.layout; // make a copy for cleanup

  // retry in case of racing writes to the bucket instance metadata
  static constexpr auto max_retries = 10;
  int tries = 0;
  int ret = 0;
  do {
    ret = commit_target_layout(store, bucket_info, bucket_attrs, fault, dpp, y);
    if (ret == -ECANCELED) {
      if (y.cancelled() != boost::asio::cancellation_type::none) {
        break;
      }

      // racing write detected, read the latest bucket info and try again
      int ret2 = store->getRados()->get_bucket_instance_info(
          bucket_info.bucket, bucket_info,
          nullptr, &bucket_attrs, y, dpp);
      if (ret2 < 0) {
        ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to read "
            "bucket info: " << cpp_strerror(ret2) << dendl;
        ret = ret2;
        break;
      }

      // check that we're still in the reshard state we started in
      if (bucket_info.layout.resharding != rgw::BucketReshardState::InProgress) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " raced with "
            "reshard cancel" << dendl;
        return -ECANCELED; // whatever canceled us already did the cleanup
      }
      if (bucket_info.layout.current_index != prev.current_index ||
          bucket_info.layout.target_index != prev.target_index) {
        ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " raced with "
            "another reshard" << dendl;
        return -ECANCELED; // whatever canceled us already did the cleanup
      }

      prev = bucket_info.layout; // update the copy
    }
    ++tries;
  } while (ret == -ECANCELED && tries < max_retries);

  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " failed to commit "
        "target index layout: " << cpp_strerror(ret) << dendl;

    bucket_info.layout = std::move(prev); // restore in-memory layout

    // unblock writes to the current index shard objects
    int ret2 = set_resharding_status(dpp, store, bucket_info,
                                     cls_rgw_reshard_status::NOT_RESHARDING);
    if (ret2 < 0) {
      ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " failed to unblock "
          "writes to current index objects: " << cpp_strerror(ret2) << dendl;
      // non-fatal error
    }
    return ret;
  }

  if (store->svc()->zone->need_to_log_data() && !prev.logs.empty() &&
      prev.current_index.layout.type == rgw::BucketIndexType::Normal) {
    // write a datalog entry for each shard of the previous index. triggering
    // sync on the old shards will force them to detect the end-of-log for that
    // generation, and eventually transition to the next
    // TODO: use a log layout to support types other than BucketLogType::InIndex
    for (uint32_t shard_id = 0; shard_id < rgw::num_shards(prev.current_index.layout.normal); ++shard_id) {
      // This null_yield can stay, for now, since we're in our own thread
      ret = store->svc()->datalog_rados->add_entry(dpp, bucket_info, prev.logs.back(), shard_id,
						   null_yield);
      if (ret < 0) {
        ldpp_dout(dpp, 1) << "WARNING: failed writing data log (bucket_info.bucket="
        << bucket_info.bucket << ", shard_id=" << shard_id << "of generation="
        << prev.logs.back().gen << ")" << dendl;
      } // datalog error is not fatal
    }
  }

  // check whether the old index objects are still needed for bilogs
  const auto& logs = bucket_info.layout.logs;
  auto log = std::find_if(logs.begin(), logs.end(),
      [&prev] (const rgw::bucket_log_layout_generation& log) {
        return log.layout.type == rgw::BucketLogType::InIndex
            && log.layout.in_index.gen == prev.current_index.gen;
      });
  if (log == logs.end()) {
    // delete the index objects (ignore errors)
    store->svc()->bi->clean_index(dpp, bucket_info, prev.current_index);
  }
  return 0;
} // commit_reshard

int RGWBucketReshard::clear_resharding(rgw::sal::RadosStore* store,
                                       RGWBucketInfo& bucket_info,
				       std::map<std::string, bufferlist>& bucket_attrs,
                                       const DoutPrefixProvider* dpp, optional_yield y)
{
  ReshardFaultInjector no_fault;
  return cancel_reshard(store, bucket_info, bucket_attrs, no_fault, dpp, y);
}

int RGWBucketReshard::cancel(const DoutPrefixProvider* dpp, optional_yield y)
{
  constexpr bool ephemeral = true;
  RGWBucketReshardLock reshard_lock(store, bucket_info, ephemeral);

  int ret = reshard_lock.lock(dpp);
  if (ret < 0) {
    return ret;
  }

  if (bucket_info.layout.resharding != rgw::BucketReshardState::InProgress &&
      bucket_info.layout.resharding != rgw::BucketReshardState::InLogrecord) {
    ldpp_dout(dpp, -1) << "ERROR: bucket is not resharding" << dendl;
    ret = -EINVAL;
  } else {
    ret = clear_resharding(store, bucket_info, bucket_attrs, dpp, y);
  }

  reshard_lock.unlock();
  return ret;
}

RGWBucketReshardLock::RGWBucketReshardLock(rgw::sal::RadosStore* _store,
					   const std::string& reshard_lock_oid,
					   bool _ephemeral) :
  store(_store),
  lock_oid(reshard_lock_oid),
  ephemeral(_ephemeral),
  internal_lock(reshard_lock_name)
{
  const int lock_dur_secs = store->ctx()->_conf.get_val<uint64_t>(
    "rgw_reshard_bucket_lock_duration");
  duration = std::chrono::seconds(lock_dur_secs);

  constexpr size_t COOKIE_LEN = 16;
  internal_lock.set_cookie(gen_rand_alphanumeric(store->ctx(), COOKIE_LEN));
  internal_lock.set_duration(duration);
}

int RGWBucketReshardLock::lock(const DoutPrefixProvider *dpp) {
  internal_lock.set_must_renew(false);

  int ret;
  if (ephemeral) {
    ret = internal_lock.lock_exclusive_ephemeral(&store->getRados()->reshard_pool_ctx,
						 lock_oid);
  } else {
    ret = internal_lock.lock_exclusive(&store->getRados()->reshard_pool_ctx, lock_oid);
  }

  if (ret == -EBUSY) {
    ldout(store->ctx(), 0) << "INFO: RGWReshardLock::" << __func__ <<
      " found lock on " << lock_oid <<
      " to be held by another RGW process; skipping for now" << dendl;
    return ret;
  } else if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: RGWReshardLock::" << __func__ <<
      " failed to acquire lock on " << lock_oid << ": " <<
      cpp_strerror(-ret) << dendl;
    return ret;
  }

  return 0;
}

void RGWBucketReshardLock::unlock() {
  int ret = internal_lock.unlock(&store->getRados()->reshard_pool_ctx, lock_oid);
  if (ret < 0) {
    ldout(store->ctx(), 0) << "WARNING: RGWBucketReshardLock::" << __func__ <<
      " failed to drop lock on " << lock_oid << " ret=" << ret << dendl;
  }
}

auto RGWBucketReshardLock::make_client(boost::asio::yield_context yield)
  -> ceph::async::RadosLockClient
{
  return {yield.get_executor(), store->getRados()->reshard_pool_ctx,
      lock_oid, internal_lock, ephemeral};
}

static int calc_target_shard(const DoutPrefixProvider* dpp, RGWRados* store,
                             const RGWBucketInfo& bucket_info,
                             const rgw_obj_key& key, int& shard)
{
  int target_shard_id, ret;

  rgw_obj obj(bucket_info.bucket, key);
  RGWMPObj mp;
  if (key.ns == RGW_OBJ_NS_MULTIPART && mp.from_meta(key.name)) {
    // place the multipart .meta object on the same shard as its head object
    obj.index_hash_source = mp.get_key();
  }
  ret = store->get_target_shard_id(bucket_info.layout.target_index->layout.normal,
                                   obj.get_hash_object(), &target_shard_id);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: get_target_shard_id() returned ret=" << ret << dendl;
    return ret;
  }
  shard = (target_shard_id > 0 ? target_shard_id : 0);

  return 0;
}

static int process_source_shard(const DoutPrefixProvider *dpp,
                                boost::asio::yield_context y,
                                RGWRados* store, const RGWBucketInfo& bucket_info,
                                const rgw::bucket_index_layout_generation& current,
                                int source_shard, int max_op_entries,
                                BucketReshardManager& target_shards_mgr,
                                bool verbose_json_out, ostream *out,
                                Formatter *formatter, uint64_t& stage_entries,
                                ReshardFaultInjector& fault, bool process_log)
{
  bool is_truncated = true;
  string marker;
  const std::string filter; // empty string since we're not filtering by object
  while (is_truncated) {
    std::list<rgw_cls_bi_entry> entries;

    int ret = store->bi_list(dpp, bucket_info, source_shard, filter,
                             marker, max_op_entries, &entries,
                             &is_truncated, process_log, y);
    if (ret == -ENOENT) {
      ldpp_dout(dpp, 1) << "WARNING: " << __func__ << " failed to find shard "
          << source_shard << ", skipping" << dendl;
      // break out of the is_truncated loop and move on to the next shard
      break;
    } else if (ret < 0) {
      derr << "ERROR: bi_list(): " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    for (auto& entry : entries) {
      marker = entry.idx;

      cls_rgw_obj_key cls_key;
      RGWObjCategory category;
      rgw_bucket_category_stats stats;
      bool account = entry.get_info(&cls_key, &category, &stats);
      rgw_obj_key key(cls_key);
      if (entry.type == BIIndexType::OLH && key.empty()) {
        // bogus entry created by https://tracker.ceph.com/issues/46456
        // to fix, skip so it doesn't get include in the new bucket instance
        ldpp_dout(dpp, 10) << "Dropping entry with empty name, idx=" << marker << dendl;
        continue;
      }

      int shard_index;
      ret = calc_target_shard(dpp, store, bucket_info, key, shard_index);
      if (ret < 0) {
        return ret;
      }

      target_shards_mgr.add_entry(shard_index, entry, account,
                                  category, stats, y);

      const uint64_t entry_id = stage_entries++;
      if (verbose_json_out) {
        formatter->open_object_section("entry");
        encode_json("shard_id", source_shard, formatter);
        encode_json("num_entry", entry_id, formatter);
        encode_json("entry", entry, formatter);
        formatter->close_section();
        formatter->flush(*out);
      } else if (out && !(stage_entries % 1000)) {
        (*out) << " " << stage_entries;
      }
    } // entries loop
  } // while truncated

  // check for injected errors at the very end. this way we can spawn several,
  // and the first to finish will trigger cancellation of the others
  return fault.check("process_source_shard");
}

int RGWBucketReshard::reshard_process(const rgw::bucket_index_layout_generation& current,
                                      int max_op_entries,
                                      librados::IoCtx& pool,
                                      const std::map<int, std::string>& oids,
                                      bool can_put_entries,
                                      bool verbose_json_out,
                                      ostream *out,
                                      Formatter *formatter, rgw::BucketReshardState reshard_stage,
                                      ReshardFaultInjector& fault,
                                      const DoutPrefixProvider *dpp,
                                      boost::asio::yield_context y)
{
  list<rgw_cls_bi_entry> entries;

  string stage;
  bool process_log = false;
  switch (reshard_stage) {
  case rgw::BucketReshardState::InLogrecord:
    stage = "inventory";
    process_log = false;
    break;
  case rgw::BucketReshardState::InProgress:
    stage = "inc";
    process_log = true;
    break;
  default:
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << " unknown reshard stage" << dendl;
    return -EINVAL;
  }
  stage.append("_entries");
  if (verbose_json_out) {
    formatter->open_array_section(stage);
  }

  uint64_t stage_entries = 0;
  stage.append(":");
  if (!verbose_json_out && out) {
    (*out) << "start time: " << real_clock::now() << std::endl;
    (*out) << stage;
  }

  auto& conf = store->ctx()->_conf;
  const uint64_t shard_max_aio = conf.get_val<uint64_t>("rgw_reshard_max_aio");
  const size_t batch_size = conf.get_val<uint64_t>("rgw_reshard_batch_size");
  const bool check_existing = process_log;
  BucketReshardManager target_shards_mgr(y.get_executor(), shard_max_aio, pool,
                                         oids, batch_size, can_put_entries,
                                         check_existing);

  const uint64_t max_aio = conf.get_val<uint64_t>("rgw_bucket_index_max_aio");
  constexpr auto on_error = ceph::async::cancel_on_error::all;
  auto group = ceph::async::spawn_throttle{y, max_aio, on_error};

  try {
    const uint32_t num_source_shards = rgw::num_shards(current.layout.normal);
    for (uint32_t i = 0; i < num_source_shards; ++i) {
      group.spawn([&] (boost::asio::yield_context yield) {
          int ret = process_source_shard(dpp, yield, store->getRados(),
                                         bucket_info, current, i, max_op_entries,
                                         target_shards_mgr, verbose_json_out, out,
                                         formatter, stage_entries, fault,
                                         process_log);
          if (ret < 0) {
            throw boost::system::system_error(
                -ret, boost::system::system_category());
          }
        });
    }

    // wait for all source shards to complete
    group.wait();

  } catch (const std::exception& e) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__
        << " process_source_shard failed: " << e.what() << dendl;
    target_shards_mgr.drain(y);
    throw;
  }

  // flush and drain all requests to target shards
  target_shards_mgr.flush();
  target_shards_mgr.drain(y);

  if (verbose_json_out) {
    formatter->close_section();
    formatter->flush(*out);
  } else if (out) {
    (*out) << " " << stage_entries << std::endl;
    (*out) << "end time: " << real_clock::now() << std::endl;
  }

  return 0;
}

int RGWBucketReshard::do_reshard(const rgw::bucket_index_layout_generation& current,
                                 const rgw::bucket_index_layout_generation& target,
                                 int max_op_entries, // max num to process per op
                                 bool support_logrecord, const jspan& trace,
				 bool verbose,
				 ostream *out,
				 Formatter *formatter,
                                 ReshardFaultInjector& fault,
                                 const DoutPrefixProvider *dpp,
                                 boost::asio::yield_context y)
{
  if (out) {
    (*out) << "tenant: " << bucket_info.bucket.tenant << std::endl;
    (*out) << "bucket name: " << bucket_info.bucket.name << std::endl;
  }

  if (max_op_entries <= 0) {
    ldpp_dout(dpp, 0) << __func__ <<
      ": can't reshard, non-positive max_op_entries" << dendl;
    return -EINVAL;
  }

  librados::IoCtx pool;
  std::map<int, std::string> oids;
  int ret = store->svc()->bi_rados->open_bucket_index(
      dpp, bucket_info, std::nullopt, target, &pool, &oids, nullptr);
  if (ret < 0) {
    return ret;
  }

  bool verbose_json_out = verbose && (formatter != nullptr) && (out != nullptr);

  if (support_logrecord) {
    // a log is written to shard going with client op at this state
    ceph_assert(bucket_info.layout.resharding == rgw::BucketReshardState::InLogrecord);
    int ret = reshard_process(current, max_op_entries, pool, oids,
                              support_logrecord, verbose_json_out, out,
                              formatter, bucket_info.layout.resharding,
                              fault, dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << ": failed in logrecord state of reshard ret = " << ret << dendl;
      return ret;
    }

    ret = change_reshard_state(store, bucket_info, bucket_attrs, fault, dpp, y);
    if (ret < 0) {
      return ret;
    }

    // block the client op and complete the resharding
    ceph_assert(bucket_info.layout.resharding == rgw::BucketReshardState::InProgress);
    [[maybe_unused]] auto span = tracing::rgw::tracer.add_span("blocked", trace.GetContext());
    ret = reshard_process(current, max_op_entries, pool, oids,
                          support_logrecord, verbose_json_out, out,
                          formatter, bucket_info.layout.resharding,
                          fault, dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << ": failed in progress state of reshard ret = " << ret << dendl;
      return ret;
    }
  } else {
    // setting InProgress state, but doing InLogrecord state
    ceph_assert(bucket_info.layout.resharding == rgw::BucketReshardState::InProgress);
    [[maybe_unused]] auto span = tracing::rgw::tracer.add_span("blocked", trace.GetContext());
    int ret = reshard_process(current, max_op_entries, pool, oids,
                              support_logrecord, verbose_json_out, out,
                              formatter, rgw::BucketReshardState::InLogrecord,
                              fault, dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << ": failed in logrecord state of reshard ret = " << ret << dendl;
      return ret;
    }
  }
  return 0;
} // RGWBucketReshard::do_reshard

int RGWBucketReshard::get_status(const DoutPrefixProvider *dpp, list<cls_rgw_bucket_instance_entry> *status)
{
  return store->svc()->bi_rados->get_reshard_status(dpp, bucket_info, status);
}

int RGWBucketReshard::execute(int num_shards,
                              ReshardFaultInjector& fault,
                              int max_op_entries,  // max num to process per op
			      const cls_rgw_reshard_initiator initiator,
                              const DoutPrefixProvider *dpp,
			      boost::asio::yield_context y,
                              const jspan& trace, bool verbose,
			      ostream *out,
                              Formatter *formatter,
                              RGWReshard* reshard_log)
{
  constexpr bool ephemeral = true;
  RGWBucketReshardLock lock(store, bucket_info, ephemeral);
  auto client = lock.make_client(y);

  try {
    // call execute_locked() under the protection of cls_lock
    return ceph::async::with_lease(client, lock.get_duration(), y,
        [&] (boost::asio::yield_context yield) {
          if (reshard_log) {
            // update initiator once the lock is acquired
            int ret = reshard_log->update(dpp, yield, bucket_info.bucket, initiator);
            if (ret < 0) {
              return ret;
            }
          }
          return execute_locked(num_shards, fault, max_op_entries,
                                dpp, yield, trace, verbose, out, formatter);
        });
  } catch (const ceph::async::lease_aborted& e) {
    ldpp_dout(dpp, 1) << "bucket reshard lease aborted with " << e.code() << dendl;
    return ceph::from_error_code(e.code());
  } catch (const boost::system::system_error& e) {
    ldpp_dout(dpp, 1) << "bucket reshard failed with " << e.what() << dendl;
    return ceph::from_error_code(e.code());
  }
}

int RGWBucketReshard::execute_locked(int num_shards, ReshardFaultInjector& fault,
                                     int max_op_entries, const DoutPrefixProvider *dpp,
                                     boost::asio::yield_context y, const jspan& trace,
                                     bool verbose, std::ostream *out,
                                     ceph::Formatter *formatter)
{
  auto current_num_shards = rgw::num_shards(bucket_info.layout.current_index);

  auto span = tracing::rgw::tracer.add_span("reshard", trace.GetContext());
  span->SetAttribute("bucket", bucket_info.bucket.name);
  span->SetAttribute("tenant", bucket_info.bucket.tenant);
  span->SetAttribute("instance", bucket_info.bucket.bucket_id);
  span->SetAttribute("source_shards", current_num_shards);
  span->SetAttribute("target_shards", num_shards);

  bool support_logrecord = true;
  // prepare the target index and add its layout the bucket info
  int ret = init_reshard(store, bucket_info, bucket_attrs, fault, num_shards,
                         support_logrecord, dpp, y);
  if (ret < 0) {
    return ret;
  }

  if (ret = fault.check("do_reshard");
      ret == 0) { // no fault injected, do the reshard
    ret = do_reshard(bucket_info.layout.current_index,
                     *bucket_info.layout.target_index,
                     max_op_entries, support_logrecord, *span,
                     verbose, out, formatter, fault, dpp, y);
  }

  if (ret < 0) {
    cancel_reshard(store, bucket_info, bucket_attrs, fault, dpp, y);

    ldpp_dout(dpp, 1) << __func__ << " INFO: reshard of bucket \""
        << bucket_info.bucket.name << "\" canceled due to errors" << dendl;
    return ret;
  }

  ret = commit_reshard(store, bucket_info, bucket_attrs, fault, dpp, y);
  if (ret < 0) {
    return ret;
  }

  ldpp_dout(dpp, 1) << __func__ << " INFO: reshard of bucket \"" <<
    bucket_info.bucket.name << "\" from " <<
    current_num_shards << " shards to " << num_shards <<
    " shards completed successfully" << dendl;

  return 0;
} // execute

bool RGWBucketReshard::should_zone_reshard_now(const RGWBucketInfo& bucket,
					       const RGWSI_Zone* zone_svc)
{
  return !zone_svc->need_to_log_data() ||
    bucket.layout.logs.size() < max_bilog_history;
}


RGWReshard::RGWReshard(rgw::sal::RadosStore* _store, bool _verbose, ostream *_out,
                       Formatter *_formatter) :
  store(_store), verbose(_verbose), out(_out), formatter(_formatter)
{
  num_logshards = store->ctx()->_conf.get_val<uint64_t>("rgw_reshard_num_logs");
}

string RGWReshard::get_logshard_key(const string& tenant,
				    const string& bucket_name)
{
  return tenant + ":" + bucket_name;
}

#define MAX_RESHARD_LOGSHARDS_PRIME 7877

void RGWReshard::get_bucket_logshard_oid(const string& tenant, const string& bucket_name, string *oid)
{
  string key = get_logshard_key(tenant, bucket_name);

  uint32_t sid = ceph_str_hash_linux(key.c_str(), key.size());
  uint32_t sid2 = sid ^ ((sid & 0xFF) << 24);
  sid = sid2 % MAX_RESHARD_LOGSHARDS_PRIME % num_logshards;

  get_logshard_oid(int(sid), oid);
}

int RGWReshard::add(const DoutPrefixProvider *dpp, cls_rgw_reshard_entry& entry, optional_yield y)
{
  if (!store->svc()->zone->can_reshard()) {
    ldpp_dout(dpp, 20) << __func__ << " Resharding is disabled"  << dendl;
    return 0;
  }

  string logshard_oid;

  get_bucket_logshard_oid(entry.tenant, entry.bucket_name, &logshard_oid);

  librados::ObjectWriteOperation op;

  // if this is dynamic resharding and we're reducing, we don't want
  // to overwrite an existing entry in order to not interfere with the
  // reshard reduction wait period
  const bool create_only =
    entry.initiator == cls_rgw_reshard_initiator::Dynamic &&
    entry.new_num_shards < entry.old_num_shards;

  cls_rgw_reshard_add(op, entry, create_only);

  int ret = rgw_rados_operate(dpp, store->getRados()->reshard_pool_ctx, logshard_oid, &op, y);
  if (create_only && ret == -EEXIST) {
    ldpp_dout(dpp, 20) <<
      "INFO: did not write reshard queue entry for oid=" <<
      logshard_oid << " tenant=" << entry.tenant << " bucket=" <<
      entry.bucket_name <<
      ", because it's a dynamic reshard reduction and an entry for that "
      "bucket already exists" << dendl;
    // this is not an error so just fall through
  } else if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to add entry to reshard log, oid=" <<
      logshard_oid << " tenant=" << entry.tenant << " bucket=" <<
      entry.bucket_name << dendl;
    return ret;
  }
  return 0;
}

int RGWReshard::update(const DoutPrefixProvider* dpp,
		       optional_yield y,
		       const rgw_bucket& bucket,
		       cls_rgw_reshard_initiator initiator)
{
  cls_rgw_reshard_entry entry;

  int ret = get(dpp, y, bucket, entry);
  if (ret < 0) {
    return ret;
  }

  entry.initiator = initiator;

  ret = add(dpp, entry, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << __func__ << ": Error in updating entry bucket " << entry.bucket_name << ": " <<
      cpp_strerror(-ret) << dendl;
  }

  return ret;
}


int RGWReshard::list(const DoutPrefixProvider *dpp, optional_yield y,
                     int logshard_num, string& marker, uint32_t max,
                     std::vector<cls_rgw_reshard_entry>& entries,
                     bool *is_truncated)
{
  string logshard_oid;

  get_logshard_oid(logshard_num, &logshard_oid);

  bufferlist bl;
  librados::ObjectReadOperation op;
  cls_rgw_reshard_list(op, marker, max, bl);

  int ret = rgw_rados_operate(dpp, store->getRados()->reshard_pool_ctx,
                              logshard_oid, &op, nullptr, y);
  if (ret == -ENOENT) {
    // these shard objects aren't created until we actually write something to
    // them, so treat ENOENT as a successful empty listing
    *is_truncated = false;
    return 0;
  }
  if (ret == -EACCES) {
    ldpp_dout(dpp, -1) << "ERROR: access denied to pool " << store->svc()->zone->get_zone_params().reshard_pool
                      << ". Fix the pool access permissions of your client" << dendl;
    return ret;
  }
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to list reshard log entries, oid="
        << logshard_oid << " marker=" << marker << " " << cpp_strerror(ret) << dendl;
    return ret;
  }

  return cls_rgw_reshard_list_decode(bl, entries, is_truncated);
}

int RGWReshard::get(const DoutPrefixProvider *dpp, optional_yield y,
                    const rgw_bucket& bucket, cls_rgw_reshard_entry& entry)
{
  string logshard_oid;

  get_bucket_logshard_oid(bucket.tenant, bucket.name, &logshard_oid);

  bufferlist bl;
  librados::ObjectReadOperation op;
  cls_rgw_reshard_get(op, bucket.tenant, bucket.name, bl);

  int ret = rgw_rados_operate(dpp, store->getRados()->reshard_pool_ctx,
                              logshard_oid, &op, nullptr, y);
  if (ret < 0) {
    if (ret != -ENOENT) {
      ldpp_dout(dpp, -1) << "ERROR: failed to get entry from reshard log, oid=" << logshard_oid << " tenant=" << entry.tenant <<
	" bucket=" << entry.bucket_name << dendl;
    }
    return ret;
  }

  return cls_rgw_reshard_get_decode(bl, entry);
}

int RGWReshard::remove(const DoutPrefixProvider *dpp, const cls_rgw_reshard_entry& entry, optional_yield y)
{
  string logshard_oid;

  get_bucket_logshard_oid(entry.tenant, entry.bucket_name, &logshard_oid);

  librados::ObjectWriteOperation op;
  cls_rgw_reshard_remove(op, entry);

  int ret = rgw_rados_operate(dpp, store->getRados()->reshard_pool_ctx, logshard_oid, &op, y);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to remove entry from reshard log, oid=" << logshard_oid << " tenant=" << entry.tenant << " bucket=" << entry.bucket_name << dendl;
    return ret;
  }

  return ret;
}

int RGWReshard::clear_bucket_resharding(const DoutPrefixProvider *dpp, const string& bucket_instance_oid, cls_rgw_reshard_entry& entry)
{
  int ret = cls_rgw_clear_bucket_resharding(store->getRados()->reshard_pool_ctx, bucket_instance_oid);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR: failed to clear bucket resharding, bucket_instance_oid=" << bucket_instance_oid << dendl;
    return ret;
  }

  return 0;
}

int RGWReshardWait::wait(const DoutPrefixProvider* dpp, optional_yield y)
{
  std::unique_lock lock(mutex);

  if (going_down) {
    return -ECANCELED;
  }

  if (y) {
    auto& yield = y.get_yield_context();

    Waiter waiter(yield.get_executor());
    waiters.push_back(waiter);
    lock.unlock();

    waiter.timer.expires_after(duration);

    boost::system::error_code ec;
    waiter.timer.async_wait(yield[ec]);

    lock.lock();
    waiters.erase(waiters.iterator_to(waiter));
    return -ec.value();
  }
  maybe_warn_about_blocking(dpp);

  cond.wait_for(lock, duration);

  if (going_down) {
    return -ECANCELED;
  }

  return 0;
}

void RGWReshardWait::stop()
{
  std::scoped_lock lock(mutex);
  going_down = true;
  cond.notify_all();
  for (auto& waiter : waiters) {
    // unblock any waiters with ECANCELED
    waiter.timer.cancel();
  }
}

int RGWReshard::process_entry(const cls_rgw_reshard_entry& entry,
                              int max_op_entries, // max num to process per op
			      const DoutPrefixProvider* dpp,
			      boost::asio::yield_context y,
			      const jspan& trace)
{
  ldpp_dout(dpp, 20) << __func__ << " resharding " <<
      entry.bucket_name  << dendl;

  rgw_bucket bucket;
  RGWBucketInfo bucket_info;
  std::map<std::string, bufferlist> bucket_attrs;

  // removes the entry and logs a message
  auto clean_up = [this, &dpp, &entry, &y](const std::string_view& reason = "") -> int {
    int ret = remove(dpp, entry, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) <<
	"ERROR removing bucket \"" << entry.bucket_name <<
	"\" from resharding queue, because " <<
	(reason.empty() ? "resharding complete" : reason) <<
	"; error is " <<
	cpp_strerror(-ret) << dendl;
      return ret;
    }

    if (! reason.empty()) {
      ldpp_dout(dpp, 10) <<
	"WARNING: processing reshard reduction on bucket \"" <<
	entry.bucket_name << "\", but cancelling because " <<
	reason << dendl;
    }

    return 0;
  };

  int ret = store->getRados()->get_bucket_info(store->svc(),
                                               entry.tenant,
					       entry.bucket_name,
                                               bucket_info, nullptr,
                                               y, dpp,
					       &bucket_attrs);
  if (ret < 0 || bucket_info.bucket.bucket_id != entry.bucket_id) {
    if (ret < 0) {
      ldpp_dout(dpp, 0) <<  __func__ <<
          ": Error in get_bucket_info for bucket " << entry.bucket_name <<
          ": " << cpp_strerror(-ret) << dendl;
      if (ret != -ENOENT) {
        // any error other than ENOENT will abort
        return ret;
      }

      // we've encountered a reshard queue entry for an apparently
      // non-existent bucket; let's try to recover by cleaning up
      return clean_up("bucket does not currently exist");
    } else {
      return clean_up("bucket already resharded");
    }
  }

  // if *dynamic* reshard reduction, perform extra sanity checks in
  // part to prevent chasing constantly changing entry count. If
  // *admin*-initiated (or unknown-initiated) reshard reduction, skip
  // this step and proceed.
  if (entry.initiator == cls_rgw_reshard_initiator::Dynamic &&
      entry.new_num_shards < entry.old_num_shards) {
    const bool may_reduce =
      store->ctx()->_conf.get_val<bool>("rgw_dynamic_resharding_may_reduce");
    if (! may_reduce) {
      return clean_up("current configuration does not allow reshard reduction");
    }

    // determine how many entries there are in the bucket index
    std::map<RGWObjCategory, RGWStorageStats> stats;
    ret = store->getRados()->get_bucket_stats(dpp, bucket_info,
					      bucket_info.layout.current_index,
					      -1, nullptr, nullptr, stats, nullptr, nullptr);

    // determine current number of bucket entries across shards
    uint64_t num_entries = 0;
    for (const auto& s : stats) {
      num_entries += s.second.num_objects;
    }

    const uint32_t current_shard_count =
      rgw::num_shards(bucket_info.get_current_index().layout.normal);

    bool needs_resharding { false };
    uint32_t suggested_shard_count { 0 };
    // calling this rados function determines various rados values
    // needed to perform the calculation before calling
    // calculating_preferred_shards() in this class
    store->getRados()->calculate_preferred_shards(
      dpp, num_entries, current_shard_count,
      needs_resharding, &suggested_shard_count);

    // if we no longer need resharding or currently need to expand
    // number of shards, drop this request
    if (! needs_resharding || suggested_shard_count > current_shard_count) {
      return clean_up("reshard reduction no longer appropriate");
    }

    // see if it's been long enough since this reshard queue entry was
    // added to actually do the reshard reduction
    ceph::real_time when_queued = entry.time;
    ceph::real_time now = real_clock::now();

    // use double so we can handle fractions
    double reshard_reduction_wait_hours =
      uint32_t(store->ctx()->_conf.get_val<uint64_t>("rgw_dynamic_resharding_reduction_wait"));

    // see if we have to reduce the waiting interval due to debug
    // config
    int debug_interval = store->ctx()->_conf.get_val<int64_t>("rgw_reshard_debug_interval");
    if (debug_interval >= 1) {
      constexpr int secs_per_day = 60 * 60 * 24;
      reshard_reduction_wait_hours = reshard_reduction_wait_hours * debug_interval / secs_per_day;
    }

    auto timespan = std::chrono::seconds(int(60 * 60 * reshard_reduction_wait_hours));
    if (now < when_queued + timespan) {
      // too early to reshard; log and skip
      ldpp_dout(dpp, 20) <<  __func__ <<
	": INFO: reshard reduction for bucket \"" <<
	entry.bucket_name << "\" will not proceed until " <<
	(when_queued + timespan) << dendl;

      return 0;
    }

    // only if we allow the resharding logic to continue should we log
    // the fact that the reduction_wait_time was shortened due to
    // debugging mode
    if (debug_interval >= 1) {
      ldpp_dout(dpp, 0) << "DEBUG: since the rgw_reshard_debug_interval is set at " <<
	debug_interval << " the rgw_dynamic_resharding_reduction_wait is now " <<
	reshard_reduction_wait_hours << " hours (" <<
	int(reshard_reduction_wait_hours * 60 * 60) << " seconds) and bucket \"" <<
	entry.bucket_name << "\" has reached the reduction wait period" << dendl;
    }

    // all checks passed; we can drop through and proceed
  }

  if (!RGWBucketReshard::should_zone_reshard_now(bucket_info, store->svc()->zone)) {
    return clean_up("bucket not eligible for resharding until peer "
		    "zones finish syncing one or more of its old log "
		    "generations");
  }

  // all checkes passed; we can reshard...

  RGWBucketReshard br(store, bucket_info, bucket_attrs);

  ReshardFaultInjector f; // no fault injected
  ret = br.execute(entry.new_num_shards, f, max_op_entries, entry.initiator,
                   dpp, y, trace, false, nullptr, nullptr, this);
  if (ret < 0) {
    ldpp_dout(dpp, 0) <<  __func__ <<
        ": Error during resharding bucket " << entry.bucket_name << ":" <<
        cpp_strerror(-ret)<< dendl;
    return ret;
  }

  ldpp_dout(dpp, 20) << __func__ <<
      " removing reshard queue entry for bucket " << entry.bucket_name <<
      dendl;

  return clean_up();
} // RGWReshard::process_entry


int RGWReshard::process_single_logshard(int logshard_num, const DoutPrefixProvider *dpp,
                                        boost::asio::yield_context y,
                                        const jspan& trace)
{
  std::string marker;
  bool is_truncated = true;

  // This is the number to request per op, whether it's reshard queue
  // entries or bucket index entries. Should not be confused with the
  // number of entries we allow in a bucket index shard. This value is
  // passed in and used deeper into the call chain as well.
  constexpr uint32_t max_op_entries = 1000;

  string logshard_oid;
  get_logshard_oid(logshard_num, &logshard_oid);

  do {
    std::vector<cls_rgw_reshard_entry> entries;
    int ret = list(dpp, y, logshard_num, marker, max_op_entries, entries, &is_truncated);
    if (ret < 0) {
      ldpp_dout(dpp, 10) << "cannot list all reshards in logshard oid=" <<
	logshard_oid << dendl;
      return ret;
    }

    for(const auto& entry : entries) { // logshard entries
      process_entry(entry, max_op_entries, dpp, y, trace);

      entry.get_key(&marker);
    } // entry for loop
  } while (is_truncated && y.cancelled() == boost::asio::cancellation_type::none);

  return 0;
}


void RGWReshard::get_logshard_oid(int shard_num, string *logshard)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%010u", (unsigned)shard_num);

  string objname(reshard_oid_prefix);
  *logshard =  objname + buf;
}

int RGWReshard::process_all_logshards(const DoutPrefixProvider *dpp,
                                      boost::asio::yield_context y,
                                      const jspan& trace)
{
  for (int i = 0; i < num_logshards; i++) {
    string logshard;
    get_logshard_oid(i, &logshard);

    ldpp_dout(dpp, 20) << "processing logshard = " << logshard << dendl;

    constexpr bool ephemeral = false;
    RGWBucketReshardLock lock(store, logshard, ephemeral);
    auto client = lock.make_client(y);

    try {
      // call process_single_logshard() under the protection of cls_lock
      ceph::async::with_lease(client, lock.get_duration(), y,
          [this, i, &trace, dpp] (boost::asio::yield_context yield) {
            process_single_logshard(i, dpp, yield, trace);
          });
    } catch (const ceph::async::lease_aborted& e) {
      ldpp_dout(dpp, 1) << "reshard log lease aborted with " << e.code() << dendl;
    } catch (const std::exception& e) {
      ldpp_dout(dpp, 1) << "reshard processing failed with " << e.what() << dendl;
    }

    ldpp_dout(dpp, 20) << "finish processing logshard = " << logshard << dendl;

    if (y.cancelled() != boost::asio::cancellation_type::none) {
      break;
    }
  }

  return 0;
}

static void reshard_worker(const DoutPrefixProvider* dpp,
                           rgw::sal::RadosStore* store,
                           boost::asio::yield_context yield)
{
  RGWReshard reshard(store);
  const auto& conf = store->ctx()->_conf;
  auto trace = tracing::rgw::tracer.start_trace("reshard_worker");

  using Clock = ceph::coarse_mono_clock;
  auto timer = boost::asio::basic_waitable_timer<Clock>{yield.get_executor()};

  for (;;) { // until cancellation
    const auto start = Clock::now();
    reshard.process_all_logshards(dpp, yield, *trace);

    timer.expires_at(start + std::chrono::seconds{
        conf.get_val<uint64_t>("rgw_reshard_thread_interval")});
    timer.async_wait(yield);
  }
}

namespace rgwrados::reshard {

auto start(rgw::sal::RadosStore* store,
           boost::asio::io_context& ctx,
           boost::asio::cancellation_signal& signal)
  -> std::future<void>
{
  // spawn reshard_worker() as a cancellable coroutine on a strand executor
  return boost::asio::spawn(
      boost::asio::make_strand(ctx),
      [store] (boost::asio::yield_context yield) {
        const DoutPrefix dpp{store->ctx(), dout_subsys, "reshard worker: "};
        reshard_worker(&dpp, store, yield);
      }, boost::asio::bind_cancellation_slot(signal.slot(),
            boost::asio::bind_executor(ctx,
                boost::asio::use_future)));
}

void stop(boost::asio::cancellation_signal& signal,
          std::future<void>& future)
{
  signal.emit(boost::asio::cancellation_type::terminal);
  try { future.get(); } catch (const std::exception&) {} // ignore
}

} // namespace rgwrados::reshard
