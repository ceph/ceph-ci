// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <map>
#include <string>
#include <iostream>

#include "common/debug.h"

#include "include/types.h"
#include "include/rados/librados.hpp"
#include "common/iso_8601.h"
#include "rgw_common.h"
#include "cls/rgw/cls_rgw_types.h"
#include "rgw_sal.h"
#include "rgw_notify.h"
#include "rgw_restore_waiter.h"

#include <future>
#include <optional>
#include <tuple>

#include <boost/asio/io_context.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/basic_waitable_timer.hpp>

#include "common/ceph_time.h"

#define HASH_PRIME 7877
#define MAX_ID_LEN 255
static constexpr std::string_view restore_oid_prefix = "restore";
static constexpr std::string_view restore_index_lock_name = "restore_process";
static constexpr std::string_view restore_lock_cookie = "restore_thrd: ";

namespace rgw::restore {

inline bool is_transient_restore_error(int ret) {
  return ret == -EIO || ret == -ECONNABORTED || ret == -EAGAIN ||
         ret == -EBUSY || ret == -ERR_INTERNAL_ERROR;
}

/** Single Restore entry state */
struct RestoreEntry {
  rgw_bucket bucket;
  rgw_obj_key obj_key;
  std::optional<uint64_t> days;
  std::string zone_id; // or should it be zone name?
  rgw::sal::RGWRestoreStatus status{rgw::sal::RGWRestoreStatus::None};
  uint32_t retry_count{0};
  ceph::real_time enqueue_time{ceph::real_time::min()};
  ceph::real_time next_retry_time{ceph::real_time::min()};

  RestoreEntry() {}

  bool schedule_retry(ceph::real_time now,
                      std::chrono::seconds retry_window,
                      std::chrono::seconds retry_period) {
    if (now - enqueue_time >= retry_window) {
      return false;
    }
    ++retry_count;
    constexpr std::chrono::seconds max_delay{60 * 60};
    std::chrono::seconds delay = std::min(retry_period, max_delay);
    for (uint32_t i = 1; i < retry_count && delay < max_delay; ++i) {
      delay = std::min(max_delay, delay * 3 / 2);
    }
    next_retry_time = now + delay;
    status = rgw::sal::RGWRestoreStatus::RestoreAlreadyInProgress;
    return true;
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(bucket, bl);
    encode(obj_key, bl);
    encode(days, bl);
    encode(zone_id, bl);
    encode(status, bl);
    encode(retry_count, bl);
    encode(enqueue_time, bl);
    encode(next_retry_time, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(bucket, bl);
    decode(obj_key, bl);
    decode(days, bl);
    decode(zone_id, bl);
    decode(status, bl);
    if (struct_v >= 2) {
      decode(retry_count, bl);
      decode(enqueue_time, bl);
      decode(next_retry_time, bl);
    } else {
      enqueue_time = ceph::real_clock::now();
    }
    DECODE_FINISH(bl);
  }
  void dump(ceph::Formatter* f) const;
  void decode_json(JSONObj* obj);
  static void generate_test_instances(std::list<rgw::restore::RestoreEntry*>& l);
};
WRITE_CLASS_ENCODER(RestoreEntry)

class Restore : public DoutPrefixProvider {
  CephContext *cct;
  rgw::sal::Driver* driver;
  std::unique_ptr<rgw::sal::Restore> sal_restore;
  int max_objs{0};
  std::vector<std::string> obj_names;
  std::shared_ptr<RestoreWaiterRegistry> waiter_registry;

  using executor_t = boost::asio::io_context::executor_type;
  std::optional<boost::asio::strand<executor_t>> proc_strand;
  std::optional<boost::asio::basic_waitable_timer<ceph::coarse_mono_clock>> proc_timer;
  boost::asio::cancellation_signal proc_signal;
  std::future<void> proc_future;

public:
  ~Restore() {
    if (proc_future.valid()) {
      ldpp_dout(this, -1) << "ERROR: Restore destructed without stop_processor" << dendl;
    }
    finalize();
  }

  friend class RGWRados;

  Restore() : cct(nullptr), driver(nullptr), max_objs(0) {}

  int initialize(CephContext *_cct, rgw::sal::Driver* _driver);
  void finalize();

  void start_processor(boost::asio::io_context& context);
  void stop_processor();
  void wake_worker();
  std::shared_ptr<RestoreWaiterRegistry> get_waiter_registry() const { return waiter_registry; }

  CephContext *get_cct() const override { return cct; }
  rgw::sal::Restore* get_restore() const { return sal_restore.get(); }
  unsigned get_subsys() const;

  std::ostream& gen_prefix(std::ostream& out) const;

  int process(boost::asio::yield_context yield);
  void process_cycles(boost::asio::yield_context yield);
  int choose_oid(const rgw::restore::RestoreEntry& e);
  int process(int index, int max_secs, boost::asio::yield_context yield);
  int process_locked(int index, int max_secs, boost::asio::yield_context yield);
  int process_restore_entry(rgw::restore::RestoreEntry& entry, optional_yield y);
  time_t thread_stop_at();

  /** Set the restore status for the given object */
  int set_cloud_restore_status(const DoutPrefixProvider* dpp, rgw::sal::Object* pobj,
		  	   optional_yield y,
			   const rgw::sal::RGWRestoreStatus& restore_status);

  /** Calculate expiration date based on expiry days */
  void get_expiration_date(const DoutPrefixProvider* dpp,
                           int expiry_days, ceph::real_time& exp_date);

  /** Update expiry date for temp restored copies */
  int update_cloud_restore_exp_date(rgw::sal::Bucket* pbucket,
	       			       rgw::sal::Object* pobj, std::optional<uint64_t> days,
				             const DoutPrefixProvider* dpp, optional_yield y);

  /** Given <bucket, obj>, restore the object from the cloud-tier. In case the
   * object cannot be restored immediately, save that restore state(/entry) 
   * to be procesed later by the restore processor. */
  int restore_obj_from_cloud(rgw::sal::Bucket* pbucket, rgw::sal::Object* pobj,
		  	     rgw::sal::PlacementTier* tier,
			     std::optional<uint64_t> days,
			     const DoutPrefixProvider* dpp,
			     optional_yield y);

  /**
   * Send notification incase of restore events
   */

  void send_notification(const DoutPrefixProvider* dpp,
                              rgw::sal::Driver* driver,
                              rgw::sal::Object* obj,
                              rgw::sal::Bucket* bucket,
                              const std::string& etag,
                              uint64_t size,
                              const std::string& version_id,
                              const rgw::notify::EventTypeList& event_types,
                              optional_yield y);
  // list restore status of objects in the bucket
  int list(const DoutPrefixProvider* dpp, RestoreEntry& entry,
           std::optional<std::string> restore_status_filter, std::string& err_msg,
           RGWFormatterFlusher& flusher, optional_yield y);

  // restore status of an object in a bucket
  int status(const DoutPrefixProvider* dpp, RestoreEntry& entry,
             std::string& err_msg, RGWFormatterFlusher& flusher,
             optional_yield y);
};

} // namespace rgw::restore
