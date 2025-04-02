// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <exception>
#include <ranges>
#include <shared_mutex> // for std::shared_lock
#include <utility>
#include <vector>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>

#include <boost/container/flat_set.hpp>
#include <boost/container/flat_map.hpp>

#include <boost/system/system_error.hpp>

#include "common/async/parallel_for_each.h"
#include "include/fs_types.h"
#include "include/neorados/RADOS.hpp"

#include "common/async/blocked_completion.h"
#include "common/async/co_throttle.h"
#include "common/async/librados_completion.h"
#include "common/async/yield_context.h"

#include "common/dout.h"
#include "common/containers.h"
#include "common/error_code.h"

#include "neorados/cls/fifo.h"
#include "neorados/cls/log.h"
#include "neorados/cls/sem_set.h"

#include "rgw_asio_thread.h"
#include "rgw_bucket.h"
#include "rgw_bucket_layout.h"
#include "rgw_datalog.h"
#include "rgw_log_backing.h"
#include "rgw_tools.h"
#include "rgw_sal_rados.h"

static constexpr auto dout_subsys = ceph_subsys_rgw;

using namespace std::literals;

namespace ranges = std::ranges;
namespace views = ranges::views;

namespace sys = boost::system;

namespace nlog = ::neorados::cls::log;
namespace fifo = ::neorados::cls::fifo;

namespace async = ceph::async;
namespace buffer = ceph::buffer;

using ceph::containers::tiny_vector;

void rgw_data_change::dump(ceph::Formatter *f) const
{
  std::string type;
  switch (entity_type) {
    case ENTITY_TYPE_BUCKET:
      type = "bucket";
      break;
    default:
      type = "unknown";
  }
  encode_json("entity_type", type, f);
  encode_json("key", key, f);
  utime_t ut(timestamp);
  encode_json("timestamp", ut, f);
  encode_json("gen", gen, f);
}

void rgw_data_change::decode_json(JSONObj *obj) {
  std::string s;
  JSONDecoder::decode_json("entity_type", s, obj);
  if (s == "bucket") {
    entity_type = ENTITY_TYPE_BUCKET;
  } else {
    entity_type = ENTITY_TYPE_UNKNOWN;
  }
  JSONDecoder::decode_json("key", key, obj);
  utime_t ut;
  JSONDecoder::decode_json("timestamp", ut, obj);
  timestamp = ut.to_real_time();
  JSONDecoder::decode_json("gen", gen, obj);
}

void rgw_data_change::generate_test_instances(std::list<rgw_data_change *>& l) {
  l.push_back(new rgw_data_change{});
  l.push_back(new rgw_data_change);
  l.back()->entity_type = ENTITY_TYPE_BUCKET;
  l.back()->key = "bucket_name";
  l.back()->timestamp = ceph::real_clock::zero();
  l.back()->gen = 0;
}

void rgw_data_change_log_entry::dump(Formatter *f) const
{
  encode_json("log_id", log_id, f);
  utime_t ut(log_timestamp);
  encode_json("log_timestamp", ut, f);
  encode_json("entry", entry, f);
}

void rgw_data_change_log_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("log_id", log_id, obj);
  utime_t ut;
  JSONDecoder::decode_json("log_timestamp", ut, obj);
  log_timestamp = ut.to_real_time();
  JSONDecoder::decode_json("entry", entry, obj);
}

void rgw_data_notify_entry::dump(Formatter *f) const
{
  encode_json("key", key, f);
  encode_json("gen", gen, f);
}

void rgw_data_notify_entry::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("key", key, obj);
  JSONDecoder::decode_json("gen", gen, obj);
}

class RGWDataChangesOmap final : public RGWDataChangesBE {
  using centries = std::vector<cls::log::entry>;
  std::vector<std::string> oids;

public:
  RGWDataChangesOmap(neorados::RADOS& r,
		     neorados::IOContext loc,
		     RGWDataChangesLog& datalog,
		     uint64_t gen_id,
		     int num_shards)
    : RGWDataChangesBE(r, std::move(loc), datalog, gen_id) {
    oids.reserve(num_shards);
    for (auto i = 0; i < num_shards; ++i) {
      oids.push_back(get_oid(i));
    }
  }
  ~RGWDataChangesOmap() override = default;

  void prepare(ceph::real_time ut, const std::string& key,
	       buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](const auto& v) { return std::empty(v); }, out));
      out = centries();
    }

    cls::log::entry e{ut, {}, key, std::move(entry)};
    std::get<centries>(out).push_back(std::move(e));
  }
  asio::awaitable<void> push(const DoutPrefixProvider *dpp, int index,
			     entries&& items) override {
    co_await r.execute(
      oids[index], loc,
      neorados::WriteOp{}.exec(nlog::add(std::get<centries>(items))),
      asio::use_awaitable);
    co_return;
  }
  asio::awaitable<void> push(const DoutPrefixProvider *dpp, int index,
			     ceph::real_time now, const std::string& key,
			     buffer::list&& bl) override {
    co_await r.execute(
      oids[index], loc,
      neorados::WriteOp{}.exec(nlog::add(now, {}, key, std::move(bl))),
      asio::use_awaitable);
    co_return;
  }

  asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>,
			     std::string>>
  list(const DoutPrefixProvider* dpp, int shard,
       std::span<rgw_data_change_log_entry> entries,
       std::string marker) override {
    std::vector<cls::log::entry> entrystore{entries.size()};

    try {
      auto [lentries, lmark] =
	co_await nlog::list(r, oids[shard], loc, {}, {}, marker, entrystore,
			    asio::use_awaitable);

      entries = entries.first(lentries.size());
      std::ranges::transform(lentries, std::begin(entries),
			     [](const auto& e) {
			       rgw_data_change_log_entry entry;
			       entry.log_id = e.id;
			       entry.log_timestamp = e.timestamp;
			       auto liter = e.data.cbegin();
			       decode(entry.entry, liter);
			       return entry;
			     });
      co_return std::make_tuple(std::move(entries), lmark);
    } catch (const buffer::error& err) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			 << ": failed to decode data changes log entry: "
			 << err.what() << dendl;
      throw;
    } catch (const sys::system_error& e) {
      if (e.code() == sys::errc::no_such_file_or_directory) {
	co_return std::make_tuple(entries.first(0), std::string{});
      } else {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			   << ": failed to list " << oids[shard]
			   << ": " << e.what() << dendl;
	throw;
      }
    }
  }
  asio::awaitable<RGWDataChangesLogInfo> get_info(const DoutPrefixProvider *dpp,
						  int index) override {
    try {
      auto header = co_await nlog::info(r, oids[index], loc,
					asio::use_awaitable);
      co_return RGWDataChangesLogInfo{.marker = header.max_marker,
				      .last_update = header.max_time};
    } catch (const sys::system_error& e) {
      if (e.code() == sys::errc::no_such_file_or_directory) {
	co_return RGWDataChangesLogInfo{};
      }
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			 << ": failed to get info from " << oids[index]
			 << ": " << e.what() << dendl;
      throw;
    }
  }
  asio::awaitable<void> trim(const DoutPrefixProvider *dpp, int index,
			     std::string_view marker) override {
    try {
      co_await nlog::trim(r, oids[index], loc, {}, std::string{marker},
			  asio::use_awaitable);
      co_return;
    } catch (const sys::system_error& e) {
      if (e.code() == sys::errc::no_such_file_or_directory) {
	co_return;
      } else {
	ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			   << ": failed to get trim " << oids[index]
			   << ": " << e.what() << dendl;
	throw;
      }
    }
  }
  std::string_view max_marker() const override {
    return "99999999";
  }
  asio::awaitable<bool> is_empty(const DoutPrefixProvider* dpp) override {
    std::vector<cls::log::entry> entrystore{1};
    for (auto oid = 0; oid < std::ssize(oids); ++oid) {
      try {
	auto [entries, marker] =
	  co_await nlog::list(r, oids[oid], loc, {}, {}, {}, entrystore,
			      asio::use_awaitable);
	if (!entries.empty()) {
	  co_return false;
	}
      } catch (const sys::system_error& e) {
	if (e.code() == sys::errc::no_such_file_or_directory) {
	  continue;
	}
      }
    }
    co_return true;
  }
};

class RGWDataChangesFIFO final : public RGWDataChangesBE {
  using centries = std::vector<buffer::list>;
  tiny_vector<LazyFIFO> fifos;

public:
  RGWDataChangesFIFO(neorados::RADOS& r,
		     neorados::IOContext loc,
		     RGWDataChangesLog& datalog,
		     uint64_t gen_id,
		     int num_shards)
    : RGWDataChangesBE(r, std::move(loc), datalog, gen_id),
      fifos(num_shards, [&r, &loc, this](std::size_t i, auto emplacer) {
	emplacer.emplace(r, get_oid(i), loc);
      }) {}
  ~RGWDataChangesFIFO() override = default;
  void prepare(ceph::real_time, const std::string&,
	       buffer::list&& entry, entries& out) override {
    if (!std::holds_alternative<centries>(out)) {
      ceph_assert(std::visit([](auto& v) { return std::empty(v); }, out));
      out = centries();
    }
    std::get<centries>(out).push_back(std::move(entry));
  }
  asio::awaitable<void> push(const DoutPrefixProvider* dpp, int index,
			     entries&& items) override {
    co_return co_await fifos[index].push(dpp, std::get<centries>(items));
  }
  asio::awaitable<void> push(const DoutPrefixProvider* dpp, int index,
			     ceph::real_time, const std::string&,
			     buffer::list&& bl) override {
    co_return co_await fifos[index].push(dpp, std::move(bl));
  }
  asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>,
			     std::string>>
  list(const DoutPrefixProvider* dpp, int shard,
       std::span<rgw_data_change_log_entry> entries,
       std::string marker) override {
    try {
      std::vector<fifo::entry> log_entries{entries.size()};
      auto [lentries, outmark] =
	co_await fifos[shard].list(dpp, marker, log_entries);
      entries = entries.first(lentries.size());
      std::ranges::transform(lentries, entries.begin(),
			     [](const auto& e) {
			       rgw_data_change_log_entry entry ;
			       entry.log_id = e.marker;
			       entry.log_timestamp = e.mtime;
			       auto liter = e.data.cbegin();
			       decode(entry.entry, liter);
			       return entry;
			     });
      co_return  std::make_tuple(std::move(entries),
				 outmark ? std::move(*outmark) : std::string{});
    } catch (const buffer::error& err) {
      ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
			 << ": failed to decode data changes log entry: "
			 << err.what() << dendl;
      throw;
    }
  }
  asio::awaitable<RGWDataChangesLogInfo>
  get_info(const DoutPrefixProvider *dpp, int index) override {
    auto& fifo = fifos[index];
    auto [marker, last_update] = co_await fifo.last_entry_info(dpp);
    co_return RGWDataChangesLogInfo{ .marker = marker,
				     .last_update = last_update };
  }
  asio::awaitable<void> trim(const DoutPrefixProvider *dpp, int index,
	   std::string_view marker) override {
    co_await fifos[index].trim(dpp, std::string{marker}, false);
  }
  std::string_view max_marker() const override {
    static const auto max_mark = fifo::FIFO::max_marker();
    return std::string_view(max_mark);
  }
  asio::awaitable<bool> is_empty(const DoutPrefixProvider *dpp) override {
    std::vector<fifo::entry> entrystore;
    for (auto shard = 0u; shard < fifos.size(); ++shard) {
      auto [lentries, outmark] =
	co_await fifos[shard].list(dpp, {}, entrystore);
      if (!lentries.empty()) {
	co_return false;
      }
    }
    co_return true;
  }
};

RGWDataChangesLog::RGWDataChangesLog(CephContext* cct)
  : cct(cct),
    num_shards(cct->_conf->rgw_data_log_num_shards),
    prefix(get_prefix()),
    changes(cct->_conf->rgw_data_log_changes_size) {}

RGWDataChangesLog::RGWDataChangesLog(CephContext* cct, bool log_data,
				     neorados::RADOS* rados)
  : cct(cct), rados(rados), log_data(log_data),
    num_shards(cct->_conf->rgw_data_log_num_shards),
    prefix(get_prefix()), changes(cct->_conf->rgw_data_log_changes_size) {}


void DataLogBackends::handle_init(entries_t e) {
  std::unique_lock l(m);
  for (const auto& [gen_id, gen] : e) {
    if (gen.pruned) {
      lderr(datalog.cct)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< ": ERROR: given empty generation: gen_id=" << gen_id << dendl;
    }
    if (count(gen_id) != 0) {
      lderr(datalog.cct)
	<< __PRETTY_FUNCTION__ << ":" << __LINE__
	<< ": ERROR: generation already exists: gen_id=" << gen_id << dendl;
    }
    try {
      switch (gen.type) {
      case log_type::omap:
	emplace(gen_id,
		boost::intrusive_ptr<RGWDataChangesBE>(
		  new RGWDataChangesOmap(rados, loc, datalog, gen_id, shards)));
	break;
      case log_type::fifo:
	emplace(gen_id,
		boost::intrusive_ptr<RGWDataChangesBE>(
		  new RGWDataChangesFIFO(rados, loc, datalog, gen_id, shards)));
	break;
      default:
	lderr(datalog.cct)
	  << __PRETTY_FUNCTION__ << ":" << __LINE__
	  << ": IMPOSSIBLE: invalid log type: gen_id=" << gen_id
	  << ", type" << gen.type << dendl;
	throw sys::system_error{EFAULT, sys::generic_category()};
      }
    } catch (const sys::system_error& err) {
      lderr(datalog.cct)
	  << __PRETTY_FUNCTION__ << ":" << __LINE__
	  << ": error setting up backend: gen_id=" << gen_id
	  << ", err=" << err.what() << dendl;
      throw;
    }
  }
}

void DataLogBackends::handle_new_gens(entries_t e) {
  handle_init(std::move(e));
}

void DataLogBackends::handle_empty_to(uint64_t new_tail) {
  std::unique_lock l(m);
  auto i = cbegin();
  if (i->first < new_tail) {
    return;
  }
  if (new_tail >= (cend() - 1)->first) {
    lderr(datalog.cct)
      << __PRETTY_FUNCTION__ << ":" << __LINE__
      << ": ERROR: attempt to trim head: new_tail=" << new_tail << dendl;
    throw sys::system_error(EFAULT, sys::system_category());
  }
  erase(i, upper_bound(new_tail));
}


int RGWDataChangesLog::start(const DoutPrefixProvider *dpp,
			     const RGWZone* zone,
			     const RGWZoneParams& zoneparams,
			     rgw::sal::RadosStore* store,
			     bool background_tasks)
{
  log_data = zone->log_data;
  rados = &store->get_neorados();
  try {
    // Blocking in startup code, not ideal, but won't hurt anything.
    std::exception_ptr eptr
      = asio::co_spawn(store->get_io_context(),
		       start(dpp, zoneparams.log_pool,
			     background_tasks, background_tasks,
			     background_tasks),
		       async::use_blocked);
    if (eptr) {
      std::rethrow_exception(eptr);
    }
  } catch (const sys::system_error& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Failed to start datalog: " << e.what()
		       << dendl;
    return ceph::from_error_code(e.code());
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Failed to start datalog: " << e.what()
		       << dendl;
    return -EIO;
  }
  return 0;
}

asio::awaitable<void>
RGWDataChangesLog::start(const DoutPrefixProvider *dpp,
			 const rgw_pool& log_pool,
			 bool recovery,
			 bool watch,
			 bool renew)
{
  down_flag = false;
  cancel_strand = asio::make_strand(rados->get_executor());
  ran_background = (recovery || watch || renew);

  auto defbacking = to_log_type(
    cct->_conf.get_val<std::string>("rgw_default_data_log_backing"));
  // Should be guaranteed by `set_enum_allowed`
  ceph_assert(defbacking);
  try {
    loc = co_await rgw::init_iocontext(dpp, *rados, log_pool,
				       rgw::create, asio::use_awaitable);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Failed to initialized ioctx: " << e.what()
		       << ", pool=" << log_pool << dendl;
    throw;
  }

  try {
    bes = co_await logback_generations::init<DataLogBackends>(
      dpp, *rados, metadata_log_oid(), loc,
      [this](uint64_t gen_id, int shard) {
	return get_oid(gen_id, shard);
      }, num_shards, *defbacking, *this);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Error initializing backends: " << e.what()
		       << dendl;
    throw;
  }

  if (!log_data) {
    co_return;
  }

  if (renew) {
    asio::co_spawn(
      co_await asio::this_coro::executor,
      renew_run(renew_signal),
      asio::bind_cancellation_slot(renew_signal->slot(),
				   asio::bind_executor(*cancel_strand,
						       asio::detached)));
  }
  if (watch) {
    // Establish watch here so we won't be 'started up' until we're watching.
    const auto oid = get_sem_set_oid(0);
    auto established = co_await establish_watch(dpp, oid);
    if (!established) {
      throw sys::system_error{ENOTCONN, sys::generic_category(),
			      "Unable to establish recovery watch!"};
    }
    asio::co_spawn(
      co_await asio::this_coro::executor,
      watch_loop(watch_signal),
      asio::bind_cancellation_slot(watch_signal->slot(),
				   asio::bind_executor(*cancel_strand,
						       asio::detached)));
  }
  if (recovery) {
    // Recovery can run concurrent with normal operation, so we don't
    // have to block startup while we do all that I/O.
    asio::co_spawn(
      co_await asio::this_coro::executor,
      recover(dpp, recovery_signal),
      asio::bind_cancellation_slot(recovery_signal->slot(),
				   asio::bind_executor(*cancel_strand,
						       asio::detached)));
  }
  co_return;
}

asio::awaitable<bool>
RGWDataChangesLog::establish_watch(const DoutPrefixProvider* dpp,
				   std::string_view oid) {
  const auto queue_depth = num_shards * 128;
  try {
    co_await rados->execute(oid, loc, neorados::WriteOp{}.create(false),
			    asio::use_awaitable);
    watchcookie = co_await rados->watch(oid, loc, asio::use_awaitable,
					std::nullopt, queue_depth);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, -1) << __PRETTY_FUNCTION__
		       << ": Unable to start watch! Error: "
		       << e.what() << dendl;
    watchcookie = 0;
  }

  if (watchcookie == 0) {
    // Dump our current working set.
    co_await renew_entries(dpp);
  }

  co_return watchcookie != 0;
}

struct recovery_check {
  uint64_t shard = 0;

  recovery_check() = default;

  recovery_check(uint64_t shard) : shard(shard) {}

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(shard, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(shard, bl);
    DECODE_FINISH(bl);
  }

  operator uint64_t() {
    return shard;
  }
};
WRITE_CLASS_ENCODER(recovery_check);


struct recovery_reply {
  std::unordered_set<std::string> reply_set;

  recovery_reply() = default;

  recovery_reply(std::unordered_set<std::string> reply_set)
    : reply_set(std::move(reply_set)) {}

  void encode(buffer::list& bl) const {
    ENCODE_START(1, 1, bl);
    encode(reply_set, bl);
    ENCODE_FINISH(bl);
  }

  void decode(buffer::list::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(reply_set, bl);
    DECODE_FINISH(bl);
  }

  operator std::unordered_set<std::string>&() {
    return reply_set;
  }
};
WRITE_CLASS_ENCODER(recovery_reply);


asio::awaitable<void>
RGWDataChangesLog::process_notification(const DoutPrefixProvider* dpp,
					std::string_view oid) {
  auto notification = co_await rados->next_notification(watchcookie,
							asio::use_awaitable);
  int shard = 0;
  // Don't send a reply if we get a bogus notification, we don't
  // want recovery to delete semaphores improperly.
  try {
    recovery_check rc;
    decode(rc, notification.bl);
    shard = rc;
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 2) << "Got malformed notification!" << dendl;
    co_return;
  }
  if (shard >= num_shards) {
    ldpp_dout(dpp, 2) << "Got unknown shard " << shard << dendl;
    co_return;
  }
  recovery_reply reply;
  std::unique_lock l(lock);
  for (const auto& bg : cur_cycle) {
    if (choose_oid(bg.shard) == shard) {
      reply.reply_set.insert(bg.get_key());
    }
  }
  std::copy(semaphores[shard].begin(),
	    semaphores[shard].end(),
	    std::inserter(reply.reply_set, reply.reply_set.end()));
  l.unlock();
  buffer::list replybl;
  encode(reply, replybl);
  try {
    co_await rados->notify_ack(oid, loc, notification.notify_id, watchcookie,
			       std::move(replybl), asio::use_awaitable);
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 10) << __PRETTY_FUNCTION__
		       << ": Failed ack. Whatever server is in "
		       << "recovery won't decrement semaphores: "
		       << e.what() << dendl;
  }
}

asio::awaitable<void> RGWDataChangesLog::watch_loop(decltype(watch_signal)) {
  const DoutPrefix dp(cct, dout_subsys, "rgw data changes log: ");
  const auto oid = get_sem_set_oid(0);
  bool need_rewatch = false;

  while (!going_down()) {
    try {
      co_await process_notification(&dp, oid);
    } catch (const sys::system_error& e) {
      if (e.code() == neorados::errc::notification_overflow) {
	ldpp_dout(&dp, 10) << __PRETTY_FUNCTION__
			   << ": Notification overflow. Whatever server is in "
			   << "recovery won't decrement semaphores." << dendl;
	continue;
      }
      if (going_down() || e.code() == asio::error::operation_aborted){
	need_rewatch = false;
	break;
      } else {
	need_rewatch = true;
      }
    }
    if (need_rewatch) {
      try {
	if (watchcookie) {
	  auto wc = watchcookie;
	  watchcookie = 0;
	  co_await rados->unwatch(wc, loc, asio::use_awaitable);
	}
      } catch (const std::exception& e) {
	// Watch may not exist, don't care.
      }
      bool rewatched = false;
      ldpp_dout(&dp, 10) << __PRETTY_FUNCTION__
			 << ": Trying to re-establish watch" << dendl;

      rewatched = co_await establish_watch(&dp, oid);
      while (!rewatched) {
	boost::asio::steady_timer t(co_await asio::this_coro::executor, 500ms);
	co_await t.async_wait(asio::use_awaitable);
	ldpp_dout(&dp, 10) << __PRETTY_FUNCTION__
			   << ": Trying to re-establish watch" << dendl;
	rewatched = co_await establish_watch(&dp, oid);
      }
    }
  }
}

int RGWDataChangesLog::choose_oid(const rgw_bucket_shard& bs) {
  const auto& name = bs.bucket.name;
  auto shard_shift = (bs.shard_id > 0 ? bs.shard_id : 0);
  auto r = (ceph_str_hash_linux(name.data(), name.size()) +
	    shard_shift) % num_shards;
  return static_cast<int>(r);
}

asio::awaitable<void>
RGWDataChangesLog::renew_entries(const DoutPrefixProvider* dpp)
{
  if (!log_data) {
    co_return;
  }

  /* we can't keep the bucket name as part of the datalog entry, and
   * we need it later, so we keep two lists under the map */
  bc::flat_map<int, std::pair<std::vector<BucketGen>,
			      RGWDataChangesBE::entries>> m;

  std::unique_lock l(lock);
  decltype(cur_cycle) entries;
  entries.swap(cur_cycle);
  for (const auto& [bs, gen] : entries) {
    unsigned index = choose_oid(bs);
    semaphores[index].insert(BucketGen{bs, gen}.get_key());
  }
  l.unlock();

  auto ut = real_clock::now();
  auto be = bes->head();

  for (const auto& [bs, gen] : entries) {
    auto index = choose_oid(bs);

    rgw_data_change change;
    buffer::list bl;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = ut;
    change.gen = gen;
    encode(change, bl);

    m[index].first.push_back({bs, gen});
    be->prepare(ut, change.key, std::move(bl), m[index].second);
  }

  auto push_failed = false;
  for (auto& [index, p] : m) {
    auto& [buckets, entries] = p;

    auto now = real_clock::now();
    // Failure on push isn't fatal.
    try {
      co_await be->push(dpp, index, std::move(entries));
    } catch (const std::exception& e) {
      push_failed = true;
      ldpp_dout(dpp, 5) << "RGWDataChangesLog::renew_entries(): Backend push failed "
			<< "with exception: " << e.what() << dendl;
    }

    auto expiration = now;
    expiration += ceph::make_timespan(cct->_conf->rgw_data_log_window);
    for (auto& [bs, gen] : buckets) {
      update_renewed(bs, gen, expiration);
    }
  }
  if (push_failed) {
    co_return;
  }

  namespace sem_set = neorados::cls::sem_set;
  // If we didn't error in pushing, we can now decrement the semaphores
  l.lock();
  for (auto index = 0u; index < unsigned(num_shards); ++index) {
    using neorados::WriteOp;
    auto& keys = semaphores[index];
    while (!keys.empty()) {
      bc::flat_set<std::string> batch;
      // Can't use a move iterator here, since the keys have to stay
      // until they're safely on the OSD to avoid the risk of
      // double-decrement from recovery.
      auto to_copy = std::min(sem_max_keys, keys.size());
      std::copy_n(keys.begin(), to_copy,
		  std::inserter(batch, batch.end()));
      auto op = WriteOp{}.exec(sem_set::decrement(std::move(batch)));
      l.unlock();
      co_await rados->execute(get_sem_set_oid(index), loc, std::move(op),
			      asio::use_awaitable);
      l.lock();
      auto iter = keys.cbegin();
      std::advance(iter, to_copy);
      keys.erase(keys.cbegin(), iter);
    }
  }
  co_return;
}

auto RGWDataChangesLog::_get_change(const rgw_bucket_shard& bs,
				    uint64_t gen)
  -> ChangeStatusPtr
{
  ChangeStatusPtr status;
  if (!changes.find({bs, gen}, status)) {
    status = std::make_shared<ChangeStatus>(rados->get_executor());
    changes.add({bs, gen}, status);
  }
  return status;
}

bool RGWDataChangesLog::register_renew(BucketGen bg)
{
  std::scoped_lock l{lock};
  return cur_cycle.insert(bg).second;
}

void RGWDataChangesLog::update_renewed(const rgw_bucket_shard& bs,
				       uint64_t gen,
				       real_time expiration)
{
  std::unique_lock l{lock};
  auto status = _get_change(bs, gen);
  l.unlock();

  ldout(cct, 20) << "RGWDataChangesLog::update_renewed() bucket_name="
		 << bs.bucket.name << " shard_id=" << bs.shard_id
		 << " expiration=" << expiration << dendl;

  std::unique_lock sl(status->lock);
  status->cur_expiration = expiration;
}

int RGWDataChangesLog::get_log_shard_id(rgw_bucket& bucket, int shard_id) {
  rgw_bucket_shard bs(bucket, shard_id);
  return choose_oid(bs);
}

asio::awaitable<bool>
RGWDataChangesLog::filter_bucket(const DoutPrefixProvider *dpp,
				 const rgw_bucket& bucket) const
{
  if (!bucket_filter) {
    co_return true;
  }

  co_return co_await asio::spawn(
    co_await asio::this_coro::executor,
    [this, dpp, &bucket](asio::yield_context yc) {
      optional_yield y(yc);
      return bucket_filter(bucket, y, dpp);
    }, asio::use_awaitable);
}

std::string RGWDataChangesLog::get_oid(uint64_t gen_id, int i) const {
  return (gen_id > 0 ?
	  fmt::format("{}@G{}.{}", prefix, gen_id, i) :
	  fmt::format("{}.{}", prefix, i));
}

std::string RGWDataChangesLog::get_sem_set_oid(int i) const {
  return fmt::format("_sem_set{}.{}", prefix, i);
}

asio::awaitable<void>
RGWDataChangesLog::add_entry(const DoutPrefixProvider* dpp,
			     const RGWBucketInfo& bucket_info,
			     const rgw::bucket_log_layout_generation& gen,
			     int shard_id)
{
  if (!log_data) {
    co_return;
  }

  auto& bucket = bucket_info.bucket;

  if (!co_await filter_bucket(dpp, bucket)) {
    co_return;
  }

  if (observer) {
    observer->on_bucket_changed(bucket.get_key());
  }

  rgw_bucket_shard bs(bucket, shard_id);

  int index = choose_oid(bs);
  if (!(watchcookie && rados->check_watch(watchcookie))) {
    auto now = real_clock::now();
    ldpp_dout(dpp, 2) << "RGWDataChangesLog::add_entry(): "
		      << "Bypassing window optimization and pushing directly: "
		      << "bucket.name=" << bucket.name
		      << " shard_id=" << shard_id << " now="
		      << now << " cur_expiration=" << dendl;

    buffer::list bl;
    rgw_data_change change;
    change.entity_type = ENTITY_TYPE_BUCKET;
    change.key = bs.get_key();
    change.timestamp = now;
    change.gen = gen.gen;
    encode(change, bl);

    auto be = bes->head();
    // Failure on push is fatal if we're bypassing semaphores.
    co_await be->push(dpp, index, now, change.key, std::move(bl));
    co_return;
  }

  mark_modified(index, bs, gen.gen);

  std::unique_lock l(lock);

  auto status = _get_change(bs, gen.gen);
  l.unlock();

  auto now = real_clock::now();

  std::unique_lock sl(status->lock);

  ldpp_dout(dpp, 20) << "RGWDataChangesLog::add_entry() bucket.name=" << bucket.name
		     << " shard_id=" << shard_id << " now=" << now
		     << " cur_expiration=" << status->cur_expiration << dendl;


  if (now < status->cur_expiration) {
    /* no need to send, recently completed */
    sl.unlock();
    auto bg = BucketGen{bs, gen.gen};
    auto key = bg.get_key();
    auto need_sem_set = register_renew(std::move(bg));
    if (need_sem_set) {
      using neorados::WriteOp;
      using neorados::cls::sem_set::increment;
      co_await rados->execute(get_sem_set_oid(index), loc,
			      WriteOp{}.exec(increment(std::move(key))),
			      asio::use_awaitable);
    }
    co_return;
  }

  if (status->pending) {
    co_await status->cond.async_wait(sl, asio::use_awaitable);
    sl.unlock();
    co_return;
  }

  status->cond.notify(sl);
  status->pending = true;

  ceph::real_time expiration;
  status->cur_sent = now;

  expiration = now;
  expiration += ceph::make_timespan(cct->_conf->rgw_data_log_window);

  sl.unlock();

  buffer::list bl;
  rgw_data_change change;
  change.entity_type = ENTITY_TYPE_BUCKET;
  change.key = bs.get_key();
  change.timestamp = now;
  change.gen = gen.gen;
  encode(change, bl);

  ldpp_dout(dpp, 20) << "RGWDataChangesLog::add_entry() sending update with now=" << now << " cur_expiration=" << expiration << dendl;

  auto be = bes->head();
  // Failure on push isn't fatal.
  try {
    co_await be->push(dpp, index, now, change.key, std::move(bl));
  } catch (const std::exception& e) {
    ldpp_dout(dpp, 5) << "RGWDataChangesLog::add_entry(): Backend push failed "
		      << "with exception: " << e.what() << dendl;
  }


  now = real_clock::now();

  sl.lock();

  status->pending = false;
  /* time of when operation started, not completed */
  status->cur_expiration = status->cur_sent;
  status->cur_expiration += make_timespan(cct->_conf->rgw_data_log_window);
  status->cond.notify(sl);
  sl.unlock();

  co_return;
}

int RGWDataChangesLog::add_entry(const DoutPrefixProvider* dpp,
				 const RGWBucketInfo& bucket_info,
				 const rgw::bucket_log_layout_generation& gen,
				 int shard_id, optional_yield y)
{
  std::exception_ptr eptr;
  if (y) {
    auto& yield = y.get_yield_context();
    try {
      asio::co_spawn(yield.get_executor(),
		     add_entry(dpp, bucket_info, gen, shard_id),
		     yield);
    } catch (const std::exception&) {
      eptr = std::current_exception();
    }
  } else {
    maybe_warn_about_blocking(dpp);
    eptr = asio::co_spawn(rados->get_executor(),
			  add_entry(dpp, bucket_info, gen, shard_id),
			  async::use_blocked);
  }
  return ceph::from_exception(eptr);
}

asio::awaitable<std::tuple<std::span<rgw_data_change_log_entry>,
			   std::string>>
DataLogBackends::list(const DoutPrefixProvider *dpp, int shard,
		      std::span<rgw_data_change_log_entry> entries,
		      std::string marker)
{
  const auto [start_id, // Starting generation
	      start_cursor // Cursor to be used when listing the
			   // starting generation
    ] = cursorgen(marker);
  auto gen_id = start_id; // Current generation being listed
  // Cursor with prepended generation, returned to caller
  std::string out_cursor;
  // Span to return to caller
  auto entries_out = entries;
  // Iterator (for inserting stuff into span)
  auto out = entries_out.begin();
  // Allocated storage for raw listings from backend
  std::vector<rgw_data_change_log_entry> gentries{entries.size()};
  while (out < entries_out.end()) {
    std::unique_lock l(m);
    auto i = lower_bound(gen_id);
    if (i == end())
      break;
    auto be = i->second;
    l.unlock();
    gen_id = be->gen_id;
    auto inspan = std::span{gentries}.first(entries_out.end() - out);
    // Since later generations continue listings from the
    // first, start them at the beginning.
    auto incursor = gen_id == start_id ? start_cursor : std::string{};
    auto [raw_entries, raw_cursor]
      = co_await be->list(dpp, shard, inspan, incursor);
    out = std::transform(std::make_move_iterator(raw_entries.begin()),
			 std::make_move_iterator(raw_entries.end()),
			 out, [gen_id](rgw_data_change_log_entry e) {
			   e.log_id = gencursor(gen_id, e.log_id);
			   return e;
			 });
    if (!raw_cursor.empty()) {
      out_cursor = gencursor(gen_id, raw_cursor);
    } else {
      out_cursor.clear();
      break;
    }
    ++gen_id;
  }
  entries_out = entries_out.first(out - entries_out.begin());
  co_return std::make_tuple(entries_out, std::move(out_cursor));
}

asio::awaitable<std::tuple<std::vector<rgw_data_change_log_entry>,
			   std::string>>
RGWDataChangesLog::list_entries(const DoutPrefixProvider* dpp, int shard,
				int max_entries, std::string marker)
{
  assert(shard < num_shards);
  if (max_entries <= 0) {
    co_return std::make_tuple(std::vector<rgw_data_change_log_entry>{},
			      std::string{});
  }
  std::vector<rgw_data_change_log_entry> entries(max_entries);
  entries.resize(max_entries);
  auto [spanentries, outmark] = co_await bes->list(dpp, shard, entries, marker);
  entries.resize(spanentries.size());
  co_return std::make_tuple(std::move(entries), std::move(outmark));
}

int RGWDataChangesLog::list_entries(
  const DoutPrefixProvider *dpp, int shard,
  int max_entries, std::vector<rgw_data_change_log_entry>& entries,
  std::string_view marker, std::string* out_marker, bool* truncated,
  optional_yield y)
{
  assert(shard < num_shards);
  std::exception_ptr eptr;
  std::tuple<std::span<rgw_data_change_log_entry>,
	     std::string> out;
  if (std::ssize(entries) < max_entries) {
    entries.resize(max_entries);
  }
  if (y) {
    auto& yield = y.get_yield_context();
    try {
      out = asio::co_spawn(yield.get_executor(),
			   bes->list(dpp, shard, entries,
				     std::string{marker}),
			   yield);
    } catch (const std::exception&) {
      eptr = std::current_exception();
    }
  } else {
    maybe_warn_about_blocking(dpp);
    std::tie(eptr, out) = asio::co_spawn(rados->get_executor(),
					 bes->list(dpp, shard, entries,
						   std::string{marker}),
					 async::use_blocked);
  }
  if (eptr) {
    return ceph::from_exception(eptr);
  }
  auto& [outries, outmark] = out;
  if (auto size = std::ssize(outries); size < std::ssize(entries)) {
    entries.resize(size);
  }
  if (truncated) {
    *truncated = !outmark.empty();
  }
  if (out_marker) {
    *out_marker = std::move(outmark);
  }
  return 0;
}

asio::awaitable<std::tuple<std::vector<rgw_data_change_log_entry>,
			   RGWDataChangesLogMarker>>
RGWDataChangesLog::list_entries(const DoutPrefixProvider *dpp,
				int max_entries, RGWDataChangesLogMarker marker)
{
  if (max_entries <= 0) {
    co_return std::make_tuple(std::vector<rgw_data_change_log_entry>{},
			      RGWDataChangesLogMarker{});
  }

  std::vector<rgw_data_change_log_entry> entries(max_entries);
  std::span remaining{entries};

  do {
    std::span<rgw_data_change_log_entry> outspan;
    std::string outmark;
    std::tie(outspan, outmark) = co_await bes->list(dpp, marker.shard,
						    remaining, marker.marker);
    remaining = remaining.last(remaining.size() - outspan.size());
    if (!outmark.empty()) {
      marker.marker = std::move(outmark);
    } else if (outmark.empty() && marker.shard < (num_shards - 1)) {
      ++marker.shard;
      marker.marker.clear();
    } else {
      marker.clear();
    }
  } while (!remaining.empty() && marker);
  if (!remaining.empty()) {
    entries.resize(entries.size() - remaining.size());
  }
  co_return std::make_tuple(std::move(entries), std::move(marker));
}

int RGWDataChangesLog::list_entries(const DoutPrefixProvider *dpp,int max_entries,
				    std::vector<rgw_data_change_log_entry>& entries,
				    RGWDataChangesLogMarker& marker, bool *ptruncated,
				    optional_yield y)
{
  std::exception_ptr eptr;
  std::tuple<std::vector<rgw_data_change_log_entry>,
	     RGWDataChangesLogMarker> out;
  if (std::ssize(entries) < max_entries) {
    entries.resize(max_entries);
  }
  if (y) {
    auto& yield = y.get_yield_context();
    try {
      out = asio::co_spawn(yield.get_executor(),
			   list_entries(dpp, max_entries,
					RGWDataChangesLogMarker{marker}),
			   yield);
    } catch (const std::exception&) {
      eptr = std::current_exception();
    }
  } else {
    maybe_warn_about_blocking(dpp);
    std::tie(eptr, out) =
      asio::co_spawn(rados->get_executor(),
		     list_entries(dpp, max_entries,
				  RGWDataChangesLogMarker{marker}),
		     async::use_blocked);
  }
  if (eptr) {
    return ceph::from_exception(eptr);
  }
  auto& [outries, outmark] = out;
  if (auto size = std::ssize(outries); size < std::ssize(entries)) {
    entries.resize(size);
  }
  if (ptruncated) {
    *ptruncated = (outmark.shard > 0 || !outmark.marker.empty());
  }
  marker = std::move(outmark);
  return 0;
}

int RGWDataChangesLog::get_info(const DoutPrefixProvider* dpp, int shard_id,
				RGWDataChangesLogInfo* info, optional_yield y)
{
  assert(shard_id < num_shards);
  auto be = bes->head();
  std::exception_ptr eptr;
  if (y) {
    auto& yield = y.get_yield_context();
    try {
      *info = asio::co_spawn(yield.get_executor(),
			     be->get_info(dpp, shard_id),
			     yield);
    } catch (const std::exception&) {
      eptr = std::current_exception();
    }
  } else {
    maybe_warn_about_blocking(dpp);
    std::tie(eptr, *info) = asio::co_spawn(rados->get_executor(),
					   be->get_info(dpp, shard_id),
					   async::use_blocked);
  }
  if (!info->marker.empty()) {
    info->marker = gencursor(be->gen_id, info->marker);
  }
  return ceph::from_exception(eptr);
}

asio::awaitable<void> DataLogBackends::trim_entries(
  const DoutPrefixProvider *dpp, int shard_id,
  std::string_view marker)
{
  auto [target_gen, cursor] = cursorgen(std::string{marker});
  std::unique_lock l(m);
  const auto head_gen = (end() - 1)->second->gen_id;
  const auto tail_gen = begin()->first;
 if (target_gen < tail_gen)
   
    co_return;
  auto r = 0;
  for (auto be = lower_bound(0)->second;
       be->gen_id <= target_gen && be->gen_id <= head_gen && r >= 0;
       be = upper_bound(be->gen_id)->second) {
    l.unlock();
    auto c = be->gen_id == target_gen ? cursor : be->max_marker();
    co_await be->trim(dpp, shard_id, c);
    if (r == -ENOENT)
      r = -ENODATA;
    if (r == -ENODATA && be->gen_id < target_gen)
      r = 0;
    if (be->gen_id == target_gen)
      break;
    l.lock();
  };
  co_return;
}

int RGWDataChangesLog::trim_entries(const DoutPrefixProvider *dpp, int shard_id,
				    std::string_view marker, optional_yield y)
{
  assert(shard_id < num_shards);
  std::exception_ptr eptr;
  if (y) {
    auto& yield = y.get_yield_context();
    try {
      asio::co_spawn(yield.get_executor(),
		     bes->trim_entries(dpp, shard_id, marker),
		     yield);
    } catch (const std::exception& e) {
      eptr = std::current_exception();
    }
  } else {
    maybe_warn_about_blocking(dpp);
    eptr = asio::co_spawn(rados->get_executor(),
			  bes->trim_entries(dpp, shard_id, marker),
			  async::use_blocked);
  }
  return ceph::from_exception(eptr);
}

int RGWDataChangesLog::trim_entries(const DoutPrefixProvider* dpp, int shard_id,
				    std::string_view marker,
				    librados::AioCompletion* c) {
  asio::co_spawn(rados->get_executor(),
		 bes->trim_entries(dpp, shard_id, marker),
		 c);
  return 0;
}


asio::awaitable<void> DataLogBackends::trim_generations(
  const DoutPrefixProvider *dpp,
  std::optional<uint64_t>& through)
{
  if (size() != 1) {
    std::vector<mapped_type> candidates;
    {
      std::scoped_lock l(m);
      auto e = cend() - 1;
      for (auto i = cbegin(); i < e; ++i) {
	candidates.push_back(i->second);
      }
    }

    std::optional<uint64_t> highest;
    for (auto& be : candidates) {
      if (co_await be->is_empty(dpp)) {
	highest = be->gen_id;
      } else {
	break;
      }
    }

    through = highest;
    if (!highest) {
      co_return;
    }
    co_await empty_to(dpp, *highest);
  }

  co_await remove_empty(dpp);
  co_return;
}

bool RGWDataChangesLog::going_down() const
{
  return down_flag;
}

asio::awaitable<void> RGWDataChangesLog::shutdown() {
  DoutPrefix dp{cct, ceph_subsys_rgw, "Datalog Shutdown"};
  if (down_flag || !ran_background) {
    co_return;
  }
  down_flag = true;
  renew_stop();
  // Revisit this later
  if (renew_signal)
    asio::dispatch(*cancel_strand,
		   [this]() {
		     renew_signal->emit(asio::cancellation_type::terminal);
		   });
  if (recovery_signal)
    asio::dispatch(*cancel_strand,
		   [this]() {
		     recovery_signal->emit(asio::cancellation_type::terminal);
		   });
  if (watch_signal)
    asio::dispatch(*cancel_strand,
		   [this]() {
		     watch_signal->emit(asio::cancellation_type::terminal);
		   });
  if (watchcookie && rados->check_watch(watchcookie)) {
    auto wc = watchcookie;
    watchcookie = 0;
    co_await rados->unwatch(wc, loc, asio::use_awaitable);
  }
  co_return;
}

asio::awaitable<void> RGWDataChangesLog::shutdown_or_timeout() {
  using namespace asio::experimental::awaitable_operators;
  asio::steady_timer t(co_await asio::this_coro::executor, 3s);
  co_await (shutdown() || t.async_wait(asio::use_awaitable));
  if (renew_signal) {
    renew_signal->emit(asio::cancellation_type::terminal);
  }
  if (recovery_signal) {
    recovery_signal->emit(asio::cancellation_type::terminal);
  }
  if (watch_signal) {
    watch_signal->emit(asio::cancellation_type::terminal);
  }
}

RGWDataChangesLog::~RGWDataChangesLog() {
  if (log_data && !down_flag) {
    lderr(cct) << __PRETTY_FUNCTION__ << ":" << __LINE__
	       << ": RGWDataChangesLog destructed without shutdown." << dendl;
  }
}

void RGWDataChangesLog::blocking_shutdown() {
  if (!down_flag) {
    try {
      auto eptr = asio::co_spawn(rados->get_io_context(),
				 shutdown_or_timeout(),
				 async::use_blocked);
      if (eptr) {
	std::rethrow_exception(eptr);
      }
    } catch (const sys::system_error& e) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": Failed to shutting down: " << e.what()
		 << dendl;
    } catch (const std::exception& e) {
      lderr(cct) << __PRETTY_FUNCTION__
		 << ": Failed to shutting down: " << e.what()
		 << dendl;
    }
  }
}

asio::awaitable<void> RGWDataChangesLog::renew_run(decltype(renew_signal)) {
  static constexpr auto runs_per_prune = 150;
  auto run = 0;
  renew_timer.emplace(co_await asio::this_coro::executor);
  std::string_view operation;
  const DoutPrefix dp(cct, dout_subsys, "rgw data changes log: ");
  for (;;) try {
      ldpp_dout(&dp, 2) << "RGWDataChangesLog::ChangesRenewThread: start"
			<< dendl;
      operation = "RGWDataChangesLog::renew_entries"sv;
      co_await renew_entries(&dp);
      operation = {};
      if (going_down())
	break;

      if (run == runs_per_prune) {
	std::optional<uint64_t> through;
	ldpp_dout(&dp, 2) << "RGWDataChangesLog::ChangesRenewThread: pruning old generations" << dendl;
	operation = "trim_generations"sv;
	co_await bes->trim_generations(&dp, through);
	operation = {};
	if (through) {
	  ldpp_dout(&dp, 2)
	    << "RGWDataChangesLog::ChangesRenewThread: pruned generations "
	    << "through " << *through << "." << dendl;
	} else {
	  ldpp_dout(&dp, 2)
	    << "RGWDataChangesLog::ChangesRenewThread: nothing to prune."
	    << dendl;
	}
        run = 0;
      } else {
	++run;
      }

      int interval = cct->_conf->rgw_data_log_window * 3 / 4;
      renew_timer->expires_after(std::chrono::seconds(interval));
      co_await renew_timer->async_wait(asio::use_awaitable);
    } catch (sys::system_error& e) {
      if (e.code() == asio::error::operation_aborted) {
	ldpp_dout(&dp, 10)
	  << "RGWDataChangesLog::renew_entries canceled, going down" << dendl;
	break;
      } else {
	ldpp_dout(&dp, 0)
	  << "renew_thread: ERROR: "
	  << (operation.empty() ? operation : "<unknown"sv)
	  << "threw exception: " << e.what() << dendl;
	continue;
      }
    }
}

void RGWDataChangesLog::renew_stop()
{
  std::lock_guard l{lock};
  if (renew_timer) {
    renew_timer->cancel();
  }
}

void RGWDataChangesLog::mark_modified(int shard_id, const rgw_bucket_shard& bs, uint64_t gen)
{
  if (!cct->_conf->rgw_data_notify_interval_msec) {
    return;
  }

  auto key = bs.get_key();
  {
    std::shared_lock rl{modified_lock}; // read lock to check for existence
    auto shard = modified_shards.find(shard_id);
    if (shard != modified_shards.end() && shard->second.count({key, gen})) {
      return;
    }
  }

  std::unique_lock wl{modified_lock}; // write lock for insertion
  modified_shards[shard_id].insert(rgw_data_notify_entry{key, gen});
}

std::string RGWDataChangesLog::max_marker() const {
  return gencursor(std::numeric_limits<uint64_t>::max(),
		   "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
}

int RGWDataChangesLog::change_format(const DoutPrefixProvider *dpp,
				     log_type type,optional_yield y) {
  std::exception_ptr eptr;
  if (y) {
    auto& yield = y.get_yield_context();
    try {
      asio::co_spawn(yield.get_executor(),
		     bes->new_backing(dpp, type),
		     yield);
    } catch (const std::exception&) {
      eptr = std::current_exception();
    }
  } else {
    maybe_warn_about_blocking(dpp);
    eptr = asio::co_spawn(rados->get_executor(),
			  bes->new_backing(dpp, type),
			  async::use_blocked);
  }
  return ceph::from_exception(eptr);
}

int RGWDataChangesLog::trim_generations(const DoutPrefixProvider *dpp,
					std::optional<uint64_t>& through,
					optional_yield y) {
  std::exception_ptr eptr;
  if (y) {
    auto& yield = y.get_yield_context();
    try {
      asio::co_spawn(yield.get_executor(),
		     bes->trim_generations(dpp, through),
		     yield);
    } catch (const std::exception& e) {
      eptr = std::current_exception();
    }

  } else {
    maybe_warn_about_blocking(dpp);
    eptr = asio::co_spawn(rados->get_executor(),
			  bes->trim_generations(dpp, through),
			  async::use_blocked);
  }
  return ceph::from_exception(eptr);
}

asio::awaitable<void>
RGWDataChangesLog::read_all_sems(int index,
				 bc::flat_map<std::string, uint64_t>* out)
{
  namespace sem_set = neorados::cls::sem_set;
  std::string cursor;
  do {
    try {
      co_await rados->execute(
	get_sem_set_oid(index), loc,
	neorados::ReadOp{}.exec(sem_set::list(sem_max_keys, cursor, out,
					      &cursor)),
	nullptr, asio::use_awaitable);
    } catch (const sys::system_error& e) {
      if (e.code() == sys::errc::no_such_file_or_directory) {
	break;
      } else {
	throw;
      }
    }
  } while (!cursor.empty());
  co_return;
}

asio::awaitable<bool>
RGWDataChangesLog::synthesize_entries(
  const DoutPrefixProvider* dpp,
  int index,
  const bc::flat_map<std::string, uint64_t>& semcount)
{
  const auto timestamp = real_clock::now();
  auto be = bes->head();
  auto push_failed = false;

  RGWDataChangesBE::entries batch;
  for (const auto& [key, sem] : semcount) {
    try {
      BucketGen bg{key};
      rgw_data_change change;
      buffer::list bl;
      change.entity_type = ENTITY_TYPE_BUCKET;
      change.key = bg.shard.get_key();
      change.timestamp = timestamp;
      change.gen = bg.gen;
      encode(change, bl);
      be->prepare(timestamp, change.key, std::move(bl), batch);
    } catch (const sys::error_code& e) {
      push_failed = true;
      ldpp_dout(dpp, -1) << "RGWDataChangesLog::synthesize_entries(): Unable to "
			 << "parse Bucketgen key: " << key << "Got exception: "
			 << e.what() << dendl;
    }
  }
  try {
    co_await be->push(dpp, index, std::move(batch));
  } catch (const std::exception& e) {
    push_failed = true;
    ldpp_dout(dpp, 5) << "RGWDataChangesLog::synthesize_entries(): Backend push "
		      << " failed with exception: " << e.what() << dendl;
  }
  co_return !push_failed;
}

asio::awaitable<bool>
RGWDataChangesLog::gather_working_sets(
  const DoutPrefixProvider* dpp,
  int index,
  bc::flat_map<std::string, uint64_t>& semcount)
{
  buffer::list bl;
  recovery_check rc = index;
  encode(rc, bl);
  auto [reply_map, missed_set] = co_await rados->notify(
      get_sem_set_oid(0), loc, bl, 60s, asio::use_awaitable);
  // If we didn't get an answer from someone, don't decrement anything.
  if (!missed_set.empty()) {
    ldpp_dout(dpp, 5) << "RGWDataChangesLog::gather_working_sets(): Missed responses: "
		      << missed_set << dendl;
    co_return false;
  }
  for (const auto& [source, reply] : reply_map) {
    recovery_reply keys;
    try {
      decode(keys, reply);
    } catch (const std::exception& e) {
      ldpp_dout(dpp, -1)
	<< "RGWDataChangesLog::gather_working_sets(): Failed decoding reply from: "
	<< source << dendl;
      co_return false;
    }
    for (const auto& key : keys.reply_set) {
      auto iter = semcount.find(key);
      if (iter == semcount.end()) {
	continue;
      }
      if (iter->second == 1) {
	semcount.erase(iter);
      } else {
	--(iter->second);
      }
    }
  }
  co_return true;
}

asio::awaitable<void>
RGWDataChangesLog::decrement_sems(
  int index,
  bc::flat_map<std::string, uint64_t>&& semcount)
{
  namespace sem_set = neorados::cls::sem_set;
  while (!semcount.empty()) {
    bc::flat_set<std::string> batch;
    for (auto j = 0u; j < sem_max_keys && !semcount.empty(); ++j) {
      auto iter = std::begin(semcount);
      batch.insert(iter->first);
      semcount.erase(std::move(iter));
    }
    co_await rados->execute(
      get_sem_set_oid(index), loc, neorados::WriteOp{}.exec(
	sem_set::decrement(std::move(batch))),
      asio::use_awaitable);
  }
}

asio::awaitable<void>
RGWDataChangesLog::recover_shard(const DoutPrefixProvider* dpp, int index)
{
  bc::flat_map<std::string, uint64_t> semcount;

  // Gather entries in the shard
  co_await read_all_sems(index, &semcount);
  // If we have none, no point doing the rest
  if (semcount.empty()) {
    co_return;
  }
  // Synthesize entries to push
  auto pushed = co_await synthesize_entries(dpp, index, semcount);
  if (!pushed) {
    // If pushing failed, don't decrement any semaphores
    ldpp_dout(dpp, 5) << "RGWDataChangesLog::recover(): Pushing shard "
		      << index << " failed, skipping decrement" << dendl;
    co_return;
  }

  // Check with other running RGWs, make sure not to decrement
  // anything they have in flight. This doesn't cause an issue for
  // partial upgrades, since older versions won't be using the
  // semaphores at all.
  auto notified = co_await gather_working_sets(dpp, index, semcount);
  if (!notified) {
    ldpp_dout(dpp, 5) << "RGWDataChangesLog::recover(): Gathering "
		      << "working sets for shard " << index
		      << "failed, skipping decrement" << dendl;
    co_return;
  }
  co_await decrement_sems(index, std::move(semcount));
  co_return;
}

asio::awaitable<void> RGWDataChangesLog::recover(const DoutPrefixProvider* dpp,
						 decltype(recovery_signal))
{
  auto strand = asio::make_strand(co_await asio::this_coro::executor);
  co_await asio::co_spawn(
    strand,
    [this](const DoutPrefixProvider* dpp)-> asio::awaitable<void, decltype(strand)> {
      auto ex = co_await boost::asio::this_coro::executor;
      auto group = async::spawn_group{ex, static_cast<size_t>(num_shards)};
      for (auto i = 0; i < num_shards; ++i) {
	boost::asio::co_spawn(ex, recover_shard(dpp, i), group);
      }
      co_await group.wait();
    }(dpp),
    asio::use_awaitable);
}

void RGWDataChangesLogInfo::dump(Formatter *f) const
{
  encode_json("marker", marker, f);
  utime_t ut(last_update);
  encode_json("last_update", ut, f);
}

void RGWDataChangesLogInfo::decode_json(JSONObj *obj)
{
  JSONDecoder::decode_json("marker", marker, obj);
  JSONDecoder::decode_json("last_update", last_update, obj);
}
