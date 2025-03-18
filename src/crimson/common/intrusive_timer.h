// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>
#include <seastar/core/condition-variable.hh>

#include "common/ceph_time.h"

namespace crimson::common {

/**
 * intrusive_timer: timer implementation with allocation-free
 * schedule/cancel
 */
class intrusive_timer_t {
  using clock_t = ceph::coarse_real_clock;

public:
  struct callback_t : boost::intrusive_ref_counter<callback_t>,
                      boost::intrusive::set_base_hook<> {
    const std::function<seastar::future<>()> f;
    clock_t::time_point schedule_point;

    template <typename F>
    callback_t(F &&f) : f(std::move(f)) {}

    callback_t(const callback_t &) = delete;
    callback_t(callback_t &&) = delete;
    callback_t &operator=(const callback_t &) = delete;
    callback_t &operator=(callback_t &&) = delete;

    seastar::future<> run() {
      return std::invoke(f);
    }

    auto operator<=>(const callback_t &rhs) const {
      return std::make_pair(schedule_point, this) <=>
	std::make_pair(rhs.schedule_point, &rhs);
    }

    using ref = boost::intrusive_ptr<callback_t>;
  };

private:
  bool stopping = false;
  seastar::condition_variable cv;
  boost::intrusive::set<callback_t> events;

  seastar::future<> complete;

  void schedule(callback_t &cb, clock_t::time_point time) {
    cb.schedule_point = time;
    intrusive_ptr_add_ref(&cb);
    events.insert(cb);
  }

  callback_t::ref peek() {
    return events.empty() ? nullptr : &*(events.begin());
  }

  void remove(callback_t &cb) {
    intrusive_ptr_release(&cb);
    events.erase(cb);
  }

  seastar::future<> _run() {
    while (true) {
      if (stopping) {
	break;
      }
      
      auto next = peek();
      if (!next) {
	co_await cv.when();
	continue;
      }
      
      auto now = clock_t::now();
      if (next->schedule_point > now) {
	co_await cv.when(next->schedule_point - now);
	continue;
      }
      
      remove(*next);
      co_await next->run();
    }
    co_return;
  }
  
public:
  intrusive_timer_t() : complete(_run()) {}

  template <typename T>
  void schedule_after(callback_t &cb, T after) {
    schedule(cb, clock_t::now() + after);
    cv.signal();
  }

  void cancel(callback_t &cb) {
    remove(cb);
  }

  seastar::future<> stop() {
    stopping = true;
    cv.signal();
    return std::move(complete);
  }
};

}
