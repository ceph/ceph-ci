// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_ASIO_UTILS_H
#define CEPH_LIBRBD_ASIO_UTILS_H

#include "include/Context.h"
#include "include/rados/librados_fwd.hpp"
#include "librbd/AsioEngine.h"
#include "librbd/asio/ContextWQExecutor.h"
#include <boost/asio/bind_executor.hpp>
#include <boost/system/error_code.hpp>

namespace librbd {
namespace asio {
namespace util {

template <typename T>
auto get_context_adapter(T&& t) {
  return [t = std::move(t)](boost::system::error_code ec) {
      t->complete(-ec.value());
    };
}

template <typename T>
auto get_callback_adapter(T&& t) {
  return [t = std::move(t)](boost::system::error_code ec, auto&& ... args) {
      t(-ec.value(), std::forward<decltype(args)>(args)...);
    };
}

/**
 * Neorados completion token that delivers onto the image ContextWQ.
 *
 * The channel is captured here, on the thread submitting the op, so the
 * completion returns to the same execution context it was submitted from.
 * Queues that do not distinguish channels deliver onto the general executor.
 */
template <typename Callback>
auto get_completion_token(AsioEngine& asio_engine, Callback&& cb) {
  auto* work_queue = asio_engine.get_work_queue();
  return boost::asio::bind_executor(
    ContextWQExecutor{work_queue, work_queue->current_channel()},
    get_callback_adapter(std::forward<Callback>(cb)));
}

} // namespace util
} // namespace asio
} // namespace librbd

#endif // CEPH_LIBRBD_ASIO_UTILS_H
