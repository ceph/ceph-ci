// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include "rgw_restore_curl.h"

#include <boost/asio/bind_executor.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/experimental/channel_error.hpp>
#include <boost/asio/experimental/concurrent_channel.hpp>
#include <boost/system/system_error.hpp>
#include <curl/curl.h>

#include "include/buffer.h"
#include "common/async/spawn_throttle.h"
#include "curl/async_perform.h"
#include "curl/shared_client.h"
#include "rgw_http_errors.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw::restore {

namespace {

using chunk_channel = boost::asio::experimental::concurrent_channel<
    void(boost::system::error_code, ceph::buffer::list)>;

/*
 * libcurl's write callback is synchronous and cannot await, so it hands each
 * chunk to the async writer over a single-slot channel. The callback runs on
 * the client's strand, the writer on the caller's; paused and the easy handle
 * stay on the client's strand.
 */
struct stream_state {
  CURL* easy;
  RGWHTTPStreamRWRequest::ReceiveCB* cb;
  boost::asio::any_io_executor client_ex;
  chunk_channel chunks;
  bool paused = false;
  int write_error = 0;
  bool body_received = false;

  stream_state(CURL* easy,
               RGWHTTPStreamRWRequest::ReceiveCB* cb,
               const boost::asio::any_io_executor& ex,
               boost::asio::any_io_executor client_ex)
    : easy(easy), cb(cb), client_ex(std::move(client_ex)),
      chunks(ex, 1)
  {}
};

size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata)
{
  auto* st = static_cast<stream_state*>(userdata);
  const size_t len = size * nmemb;

  ceph::buffer::list bl;
  bl.append(ptr, len);
  // slot full: pause; libcurl redelivers this buffer on resume
  if (!st->chunks.try_send(boost::system::error_code{}, std::move(bl))) {
    st->paused = true;
    return CURL_WRITEFUNC_PAUSE;
  }
  st->body_received = true;
  return len;
}

/*
 * Drain into the put-object sink; the throttle inside handle_data() suspends
 * here instead of blocking the transport thread.
 */
void writer_coro(boost::asio::yield_context yield, stream_state& st)
{
  for (;;) {
    boost::system::error_code ec;
    ceph::buffer::list bl = st.chunks.async_receive(yield[ec]);
    // a cancel racing close() can deliver channel_closed, so check the state
    if (yield.cancelled() != boost::asio::cancellation_type::none) {
      throw boost::system::system_error(boost::asio::error::operation_aborted);
    }
    if (ec == boost::asio::experimental::error::channel_closed) {
      return;
    }
    if (ec) {
      throw boost::system::system_error(ec);
    }

    int r = st.cb->handle_data(bl);
    if (r < 0) {
      st.write_error = r;
      return;
    }

    // pause state and the easy handle belong to the client's executor; hop
    // there to resume the transfer
    boost::asio::dispatch(boost::asio::bind_executor(st.client_ex, yield));
    if (st.paused) {
      st.paused = false;
      curl_easy_pause(st.easy, CURLPAUSE_CONT);
    }
  }
}

} // namespace

int cloud_get_streamed(const DoutPrefixProvider* dpp,
                       boost::asio::yield_context yield,
                       void* easy_handle,
                       RGWHTTPStreamRWRequest::ReceiveCB* cb,
                       bool& connect_failed)
{
  CURL* easy = static_cast<CURL*>(easy_handle);
  connect_failed = false;
  stream_state st{easy, cb, yield.get_executor(),
                  rgw::curl::get_shared_client(yield.get_executor()).get_executor()};

  // install our streaming write callback on the (already signed) easy handle
  curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, write_callback);
  curl_easy_setopt(easy, CURLOPT_WRITEDATA, &st);

  int transfer_ret = 0;

  /*
   * The writer runs on the caller's coroutine because the throttle is bound to
   * that yield and may only be driven there. The transport runs as a child;
   * both finish before returning so `st` outlives the write callback.
   */
  ceph::async::spawn_throttle group{yield, 1};

  try {
    group.spawn([&] (boost::asio::yield_context y) {
      boost::system::error_code ec;
      rgw::curl::async_perform(y.get_executor(), easy, y[ec]);
      st.chunks.close();
      // operation_aborted is our own cancel; the writer handles it
      if (ec && ec != boost::asio::error::operation_aborted) {
        long http_status = 0;
        curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &http_status);
        if (http_status >= 400) {
          transfer_ret = rgw_http_error_to_errno(http_status);
        } else if (st.body_received) {
          /*
           * Dropped mid-object: a retry would re-append into the same processor,
           * so fail non-retryably; the restore retries later from a fresh sink.
           */
          transfer_ret = -ECONNABORTED;
          connect_failed = true;
        } else {
          transfer_ret = -EIO;   // pre-first-byte failure (DNS/connect/TLS): retryable
          connect_failed = true;
        }
        ldpp_dout(dpp, 0) << "ERROR: cloud GET transfer failed: " << ec.message()
                          << " (http " << http_status << ")" << dendl;
      }
    });

    writer_coro(yield, st);            // on the caller's coroutine (throttle's yield)

    if (st.write_error < 0) {
      group.cancel();                  // sink failed: abort the in-flight transfer
    }
    group.wait();
  } catch (...) {
    // clear cancellation so wait() can join before local state is destroyed
    yield.reset_cancellation_state();
    group.cancel();
    try {
      group.wait();
    } catch (...) {}                   // preserve the original exception
    throw;
  }

  /*
   * Sink errors return verbatim and take precedence over the transfer
   * result. The rados sink cannot produce -EBUSY or -ERR_INTERNAL_ERROR,
   * so the caller's transient retry never re-streams into a sink that
   * already consumed chunks; a sink -EIO reads like a transport -EIO.
   */
  if (st.write_error < 0) {
    return st.write_error;
  }
  if (transfer_ret < 0) {
    return transfer_ret;
  }

  long http_status = 0;
  curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &http_status);
  if (http_status < 200 || http_status >= 300) {
    ldpp_dout(dpp, 0) << "ERROR: cloud GET returned HTTP status " << http_status << dendl;
    if (st.body_received) {
      // an error body reached the sink; requeue with a fresh one
      return -ECONNABORTED;
    }
    return rgw_http_error_to_errno(http_status);
  }
  return 0;
}

} // namespace rgw::restore
