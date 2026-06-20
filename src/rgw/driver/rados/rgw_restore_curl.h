// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#pragma once

#include <boost/asio/spawn.hpp>

#include "common/dout.h"
#include "rgw_rest_client.h"

namespace rgw::restore {

/*
 * Stream a signed cloud GET through rgw::curl into the restore put-object sink.
 *
 * Curl's write callback hands one buffer at a time to the caller's yield
 * coroutine, which drains it into the sink and pauses the transfer while the
 * async write is in flight. `easy` must already be configured; this helper
 * owns the write callback and returns 0 or a negative errno. connect_failed
 * is set only on a transport-layer failure, so the caller rotates the endpoint
 * on connectivity errors but not on sink or HTTP errors.
 */
int cloud_get_streamed(const DoutPrefixProvider* dpp,
                       boost::asio::yield_context yield,
                       void* easy,
                       RGWHTTPStreamRWRequest::ReceiveCB* cb,
                       bool& connect_failed);

} // namespace rgw::restore
