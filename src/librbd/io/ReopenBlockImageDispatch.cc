// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/io/ReopenBlockImageDispatch.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ReopenBlockImageDispatch: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

template <typename I>
ReopenBlockImageDispatch<I>::ReopenBlockImageDispatch(I* image_ctx)
  : m_image_ctx(image_ctx),
    m_lock(ceph::make_shared_mutex(
      util::unique_lock_name("librbd::io::ReopenBlockImageDispatch::m_lock",
                             this))) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "ictx=" << image_ctx << dendl;
}

template <typename I>
void ReopenBlockImageDispatch<I>::shut_down(Context* on_finish) {
  on_finish->complete(0);
}

template <typename I>
void ReopenBlockImageDispatch<I>::block_io(Context* on_blocked) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << dendl;

  {
    std::unique_lock locker{m_lock};
    ++m_blockers;
    ldout(cct, 5) << "num=" << m_blockers << dendl;

    // wait for everything that already made it past this layer to finish. It
    // is not enough to flush, nor to drain ImageCtx::async_ops: a request can
    // be sitting in a layer below without having reached the core dispatch
    // layer at all -- waiting on the exclusive lock, say -- and the re-target
    // is about to tear that layer down underneath it
    if (m_in_flight_ios > 0) {
      ldout(cct, 5) << "waiting for " << m_in_flight_ios
                    << " in-flight request(s)" << dendl;
      m_on_blocked_contexts.push_back(on_blocked);
      return;
    }
  }

  on_blocked->complete(0);
}

template <typename I>
void ReopenBlockImageDispatch<I>::handle_finished(int r, uint64_t tid) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "r=" << r << ", tid=" << tid << dendl;

  Contexts on_blocked_contexts;
  {
    std::unique_lock locker{m_lock};
    ceph_assert(m_in_flight_ios > 0);
    --m_in_flight_ios;

    if (m_blockers > 0 && m_in_flight_ios == 0) {
      std::swap(on_blocked_contexts, m_on_blocked_contexts);
    }
  }

  for (auto ctx : on_blocked_contexts) {
    ctx->complete(0);
  }
}

template <typename I>
void ReopenBlockImageDispatch<I>::unblock_io(int r) {
  auto cct = m_image_ctx->cct;

  Contexts dispatch_contexts;
  {
    std::unique_lock locker{m_lock};
    ceph_assert(m_blockers > 0);

    // hold on to the first failure so that it is not lost behind an outer
    // blocker that has nothing to report
    if (r < 0 && m_unblock_result == 0) {
      m_unblock_result = r;
    }

    if (--m_blockers > 0) {
      ldout(cct, 5) << "num=" << m_blockers << dendl;
      return;
    }

    r = m_unblock_result;
    m_unblock_result = 0;
    std::swap(dispatch_contexts, m_on_dispatches);
  }

  ldout(cct, 5) << "releasing " << dispatch_contexts.size() << " request(s)"
                << ", r=" << r << dendl;

  for (auto ctx : dispatch_contexts) {
    ctx->complete(r);
  }
}

template <typename I>
bool ReopenBlockImageDispatch<I>::read(
    AioCompletion* aio_comp, Extents &&image_extents,
    ReadResult &&read_result, IOContext io_context, int op_flags,
    int read_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return process_io(tid, image_dispatch_flags, dispatch_result, on_finish,
                    on_dispatched);
}

template <typename I>
bool ReopenBlockImageDispatch<I>::write(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return process_io(tid, image_dispatch_flags, dispatch_result, on_finish,
                    on_dispatched);
}

template <typename I>
bool ReopenBlockImageDispatch<I>::discard(
    AioCompletion* aio_comp, Extents &&image_extents,
    uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return process_io(tid, image_dispatch_flags, dispatch_result, on_finish,
                    on_dispatched);
}

template <typename I>
bool ReopenBlockImageDispatch<I>::write_same(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return process_io(tid, image_dispatch_flags, dispatch_result, on_finish,
                    on_dispatched);
}

template <typename I>
bool ReopenBlockImageDispatch<I>::compare_and_write(
    AioCompletion* aio_comp, Extents &&image_extents,
    bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return process_io(tid, image_dispatch_flags, dispatch_result, on_finish,
                    on_dispatched);
}

template <typename I>
bool ReopenBlockImageDispatch<I>::flush(
    AioCompletion* aio_comp, FlushSource flush_source,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  if (flush_source != FLUSH_SOURCE_USER) {
    // the re-target drives internal flushes of its own -- image::CloseRequest
    // injects one at the top of the stack -- and parking those would deadlock
    // the very teardown this layer is holding IO for
    return false;
  }

  return process_io(tid, image_dispatch_flags, dispatch_result, on_finish,
                    on_dispatched);
}

template <typename I>
bool ReopenBlockImageDispatch<I>::list_snaps(
    AioCompletion* aio_comp, Extents&& image_extents, SnapIds&& snap_ids,
    int list_snaps_flags, SnapshotDelta* snapshot_delta,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return process_io(tid, image_dispatch_flags, dispatch_result, on_finish,
                    on_dispatched);
}

template <typename I>
bool ReopenBlockImageDispatch<I>::process_io(
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  std::unique_lock locker{m_lock};
  if (m_blockers == 0 && m_on_dispatches.empty()) {
    // track it so that a block can tell when everything below has drained
    ++m_in_flight_ios;
    *on_finish = new LambdaContext([this, tid, on_finish=*on_finish](int r) {
        handle_finished(r, tid);
        on_finish->complete(r);
      });
    return false;
  }

  // the image context may be re-targeted at another image while this request
  // waits, so its IO context -- which names the pool, namespace and snapshot
  // it was issued against -- has to be restamped before it is dispatched.
  // ImageDispatcher::send() does that when it sees this flag
  *image_dispatch_flags |= IMAGE_DISPATCH_FLAG_REOPENED;

  *dispatch_result = DISPATCH_RESULT_RESTART;
  m_on_dispatches.push_back(on_dispatched);
  return true;
}

} // namespace io
} // namespace librbd

template class librbd::io::ReopenBlockImageDispatch<librbd::ImageCtx>;
