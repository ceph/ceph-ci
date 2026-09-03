// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_IO_REOPEN_BLOCK_IMAGE_DISPATCH_H
#define CEPH_LIBRBD_IO_REOPEN_BLOCK_IMAGE_DISPATCH_H

#include "librbd/io/ImageDispatchInterface.h"
#include "include/int_types.h"
#include "include/buffer.h"
#include "common/ceph_mutex.h"
#include "common/zipkin_trace.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Types.h"

#include <list>
#include <shared_mutex> // for std::shared_lock

struct Context;

namespace librbd {

struct ImageCtx;

namespace io {

struct AioCompletion;

/**
 * Parks incoming IO while the image context is re-targeted at a different
 * image, so that a reopen is invisible to whoever holds the handle.
 *
 * This sits at the top of the dispatch stack, above the layers that describe
 * the image, and it is the one layer ImageDispatcher::shut_down_for_reopen()
 * keeps: the requests it has parked have to outlive the teardown that the
 * re-target runs.
 *
 * Nothing is parked below the core image dispatch layer, so parked requests
 * are not registered in ImageCtx::async_ops and the drain the teardown does
 * cannot wait on them.
 */
template <typename ImageCtxT>
class ReopenBlockImageDispatch : public ImageDispatchInterface {
public:
  ReopenBlockImageDispatch(ImageCtxT* image_ctx);

  ImageDispatchLayer get_dispatch_layer() const override {
    return IMAGE_DISPATCH_LAYER_REOPEN_BLOCK;
  }

  void shut_down(Context* on_finish) override;

  /// park further IO and complete on_blocked once everything already in
  /// flight below this layer has drained. Blockers nest: a migration holds
  /// one across the whole prepare, and the re-target it ends with takes
  /// another of its own
  void block_io(Context* on_blocked);
  /// drop a blocker, releasing the parked requests once the last one goes. A
  /// negative r fails them instead of letting them through, for a re-target
  /// that did not complete
  void unblock_io(int r);

  inline bool io_blocked() const {
    std::shared_lock locker{m_lock};
    return (m_blockers > 0);
  }

  bool read(
      AioCompletion* aio_comp, Extents &&image_extents,
      ReadResult &&read_result, IOContext io_context, int op_flags,
      int read_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool write(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool discard(
      AioCompletion* aio_comp, Extents &&image_extents,
      uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool write_same(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool compare_and_write(
      AioCompletion* aio_comp, Extents &&image_extents,
      bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool flush(
      AioCompletion* aio_comp, FlushSource flush_source,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool list_snaps(
      AioCompletion* aio_comp, Extents&& image_extents, SnapIds&& snap_ids,
      int list_snaps_flags, SnapshotDelta* snapshot_delta,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool invalidate_cache(Context* on_finish) override {
    return false;
  }

private:
  typedef std::list<Context*> Contexts;

  ImageCtxT* m_image_ctx;

  mutable ceph::shared_mutex m_lock;
  uint32_t m_blockers = 0;
  int m_unblock_result = 0;
  Contexts m_on_dispatches;

  uint64_t m_in_flight_ios = 0;
  Contexts m_on_blocked_contexts;

  void handle_finished(int r, uint64_t tid);

  bool process_io(uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
                  DispatchResult* dispatch_result, Context** on_finish,
                  Context* on_dispatched);
};

} // namespace io
} // namespace librbd

extern template class librbd::io::ReopenBlockImageDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_REOPEN_BLOCK_IMAGE_DISPATCH_H
