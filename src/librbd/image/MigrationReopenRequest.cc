// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/image/MigrationReopenRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"

#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::MigrationReopenRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace image {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
MigrationReopenRequest<I>::MigrationReopenRequest(I *image_ctx,
                                                  Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish) {
}

template <typename I>
void MigrationReopenRequest<I>::send() {
  send_get_migration_header();
}

template <typename I>
void MigrationReopenRequest<I>::send_get_migration_header() {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  librados::ObjectReadOperation op;
  cls_client::migration_get_start(&op);

  auto comp = create_rados_callback<
    MigrationReopenRequest<I>,
    &MigrationReopenRequest<I>::handle_get_migration_header>(this);
  m_out_bl.clear();
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op,
                                          &m_out_bl);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void MigrationReopenRequest<I>::handle_get_migration_header(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = cls_client::migration_get_finish(&it, &m_migration_spec);
  }

  if (r == -ENOENT || r == -EOPNOTSUPP) {
    // the prepare was rolled back, so there is nothing to follow
    ldout(cct, 5) << "no migration header: nothing to re-target at" << dendl;
    finish(0);
    return;
  } else if (r < 0) {
    lderr(cct) << "failed to retrieve migration header: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  ldout(cct, 10) << "migration_spec=" << m_migration_spec << dendl;

  if (m_migration_spec.header_type != cls::rbd::MIGRATION_HEADER_TYPE_SRC) {
    // this image context is already the destination
    ldout(cct, 5) << "not a migration source: nothing to re-target at"
                  << dendl;
    finish(0);
    return;
  }

  switch (m_migration_spec.state) {
  case cls::rbd::MIGRATION_STATE_PREPARED:
  case cls::rbd::MIGRATION_STATE_EXECUTING:
  case cls::rbd::MIGRATION_STATE_EXECUTED:
    break;
  case cls::rbd::MIGRATION_STATE_ABORTING:
    // the destination is on its way out
    ldout(cct, 5) << "migration is being aborted: staying put" << dendl;
    finish(0);
    return;
  default:
    lderr(cct) << "migration is in an unexpected state: "
               << m_migration_spec.state << dendl;
    finish(-EINVAL);
    return;
  }

  send_reopen();
}

template <typename I>
void MigrationReopenRequest<I>::send_reopen() {
  auto cct = m_image_ctx->cct;

  librados::IoCtx io_ctx;
  int r = util::create_ioctx(m_image_ctx->md_ctx, "destination image",
                             m_migration_spec.pool_id,
                             m_migration_spec.pool_namespace, &io_ctx);
  if (r < 0) {
    lderr(cct) << "failed to open destination pool: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  // a migration carries the snapshots over by name, so an image context that
  // is pinned to one follows it by name too -- the ids on the destination are
  // its own
  std::string snap_name;
  {
    std::shared_lock image_locker{m_image_ctx->image_lock};
    snap_name = m_image_ctx->snap_name;
  }

  ldout(cct, 10) << "re-targeting at " << io_ctx.get_pool_name() << "/"
                 << m_migration_spec.image_name
                 << (snap_name.empty() ? "" : "@" + snap_name) << dendl;

  auto ctx = create_context_callback<
    MigrationReopenRequest<I>,
    &MigrationReopenRequest<I>::handle_reopen>(this);
  m_image_ctx->state->reopen(io_ctx, m_migration_spec.image_name,
                             m_migration_spec.image_id,
                             snap_name.empty() ? nullptr : snap_name.c_str(),
                             ctx);
}

template <typename I>
void MigrationReopenRequest<I>::handle_reopen(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to re-target at the migration destination: "
               << cpp_strerror(r) << dendl;
  }

  finish(r);
}

template <typename I>
void MigrationReopenRequest<I>::finish(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::MigrationReopenRequest<librbd::ImageCtx>;
