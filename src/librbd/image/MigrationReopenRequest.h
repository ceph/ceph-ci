// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_IMAGE_MIGRATION_REOPEN_REQUEST_H
#define CEPH_LIBRBD_IMAGE_MIGRATION_REOPEN_REQUEST_H

#include "include/buffer.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"

class Context;

namespace librbd {

class ImageCtx;

namespace image {

/**
 * Follows an image through a live migration that has just been prepared.
 *
 * The destination is read back from the source header rather than being told
 * to us: that header is the record the migration itself goes by, so a watcher
 * that reads it cannot end up pointed at something the migration has since
 * changed its mind about.
 *
 * A source header with no migration spec on it means the prepare was rolled
 * back, and there is nothing to follow -- that is a success, not an error, so
 * that one notify serves both outcomes.
 */
template <typename ImageCtxT = ImageCtx>
class MigrationReopenRequest {
public:
  static MigrationReopenRequest *create(ImageCtxT *image_ctx,
                                        Context *on_finish) {
    return new MigrationReopenRequest(image_ctx, on_finish);
  }

  MigrationReopenRequest(ImageCtxT *image_ctx, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_MIGRATION_HEADER
   *    |
   *    v (skip if the prepare was rolled back)
   * REOPEN
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  Context *m_on_finish;

  bufferlist m_out_bl;
  cls::rbd::MigrationSpec m_migration_spec;

  void send_get_migration_header();
  void handle_get_migration_header(int r);

  void send_reopen();
  void handle_reopen(int r);

  void finish(int r);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::MigrationReopenRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_MIGRATION_REOPEN_REQUEST_H
