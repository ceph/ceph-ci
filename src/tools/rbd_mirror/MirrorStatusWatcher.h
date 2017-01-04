// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_MIRROR_STATUS_WATCHER_H
#define CEPH_RBD_MIRROR_MIRROR_STATUS_WATCHER_H

#include "librbd/Watcher.h"

namespace librbd {
  class ImageCtx;
}

namespace rbd {
namespace mirror {

class MirrorStatusWatcher : public librbd::Watcher {
public:
  MirrorStatusWatcher(librados::IoCtx &io_ctx, ContextWQ *work_queue);

  void init(Context *on_finish);
  void shut_down(Context *on_finish);

  virtual void handle_notify(uint64_t notify_id, uint64_t handle,
			     uint64_t notifier_id, bufferlist &bl);
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_MIRROR_STATUS_WATCHER_H
