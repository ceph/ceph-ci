// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_WATCHER_H
#define CEPH_LIBRBD_IMAGE_WATCHER_H

#include "cls/rbd/cls_rbd_types.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
#include "include/Context.h"
#include "include/rbd/librbd.hpp"
#include "librbd/Watcher.h"
#include "librbd/WatchNotifyTypes.h"
#include <set>
#include <string>
#include <utility>

class entity_name_t;

namespace librbd {

namespace watcher {
template <typename> struct HandlePayloadVisitor;
}

class ImageCtx;
template <typename> class TaskFinisher;

template <typename ImageCtxT = ImageCtx>
class ImageWatcher : public Watcher {
  friend struct watcher::HandlePayloadVisitor<ImageWatcher<ImageCtxT>>;

public:
  ImageWatcher(ImageCtxT& image_ctx);
  virtual ~ImageWatcher();

  void unregister_watch(Context *on_finish);

  void notify_flatten(uint64_t request_id, ProgressContext &prog_ctx,
                      Context *on_finish);
  void notify_resize(uint64_t request_id, uint64_t size, bool allow_shrink,
                     ProgressContext &prog_ctx, Context *on_finish);
  void notify_snap_create(const std::string &snap_name,
			  const cls::rbd::SnapshotNamespace &snap_namespace,
			  Context *on_finish);
  void notify_snap_rename(const snapid_t &src_snap_id,
                          const std::string &dst_snap_name,
                          Context *on_finish);
  void notify_snap_remove(const std::string &snap_name, Context *on_finish);
  void notify_snap_protect(const std::string &snap_name, Context *on_finish);
  void notify_snap_unprotect(const std::string &snap_name, Context *on_finish);
  void notify_rebuild_object_map(uint64_t request_id,
                                 ProgressContext &prog_ctx, Context *on_finish);
  void notify_rename(const std::string &image_name, Context *on_finish);

  void notify_update_features(uint64_t features, bool enabled,
                              Context *on_finish);

  void notify_acquired_lock();
  void notify_released_lock();
  void notify_request_lock();

  void notify_header_update(Context *on_finish);
  static void notify_header_update(librados::IoCtx &io_ctx,
                                   const std::string &oid);

private:
  enum TaskCode {
    TASK_CODE_REQUEST_LOCK,
    TASK_CODE_CANCEL_ASYNC_REQUESTS,
    TASK_CODE_REREGISTER_WATCH,
    TASK_CODE_ASYNC_REQUEST,
    TASK_CODE_ASYNC_PROGRESS
  };

  typedef std::pair<Context *, ProgressContext *> AsyncRequest;

  class Task {
  public:
    Task(TaskCode task_code) : m_task_code(task_code) {}
    Task(TaskCode task_code, const watch_notify::AsyncRequestId &id)
      : m_task_code(task_code), m_async_request_id(id) {}

    inline bool operator<(const Task& rhs) const {
      if (m_task_code != rhs.m_task_code) {
        return m_task_code < rhs.m_task_code;
      } else if ((m_task_code == TASK_CODE_ASYNC_REQUEST ||
                  m_task_code == TASK_CODE_ASYNC_PROGRESS) &&
                 m_async_request_id != rhs.m_async_request_id) {
        return m_async_request_id < rhs.m_async_request_id;
      }
      return false;
    }
  private:
    TaskCode m_task_code;
    watch_notify::AsyncRequestId m_async_request_id;
  };

  class RemoteProgressContext : public ProgressContext {
  public:
    RemoteProgressContext(ImageWatcher &image_watcher,
                          const watch_notify::AsyncRequestId &id)
      : m_image_watcher(image_watcher), m_async_request_id(id)
    {
    }

    virtual int update_progress(uint64_t offset, uint64_t total) {
      m_image_watcher.schedule_async_progress(m_async_request_id, offset,
                                              total);
      return 0;
    }

  private:
    ImageWatcher &m_image_watcher;
    watch_notify::AsyncRequestId m_async_request_id;
  };

  class RemoteContext : public Context {
  public:
    RemoteContext(ImageWatcher &image_watcher,
      	          const watch_notify::AsyncRequestId &id,
      	          ProgressContext *prog_ctx)
      : m_image_watcher(image_watcher), m_async_request_id(id),
        m_prog_ctx(prog_ctx)
    {
    }

    virtual ~RemoteContext() {
      delete m_prog_ctx;
    }

    virtual void finish(int r);

  private:
    ImageWatcher &m_image_watcher;
    watch_notify::AsyncRequestId m_async_request_id;
    ProgressContext *m_prog_ctx;
  };

  struct C_ProcessPayload : public Context {
    ImageWatcher *image_watcher;
    uint64_t notify_id;
    uint64_t handle;
    watch_notify::Payload payload;

    C_ProcessPayload(ImageWatcher *image_watcher_, uint64_t notify_id_,
                     uint64_t handle_, const watch_notify::Payload &payload)
      : image_watcher(image_watcher_), notify_id(notify_id_), handle(handle_),
        payload(payload) {
    }

    virtual void finish(int r) override {
      image_watcher->process_payload(notify_id, handle, payload, r);
    }
  };

  struct C_ResponseMessage : public Context {
    watcher::C_NotifyAck *notify_ack;

    C_ResponseMessage(watcher::C_NotifyAck *notify_ack) : notify_ack(notify_ack) {
    }
    virtual void finish(int r);
  };

  ImageCtxT &m_image_ctx;

  TaskFinisher<Task> *m_task_finisher;

  RWLock m_async_request_lock;
  std::map<watch_notify::AsyncRequestId, AsyncRequest> m_async_requests;
  std::set<watch_notify::AsyncRequestId> m_async_pending;

  Mutex m_owner_client_id_lock;
  watch_notify::ClientId m_owner_client_id;

  void handle_register_watch(int r);

  void schedule_cancel_async_requests();
  void cancel_async_requests();

  void set_owner_client_id(const watch_notify::ClientId &client_id);
  watch_notify::ClientId get_client_id();

  void handle_request_lock(int r);
  void schedule_request_lock(bool use_timer, int timer_delay = -1);

  void notify_lock_owner(const watch_notify::Payload& payload,
                         Context *on_finish);

  Context *remove_async_request(const watch_notify::AsyncRequestId &id);
  void schedule_async_request_timed_out(const watch_notify::AsyncRequestId &id);
  void async_request_timed_out(const watch_notify::AsyncRequestId &id);
  void notify_async_request(const watch_notify::AsyncRequestId &id,
                            const watch_notify::Payload &payload,
                            ProgressContext& prog_ctx,
                            Context *on_finish);

  void schedule_async_progress(const watch_notify::AsyncRequestId &id,
                               uint64_t offset, uint64_t total);
  int notify_async_progress(const watch_notify::AsyncRequestId &id,
                            uint64_t offset, uint64_t total);
  void schedule_async_complete(const watch_notify::AsyncRequestId &id, int r);
  void notify_async_complete(const watch_notify::AsyncRequestId &id, int r);
  void handle_async_complete(const watch_notify::AsyncRequestId &request, int r,
                             int ret_val);

  int prepare_async_request(const watch_notify::AsyncRequestId& id,
                            bool* new_request, Context** ctx,
                            ProgressContext** prog_ctx);

  bool handle_payload(const watch_notify::HeaderUpdatePayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::AcquiredLockPayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::ReleasedLockPayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::RequestLockPayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::AsyncProgressPayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::AsyncCompletePayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::FlattenPayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::ResizePayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapCreatePayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapRenamePayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapRemovePayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapProtectPayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::SnapUnprotectPayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::RebuildObjectMapPayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::RenamePayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::UpdateFeaturesPayload& payload,
                      watcher::C_NotifyAck *ctx);
  bool handle_payload(const watch_notify::UnknownPayload& payload,
                      watcher::C_NotifyAck *ctx);
  void process_payload(uint64_t notify_id, uint64_t handle,
                             const watch_notify::Payload &payload, int r);

  virtual void handle_notify(uint64_t notify_id, uint64_t handle,
                             uint64_t notifier_id, bufferlist &bl);
  virtual void handle_error(uint64_t cookie, int err);
  virtual void handle_rewatch_complete(int r);

  void send_notify(const watch_notify::Payload& payload,
                   Context *ctx = nullptr);

};

} // namespace librbd

extern template class librbd::ImageWatcher<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_WATCHER_H
