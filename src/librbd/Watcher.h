// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_WATCHER_H
#define CEPH_LIBRBD_WATCHER_H

#include "common/Mutex.h"
#include "common/RWLock.h"
#include "include/rados/librados.hpp"
#include "librbd/watcher/Notifier.h"
#include "librbd/watcher/Types.h"
#include <string>
#include <utility>

class ContextWQ;

namespace librbd {

class Watcher {
  friend struct watcher::C_NotifyAck;

public:
  Watcher(librados::IoCtx& ioctx, ContextWQ *work_queue,
          const std::string& oid);
  virtual ~Watcher();

  void register_watch(Context *on_finish);
  void unregister_watch(Context *on_finish);
  void flush(Context *on_finish);

  void set_oid(const string& oid);

  uint64_t get_watch_handle() const {
    RWLock::RLocker watch_locker(m_watch_lock);
    return m_watch_handle;
  }

  bool is_registered() const {
    RWLock::RLocker locker(m_watch_lock);
    return m_watch_state == WATCH_STATE_REGISTERED;
  }

protected:
  enum WatchState {
    WATCH_STATE_UNREGISTERED,
    WATCH_STATE_REGISTERED,
    WATCH_STATE_ERROR,
    WATCH_STATE_REWATCHING
  };

  librados::IoCtx& m_ioctx;
  ContextWQ *m_work_queue;
  std::string m_oid;
  CephContext *m_cct;
  mutable RWLock m_watch_lock;
  uint64_t m_watch_handle;
  watcher::Notifier m_notifier;
  WatchState m_watch_state;

  void send_notify(bufferlist &payload, bufferlist *out_bl = nullptr,
                   Context *on_finish = nullptr);

  virtual void handle_notify(uint64_t notify_id, uint64_t handle,
                             uint64_t notifier_id, bufferlist &bl) = 0;

  virtual void handle_error(uint64_t cookie, int err);

  void acknowledge_notify(uint64_t notify_id, uint64_t handle,
                          bufferlist &out);

  virtual void handle_rewatch_complete(int r) { }

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * UNREGISTERED
   *    |
   *    | (register_watch)
   *    |
   *    v      (watch error)
   * REGISTERED * * * * * * * > ERROR
   *    |   ^                     |
   *    |   |                     | (rewatch)
   *    |   |                     v
   *    |   |                   REWATCHING
   *    |   |                     |
   *    |   |                     |
   *    |   \---------------------/
   *    |
   *    | (unregister_watch)
   *    |
   *    v
   * UNREGISTERED
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  struct WatchCtx : public librados::WatchCtx2 {
    Watcher &watcher;

    WatchCtx(Watcher &parent) : watcher(parent) {}

    virtual void handle_notify(uint64_t notify_id,
                               uint64_t handle,
      			       uint64_t notifier_id,
                               bufferlist& bl);
    virtual void handle_error(uint64_t handle, int err);
  };

  struct C_RegisterWatch : public Context {
    Watcher *watcher;
    Context *on_finish;

    C_RegisterWatch(Watcher *watcher, Context *on_finish)
       : watcher(watcher), on_finish(on_finish) {
    }
    virtual void finish(int r) override {
      watcher->handle_register_watch(r);
      on_finish->complete(r);
    }
  };

  WatchCtx m_watch_ctx;
  Context *m_unregister_watch_ctx = nullptr;

  void handle_register_watch(int r);

  void rewatch();
  void handle_rewatch(int r);

};

} // namespace librbd

#endif // CEPH_LIBRBD_WATCHER_H
