// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_LEADER_WATCHER_H
#define CEPH_RBD_MIRROR_LEADER_WATCHER_H

#include "librbd/ManagedLock.h"
#include "librbd/Watcher.h"
#include "MirrorStatusWatcher.h"
#include "tools/rbd_mirror/leader_watcher/Types.h"

namespace librbd {
  class ImageCtx;
}

namespace rbd {
namespace mirror {

class LeaderWatcher : public librbd::Watcher {
public:
  LeaderWatcher(librados::IoCtx &io_ctx, ContextWQ *work_queue, Mutex &lock,
                Cond &cond);

  std::string get_oid() const {
    return m_oid;
  }

  int init();
  void shut_down();

  bool is_leader() {
    assert(m_lock.is_locked());
    return m_leader;
  }

  void check_leader_alive(utime_t &now, int heartbeat_interval);

  void notify_heartbeat(Context *on_finish);
  void notify_lock_acquired(Context *on_finish);
  void notify_lock_released(Context *on_finish);

protected:
  void handle_heartbeat(Context *on_ack);
  void handle_lock_acquired(Context *on_ack);
  void handle_lock_released(Context *on_ack);

  void handle_notify(uint64_t notify_id, uint64_t handle,
                     uint64_t notifier_id, bufferlist &bl);

private:
  typedef librbd::ManagedLock<librbd::ImageCtx> LeaderLock;

  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    LeaderWatcher *leader_watcher;
    Context *on_notify_ack;

    HandlePayloadVisitor(LeaderWatcher *leader_watcher, Context *on_notify_ack)
      : leader_watcher(leader_watcher), on_notify_ack(on_notify_ack) {
    }

    template <typename Payload>
    inline void operator()(const Payload &payload) const {
      leader_watcher->handle_payload(payload, on_notify_ack);
    }
  };

  Mutex &m_lock;
  Cond &m_cond;
  uint64_t m_notifier_id;

  bool m_leader = false;
  utime_t m_leader_last_heartbeat;
  std::unique_ptr<LeaderLock> m_leader_lock;
  std::unique_ptr<MirrorStatusWatcher> m_status_watcher;
  LeaderLock::LockOwner m_leader_lock_owner;
  LeaderLock::LockOwner m_lock_owner;

  void acquire_leader_lock(bool blacklist_on_break_lock = false);
  void handle_acquire_leader_lock(int r);

  void release_leader_lock(Context *on_finish);

  void handle_payload(const leader_watcher::HeartbeatPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const leader_watcher::LockAcquiredPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const leader_watcher::LockReleasedPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const leader_watcher::UnknownPayload &payload,
                      Context *on_notify_ack);
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_LEADER_WATCHER_H
