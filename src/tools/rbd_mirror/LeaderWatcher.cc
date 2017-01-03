// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LeaderWatcher.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/Utils.h"
#include "librbd/watcher/Types.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::LeaderWatcher: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {

using namespace leader_watcher;

using librbd::util::create_context_callback;
using librbd::util::create_rados_ack_callback;

namespace {

static const uint64_t NOTIFY_TIMEOUT_MS = 5000;

} // anonymous namespace

LeaderWatcher::LeaderWatcher(librados::IoCtx &io_ctx, ContextWQ *work_queue,
                             Mutex &lock, Cond &cond)
  : Watcher(io_ctx, work_queue, RBD_MIRROR_LEADER), m_lock(lock), m_cond(cond),
    m_notifier_id(librados::Rados(io_ctx).get_instance_id()) {
}

int LeaderWatcher::init() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  if (m_leader_lock) {
    dout(20) << "already initialized" << dendl;
    return 0;
  }

  int r = m_ioctx.create(m_oid, false);
  if (r < 0) {
    derr << "error creating " << m_oid << " object: " << cpp_strerror(r)
         << dendl;
    return r;
  }

  C_SaferCond register_ctx;
  register_watch(&register_ctx);
  r = register_ctx.wait();
  if (r < 0) {
    derr << "error registering leader watcher for " << m_oid << " object: "
         << cpp_strerror(r) << dendl;
    return r;
  }

  m_leader_lock.reset(new LeaderLock(m_ioctx, m_work_queue, get_oid(), this));
  m_leader_last_heartbeat = ceph_clock_now(nullptr);
  acquire_leader_lock();

  return 0;
}

void LeaderWatcher::shut_down() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  if (!m_leader_lock) {
    dout(20) << "not initialized" << dendl;
    return;
  }

  unique_ptr<LeaderLock> leader_lock(m_leader_lock.release());

  if (m_leader) { // XXXMG: race?
    C_SaferCond release_lock_ctx;
    release_leader_lock(&release_lock_ctx);
    m_lock.Unlock();
    release_lock_ctx.wait();
  } else {
    m_lock.Unlock();
  }

  C_SaferCond shutdown_lock_ctx;
  leader_lock->shut_down(&shutdown_lock_ctx);
  shutdown_lock_ctx.wait();

  C_SaferCond unregister_ctx;
  unregister_watch(&unregister_ctx);
  int r = unregister_ctx.wait();
  if (r < 0) {
    derr << "error unregistering leader watcher for " << m_oid
         << " object: " << cpp_strerror(r) << dendl;
  }

  m_lock.Lock();
}

void LeaderWatcher::check_leader_alive(utime_t &now, int heartbeat_interval) {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  if (now < m_leader_last_heartbeat + 2 * heartbeat_interval) {
    return;
  }

  dout(0) << "no hearbeat from the leader for more than "
          << 2 * heartbeat_interval << " sec -- reacquiring the leader lock"
          << dendl;

  acquire_leader_lock(true);
  m_leader_last_heartbeat = now;
  m_leader_lock_owner = {};
}

void LeaderWatcher::acquire_leader_lock(bool blacklist_on_break_lock) {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  if (blacklist_on_break_lock && !m_leader_lock_owner.cookie.empty()) {
    FunctionContext *ctx = new FunctionContext(
      [this](int r) {
        if (r < 0 && r != -ENOENT) {
          derr << "error beaking leader lock: " << cpp_strerror(r)  << dendl;
          return;
        }

        Mutex::Locker locker(m_lock);
        acquire_leader_lock(false);
      });

    m_leader_lock->break_lock(m_leader_lock_owner, true, ctx);
    return;
  }

  Context *ctx = create_context_callback<
    LeaderWatcher, &LeaderWatcher::handle_acquire_leader_lock>(this);

  m_leader_lock->try_acquire_lock(ctx);
}

void LeaderWatcher::handle_acquire_leader_lock(int r) {
  dout(20) << dendl;

  if (r < 0) {
    derr << "error acquiring leader lock: " << cpp_strerror(r)
         << dendl;

    FunctionContext *ctx = new FunctionContext(
      [this](int r) {
        Mutex::Locker locker(m_lock);

        if (r < 0) {
          derr << "error retrieving leader lock owner: " << cpp_strerror(r)
               << dendl;
          m_leader_lock_owner = {};
        } else {
          m_leader_lock_owner = m_lock_owner;
          m_leader_last_heartbeat = ceph_clock_now(nullptr);
        }
        m_leader = false;
        m_cond.Signal();
      });

    m_leader_lock->get_lock_owner(&m_lock_owner, ctx);
    return;
  }

  MirrorStatusWatcher *status_watcher = new MirrorStatusWatcher(m_ioctx,
                                                                m_work_queue);
  FunctionContext *ctx = new FunctionContext(
    [this, status_watcher](int r) {
      Mutex::Locker locker(m_lock);
      if (r < 0) {
        derr << "error initializing mirror status watcher: " << cpp_strerror(r)
             << dendl;
        delete status_watcher;
        return;
      }

      m_status_watcher.reset(status_watcher);
      m_leader = true;
      m_cond.Signal();
    });

  ctx = new FunctionContext(
    [this, status_watcher, ctx](int r) {
      if (r < 0) {
        derr << "error notifying leader lock acquired: " << cpp_strerror(r)
             << dendl;
        delete status_watcher;
        return;
      }

      status_watcher->init(ctx);
    });

  ctx = new FunctionContext(
    [this, ctx](int r) {
      Mutex::Locker locker(m_lock);
      notify_lock_acquired(ctx);
    });

  m_work_queue->queue(ctx, 0);
}

void LeaderWatcher::release_leader_lock(Context *on_finish) {
  dout(20) << dendl;

  assert(m_lock.is_locked());
  assert(m_status_watcher);

  MirrorStatusWatcher *status_watcher = m_status_watcher.release();

  on_finish = new FunctionContext(
    [this, status_watcher, on_finish](int r) {
      if (r < 0) {
        derr << "error shutting mirror status watcher down: " << cpp_strerror(r)
             << dendl;
      }
      Mutex::Locker locker(m_lock);
      delete status_watcher;
      m_leader = false;
      m_leader_last_heartbeat = ceph_clock_now(nullptr);
      m_cond.Signal();
      on_finish->complete(r);
    });

  on_finish = new FunctionContext(
    [this, status_watcher, on_finish](int r) {
      if (r < 0) {
        derr << "error notifying leader lock released: " << cpp_strerror(r)
             << dendl;
      }
      status_watcher->shut_down(on_finish);
    });

  on_finish = new FunctionContext(
    [this, on_finish](int r) {
      if (r < 0) {
        derr << "error releasing leader lock: " << cpp_strerror(r) << dendl;
      }
      notify_lock_released(on_finish);
    });

  m_leader_lock->release_lock(on_finish);
}

void LeaderWatcher::notify_heartbeat(Context *on_finish) {
  dout(20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{HeartbeatPayload{}}, bl);

  librados::AioCompletion *comp = create_rados_ack_callback(on_finish);
  int r = m_ioctx.aio_notify(m_oid, comp, bl, NOTIFY_TIMEOUT_MS, nullptr);
  assert(r == 0);
  comp->release();
}

void LeaderWatcher::notify_lock_acquired(Context *on_finish) {
  dout(20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{LockAcquiredPayload{}}, bl);

  librados::AioCompletion *comp = create_rados_ack_callback(on_finish);
  int r = m_ioctx.aio_notify(m_oid, comp, bl, NOTIFY_TIMEOUT_MS, nullptr);
  assert(r == 0);
  comp->release();
}

void LeaderWatcher::notify_lock_released(Context *on_finish) {
  dout(20) << dendl;

  bufferlist bl;
  ::encode(NotifyMessage{LockReleasedPayload{}}, bl);

  librados::AioCompletion *comp = create_rados_ack_callback(on_finish);
  int r = m_ioctx.aio_notify(m_oid, comp, bl, NOTIFY_TIMEOUT_MS, nullptr);
  assert(r == 0);
  comp->release();
}

void LeaderWatcher::handle_heartbeat(Context *on_notify_ack) {
  dout(20) << dendl;

  {
    Mutex::Locker locker(m_lock);
    if (m_leader) {
      derr << "got another leader heartbeat" << dendl;
    }
    m_leader_last_heartbeat = ceph_clock_now(nullptr);
  }

  on_notify_ack->complete(0);
}

void LeaderWatcher::handle_lock_acquired(Context *on_notify_ack) {
  dout(20) << dendl;

  {
    Mutex::Locker locker(m_lock);
    m_leader = false;
    m_cond.Signal();

    FunctionContext *ctx = new FunctionContext(
      [this](int r) {
        Mutex::Locker locker(m_lock);

        if (r < 0) {
          derr << "error retrieving leader lock owner: " << cpp_strerror(r)
               << dendl;
          m_leader_lock_owner = {};
        } else {
          m_leader_lock_owner = m_lock_owner;
          m_leader_last_heartbeat = ceph_clock_now(nullptr);
        }
      });

    m_leader_lock->get_lock_owner(&m_lock_owner, ctx);
  }

  on_notify_ack->complete(0);
}

void LeaderWatcher::handle_lock_released(Context *on_notify_ack) {
  dout(20) << dendl;

  {
    Mutex::Locker locker(m_lock);
    acquire_leader_lock();
  }

  on_notify_ack->complete(0);
}


void LeaderWatcher::handle_notify(uint64_t notify_id, uint64_t handle,
                                  uint64_t notifier_id, bufferlist &bl) {
  dout(20) << "notify_id=" << notify_id << ", handle=" << handle << ", "
           << "notifier_id=" << notifier_id << dendl;

  Context *ctx = new librbd::watcher::C_NotifyAck(this, notify_id, handle);

  if (notifier_id == m_notifier_id) {
    dout(20) << "our own notification, ignoring" << dendl;
    ctx->complete(0);
    return;
  }

  NotifyMessage notify_message;
  try {
    bufferlist::iterator iter = bl.begin();
    ::decode(notify_message, iter);
  } catch (const buffer::error &err) {
    derr << ": error decoding image notification: " << err.what() << dendl;
    ctx->complete(0);
    return;
  }

  apply_visitor(HandlePayloadVisitor(this, ctx), notify_message.payload);
}

void LeaderWatcher::handle_payload(const HeartbeatPayload &payload,
                                   Context *on_notify_ack) {
  dout(20) << "heartbeat" << dendl;

  handle_heartbeat(on_notify_ack);
}

void LeaderWatcher::handle_payload(const LockAcquiredPayload &payload,
                                   Context *on_notify_ack) {
  dout(20) << "lock_acquired" << dendl;

  handle_lock_acquired(on_notify_ack);
}

void LeaderWatcher::handle_payload(const LockReleasedPayload &payload,
                                   Context *on_notify_ack) {
  dout(20) << "lock_released" << dendl;

  handle_lock_released(on_notify_ack);
}

void LeaderWatcher::handle_payload(const UnknownPayload &payload,
                                   Context *on_notify_ack) {
  dout(20) << "unknown" << dendl;

  on_notify_ack->complete(0);
}

} // namespace mirror
} // namespace rbd
