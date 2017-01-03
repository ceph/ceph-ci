// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ManagedLock.h"
#include "librbd/managed_lock/AcquireRequest.h"
#include "librbd/managed_lock/BreakLockRequest.h"
#include "librbd/managed_lock/GetLockOwnerRequest.h"
#include "librbd/managed_lock/ReleaseRequest.h"
#include "librbd/managed_lock/ReacquireRequest.h"
#include "librbd/Watcher.h"
#include "librbd/ImageCtx.h"
#include "cls/lock/cls_lock_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/Utils.h"
#include <sstream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ManagedLock: " << this << " " <<  __func__

namespace librbd {

using std::string;

namespace {

const std::string WATCHER_LOCK_COOKIE_PREFIX = "auto";

template <typename R>
struct C_SendLockRequest : public Context {
  R* request;
  explicit C_SendLockRequest(R* request) : request(request) {
  }
  virtual void finish(int r) override {
    request->send();
  }
};

} // anonymous namespace

template <typename I>
const std::string ManagedLock<I>::WATCHER_LOCK_TAG("internal");

template <typename I>
ManagedLock<I>::ManagedLock(librados::IoCtx &ioctx, ContextWQ *work_queue,
                            const string& oid, Watcher *watcher)
  : m_lock(util::unique_lock_name("librbd::ManagedLock<I>::m_lock", this)),
    m_state(STATE_UNLOCKED),
    m_ioctx(ioctx), m_cct(reinterpret_cast<CephContext *>(ioctx.cct())),
    m_work_queue(work_queue),
    m_oid(oid),
    m_watcher(watcher) {
}

template <typename I>
ManagedLock<I>::~ManagedLock() {
  Mutex::Locker locker(m_lock);
  assert(m_state == STATE_SHUTDOWN || m_state == STATE_UNLOCKED ||
         m_state == STATE_UNINITIALIZED);
}

template <typename I>
bool ManagedLock<I>::is_lock_owner() const {
  Mutex::Locker locker(m_lock);

  return is_lock_owner(m_lock);
}

template <typename I>
bool ManagedLock<I>::is_lock_owner(Mutex &lock) const {
  bool lock_owner;

  switch (m_state) {
  case STATE_LOCKED:
  case STATE_REACQUIRING:
  case STATE_PRE_SHUTTING_DOWN:
  case STATE_POST_ACQUIRING:
  case STATE_PRE_RELEASING:
    lock_owner = true;
    break;
  default:
    lock_owner = false;
    break;
  }

  ldout(m_cct, 20) << "=" << lock_owner << dendl;
  return lock_owner;
}

template <typename I>
void ManagedLock<I>::shut_down(Context *on_shut_down) {
  ldout(m_cct, 10) << dendl;

  Mutex::Locker locker(m_lock);
  assert(!is_shutdown_locked());
  execute_action(ACTION_SHUT_DOWN, on_shut_down);
}

template <typename I>
void ManagedLock<I>::acquire_lock(Context *on_acquired) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    if (is_shutdown_locked()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_LOCKED || !m_actions_contexts.empty()) {
      ldout(m_cct, 10) << dendl;
      execute_action(ACTION_ACQUIRE_LOCK, on_acquired);
      return;
    }
  }

  on_acquired->complete(r);
}

template <typename I>
void ManagedLock<I>::try_acquire_lock(Context *on_acquired) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    if (is_shutdown_locked()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_LOCKED || !m_actions_contexts.empty()) {
      ldout(m_cct, 10) << dendl;
      execute_action(ACTION_TRY_LOCK, on_acquired);
      return;
    }
  }

  on_acquired->complete(r);
}

template <typename I>
void ManagedLock<I>::release_lock(Context *on_released) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    if (is_shutdown_locked()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_UNLOCKED || !m_actions_contexts.empty()) {
      ldout(m_cct, 10) << dendl;
      execute_action(ACTION_RELEASE_LOCK, on_released);
      return;
    }
  }

  on_released->complete(r);
}

template <typename I>
void ManagedLock<I>::reacquire_lock(Context *on_reacquired) {
  {
    Mutex::Locker locker(m_lock);

    if (m_state == STATE_WAITING_FOR_REGISTER) {
      // restart the acquire lock process now that watch is valid
      ldout(m_cct, 10) << ": " << "woke up waiting acquire" << dendl;
      Action active_action = get_active_action();
      assert(active_action == ACTION_TRY_LOCK ||
             active_action == ACTION_ACQUIRE_LOCK);
      execute_next_action();
    } else if (!is_shutdown_locked() &&
               (m_state == STATE_LOCKED ||
                m_state == STATE_ACQUIRING ||
                m_state == STATE_POST_ACQUIRING ||
                m_state == STATE_WAITING_FOR_LOCK)) {
      // interlock the lock operation with other state ops
      ldout(m_cct, 10) << dendl;
      execute_action(ACTION_REACQUIRE_LOCK, on_reacquired);
      return;
    }
  }

  // ignore request if shutdown or not in a locked-related state
  if (on_reacquired != nullptr) {
    on_reacquired->complete(0);
  }
}

template <typename I>
void ManagedLock<I>::get_lock_owner(ManagedLock<I>::LockOwner *lock_owner,
                                    Context *on_finish) {
  managed_lock::GetLockOwnerRequest<I>* req =
    managed_lock::GetLockOwnerRequest<I>::create(m_ioctx, m_oid, lock_owner,
                                                 on_finish);
  req->send();
}

template <typename I>
void ManagedLock<I>::break_lock(const ManagedLock<I>::LockOwner &lock_owner,
                                bool blacklist_lock_owner, Context *on_finish) {
  {
    Mutex::Locker locker(m_lock);
    if (!is_lock_owner(m_lock)) {
      managed_lock::BreakLockRequest<I>* req =
        managed_lock::BreakLockRequest<I>::create(m_ioctx, m_work_queue, m_oid,
                                                  lock_owner,
                                                  blacklist_lock_owner,
                                                  on_finish);
      req->send();
      return;
    }
  }

  on_finish->complete(-EBUSY);
}

template <typename I>
void ManagedLock<I>::shutdown_handler(int r, Context *on_finish) {
  on_finish->complete(r);
}

template <typename I>
void ManagedLock<I>::pre_acquire_lock_handler(Context *on_finish) {
  on_finish->complete(0);
}

template <typename I>
void  ManagedLock<I>::post_acquire_lock_handler(int r, Context *on_finish) {
  on_finish->complete(r);
}

template <typename I>
void  ManagedLock<I>::pre_release_lock_handler(bool shutting_down,
                                               Context *on_finish) {
  {
    Mutex::Locker locker(m_lock);
    m_state = shutting_down ? STATE_SHUTTING_DOWN : STATE_RELEASING;
  }
  on_finish->complete(0);
}

template <typename I>
void  ManagedLock<I>::post_release_lock_handler(bool shutting_down, int r,
                                                Context *on_finish) {
  on_finish->complete(r);
}

template <typename I>
void ManagedLock<I>::assert_locked(librados::ObjectWriteOperation *op,
                                   ClsLockType type) {
  Mutex::Locker locker(m_lock);
  rados::cls::lock::assert_locked(op, RBD_LOCK_NAME, type, m_cookie,
                                  WATCHER_LOCK_TAG);
}

template <typename I>
bool ManagedLock<I>::decode_lock_cookie(const std::string &tag,
                                        uint64_t *handle) {
  std::string prefix;
  std::istringstream ss(tag);
  if (!(ss >> prefix >> *handle) || prefix != WATCHER_LOCK_COOKIE_PREFIX) {
    return false;
  }
  return true;
}

template <typename I>
string ManagedLock<I>::encode_lock_cookie(uint64_t watch_handle) {
  assert(watch_handle != 0);
  std::ostringstream ss;
  ss << WATCHER_LOCK_COOKIE_PREFIX << " " << watch_handle;
  return ss.str();
}

template <typename I>
bool ManagedLock<I>::is_transition_state() const {
  switch (m_state) {
  case STATE_ACQUIRING:
  case STATE_WAITING_FOR_REGISTER:
  case STATE_REACQUIRING:
  case STATE_RELEASING:
  case STATE_PRE_SHUTTING_DOWN:
  case STATE_SHUTTING_DOWN:
  case STATE_INITIALIZING:
  case STATE_WAITING_FOR_LOCK:
  case STATE_POST_ACQUIRING:
  case STATE_PRE_RELEASING:
    return true;
  case STATE_UNLOCKED:
  case STATE_LOCKED:
  case STATE_SHUTDOWN:
  case STATE_UNINITIALIZED:
    break;
  }
  return false;
}

template <typename I>
void ManagedLock<I>::append_context(Action action, Context *ctx) {
  assert(m_lock.is_locked());

  for (auto &action_ctxs : m_actions_contexts) {
    if (action == action_ctxs.first) {
      if (ctx != nullptr) {
        action_ctxs.second.push_back(ctx);
      }
      return;
    }
  }

  Contexts contexts;
  if (ctx != nullptr) {
    contexts.push_back(ctx);
  }
  m_actions_contexts.push_back({action, std::move(contexts)});
}

template <typename I>
void ManagedLock<I>::execute_action(Action action, Context *ctx) {
  assert(m_lock.is_locked());

  append_context(action, ctx);
  if (!is_transition_state()) {
    execute_next_action();
  }
}

template <typename I>
void ManagedLock<I>::execute_next_action() {
  assert(m_lock.is_locked());
  assert(!m_actions_contexts.empty());
  switch (get_active_action()) {
  case ACTION_ACQUIRE_LOCK:
  case ACTION_TRY_LOCK:
    send_acquire_lock();
    break;
  case ACTION_REACQUIRE_LOCK:
    send_reacquire_lock();
    break;
  case ACTION_RELEASE_LOCK:
    send_release_lock();
    break;
  case ACTION_SHUT_DOWN:
    send_shutdown();
    break;
  default:
    assert(false);
    break;
  }
}

template <typename I>
typename ManagedLock<I>::Action ManagedLock<I>::get_active_action() const {
  assert(m_lock.is_locked());
  assert(!m_actions_contexts.empty());
  return m_actions_contexts.front().first;
}

template <typename I>
void ManagedLock<I>::complete_active_action(State next_state, int r) {
  assert(m_lock.is_locked());
  assert(!m_actions_contexts.empty());

  ActionContexts action_contexts(std::move(m_actions_contexts.front()));
  m_actions_contexts.pop_front();
  m_state = next_state;

  m_lock.Unlock();
  for (auto ctx : action_contexts.second) {
    ctx->complete(r);
  }
  m_lock.Lock();

  if (!is_transition_state() && !m_actions_contexts.empty()) {
    execute_next_action();
  }
}

template <typename I>
bool ManagedLock<I>::is_shutdown_locked() const {
  assert(m_lock.is_locked());

  return ((m_state == STATE_SHUTDOWN) ||
          (!m_actions_contexts.empty() &&
           m_actions_contexts.back().first == ACTION_SHUT_DOWN));
}

template <typename I>
void ManagedLock<I>::send_acquire_lock() {
  assert(m_lock.is_locked());
  if (m_state == STATE_LOCKED) {
    complete_active_action(STATE_LOCKED, 0);
    return;
  }

  ldout(m_cct, 10) << dendl;
  m_state = STATE_ACQUIRING;

  uint64_t watch_handle = m_watcher->get_watch_handle();
  if (watch_handle == 0) {
    lderr(m_cct) << "watcher not registered - delaying request" << dendl;
    m_state = STATE_WAITING_FOR_REGISTER;
    return;
  }
  m_cookie = ManagedLock<I>::encode_lock_cookie(watch_handle);

  m_work_queue->queue(new FunctionContext([this](int r) {
    pre_acquire_lock_handler(util::create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_pre_acquire_lock>(this));
  }));
}

template <typename I>
void ManagedLock<I>::handle_pre_acquire_lock(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  if (r < 0) {
    handle_acquire_lock(r);
    return;
  }

  using managed_lock::AcquireRequest;
  AcquireRequest<I>* req = AcquireRequest<I>::create(m_ioctx, m_watcher,
      m_work_queue, m_oid, m_cookie,
      util::create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_acquire_lock>(this));
  m_work_queue->queue(new C_SendLockRequest<AcquireRequest<I>>(req), 0);
}

template <typename I>
void ManagedLock<I>::handle_acquire_lock(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  if (r == -EBUSY || r == -EAGAIN) {
    ldout(m_cct, 5) << ": unable to acquire exclusive lock" << dendl;
  } else if (r < 0) {
    lderr(m_cct) << ": failed to acquire exclusive lock:" << cpp_strerror(r)
               << dendl;
  } else {
    ldout(m_cct, 5) << ": successfully acquired exclusive lock" << dendl;
  }

  m_post_next_state = (r < 0 ? STATE_UNLOCKED : STATE_LOCKED);

  m_work_queue->queue(new FunctionContext([this, r](int ret) {
    post_acquire_lock_handler(r, util::create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_post_acquire_lock>(this));
  }));
}

template <typename I>
void ManagedLock<I>::handle_post_acquire_lock(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0 && m_post_next_state == STATE_LOCKED) {
    // release_lock without calling pre and post handlers
    revert_to_unlock_state(r);
  } else {
    complete_active_action(m_post_next_state, r);
  }
}

template <typename I>
void ManagedLock<I>::revert_to_unlock_state(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  using managed_lock::ReleaseRequest;
  ReleaseRequest<I>* req = ReleaseRequest<I>::create(m_ioctx, m_watcher,
      m_work_queue, m_oid, m_cookie,
      new FunctionContext([this, r](int ret) {
        Mutex::Locker locker(m_lock);
        assert(ret == 0);
        complete_active_action(STATE_UNLOCKED, r);
      }));
  m_work_queue->queue(new C_SendLockRequest<ReleaseRequest<I>>(req));
}

template <typename I>
void ManagedLock<I>::send_reacquire_lock() {
  assert(m_lock.is_locked());

  if (m_state != STATE_LOCKED) {
    complete_active_action(m_state, 0);
    return;
  }

  uint64_t watch_handle = m_watcher->get_watch_handle();
  if (watch_handle == 0) {
     // watch (re)failed while recovering
     lderr(m_cct) << ": aborting reacquire due to invalid watch handle"
                  << dendl;
     complete_active_action(STATE_LOCKED, 0);
     return;
  }

  m_new_cookie = ManagedLock<I>::encode_lock_cookie(watch_handle);
  if (m_cookie == m_new_cookie) {
    ldout(m_cct, 10) << ": skipping reacquire since cookie still valid"
                     << dendl;
    complete_active_action(STATE_LOCKED, 0);
    return;
  }

  ldout(m_cct, 10) << dendl;
  m_state = STATE_REACQUIRING;

  using managed_lock::ReacquireRequest;
  ReacquireRequest<I>* req = ReacquireRequest<I>::create(m_ioctx, m_oid,
      m_cookie, m_new_cookie,
      util::create_context_callback<
        ManagedLock, &ManagedLock<I>::handle_reacquire_lock>(this));
  m_work_queue->queue(new C_SendLockRequest<ReacquireRequest<I>>(req));
}

template <typename I>
void ManagedLock<I>::handle_reacquire_lock(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_state == STATE_REACQUIRING);

  if (r < 0) {
    if (r == -EOPNOTSUPP) {
      ldout(m_cct, 10) << ": updating lock is not supported" << dendl;
    } else {
      lderr(m_cct) << ": failed to update lock cookie: " << cpp_strerror(r)
                   << dendl;
    }

    if (!is_shutdown_locked()) {
      // queue a release and re-acquire of the lock since cookie cannot
      // be updated on older OSDs
      execute_action(ACTION_RELEASE_LOCK, nullptr);

      assert(!m_actions_contexts.empty());
      ActionContexts &action_contexts(m_actions_contexts.front());

      // reacquire completes when the request lock completes
      Contexts contexts;
      std::swap(contexts, action_contexts.second);
      if (contexts.empty()) {
        execute_action(ACTION_ACQUIRE_LOCK, nullptr);
      } else {
        for (auto ctx : contexts) {
          ctx = new FunctionContext([ctx, r](int acquire_ret_val) {
              if (acquire_ret_val >= 0) {
                acquire_ret_val = r;
              }
              ctx->complete(acquire_ret_val);
            });
          execute_action(ACTION_ACQUIRE_LOCK, ctx);
        }
      }
    }
  } else {
    m_cookie = m_new_cookie;
  }

  complete_active_action(STATE_LOCKED, r);
}

template <typename I>
void ManagedLock<I>::send_release_lock() {
  assert(m_lock.is_locked());
  if (m_state == STATE_UNLOCKED) {
    complete_active_action(STATE_UNLOCKED, 0);
    return;
  }

  ldout(m_cct, 10) << dendl;
  m_state = STATE_PRE_RELEASING;

  m_work_queue->queue(new FunctionContext([this](int r) {
    pre_release_lock_handler(false, util::create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_pre_release_lock>(this));
  }));
}

template <typename I>
void ManagedLock<I>::handle_pre_release_lock(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  if (r < 0) {
    handle_release_lock(r);
    return;
  }

  using managed_lock::ReleaseRequest;
  ReleaseRequest<I>* req = ReleaseRequest<I>::create(m_ioctx, m_watcher,
      m_work_queue, m_oid, m_cookie,
      util::create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_release_lock>(this));
  m_work_queue->queue(new C_SendLockRequest<ReleaseRequest<I>>(req), 0);
}

template <typename I>
void ManagedLock<I>::handle_release_lock(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_state == STATE_RELEASING);

  if (r >= 0) {
    m_cookie = "";
  }

  m_post_next_state = r < 0 ? STATE_LOCKED : STATE_UNLOCKED;

  m_work_queue->queue(new FunctionContext([this, r](int ret) {
    post_release_lock_handler(false, r, util::create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_post_release_lock>(this));
  }));
}

template <typename I>
void ManagedLock<I>::handle_post_release_lock(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  Mutex::Locker locker(m_lock);
  complete_active_action(m_post_next_state, r);
}

template <typename I>
void ManagedLock<I>::send_shutdown() {
  ldout(m_cct, 10) << dendl;
  assert(m_lock.is_locked());
  if (m_state == STATE_UNLOCKED) {
    m_state = STATE_SHUTTING_DOWN;
    m_work_queue->queue(new FunctionContext([this](int r) {
      shutdown_handler(r, util::create_context_callback<
          ManagedLock<I>, &ManagedLock<I>::handle_shutdown>(this));
    }));
    return;
  }

  ldout(m_cct, 10) << dendl;
  assert(m_state == STATE_LOCKED);
  m_state = STATE_PRE_SHUTTING_DOWN;

  m_lock.Unlock();
  m_work_queue->queue(new C_ShutDownRelease(this), 0);
  m_lock.Lock();
}

template <typename I>
void ManagedLock<I>::handle_shutdown(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  complete_shutdown(r);
}

template <typename I>
void ManagedLock<I>::send_shutdown_release() {
  ldout(m_cct, 10) << dendl;

  Mutex::Locker locker(m_lock);

  m_work_queue->queue(new FunctionContext([this](int r) {
    pre_release_lock_handler(true, util::create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_shutdown_pre_release>(this));
  }));
}

template <typename I>
void ManagedLock<I>::handle_shutdown_pre_release(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  std::string cookie;
  {
    Mutex::Locker locker(m_lock);
    cookie = m_cookie;
  }

  using managed_lock::ReleaseRequest;
  ReleaseRequest<I>* req = ReleaseRequest<I>::create(m_ioctx, m_watcher,
      m_work_queue, m_oid, cookie,
      new FunctionContext([this](int r) {
        post_release_lock_handler(true, r, util::create_context_callback<
            ManagedLock<I>, &ManagedLock<I>::handle_shutdown_post_release>(this));
      }));
  req->send();

}

template <typename I>
void ManagedLock<I>::handle_shutdown_post_release(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  complete_shutdown(r);
}

template <typename I>
void ManagedLock<I>::complete_shutdown(int r) {
  ldout(m_cct, 10) << ": r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to shut down lock: " << cpp_strerror(r)
               << dendl;
  }

  ActionContexts action_contexts;
  {
    Mutex::Locker locker(m_lock);
    assert(m_lock.is_locked());
    assert(m_actions_contexts.size() == 1);

    action_contexts = std::move(m_actions_contexts.front());
    m_actions_contexts.pop_front();
    m_state = STATE_SHUTDOWN;
  }

  // expect to be destroyed after firing callback
  for (auto ctx : action_contexts.second) {
    ctx->complete(r);
  }
}

} // namespace librbd

template class librbd::ManagedLock<librbd::ImageCtx>;
