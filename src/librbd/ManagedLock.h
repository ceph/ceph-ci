// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_H
#define CEPH_LIBRBD_MANAGED_LOCK_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "cls/lock/cls_lock_types.h"
#include "librbd/watcher/Types.h"
#include "common/Mutex.h"
#include <list>
#include <string>
#include <utility>

class ContextWQ;

namespace librbd {

template <typename ImageCtxT>
class ManagedLock {
private:
  typedef watcher::Traits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Watcher Watcher;

public:
  static const std::string WATCHER_LOCK_TAG;

  enum Mode {
    EXCLUSIVE,
    SHARED
  };

  static ManagedLock *create(librados::IoCtx& ioctx, ContextWQ *work_queue,
                             const std::string& oid, Watcher *watcher,
                             Mode mode) {
    return new ManagedLock(ioctx, work_queue, oid, watcher, mode);
  }

  ManagedLock(librados::IoCtx& ioctx, ContextWQ *work_queue,
              const std::string& oid, Watcher *watcher, Mode mode);
  virtual ~ManagedLock();

  bool is_lock_owner() const;

  void shut_down(Context *on_shutdown);
  void acquire_lock(Context *on_acquired);
  void try_acquire_lock(Context *on_acquired);
  void release_lock(Context *on_released);
  void reacquire_lock(Context *on_reacquired = nullptr);

  void assert_locked(librados::ObjectWriteOperation *op, ClsLockType type);

  bool is_shutdown() const {
    Mutex::Locker l(m_lock);
    return is_shutdown_locked();
  }

  bool is_locked_state() const {
    return m_state == STATE_LOCKED;
  }

  static bool decode_lock_cookie(const std::string &tag, uint64_t *handle);

protected:

  /**
   * @verbatim
   *
   *       <start>
   *          |
   *          |
   *          v           (acquire_lock)
   *       UNLOCKED -----------------------------------------> ACQUIRING
   *          ^                                                    |
   *          |                                                    |
   *      RELEASING                                                |
   *          |                                                    |
   *          |                                                    |
   *          |                    (release_lock)                  v
   *    PRE_RELEASING <----------------------------------------- LOCKED
   *
   * <LOCKED state>
   *    |
   *    v
   * REACQUIRING -------------------------------------> <finish>
   *    .                                                 ^
   *    .                                                 |
   *    . . . > <RELEASE action> ---> <ACQUIRE action> ---/
   *
   * <UNLOCKED/LOCKED states>
   *    |
   *    |
   *    v
   * PRE_SHUTTING_DOWN ---> SHUTTING_DOWN ---> SHUTDOWN ---> <finish>
   *
   * @endverbatim
   */
  enum State {
    STATE_UNINITIALIZED,
    STATE_INITIALIZING,
    STATE_UNLOCKED,
    STATE_LOCKED,
    STATE_ACQUIRING,
    STATE_POST_ACQUIRING,
    STATE_WAITING_FOR_REGISTER,
    STATE_WAITING_FOR_LOCK,
    STATE_REACQUIRING,
    STATE_PRE_RELEASING,
    STATE_RELEASING,
    STATE_PRE_SHUTTING_DOWN,
    STATE_SHUTTING_DOWN,
    STATE_SHUTDOWN,
  };

  enum Action {
    ACTION_TRY_LOCK,
    ACTION_ACQUIRE_LOCK,
    ACTION_REACQUIRE_LOCK,
    ACTION_RELEASE_LOCK,
    ACTION_SHUT_DOWN
  };

  mutable Mutex m_lock;
  State m_state;

  virtual void shutdown_handler(int r, Context *on_finish);
  virtual void pre_acquire_lock_handler(Context *on_finish);
  virtual void post_acquire_lock_handler(int r, Context *on_finish);
  virtual void pre_release_lock_handler(bool shutting_down,
                                        Context *on_finish);
  virtual void post_release_lock_handler(bool shutting_down, int r,
                                          Context *on_finish);

  Action get_active_action() const;
  bool is_shutdown_locked() const;
  void execute_next_action();

private:
  typedef std::list<Context *> Contexts;
  typedef std::pair<Action, Contexts> ActionContexts;
  typedef std::list<ActionContexts> ActionsContexts;

  struct C_ShutDownRelease : public Context {
    ManagedLock *lock;
    C_ShutDownRelease(ManagedLock *lock)
      : lock(lock) {
    }
    virtual void finish(int r) override {
      lock->send_shutdown_release();
    }
  };

  librados::IoCtx& m_ioctx;
  CephContext *m_cct;
  ContextWQ *m_work_queue;
  std::string m_oid;
  Watcher *m_watcher;
  Mode m_mode;

  std::string m_cookie;
  std::string m_new_cookie;

  State m_post_next_state;

  ActionsContexts m_actions_contexts;

  static std::string encode_lock_cookie(uint64_t watch_handle);

  bool is_transition_state() const;

  void append_context(Action action, Context *ctx);
  void execute_action(Action action, Context *ctx);

  void complete_active_action(State next_state, int r);


  void send_acquire_lock();
  void handle_pre_acquire_lock(int r);
  void handle_acquire_lock(int r);
  void handle_post_acquire_lock(int r);
  void revert_to_unlock_state(int r);

  void send_reacquire_lock();
  void handle_reacquire_lock(int r);

  void send_release_lock();
  void handle_pre_release_lock(int r);
  void handle_release_lock(int r);
  void handle_post_release_lock(int r);

  void send_shutdown();
  void handle_shutdown(int r);
  void send_shutdown_release();
  void handle_shutdown_pre_release(int r);
  void handle_shutdown_post_release(int r);
  void complete_shutdown(int r);
};

} // namespace librbd

#endif // CEPH_LIBRBD_MANAGED_LOCK_H
