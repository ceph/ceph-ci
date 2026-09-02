// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_IMAGE_STATE_H
#define CEPH_LIBRBD_IMAGE_STATE_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include <list>
#include <string>
#include <utility>
#include "cls/rbd/cls_rbd_types.h"

class Context;
class RWLock;

namespace librbd {

class QuiesceWatchCtx;
class QuiesceWatchers;
class ImageCtx;
class ImageUpdateWatchers;
class UpdateWatchCtx;

template <typename ImageCtxT = ImageCtx>
class ImageState {
public:
  ImageState(ImageCtxT *image_ctx);
  ~ImageState();

  int open(uint64_t flags);
  void open(uint64_t flags, Context *on_finish);

  int close();
  void close(Context *on_finish);

  /**
   * Re-target the image context at a different image without the caller's
   * handle to it ever changing -- the close and open halves run as a single
   * action, so nothing else can slip in between them and the image context is
   * never deleted.
   *
   * The image is opened with the flags it was originally opened with. State
   * the caller owns rather than the image survives: the update and quiesce
   * watchers registered here keep their handles, and ImageCtx::reinit()
   * preserves the read-only mode, the lock and journal policies, and the event
   * socket. The new image has to live in the same cluster.
   *
   * If the open half fails the image context is left closed but alive, since
   * the caller still holds a handle to it and has to close it itself.
   */
  int reopen(librados::IoCtx& io_ctx, const std::string &image_name,
             const std::string &image_id, const char *snap_name);
  void reopen(librados::IoCtx& io_ctx, const std::string &image_name,
              const std::string &image_id, const char *snap_name,
              Context *on_finish);

  void handle_update_notification();

  bool is_refresh_required() const;

  int refresh();
  int refresh_if_required();
  void refresh(Context *on_finish);

  void snap_set(uint64_t snap_id, Context *on_finish);

  void prepare_lock(Context *on_ready);
  void handle_prepare_lock_complete();

  int register_update_watcher(UpdateWatchCtx *watcher, uint64_t *handle);
  void unregister_update_watcher(uint64_t handle, Context *on_finish);
  int unregister_update_watcher(uint64_t handle);
  void flush_update_watchers(Context *on_finish);
  void shut_down_update_watchers(Context *on_finish);

  int register_quiesce_watcher(QuiesceWatchCtx *watcher, uint64_t *handle);
  int unregister_quiesce_watcher(uint64_t handle);
  void notify_quiesce(Context *on_finish);
  void notify_unquiesce(Context *on_finish);
  void quiesce_complete(uint64_t handle, int r);

private:
  enum State {
    STATE_UNINITIALIZED,
    STATE_OPEN,
    STATE_CLOSED,
    STATE_OPENING,
    STATE_CLOSING,
    STATE_REFRESHING,
    STATE_SETTING_SNAP,
    STATE_PREPARING_LOCK,
    STATE_REOPENING
  };

  enum ActionType {
    ACTION_TYPE_OPEN,
    ACTION_TYPE_CLOSE,
    ACTION_TYPE_REFRESH,
    ACTION_TYPE_SET_SNAP,
    ACTION_TYPE_LOCK,
    ACTION_TYPE_REOPEN
  };

  struct Action {
    ActionType action_type;
    uint64_t refresh_seq = 0;
    uint64_t snap_id = CEPH_NOSNAP;
    Context *on_ready = nullptr;

    // ACTION_TYPE_REOPEN: the image to re-target at. Carried on the action so
    // that queued re-targets cannot clobber each other's spec.
    librados::IoCtx io_ctx;
    std::string image_name;
    std::string image_id;
    std::string snap_name;

    Action(ActionType action_type) : action_type(action_type) {
    }
    inline bool operator==(const Action &action) const {
      if (action_type != action.action_type) {
        return false;
      }
      switch (action_type) {
      case ACTION_TYPE_REFRESH:
        return (refresh_seq == action.refresh_seq);
      case ACTION_TYPE_SET_SNAP:
        return (snap_id == action.snap_id);
      case ACTION_TYPE_LOCK:
      case ACTION_TYPE_REOPEN:
        return false;
      default:
        return true;
      }
    }
  };

  typedef std::list<Context *> Contexts;
  typedef std::pair<Action, Contexts> ActionContexts;
  typedef std::list<ActionContexts> ActionsContexts;

  ImageCtxT *m_image_ctx;
  State m_state;

  mutable ceph::mutex m_lock;
  ActionsContexts m_actions_contexts;

  uint64_t m_last_refresh;
  uint64_t m_refresh_seq;

  ImageUpdateWatchers *m_update_watchers;
  QuiesceWatchers *m_quiesce_watchers;

  uint64_t m_open_flags;

  bool is_transition_state() const;
  bool is_closed() const;

  const Action *find_pending_refresh() const;

  void append_context(const Action &action, Context *context);
  void execute_next_action_unlock();
  void execute_action_unlock(const Action &action, Context *context);
  void complete_action_unlock(State next_state, int r);

  void send_open_unlock();
  void handle_open(int r);

  void send_close_unlock();
  void handle_close(int r);

  void send_reopen_unlock();
  void handle_reopen_close(int r);
  void handle_reopen_open(int r);

  void send_refresh_unlock();
  void handle_refresh(int r);

  void send_set_snap_unlock();
  void handle_set_snap(int r);

  void send_prepare_lock_unlock();

};

} // namespace librbd

extern template class librbd::ImageState<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_STATE_H
