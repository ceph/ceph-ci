// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CEPH_TRASH_MANAGER_H
#define CEPH_TRASH_MANAGER_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include <boost/intrusive_ptr.hpp>

#include "include/frag.h"
#include "include/types.h"
#include "mds/MDSMetaRequest.h"
#include "mds/metareqid_t.h"
#include "messages/MMDSBeacon.h"

class CDentry;
class CInode;
class MDSContext;
class MDSRank;
class MDCache;
struct MDRequestImpl;

namespace ceph { class Formatter; }

/**
 * Background reclamation of directory trees that a client removed with
 * CEPH_MDS_OP_RMTREE.
 *
 * The client request renames the directory into this rank's trash directory.
 * TrashManager then walks the tree with ordinary unlink and rmdir, recalling
 * client caps and leases as it goes. Nested subtrees that belong to other
 * ranks are witnessed at rename time and purged by the rank that auths them.
 * After failover the walker is rebuilt from the trash directory and from auth
 * subtrees that sit under it.
 */
class TrashManager {
public:
  static constexpr std::string_view TRASH_NAME = "trash";

  explicit TrashManager(MDSRank *mds);

  void activate();
  void advance();
  void kick();

  void note_trashed(CInode *in);
  void note_imported(CInode *in);

  void dispatch_recall(const boost::intrusive_ptr<MDRequestImpl>& mdr);
  void add_subtree_witnesses(const boost::intrusive_ptr<MDRequestImpl>& mdr,
                             CInode *srci);

  void move_to_trash(CInode *in, MDSContext *fin);

  struct TrashMetaRequest : public MDSMetaRequest {
    CDentry *dn = nullptr;
    inodeno_t root_ino;
    inodeno_t entry_ino;
    MDSContext *fin = nullptr;
    bool migration = false;
    bool counts_inflight = false;

    TrashMetaRequest(int op, ceph_tid_t tid, mds_rank_t to, CDentry *dn,
                     inodeno_t root, inodeno_t entry, MDSContext *fin, bool mig);
    ~TrashMetaRequest() override;
  };

  void handle_reply(TrashMetaRequest *req, int result);
  void drop_requests_to(mds_rank_t who);

  /// Rank 0 purges. Other ranks migrate their trash entries to rank 0.
  /// Returns true when this rank has nothing left that would block shutdown.
  bool shutdown_pass();

  /// MDCache has dropped its pin and cleared the trash inode pointer.
  /// A later pass must not treat that as "trash is not open yet".
  void note_cache_released();

  void dump(ceph::Formatter *f);
  void gather_health(std::vector<MDSHealthMetric>& metrics) const;

  void pause() { paused = true; }
  void resume();

private:
  struct Frame {
    inodeno_t ino;
    std::vector<frag_t> frags;
    size_t frag_i = 0;
    bool frags_ready = false;
    bool caps_recalled = false;
    bool recalling = false;
    bool skipped_remote = false;
    bool has_cursor = false;
    snapid_t cursor_snap = 0;
    uint32_t cursor_hash = 0;
    std::string cursor_name;
    metareqid_t recall_req;

    void reset_scan() {
      frags.clear();
      frag_i = 0;
      frags_ready = false;
      caps_recalled = false;
      recalling = false;
      skipped_remote = false;
      has_cursor = false;
      cursor_name.clear();
      cursor_snap = 0;
      cursor_hash = 0;
    }
  };

  enum class State { ACTIVE, PARKED, DONE };

  struct Root {
    inodeno_t ino;
    State state = State::ACTIVE;
    bool remove_when_empty = false;
    bool pinned = false;
    bool waiting = false;
    int inflight = 0;
    double backoff = 1.0;
    utime_t backoff_until;
    utime_t parked_since;
    std::string prior_path;
    std::vector<Frame> stack;
  };

  enum class Step { Wait, Again, Erase };

  MDSRank *mds;
  bool activated = false;
  bool paused = false;
  bool in_advance = false;
  bool need_advance = false;
  bool kick_scheduled = false;
  bool shutdown_stopped = false;
  bool cache_released = false;
  bool discovering_peer = false;
  bool peer_missing_logged = false;
  inodeno_t peer_trash_ino;
  utime_t peer_discover_after;
  utime_t migrate_backoff_until;
  int global_inflight = 0;
  std::map<inodeno_t, Root> roots;
  std::set<inodeno_t> migrating;

  MDCache *cache() const;

  void advance_body();
  void arm_timer();
  void discover_roots();
  void ensure_root(CInode *in, bool remove_when_empty);
  void cleanup_done();
  Step advance_root(Root &root, int &budget);
  bool submit_purge(Root &root, CDentry *dn, int op);
  bool send_request(int op, CDentry *dn, mds_rank_t to, inodeno_t root_ino,
                    inodeno_t entry_ino, bool migration, MDSContext *fin,
                    inodeno_t dest_dir = 0);
  void on_walker_reply(inodeno_t root_ino, int op, int result);
  void recall_finished(inodeno_t root_ino, inodeno_t dir_ino, int result);
  void wake(inodeno_t root_ino, int result);
  void begin_recall(Root &root, Frame &frame, CInode *diri);
  void rewind_top(Root &root);
  void finish_frame(Root &root, bool removed, bool &erase_root);
  void drop_pins(Root &root);
  void backoff(Root &root);
  void park(Root &root);
  bool has_snaps(CInode *in) const;
  bool throttled() const;
  uint64_t max_inflight() const;
  CInode *top_under_trash(CInode *in) const;
  void start_discover_peer();
  void discover_peer_step();
  void migrate_some();
  int count_entries(bool &incomplete);

  friend struct C_TrashNotify;
  friend struct C_TrashKick;
  friend struct C_TrashWait;
  friend struct C_TrashRecall;
  friend struct C_TrashDiscover;
};

#endif
