// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "include/object.h"
#include "include/fs_types.h"
#include "common/snap_types.h"
#include "mds/CDentry.h"
#include "mds/Mutation.h"
#include "mds/ScrubHeader.h"

class CDir;
class CInode;
class MDCache;
class MDSContext;

/**
 * Roll back the entries of a directory to a snapshot. Driven by the
 * ScrubStack for scrubs started with `scrub rollback`: for each dirfrag
 * that is scrubbed, every non-directory entry is rolled back (metadata
 * in the metadata pool, file data in the data pool via
 * CEPH_OSD_OP_ROLLBACK). Directory entries are not rolled back; the
 * scrub recurses into directories which exist in both head and the
 * snapshot as the same inode.
 *
 * An entry is a link to an inode. Each name ends up linking the inode
 * that it linked in the snapshot, as a primary or a remote dentry
 * regardless of which of the two it was then, and an inode is rolled
 * back once, when the first of its links is reached.
 *
 * The directory that the scrub starts at is the root of a subvolume, so
 * all the links of an inode are found under it, and clients are expected
 * to have been evicted before the rollback starts, i.e., there are no
 * caps on any inode under the directory.
 */
class SnapRollback {
public:
  enum action_t {
    ACTION_NONE = 0,      // nothing to do
    ACTION_SKIP,          // unsupported (directory, linked outside, ...)
    ACTION_ROLLBACK,      // same inode in snapshot and head
    ACTION_REMOVE,        // remove head link (created/replaced after snapshot)
    ACTION_REINSTATE,     // recreate head inode from snapshot (head inode is gone)
    ACTION_RELINK,        // move live head inode back to this name
    ACTION_LINK,          // add a link to the live head inode at this name
  };

  /**
   * Where the head version of the inode that a name refers to in the
   * snapshot is, when it is not linked at that name.
   */
  enum head_state_t {
    HEAD_GONE = 0,        // not in cache
    HEAD_PURGING,         // being purged; gone once the purge finishes
    HEAD_RELINKABLE,      // can be moved back to the name
    HEAD_LINKABLE,        // stays where it is, the name gets a link to it
    HEAD_PINNED,          // alive, but must not be touched (linked outside, ...)
  };

  explicit SnapRollback(MDCache *mdc) : mdcache(mdc) {}

  /**
   * Roll back the non-directory entries of a dirfrag to the snapshot
   * recorded in the scrub header. `fin` is queued (never completed
   * inline) once every entry has been processed.
   */
  void rollback_dirfrag(CDir *dir, const ScrubHeaderRef& header, MDSContext *fin);

  /**
   * Is `dn` a head directory dentry that the rollback should recurse
   * into (the directory exists in the snapshot as the same inode)?
   */
  static bool should_recurse(CDentry *dn, snapid_t snapid);

  /**
   * Entry point for CEPH_MDS_OP_SNAP_ROLLBACK internal requests.
   */
  void dispatch(const MDRequestRef& mdr);

  static std::string_view action_name(action_t action);

private:
  struct dirfrag_rollback_t;
  struct rollback_op_t;
  struct snap_version_t;

  friend class C_SnapRollback_Next;
  friend class C_SnapRollback_EntryDone;
  friend class C_SnapRollback_Logged;
  friend class C_IO_SnapRollback_Data;
  friend class C_IO_SnapRollback_Stat;
  friend class C_SnapRollback_Loaded;

  action_t classify(CDir *dir, std::string_view name,
                    const ScrubHeaderRef& header, std::string *why);
  head_state_t get_head_state(CDentry::linkage_t *sl,
                              const ScrubHeaderRef& header, std::string *why);
  bool head_gone_from_disk(const MDRequestRef& mdr, rollback_op_t *op,
                           CInode *sin);

  bool get_snap_version(CDentry::linkage_t *sl, CInode *in, snapid_t snapid,
                        snap_version_t *sv);
  bool is_linked_outside(CInode *in, const ScrubHeaderRef& header);
  bool is_snapshot_covered(CInode *in, snapid_t snapid);
  CInode *get_linked_inode(const MDRequestRef& mdr, rollback_op_t *op,
                           CDentry::linkage_t *dnl);
  void load_inode(const MDRequestRef& mdr, rollback_op_t *op, inodeno_t ino);

  void next_entry(dirfrag_rollback_t *dr);
  void entry_done(dirfrag_rollback_t *dr, action_t action, int r);

  bool acquire_locks(const MDRequestRef& mdr, rollback_op_t *op,
                     CDentry *dn, CInode *in, CDentry *straydn);
  CDentry *prepare_stray_dentry(const MDRequestRef& mdr, CInode *in);

  void rollback_inode(const MDRequestRef& mdr, rollback_op_t *op);
  void remove_inode(const MDRequestRef& mdr, rollback_op_t *op);
  void reinstate_inode(const MDRequestRef& mdr, rollback_op_t *op);
  void relink_inode(const MDRequestRef& mdr, rollback_op_t *op);
  void link_inode(const MDRequestRef& mdr, rollback_op_t *op);

  void _rollback_inode_logged(const MDRequestRef& mdr, CInode *in, bool track);
  void _remove_inode_logged(const MDRequestRef& mdr, CDentry *dn,
                            CDentry *straydn, version_t dnpv);
  void _reinstate_inode_logged(const MDRequestRef& mdr, CDentry *dn,
                               CInode *in, bool track);
  void _relink_inode_logged(const MDRequestRef& mdr, CDentry *srcdn,
                            CDentry *destdn, CInode *in);
  void _link_inode_logged(const MDRequestRef& mdr, CDentry *dn, CInode *in,
                          version_t dnpv, bool adjust_realm);

  /**
   * Issue CEPH_OSD_OP_ROLLBACK for the data objects of an inode, in
   * windows of `max_inflight` objects. The request is redispatched once
   * all objects have been rolled back.
   */
  void rollback_data(const MDRequestRef& mdr, rollback_op_t *op);
  void _rollback_data_window_done(const MDRequestRef& mdr, int r);

  void retry_later(const MDRequestRef& mdr, rollback_op_t *op,
                   std::string_view reason, int limit = max_retries);
  void wait_for_purge(const MDRequestRef& mdr, rollback_op_t *op,
                      std::string_view reason);
  void finish_request(const MDRequestRef& mdr, int r);

  static constexpr uint64_t max_inflight = 64;
  static constexpr int max_retries = 60;
  // seconds without the purge queue completing anything
  static constexpr int max_purge_stall = 600;
  static constexpr int max_reclassify = 8;

  MDCache *mdcache;
};
