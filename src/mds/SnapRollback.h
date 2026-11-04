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
#include "mds/Mutation.h"
#include "mds/ScrubHeader.h"

class CDentry;
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
 * Clients are expected to have been evicted before the rollback starts,
 * i.e., there are no caps on any inode under the directory.
 */
class SnapRollback {
public:
  enum action_t {
    ACTION_NONE = 0,      // nothing to do
    ACTION_SKIP,          // unsupported (directory, hardlink, ...)
    ACTION_ROLLBACK,      // same inode in snapshot and head
    ACTION_REMOVE,        // remove head inode (created after snapshot)
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

  friend class C_SnapRollback_Next;
  friend class C_SnapRollback_EntryDone;
  friend class C_SnapRollback_Logged;
  friend class C_IO_SnapRollback_Data;

  action_t classify(CDir *dir, std::string_view name,
                    const ScrubHeaderRef& header, std::string *why);

  void next_entry(dirfrag_rollback_t *dr);
  void entry_done(dirfrag_rollback_t *dr, action_t action, int r);

  bool acquire_locks(const MDRequestRef& mdr, rollback_op_t *op,
                     CDentry *dn, CInode *in, CDentry *straydn);
  CDentry *prepare_stray_dentry(const MDRequestRef& mdr, CInode *in);

  void rollback_inode(const MDRequestRef& mdr, rollback_op_t *op);
  void remove_inode(const MDRequestRef& mdr, rollback_op_t *op);

  void _rollback_inode_logged(const MDRequestRef& mdr);
  void _remove_inode_logged(const MDRequestRef& mdr, CDentry *dn,
                            CDentry *straydn, version_t dnpv);

  /**
   * Issue CEPH_OSD_OP_ROLLBACK for the data objects of an inode, in
   * windows of `max_inflight` objects. The request is redispatched once
   * all objects have been rolled back.
   */
  void rollback_data(const MDRequestRef& mdr, rollback_op_t *op);
  void _rollback_data_window_done(const MDRequestRef& mdr, int r);

  void finish_request(const MDRequestRef& mdr, int r);

  static constexpr uint64_t max_inflight = 64;
  static constexpr int max_reclassify = 8;

  MDCache *mdcache;
};
