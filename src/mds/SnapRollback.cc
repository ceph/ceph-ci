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

#include "SnapRollback.h"

#include <algorithm>
#include <functional>

#include "CDentry.h"
#include "CDir.h"
#include "CInode.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDLog.h"
#include "MDSRank.h"
#include "RetryRequest.h"
#include "Server.h"
#include "SnapRealm.h"
#include "events/EUpdate.h"
#include "common/debug.h"
#include "common/errno.h"
#include "osdc/Objecter.h"
#include "osdc/Striper.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix _prefix(_dout, mdcache->mds)

static std::ostream& _prefix(std::ostream *_dout, MDSRank *mds) {
  return *_dout << "mds." << mds->get_nodeid() << ".snap_rollback ";
}

struct SnapRollback::dirfrag_rollback_t {
  CDir *dir;
  ScrubHeaderRef header;
  std::vector<std::string> names;
  size_t next = 0;
  MDSContext *fin;

  // bounds re-evaluating an entry whose action changed under the request
  size_t reclassify_at = 0;
  int reclassified = 0;
};

struct SnapRollback::rollback_op_t {
  CDir *dir;
  std::string name;
  ScrubHeaderRef header;
  action_t action;

  // data rollback
  bool data_done = false;
  uint64_t next_obj = 0;
  uint64_t num_objs = 0;
  inodeno_t data_ino;
  file_layout_t layout;
  SnapContext snapc;

  // has the head backtrace object been looked up?
  bool head_checked = false;
  bool head_exists = false;

  int retries = 0;
  uint64_t purge_progress = 0;
};

class C_SnapRollback_Next : public MDSInternalContext {
  SnapRollback *sr;
  SnapRollback::dirfrag_rollback_t *dr;
public:
  C_SnapRollback_Next(SnapRollback *s, SnapRollback::dirfrag_rollback_t *d)
    : MDSInternalContext(s->mdcache->mds), sr(s), dr(d) {}
  void finish(int r) override {
    sr->next_entry(dr);
  }
};

class C_SnapRollback_EntryDone : public Context {
  SnapRollback *sr;
  SnapRollback::dirfrag_rollback_t *dr;
  SnapRollback::rollback_op_t *op;
public:
  C_SnapRollback_EntryDone(SnapRollback *s, SnapRollback::dirfrag_rollback_t *d,
                           SnapRollback::rollback_op_t *o)
    : sr(s), dr(d), op(o) {}
  void finish(int r) override {
    auto action = op->action;
    delete op;
    sr->entry_done(dr, action, r);
  }
};

class C_SnapRollback_Logged : public MDSLogContextBase {
  MDSRank *mds;
  std::function<void()> fn;
protected:
  MDSRank *get_mds() override { return mds; }
public:
  C_SnapRollback_Logged(MDSRank *m, std::function<void()>&& f)
    : mds(m), fn(std::move(f)) {}
  void finish(int r) override {
    ceph_assert(r == 0);
    fn();
  }
};

class C_IO_SnapRollback_Data : public MDSIOContext {
  SnapRollback *sr;
  MDRequestRef mdr;
public:
  C_IO_SnapRollback_Data(SnapRollback *s, const MDRequestRef& r)
    : MDSIOContext(s->mdcache->mds), sr(s), mdr(r) {}
  void finish(int r) override {
    sr->_rollback_data_window_done(mdr, r);
  }
  void print(std::ostream& out) const override {
    out << "snap_rollback_data";
  }
};

class C_IO_SnapRollback_Stat : public MDSIOContext {
  SnapRollback *sr;
  MDRequestRef mdr;
public:
  C_IO_SnapRollback_Stat(SnapRollback *s, const MDRequestRef& r)
    : MDSIOContext(s->mdcache->mds), sr(s), mdr(r) {}
  void finish(int r) override {
    auto op = static_cast<SnapRollback::rollback_op_t*>(mdr->internal_op_private);
    if (r < 0 && r != -ENOENT) {
      sr->finish_request(mdr, r);
      return;
    }
    op->head_checked = true;
    op->head_exists = (r == 0);
    sr->dispatch(mdr);
  }
  void print(std::ostream& out) const override {
    out << "snap_rollback_stat";
  }
};

std::string_view SnapRollback::action_name(action_t action)
{
  switch (action) {
  case ACTION_NONE: return "none";
  case ACTION_SKIP: return "skip";
  case ACTION_ROLLBACK: return "rollback";
  case ACTION_REMOVE: return "remove";
  case ACTION_REINSTATE: return "reinstate";
  case ACTION_RELINK: return "relink";
  }
  return "unknown";
}

bool SnapRollback::should_recurse(CDentry *dn, snapid_t snapid)
{
  if (dn->last != CEPH_NOSNAP || dn->first > snapid) {
    return false;
  }
  auto dnl = dn->get_projected_linkage();
  return dnl->is_primary() && dnl->get_inode()->is_dir();
}

/*
 * Compare what `name` refers to in head and in the snapshot. Directory
 * inodes are multiversion, so a directory that existed in the snapshot
 * as the same inode has a head dentry that covers the snapshot.
 */
SnapRollback::action_t SnapRollback::classify(CDir *dir, std::string_view name,
                                              const ScrubHeaderRef& header,
                                              std::string *why)
{
  snapid_t snapid = header->get_rollback_snapid();
  CDentry *hdn = dir->lookup(name);
  CDentry *sdn = dir->lookup(name, snapid);

  CDentry::linkage_t *hl = hdn ? hdn->get_projected_linkage() : nullptr;
  CDentry::linkage_t *sl = sdn ? sdn->get_projected_linkage() : nullptr;
  if (hl && hl->is_null()) {
    hl = nullptr;
  }
  if (sl && sl->is_null()) {
    sl = nullptr;
  }

  if (!hl && !sl) {
    return ACTION_NONE;
  }
  if ((hl && hl->is_remote()) || (sl && sl->is_remote())) {
    *why = "remote dentry";
    return ACTION_SKIP;
  }

  CInode *hin = hl ? hl->get_inode() : nullptr;
  CInode *sin = sl ? sl->get_inode() : nullptr;

  if ((hin && hin->is_dir()) || (sin && sin->is_dir())) {
    if (hdn == sdn) {
      // same directory in head and snapshot -- recursed into by the scrub
      return ACTION_NONE;
    }
    *why = "directory";
    return ACTION_SKIP;
  }
  // A file that was unlinked while covered by a snapshot has a (non-global)
  // snaprealm for its past parents; that is not a hardlink.
  if ((hin && (hin->get_projected_inode()->nlink > 1 ||
               hin->is_projected_snaprealm_global())) ||
      (sin && sin->get_inode()->nlink > 1)) {
    *why = "hardlinked inode";
    return ACTION_SKIP;
  }

  if (!sin) {
    // created after the snapshot
    return ACTION_REMOVE;
  }
  if (hin && hin->ino() == sin->ino()) {
    if (hin == sin && hin->is_any_old_inodes()) {
      // the snapshot's metadata may be in old_inodes, which is not handled
      *why = "metadata in old_inodes";
      return ACTION_SKIP;
    }
    return ACTION_ROLLBACK;
  }

  // The snapshot inode is not linked at this name. Decide what can be done
  // about it before touching the head inode at this name, if any: removing
  // can't be undone.
  auto state = get_head_state(sin->ino(), header, why);
  if (state == HEAD_PINNED) {
    return ACTION_SKIP;
  }
  if (hin) {
    // the entry is reclassified after the head inode has been removed
    return ACTION_REMOVE;
  }
  return state == HEAD_RELINKABLE ? ACTION_RELINK : ACTION_REINSTATE;
}

/*
 * Find out what happened to the head version of a snapshot inode that
 * isn't linked at the snapshot's name anymore. An inode that isn't in
 * cache is reported as gone; head_gone_from_disk() tells whether it is.
 */
SnapRollback::head_state_t
SnapRollback::get_head_state(inodeno_t ino, const ScrubHeaderRef& header,
                             std::string *why)
{
  CInode *in = mdcache->get_inode(ino);
  if (!in) {
    return HEAD_GONE;
  }

  CDentry *pdn = in->get_projected_parent_dn();
  if (in->state_test(CInode::STATE_PURGING) ||
      (pdn && pdn->state_test(CDentry::STATE_PURGING))) {
    return HEAD_PURGING;
  }
  // STATE_PURGING is gone once the item is durable in the purge queue. The
  // item, say a truncate of an inode that stays in stray, runs after that.
  if (mdcache->mds->purge_queue.is_ino_pending(ino)) {
    return HEAD_PURGING;
  }
  if (!pdn) {
    *why = "head inode without a parent";
    return HEAD_PINNED;
  }

  const auto& pi = in->get_projected_inode();
  CInode *pdiri = pdn->get_dir()->get_inode();
  if (pdiri->is_stray()) {
    if (pi->nlink != 0) {
      // remote dentries refer to it, it can't be moved from under them
      *why = "head inode with other links";
      return HEAD_PINNED;
    }
    return HEAD_RELINKABLE;
  }

  if (pi->nlink != 1 || in->is_projected_snaprealm_global()) {
    *why = "head inode with other links";
    return HEAD_PINNED;
  }
  // renamed after the snapshot: don't take it away from outside the
  // directory being rolled back.
  CInode *origin = mdcache->get_inode(header->get_origin());
  if (!origin || !origin->is_projected_ancestor_of(pdiri)) {
    *why = "head inode linked outside the rollback directory";
    return HEAD_PINNED;
  }
  return HEAD_RELINKABLE;
}

/*
 * The head version of `sin` isn't in cache: is it really gone? Inode
 * numbers are never reused, so a head backtrace object means that the
 * inode is alive and just not loaded. Returns true if the inode is gone,
 * otherwise the request gets redispatched later. The request must not hold
 * any locks, loading the inode may have to traverse the directories it
 * would lock.
 */
bool SnapRollback::head_gone_from_disk(const MDRequestRef& mdr,
                                       rollback_op_t *op, CInode *sin)
{
  MDSRank *mds = mdcache->mds;

  if (mds->purge_queue.is_ino_pending(sin->ino())) {
    wait_for_purge(mdr, op, "purge of head inode pending");
    return false;
  }
  const auto& si = *sin->get_inode();
  if (!op->head_checked) {
    object_t oid = CInode::get_object_name(sin->ino(), frag_t(), "");
    object_locator_t oloc = OSDMap::file_to_object_locator(si.layout);
    mds->objecter->stat(oid, oloc, CEPH_NOSNAP, nullptr, nullptr, 0,
                        new C_IO_SnapRollback_Stat(this, mdr));
    return false;
  }
  if (!op->head_exists) {
    return true;
  }

  op->head_checked = false;
  if (++op->retries > max_retries) {
    dout(5) << __func__ << " " << op->name << ": can't load head inode "
            << sin->ino() << dendl;
    finish_request(mdr, -EBUSY);
    return false;
  }
  dout(10) << __func__ << " " << op->name << ": loading head inode "
           << sin->ino() << dendl;
  mdcache->open_ino(sin->ino(), si.layout.pool_id,
                    new C_MDS_RetryRequest(mdcache, mdr), false);
  return false;
}

void SnapRollback::rollback_dirfrag(CDir *dir, const ScrubHeaderRef& header,
                                    MDSContext *fin)
{
  dout(10) << __func__ << " " << *dir << " to snap "
           << header->get_rollback_snapid() << dendl;

  auto dr = new dirfrag_rollback_t{dir, header, {}, 0, fin};
  for (auto& [key, dn] : *dir) {
    if (dr->names.empty() || dr->names.back() != dn->get_name()) {
      dr->names.emplace_back(dn->get_name());
    }
  }
  // entries of the same name are adjacent in a dirfrag, but be safe
  std::sort(dr->names.begin(), dr->names.end());
  dr->names.erase(std::unique(dr->names.begin(), dr->names.end()),
                  dr->names.end());

  next_entry(dr);
}

void SnapRollback::next_entry(dirfrag_rollback_t *dr)
{
  MDSRank *mds = mdcache->mds;
  CDir *dir = dr->dir;

  while (dr->next < dr->names.size()) {
    if (!dir->is_complete()) {
      dout(10) << __func__ << " incomplete, fetching " << *dir << dendl;
      dir->fetch(new C_SnapRollback_Next(this, dr), true);
      return;
    }

    const auto& name = dr->names[dr->next];
    std::string why;
    auto action = classify(dir, name, dr->header, &why);
    dout(15) << __func__ << " " << name << ": " << action_name(action) << dendl;

    if (action == ACTION_NONE) {
      ++dr->next;
      continue;
    }
    if (action == ACTION_SKIP) {
      std::string path;
      dir->get_inode()->make_path_string(path, true);
      mds->clog->warn() << "snap rollback skipping " << path << "/" << name
                        << ": " << why << " not supported";
      dr->header->get_rollback_stats().skipped++;
      ++dr->next;
      continue;
    }

    auto op = new rollback_op_t{dir, name, dr->header, action};
    MDRequestRef mdr = mdcache->request_start_internal(CEPH_MDS_OP_SNAP_ROLLBACK);
    mdr->internal_op_private = op;
    mdr->internal_op_finish = new C_SnapRollback_EntryDone(this, dr, op);
    dispatch(mdr);
    return;
  }

  dout(10) << __func__ << " done " << *dir << dendl;
  mds->queue_waiter(dr->fin);
  delete dr;
}

void SnapRollback::entry_done(dirfrag_rollback_t *dr, action_t action, int r)
{
  MDSRank *mds = mdcache->mds;
  auto& stats = dr->header->get_rollback_stats();
  const auto& name = dr->names[dr->next];

  if (r == -EAGAIN) {
    // the action changed under the request, not an error (yet)
    if (dr->reclassify_at != dr->next) {
      dr->reclassify_at = dr->next;
      dr->reclassified = 0;
    }
    if (++dr->reclassified <= max_reclassify) {
      mds->queue_waiter(new C_SnapRollback_Next(this, dr));
      return;
    }
  }

  if (r < 0) {
    std::string path;
    dr->dir->get_inode()->make_path_string(path, true);
    mds->clog->warn() << "snap rollback (" << action_name(action) << ") failed on "
                      << path << "/" << name << ": " << cpp_strerror(r);
    stats.failed++;
    ++dr->next;
  } else {
    switch (action) {
    case ACTION_ROLLBACK:
      stats.rolled_back++;
      ++dr->next;
      break;
    case ACTION_REINSTATE:
      stats.reinstated++;
      ++dr->next;
      break;
    case ACTION_REMOVE:
      // reclassify -- the snapshot inode may need to be reinstated
      stats.removed++;
      break;
    case ACTION_RELINK:
      // reclassify -- same inode now, data and metadata get rolled back
      stats.relinked++;
      break;
    default:
      ceph_abort();
    }
  }

  // called from request_finish(); don't start the next request from here
  mds->queue_waiter(new C_SnapRollback_Next(this, dr));
}

void SnapRollback::dispatch(const MDRequestRef& mdr)
{
  auto op = static_cast<rollback_op_t*>(mdr->internal_op_private);
  CDir *dir = op->dir;

  dout(10) << __func__ << " " << action_name(op->action) << " " << op->name
           << " in " << *dir << dendl;

  if (!dir->is_auth()) {
    finish_request(mdr, -ESTALE);
    return;
  }
  if (!dir->is_complete()) {
    dir->fetch(new C_MDS_RetryRequest(mdcache, mdr), true);
    return;
  }

  std::string why;
  auto action = classify(dir, op->name, op->header, &why);
  if (action != op->action) {
    dout(5) << __func__ << " " << op->name << " changed from "
            << action_name(op->action) << " to " << action_name(action) << dendl;
    finish_request(mdr, -EAGAIN);
    return;
  }

  switch (op->action) {
  case ACTION_ROLLBACK:
    rollback_inode(mdr, op);
    break;
  case ACTION_REMOVE:
    remove_inode(mdr, op);
    break;
  case ACTION_REINSTATE:
    reinstate_inode(mdr, op);
    break;
  case ACTION_RELINK:
    relink_inode(mdr, op);
    break;
  default:
    ceph_abort();
  }
}

void SnapRollback::finish_request(const MDRequestRef& mdr, int r)
{
  mdcache->mds->server->respond_to_request(mdr, r);
}

/*
 * A purge can't be called back: the item is in the purge queue's journal
 * and its operations may be on their way to the OSDs. Started before the
 * rollback (purges are on hold during it), so it just has to finish. A
 * long queue is fine as long as it moves.
 */
void SnapRollback::wait_for_purge(const MDRequestRef& mdr, rollback_op_t *op,
                                  std::string_view reason)
{
  uint64_t progress = mdcache->mds->purge_queue.get_progress();
  if (progress != op->purge_progress) {
    op->purge_progress = progress;
    op->retries = 0;
  }
  retry_later(mdr, op, reason, max_purge_stall);
}

void SnapRollback::retry_later(const MDRequestRef& mdr, rollback_op_t *op,
                               std::string_view reason, int limit)
{
  if (++op->retries > limit) {
    dout(5) << __func__ << " " << op->name << ": giving up, " << reason << dendl;
    finish_request(mdr, -EBUSY);
    return;
  }
  dout(10) << __func__ << " " << op->name << ": " << reason << ", retry "
           << op->retries << dendl;
  mdcache->mds->timer.add_event_after(1.0, new C_MDS_RetryRequest(mdcache, mdr));
}

bool SnapRollback::acquire_locks(const MDRequestRef& mdr, rollback_op_t *op,
                                 CDentry *dn, CInode *in, CDentry *straydn)
{
  if (mdr->locking_state & MutationImpl::ALL_LOCKED) {
    return true;
  }

  MutationImpl::LockOpVec lov;
  CInode *diri = dn->get_dir()->get_inode();
  lov.add_rdlock(&diri->snaplock);
  lov.add_wrlock(&diri->filelock);
  lov.add_wrlock(&diri->nestlock);
  lov.add_xlock(&dn->lock);
  if (in) {
    lov.add_xlock(&in->linklock);
    lov.add_xlock(&in->snaplock);
    lov.add_xlock(&in->authlock);
    lov.add_xlock(&in->filelock);
    lov.add_xlock(&in->xattrlock);
  }
  if (straydn) {
    lov.add_wrlock(&straydn->get_dir()->inode->filelock);
    lov.add_wrlock(&straydn->get_dir()->inode->nestlock);
    lov.add_xlock(&straydn->lock);
  }

  if (!mdcache->mds->locker->acquire_locks(mdr, lov)) {
    return false;
  }
  mdr->locking_state |= MutationImpl::ALL_LOCKED;
  return true;
}

void SnapRollback::rollback_data(const MDRequestRef& mdr, rollback_op_t *op)
{
  MDSRank *mds = mdcache->mds;
  snapid_t snapid = op->header->get_rollback_snapid();
  object_locator_t oloc = OSDMap::file_to_object_locator(op->layout);

  uint64_t end = std::min(op->num_objs, op->next_obj + max_inflight);
  dout(15) << __func__ << " " << op->data_ino << " objects [" << op->next_obj
           << ", " << end << ") of " << op->num_objs << " to snap " << snapid
           << dendl;

  C_GatherBuilder gather(g_ceph_context, new C_IO_SnapRollback_Data(this, mdr));
  for (; op->next_obj < end; ++op->next_obj) {
    object_t oid = file_object_t(op->data_ino, op->next_obj);
    auto sub = gather.new_sub();
    // no object (and no clone) is fine: the object did not exist at the
    // snapshot and the head object, if any, gets removed by the rollback.
    mds->objecter->rollback_object(
      oid, oloc, op->snapc, snapid, ceph::real_clock::now(),
      new LambdaContext([sub](int r) {
        sub->complete(r == -ENOENT ? 0 : r);
      }));
  }
  gather.activate();
}

void SnapRollback::_rollback_data_window_done(const MDRequestRef& mdr, int r)
{
  auto op = static_cast<rollback_op_t*>(mdr->internal_op_private);
  if (r < 0) {
    derr << __func__ << " " << op->data_ino << ": " << cpp_strerror(r) << dendl;
    finish_request(mdr, r);
    return;
  }
  if (op->next_obj < op->num_objs) {
    rollback_data(mdr, op);
    return;
  }
  op->data_done = true;
  dispatch(mdr);
}

static uint64_t num_data_objects(const file_layout_t& layout,
                                 const CInode::mempool_inode& si,
                                 const CInode::mempool_inode& hi)
{
  uint64_t size = std::max({si.size, si.max_size_ever, hi.size, hi.max_size_ever});
  return std::max<uint64_t>(1, Striper::get_num_objects(layout, size));
}

/*
 * Copy the metadata of the snapshot inode `si` into the head inode `ni`,
 * keeping the head's identity and bookkeeping. Sequence numbers that
 * clients/OSDs order on are moved forward.
 */
static void copy_snap_inode(CInode::mempool_inode& ni,
                            const CInode::mempool_inode& si, utime_t now)
{
  ni.mode = si.mode;
  ni.uid = si.uid;
  ni.gid = si.gid;
  ni.rdev = si.rdev;
  ni.layout = si.layout;
  ni.size = si.size;
  ni.mtime = si.mtime;
  ni.atime = si.atime;
  ni.btime = si.btime;
  ni.inline_data = si.inline_data;
  ni.fscrypt_auth = si.fscrypt_auth;
  ni.fscrypt_file = si.fscrypt_file;

  ni.time_warp_seq = std::max(ni.time_warp_seq, si.time_warp_seq) + 1;
  ni.xattr_version = std::max(ni.xattr_version, si.xattr_version) + 1;
  ni.max_size_ever = std::max(ni.max_size_ever, si.max_size_ever);

  // the data objects were rolled back by the OSD, not truncated; bump the
  // truncate seq so that stale truncate_size from clients is not applied.
  ni.truncate_seq = std::max(ni.truncate_seq, si.truncate_seq) + 1;
  ni.truncate_size = ni.size;
  ni.truncate_from = 0;
  ni.truncate_pending = 0;
  ni.client_ranges.clear();

  ni.rstat.rbytes = ni.size;
  ni.ctime = now;
  if (now > ni.rstat.rctime) {
    ni.rstat.rctime = now;
  }
  ni.change_attr++;
}

/*
 * CASE I/II: the name refers to the same inode in head and in the
 * snapshot. The head dentry/inode is COWed for snapshots taken after
 * the rollback snapshot (head becomes [newest_seq+1, head]) and the
 * head inode is replaced with the snapshot version.
 */
void SnapRollback::rollback_inode(const MDRequestRef& mdr, rollback_op_t *op)
{
  MDSRank *mds = mdcache->mds;
  CDir *dir = op->dir;
  snapid_t snapid = op->header->get_rollback_snapid();

  CDentry *dn = dir->lookup(op->name);
  CInode *in = dn->get_projected_linkage()->get_inode();
  CDentry *sdn = dir->lookup(op->name, snapid);
  CInode *sin = sdn->get_projected_linkage()->get_inode();

  if (!acquire_locks(mdr, op, dn, in, nullptr)) {
    return;
  }

  const auto& si = *sin->get_inode();
  if (in->is_file() && !op->data_done) {
    if (op->num_objs == 0) {
      const auto& hi = *in->get_projected_inode();
      if (si.layout != hi.layout) {
        dout(5) << __func__ << " layout changed since snapshot on " << *in << dendl;
        finish_request(mdr, -EOPNOTSUPP);
        return;
      }
      op->data_ino = in->ino();
      op->layout = si.layout;
      op->snapc = in->find_snaprealm()->get_snap_context();
      op->num_objs = num_data_objects(op->layout, si, hi);
    }
    rollback_data(mdr, op);
    return;
  }

  dout(10) << __func__ << " " << *in << " from " << *sin << dendl;

  MDLog *mdlog = mds->mdlog;
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "snap_rollback");

  auto pi = in->project_inode(mdr, true);
  copy_snap_inode(*pi.inode, si, mdr->get_op_stamp());
  pi.inode->version = in->pre_dirty();
  pi.inode->update_backtrace();
  if (auto& sx = sin->get_xattrs(); sx) {
    *pi.xattrs = *sx;
  } else {
    pi.xattrs->clear();
  }
  if (sin != in) {
    in->symlink = sin->symlink;
  }

  mdcache->predirty_journal_parents(mdr, &le->metablob, in, nullptr, PREDIRTY_PRIMARY);
  // COWs the head dentry for snapshots newer than the rollback snapshot
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, in);

  mdr->committing = true;
  mdlog->submit_entry(le, new C_SnapRollback_Logged(mds, [this, mdr]() {
    _rollback_inode_logged(mdr);
  }));
  mdlog->flush();
}

void SnapRollback::_rollback_inode_logged(const MDRequestRef& mdr)
{
  mdr->apply();
  finish_request(mdr, 0);
}

CDentry *SnapRollback::prepare_stray_dentry(const MDRequestRef& mdr, CInode *in)
{
  std::string straydname;
  in->name_stray_dentry(straydname);

  if (mdr->straydn) {
    ceph_assert(mdr->straydn->get_name() == straydname);
    return mdr->straydn;
  }

  CDir *straydir = mdcache->get_stray_dir(in);
  CDentry *straydn = straydir->lookup(straydname);
  if (!straydn) {
    if (straydir->is_frozen_dir()) {
      dout(10) << __func__ << ": " << *straydir << " is frozen, waiting" << dendl;
      mdcache->mds->locker->drop_locks(mdr.get());
      mdr->drop_local_auth_pins();
      straydir->add_waiter(CDir::WAIT_UNFREEZE, new C_MDS_RetryRequest(mdcache, mdr));
      return nullptr;
    }
    straydn = straydir->add_null_dentry(straydname);
    straydn->mark_new();
  } else {
    ceph_assert(straydn->get_projected_linkage()->is_null());
  }

  straydn->state_set(CDentry::STATE_STRAY);
  mdr->straydn = straydn;
  mdr->pin(straydn);
  return straydn;
}

/*
 * The head inode did not exist at the snapshot (or is a different inode
 * than the one in the snapshot): unlink it into the stray directory, the
 * same as an unlink(2). Snapshots taken after the rollback snapshot keep
 * seeing it via the COWed dentry; the stray manager purges the head.
 */
void SnapRollback::remove_inode(const MDRequestRef& mdr, rollback_op_t *op)
{
  MDSRank *mds = mdcache->mds;
  CDir *dir = op->dir;

  CDentry *dn = dir->lookup(op->name);
  CInode *in = dn->get_projected_linkage()->get_inode();

  // Replaced after the snapshot: classify() can only rule out that the
  // snapshot inode is unreachable if it's in cache. Find out before removing.
  CDentry *sdn = dir->lookup(op->name, op->header->get_rollback_snapid());
  CDentry::linkage_t *sl = sdn ? sdn->get_projected_linkage() : nullptr;
  if (sl && !sl->is_null() && !mdcache->get_inode(sl->get_inode()->ino()) &&
      !head_gone_from_disk(mdr, op, sl->get_inode())) {
    return;
  }

  CDentry *straydn = prepare_stray_dentry(mdr, in);
  if (!straydn) {
    return;
  }
  if (!acquire_locks(mdr, op, dn, in, straydn)) {
    return;
  }

  dout(10) << __func__ << " " << *dn << " " << *in << dendl;

  straydn->first = mdcache->get_global_snaprealm()->get_newest_seq() + 1;

  if (!mdr->more()->desti_srnode) {
    SnapRealm *realm = in->find_snaprealm();
    snapid_t follows = realm->get_newest_seq();
    if (in->snaprealm || follows + 1 > in->get_oldest_snap()) {
      sr_t *new_srnode = in->prepare_new_srnode(follows);
      in->record_snaprealm_past_parent(new_srnode, straydn->get_dir()->inode->find_snaprealm());
      mdr->more()->desti_srnode = new_srnode;
    }
  }

  MDLog *mdlog = mds->mdlog;
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "snap_rollback_remove");

  straydn->push_projected_linkage(in);
  dn->pre_dirty();

  auto pi = in->project_inode(mdr);
  {
    std::string t;
    dn->make_path_string(t, true);
    pi.inode->stray_prior_path = std::move(t);
  }
  pi.inode->version = in->pre_dirty();
  pi.inode->ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pi.inode->rstat.rctime) {
    pi.inode->rstat.rctime = mdr->get_op_stamp();
  }
  pi.inode->change_attr++;
  pi.inode->nlink--;
  if (pi.inode->nlink == 0) {
    in->state_set(CInode::STATE_ORPHAN);
  }

  if (mdr->more()->desti_srnode) {
    auto& desti_srnode = mdr->more()->desti_srnode;
    in->project_snaprealm(desti_srnode);
    desti_srnode = NULL;
  }

  mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(),
                                    PREDIRTY_PRIMARY|PREDIRTY_DIR, -1);
  mdcache->predirty_journal_parents(mdr, &le->metablob, in, straydn->get_dir(),
                                    PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);

  pi.inode->update_backtrace();
  le->metablob.add_primary_dentry(straydn, in, true, true);

  mdcache->journal_cow_dentry(mdr.get(), &le->metablob, dn);
  le->metablob.add_null_dentry(dn, true);

  dn->push_projected_linkage();

  ceph_assert(in->first <= straydn->first);
  in->first = straydn->first;

  version_t dnpv = dn->get_projected_version();
  mdr->committing = true;
  mdlog->submit_entry(le, new C_SnapRollback_Logged(mds, [this, mdr, dn, straydn, dnpv]() {
    _remove_inode_logged(mdr, dn, straydn, dnpv);
  }));
  mdlog->flush();
}

void SnapRollback::_remove_inode_logged(const MDRequestRef& mdr, CDentry *dn,
                                        CDentry *straydn, version_t dnpv)
{
  CInode *strayin = dn->get_linkage()->get_inode();
  bool hadrealm = strayin->snaprealm ? true : false;
  strayin->early_pop_projected_snaprealm();

  dn->get_dir()->unlink_inode(dn);
  dn->pop_projected_linkage();
  dn->mark_dirty(dnpv, mdr->ls);

  straydn->pop_projected_linkage();
  mdcache->touch_dentry_bottom(straydn);

  mdr->apply();

  mdcache->send_dentry_unlink(dn, straydn, mdr);
  if (strayin->snaprealm && !hadrealm) {
    mdcache->do_realm_invalidate_and_update_notify(strayin, CEPH_SNAP_OP_SPLIT, false);
  }

  finish_request(mdr, 0);

  dn->get_dir()->try_remove_unlinked_dn(dn);
  if (!straydn->get_projected_linkage()->is_null()) {
    mdcache->notify_stray(straydn);
  }
}

/*
 * The name does not exist in head and the head inode is gone (purged):
 * create the head inode again under its original inode number, so that
 * the data objects (rolled back from their snapshot clones) are reachable
 * again. If the head inode is alive it gets relinked instead, see
 * relink_inode().
 */
void SnapRollback::reinstate_inode(const MDRequestRef& mdr, rollback_op_t *op)
{
  MDSRank *mds = mdcache->mds;
  CDir *dir = op->dir;
  snapid_t snapid = op->header->get_rollback_snapid();

  CDentry *sdn = dir->lookup(op->name, snapid);
  CInode *sin = sdn->get_projected_linkage()->get_inode();

  // A purge in progress; a head inode in any other state means the action
  // changes (see classify()).
  if (mdcache->get_inode(sin->ino())) {
    wait_for_purge(mdr, op, "head inode being purged");
    return;
  }
  if (!head_gone_from_disk(mdr, op, sin)) {
    return;
  }

  CDentry *dn = dir->lookup(op->name);
  if (!dn) {
    dn = dir->add_null_dentry(op->name, mdcache->get_global_snaprealm()->get_newest_seq() + 1);
    dn->mark_new();
  }
  if (!acquire_locks(mdr, op, dn, nullptr, nullptr)) {
    return;
  }

  const auto& si = *sin->get_inode();
  CInode *diri = dir->get_inode();
  if (sin->is_file() && !op->data_done) {
    if (op->num_objs == 0) {
      op->data_ino = sin->ino();
      op->layout = si.layout;
      op->snapc = diri->find_snaprealm()->get_snap_context();
      op->num_objs = num_data_objects(op->layout, si, si);
    }
    rollback_data(mdr, op);
    return;
  }

  snapid_t follows = mdcache->get_global_snaprealm()->get_newest_seq();
  dn->first = follows + 1;

  CInode *in = new CInode(mdcache, true, dn->first, CEPH_NOSNAP);
  auto _inode = in->_get_inode();
  *_inode = si;
  _inode->version = dn->pre_dirty();
  _inode->nlink = 1;
  _inode->stray_prior_path.clear();
  copy_snap_inode(*_inode, si, mdr->get_op_stamp());

  auto xattrs = CInode::allocate_xattr_map();
  if (auto& sx = sin->get_xattrs(); sx) {
    *xattrs = *sx;
  }
  in->reset_xattrs(std::move(xattrs));
  in->symlink = sin->symlink;

  mdcache->add_inode(in);
  dn->push_projected_linkage(in);

  dout(10) << __func__ << " " << *dn << " " << *in << " from " << *sin << dendl;

  MDLog *mdlog = mds->mdlog;
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "snap_rollback_reinstate");

  mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(),
                                    PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);
  le->metablob.add_primary_dentry(dn, in, true, true, true);

  mdr->committing = true;
  mdlog->submit_entry(le, new C_SnapRollback_Logged(mds, [this, mdr, dn, in]() {
    _reinstate_inode_logged(mdr, dn, in);
  }));
  mdlog->flush();
}

void SnapRollback::_reinstate_inode_logged(const MDRequestRef& mdr, CDentry *dn,
                                           CInode *in)
{
  dn->pop_projected_linkage();

  in->mark_dirty(mdr->ls);
  in->mark_dirty_parent(mdr->ls, true);

  mdr->apply();

  mdcache->send_dentry_link(dn, MDRequestRef());
  finish_request(mdr, 0);
}

/*
 * The name does not exist in head but the head inode is alive: in stray
 * (not purged yet, or kept for its old_inodes) or at another name (renamed
 * after the snapshot). Move it back to this name with the rename machinery,
 * which also takes care of snapshots covering the location it is moved
 * away from. The entry then classifies as ACTION_ROLLBACK, which restores
 * the data and metadata.
 */
void SnapRollback::relink_inode(const MDRequestRef& mdr, rollback_op_t *op)
{
  MDSRank *mds = mdcache->mds;
  CDir *dir = op->dir;

  CDentry *sdn = dir->lookup(op->name, op->header->get_rollback_snapid());
  CInode *sin = sdn->get_projected_linkage()->get_inode();

  // dispatch() just classified this entry, so the head inode is in cache
  CInode *in = mdcache->get_inode(sin->ino());
  ceph_assert(in);
  CDentry *srcdn = in->get_projected_parent_dn();

  if (in->state_test(CInode::STATE_NEEDSRECOVER) ||
      in->state_test(CInode::STATE_RECOVERING)) {
    retry_later(mdr, op, "head inode size recovery pending");
    return;
  }

  CDentry *destdn = dir->lookup(op->name);
  if (!destdn) {
    destdn = dir->add_null_dentry(op->name, mdcache->get_global_snaprealm()->get_newest_seq() + 1);
    destdn->mark_new();
  }
  if (!acquire_locks(mdr, op, destdn, in, srcdn)) {
    return;
  }

  dout(10) << __func__ << " " << *srcdn << " to " << *destdn << " " << *in << dendl;

  if (!mdr->more()->srci_srnode) {
    SnapRealm *src_realm = srcdn->get_dir()->inode->find_snaprealm();
    SnapRealm *dest_realm = dir->inode->find_snaprealm();
    snapid_t follows = src_realm->get_newest_seq();
    if (src_realm != dest_realm &&
        (in->snaprealm || follows + 1 > in->get_oldest_snap())) {
      sr_t *new_srnode = in->prepare_new_srnode(follows);
      in->record_snaprealm_past_parent(new_srnode, dest_realm);
      mdr->more()->srci_srnode = new_srnode;
    }
  }

  MDLog *mdlog = mds->mdlog;
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "snap_rollback_relink");

  mds->server->_rename_prepare(mdr, &le->metablob, nullptr, srcdn, destdn,
                               sdn->get_alternate_name(), nullptr);

  // A stray inode is linked again. The journal event refers to the projected
  // inode and gets encoded after submitting, so this makes it to the journal.
  auto pi = in->_get_projected_inode();
  pi->nlink = 1;
  pi->stray_prior_path.clear();

  mdr->committing = true;
  mdlog->submit_entry(le, new C_SnapRollback_Logged(mds, [this, mdr, srcdn, destdn, in]() {
    _relink_inode_logged(mdr, srcdn, destdn, in);
  }));
  mdlog->flush();
}

void SnapRollback::_relink_inode_logged(const MDRequestRef& mdr, CDentry *srcdn,
                                        CDentry *destdn, CInode *in)
{
  mdcache->mds->server->_rename_apply(mdr, srcdn, destdn, nullptr);
  in->state_clear(CInode::STATE_ORPHAN);

  mdcache->send_dentry_link(destdn, mdr);
  finish_request(mdr, 0);
}
