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

std::string_view SnapRollback::action_name(action_t action)
{
  switch (action) {
  case ACTION_NONE: return "none";
  case ACTION_SKIP: return "skip";
  case ACTION_ROLLBACK: return "rollback";
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
    *why = "created after the snapshot";
    return ACTION_SKIP;
  }
  if (hin && hin->ino() == sin->ino()) {
    if (hin == sin && hin->is_any_old_inodes()) {
      // the snapshot's metadata may be in old_inodes, which is not handled
      *why = "metadata in old_inodes";
      return ACTION_SKIP;
    }
    return ACTION_ROLLBACK;
  }

  // the snapshot inode is not linked at this name
  *why = "snapshot inode not linked at the name";
  return ACTION_SKIP;
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
  default:
    ceph_abort();
  }
}

void SnapRollback::finish_request(const MDRequestRef& mdr, int r)
{
  mdcache->mds->server->respond_to_request(mdr, r);
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
