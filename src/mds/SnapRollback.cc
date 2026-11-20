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

/*
 * The version of an inode that a snapshot sees: a snapshot inode of its
 * own (the dentry was COWed along with the inode), an old_inode of the
 * head inode (multiversion: hardlinked, or with a snaprealm), or the
 * head itself if it wasn't modified since. The references keep what the
 * pointers point into alive across projecting the head inode.
 */
struct SnapRollback::snap_version_t {
  const CInode::mempool_inode *inode = nullptr;
  const CInode::mempool_xattr_map *xattrs = nullptr;

  CInode::inode_const_ptr inode_ref;
  CInode::xattr_map_const_ptr xattrs_ref;
  CInode::old_inode_map_const_ptr old_inodes_ref;
};

static inodeno_t linkage_ino(const CDentry::linkage_t *dnl)
{
  return dnl->is_remote() ? dnl->get_remote_ino() : dnl->get_inode()->ino();
}

static bool linkage_is_dir(const CDentry::linkage_t *dnl)
{
  return dnl->is_remote() ? dnl->get_remote_d_type() == DT_DIR
                          : dnl->get_inode()->is_dir();
}

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

class C_SnapRollback_Loaded : public MDSInternalContext {
  SnapRollback *sr;
  MDRequestRef mdr;
public:
  C_SnapRollback_Loaded(SnapRollback *s, const MDRequestRef& r)
    : MDSInternalContext(s->mdcache->mds), sr(s), mdr(r) {}
  void finish(int r) override {
    if (r < 0) {
      sr->finish_request(mdr, r);
      return;
    }
    sr->dispatch(mdr);
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
  case ACTION_LINK: return "link";
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
 * Compare what `name` links in head and in the snapshot. Directory
 * inodes are multiversion, so a directory that existed in the snapshot
 * as the same inode has a head dentry that covers the snapshot.
 *
 * For anything else only the inode number matters: whether a link is the
 * primary or a remote one says where the inode is stored, and that may
 * have changed since the snapshot (unlinking the primary link moves the
 * inode to another link).
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

  if ((hl && linkage_is_dir(hl)) || (sl && linkage_is_dir(sl))) {
    if (hdn == sdn && hl->is_primary()) {
      // same directory in head and snapshot -- recursed into by the scrub
      return ACTION_NONE;
    }
    *why = "directory";
    return ACTION_SKIP;
  }

  if (!sl) {
    // linked after the snapshot
    return ACTION_REMOVE;
  }

  inodeno_t sino = linkage_ino(sl);
  if (hl && linkage_ino(hl) == sino) {
    if (header->is_rollback_done(sino)) {
      // rolled back from another of its links
      return ACTION_NONE;
    }
    // rollback_inode() loads the inode if it isn't in cache, after which
    // the entry is classified again.
    CInode *in = hl->is_primary() ? hl->get_inode() : mdcache->get_inode(sino);
    if (in) {
      if (is_linked_outside(in, header)) {
        *why = "inode linked outside the rollback directory";
        return ACTION_SKIP;
      }
      if (!is_snapshot_covered(in, snapid)) {
        *why = "inode not covered by the snapshot";
        return ACTION_SKIP;
      }
    }
    return ACTION_ROLLBACK;
  }

  // The name doesn't link the snapshot inode. Decide what can be done
  // about that before touching what the name links now, if anything:
  // removing can't be undone.
  auto state = get_head_state(sl, header, why);
  if (state == HEAD_PINNED) {
    return ACTION_SKIP;
  }
  if (hl) {
    // the entry is reclassified after the head link has been removed
    return ACTION_REMOVE;
  }
  switch (state) {
  case HEAD_RELINKABLE:
    return ACTION_RELINK;
  case HEAD_LINKABLE:
    return ACTION_LINK;
  case HEAD_PURGING:
    // waits for the purge, then the entry gets classified again
    return ACTION_REINSTATE;
  default:
    break;
  }
  // Not in cache. An inode that a remote dentry of the snapshot refers to
  // is kept (in stray, if it has no links) for its old_inodes: it has to
  // be loaded. Otherwise it may have been purged.
  return sl->is_primary() ? ACTION_REINSTATE : ACTION_LINK;
}

/*
 * Find out what happened to the head version of the inode that the
 * snapshot dentry `sl` links, which isn't linked at the snapshot's name
 * anymore. An inode that isn't in cache is reported as gone;
 * head_gone_from_disk() tells whether it is.
 */
SnapRollback::head_state_t
SnapRollback::get_head_state(CDentry::linkage_t *sl,
                             const ScrubHeaderRef& header, std::string *why)
{
  inodeno_t ino = linkage_ino(sl);
  snapid_t snapid = header->get_rollback_snapid();
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
  // Rolling it back changes what its other links show: leave it alone if
  // any of them is outside of the directory being rolled back.
  if (is_linked_outside(in, header)) {
    *why = "head inode linked outside the rollback directory";
    return HEAD_PINNED;
  }
  // Its data can't be rolled back otherwise; find out before the name is
  // changed for it.
  if (!is_snapshot_covered(in, snapid)) {
    *why = "head inode not covered by the snapshot";
    return HEAD_PINNED;
  }

  snap_version_t sv;
  if (!get_snap_version(sl, in, snapid, &sv)) {
    *why = "head inode without a version at the snapshot";
    return HEAD_PINNED;
  }

  if (pdn->get_dir()->get_inode()->is_stray()) {
    // Unlinked, or its primary link was removed and it is waiting to be
    // moved to one of its remote links (not during a rollback). Remote
    // dentries refer to it by number, so it can become primary here.
    return HEAD_RELINKABLE;
  }

  const auto& pi = in->get_projected_inode();
  if (pi->nlink == 1 && !in->is_projected_snaprealm_global()) {
    if (sv.inode->nlink == 1) {
      // Renamed after the snapshot: its only link in the snapshot is the
      // name being rolled back, so the link it has now is not in the
      // snapshot. Move it back.
      return HEAD_RELINKABLE;
    }
  }
  // Hardlinked now or in the snapshot. The link it is stored at may be in
  // the snapshot, too: leave it, any link that isn't in the snapshot gets
  // removed when the rollback gets there.
  return HEAD_LINKABLE;
}

bool SnapRollback::get_snap_version(CDentry::linkage_t *sl, CInode *in,
                                    snapid_t snapid, snap_version_t *sv)
{
  CInode *sin = sl->is_primary() ? sl->get_inode() : nullptr;
  if (sin && sin->last != CEPH_NOSNAP) {
    sv->inode_ref = sin->get_inode();
    sv->xattrs_ref = sin->get_xattrs();
    sv->inode = sv->inode_ref.get();
    sv->xattrs = sv->xattrs_ref.get();
    return true;
  }
  if (sin) {
    // the head dentry covers the snapshot
    in = sin;
  }
  ceph_assert(in && in->last == CEPH_NOSNAP);

  // same as what a stat of the snapshot gets, see CInode::encode_inodestat()
  if (in->is_any_old_inodes()) {
    sv->old_inodes_ref = in->get_old_inodes();
    auto& old_inodes = *sv->old_inodes_ref;
    auto it = old_inodes.lower_bound(snapid);
    if (it != old_inodes.end()) {
      if (it->second.first > snapid && it != old_inodes.begin()) {
        --it;
      }
      if (it->second.first <= snapid && snapid <= it->first) {
        sv->inode = &it->second.inode;
        sv->xattrs = &it->second.xattrs;
        return true;
      }
    }
  }
  if (in->first > snapid) {
    // modified since, without the snapshot's version having been kept
    return false;
  }
  sv->inode_ref = in->get_projected_inode();
  sv->xattrs_ref = in->get_projected_xattrs();
  sv->inode = sv->inode_ref.get();
  sv->xattrs = sv->xattrs_ref.get();
  return true;
}

/*
 * Does `in` have a link outside of the directory being rolled back? Only
 * the primary link is always known: remote links are found if they are in
 * cache. Hardlinks do not cross subvolumes, so there is none when a
 * subvolume is rolled back.
 */
bool SnapRollback::is_linked_outside(CInode *in, const ScrubHeaderRef& header)
{
  CInode *origin = mdcache->get_inode(header->get_origin());
  if (!origin) {
    return true;
  }
  if (CDentry *pdn = in->get_projected_parent_dn(); pdn) {
    CInode *pdiri = pdn->get_dir()->get_inode();
    if (!pdiri->is_stray() && !origin->is_projected_ancestor_of(pdiri)) {
      return true;
    }
  }
  for (auto rdn : in->get_remote_parents()) {
    if (rdn->last != CEPH_NOSNAP || !rdn->get_projected_linkage()->is_remote()) {
      continue;
    }
    if (!origin->is_projected_ancestor_of(rdn->get_dir()->get_inode())) {
      return true;
    }
  }
  return false;
}

/*
 * A link in the directory at the snapshot doesn't mean that the snapshot
 * is one of the inode's: the inode may be stored in, and get its snapshots
 * from, somewhere else. The OSD removes an object that has no clone for the
 * snapshot it is asked to roll back to.
 */
bool SnapRollback::is_snapshot_covered(CInode *in, snapid_t snapid)
{
  return in->find_snaprealm()->get_snaps().count(snapid);
}

/*
 * The head inode that `dnl` links. If it isn't in cache, returns nullptr
 * and the request gets redispatched once it is.
 */
CInode *SnapRollback::get_linked_inode(const MDRequestRef& mdr, rollback_op_t *op,
                                       CDentry::linkage_t *dnl)
{
  if (dnl->is_primary()) {
    return dnl->get_inode();
  }
  CInode *in = dnl->get_inode();
  if (!in) {
    in = mdcache->get_inode(dnl->get_remote_ino());
  }
  if (!in) {
    load_inode(mdr, op, dnl->get_remote_ino());
  }
  return in;
}

/*
 * Load an inode that a remote dentry refers to. The request must not hold
 * any locks, loading the inode may have to traverse the directories it
 * would lock.
 */
void SnapRollback::load_inode(const MDRequestRef& mdr, rollback_op_t *op,
                              inodeno_t ino)
{
  if (++op->retries > max_retries) {
    dout(5) << __func__ << " " << op->name << ": can't load inode " << ino
            << dendl;
    finish_request(mdr, -EBUSY);
    return;
  }
  dout(10) << __func__ << " " << op->name << ": loading inode " << ino << dendl;
  mdcache->open_ino(ino, -1, new C_SnapRollback_Loaded(this, mdr), false);
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
    case ACTION_LINK:
      // reclassify -- same inode now, rolled back unless it already was
      stats.linked++;
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
  case ACTION_LINK:
    link_inode(mdr, op);
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
 * CASE I/II: the name links the same inode in head and in the snapshot.
 * The head inode is COWed for snapshots taken after the rollback snapshot
 * (a snapshot inode along with its dentry, or an old_inode if it is
 * multiversion; head becomes [newest_seq+1, head]) and is replaced with
 * the snapshot version.
 *
 * The inode may be stored elsewhere (the name is a remote link), in which
 * case its other links see it rolled back as well.
 */
void SnapRollback::rollback_inode(const MDRequestRef& mdr, rollback_op_t *op)
{
  MDSRank *mds = mdcache->mds;
  CDir *dir = op->dir;
  snapid_t snapid = op->header->get_rollback_snapid();

  CDentry *dn = dir->lookup(op->name);
  CDentry::linkage_t *hl = dn->get_projected_linkage();
  CDentry *sdn = dir->lookup(op->name, snapid);
  CDentry::linkage_t *sl = sdn->get_projected_linkage();

  CInode *in = get_linked_inode(mdr, op, hl);
  if (!in) {
    return;
  }

  if (!acquire_locks(mdr, op, dn, in, nullptr)) {
    return;
  }

  snap_version_t sv;
  if (!get_snap_version(sl, in, snapid, &sv)) {
    dout(5) << __func__ << " no version at snap " << snapid << " of " << *in << dendl;
    finish_request(mdr, -ESTALE);
    return;
  }
  const auto& si = *sv.inode;

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

  dout(10) << __func__ << " " << *in << " to snap " << snapid << " via " << *sdn
           << dendl;

  // more links, now or in the snapshot: don't roll it back again from those
  bool track = hl->is_remote() || sl->is_remote() ||
               in->get_projected_inode()->nlink > 1 || si.nlink > 1;

  MDLog *mdlog = mds->mdlog;
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "snap_rollback");

  auto pi = in->project_inode(mdr, true);
  copy_snap_inode(*pi.inode, si, mdr->get_op_stamp());
  pi.inode->version = in->pre_dirty();
  pi.inode->update_backtrace();
  if (sv.xattrs) {
    *pi.xattrs = *sv.xattrs;
  } else {
    pi.xattrs->clear();
  }
  if (CInode *sin = sl->is_primary() ? sl->get_inode() : nullptr; sin && sin != in) {
    in->symlink = sin->symlink;
  }

  mdcache->predirty_journal_parents(mdr, &le->metablob, in, nullptr, PREDIRTY_PRIMARY);
  // COWs the head inode for snapshots newer than the rollback snapshot
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, in);

  mdr->committing = true;
  mdlog->submit_entry(le, new C_SnapRollback_Logged(mds, [this, mdr, in, track]() {
    _rollback_inode_logged(mdr, in, track);
  }));
  mdlog->flush();
}

void SnapRollback::_rollback_inode_logged(const MDRequestRef& mdr, CInode *in,
                                          bool track)
{
  auto op = static_cast<rollback_op_t*>(mdr->internal_op_private);

  mdr->apply();
  if (track) {
    op->header->set_rollback_done(in->ino());
  }
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
 * The head link did not exist at the snapshot (or links a different inode
 * than the one in the snapshot): remove it, the same as an unlink(2).
 * Snapshots taken after the rollback snapshot keep seeing it via the COWed
 * dentry.
 *
 * A primary link has its inode moved to the stray directory. With no links
 * left the stray manager purges the head; otherwise it would normally move
 * the inode to one of its remote links, which is on hold until the
 * rollback is done with all of them. A remote link just goes away.
 */
void SnapRollback::remove_inode(const MDRequestRef& mdr, rollback_op_t *op)
{
  MDSRank *mds = mdcache->mds;
  CDir *dir = op->dir;

  CDentry *dn = dir->lookup(op->name);
  CDentry::linkage_t *dnl = dn->get_projected_linkage();

  // Replaced after the snapshot: classify() can only rule out that the
  // snapshot inode is unreachable if it's in cache. Find out before removing.
  CDentry *sdn = dir->lookup(op->name, op->header->get_rollback_snapid());
  CDentry::linkage_t *sl = sdn ? sdn->get_projected_linkage() : nullptr;
  if (sl && !sl->is_null() && !mdcache->get_inode(linkage_ino(sl))) {
    if (!sl->is_primary()) {
      load_inode(mdr, op, linkage_ino(sl));
      return;
    }
    if (!head_gone_from_disk(mdr, op, sl->get_inode())) {
      return;
    }
  }

  CInode *in = get_linked_inode(mdr, op, dnl);
  if (!in) {
    return;
  }

  bool primary = dnl->is_primary();
  CDentry *straydn = nullptr;
  if (primary) {
    straydn = prepare_stray_dentry(mdr, in);
    if (!straydn) {
      return;
    }
  }
  if (!acquire_locks(mdr, op, dn, in, straydn)) {
    return;
  }

  dout(10) << __func__ << " " << *dn << " " << *in << dendl;

  if (straydn) {
    straydn->first = mdcache->get_global_snaprealm()->get_newest_seq() + 1;
  }

  if (!mdr->more()->desti_srnode) {
    if (in->is_projected_snaprealm_global()) {
      sr_t *new_srnode = in->prepare_new_srnode(0);
      in->record_snaprealm_parent_dentry(new_srnode, nullptr, dn, primary);
      // dropping the last linkage or dropping the last remote linkage,
      // detach the inode from global snaprealm
      auto nlink = in->get_projected_inode()->nlink;
      if (nlink == 1 ||
          (nlink == 2 && !primary &&
           !in->get_projected_parent_dir()->inode->is_stray())) {
        in->clear_snaprealm_global(new_srnode);
      }
      mdr->more()->desti_srnode = new_srnode;
    } else if (primary) {
      SnapRealm *realm = in->find_snaprealm();
      snapid_t follows = realm->get_newest_seq();
      if (in->snaprealm || follows + 1 > in->get_oldest_snap()) {
        sr_t *new_srnode = in->prepare_new_srnode(follows);
        in->record_snaprealm_past_parent(new_srnode, straydn->get_dir()->inode->find_snaprealm());
        mdr->more()->desti_srnode = new_srnode;
      }
    }
  }

  MDLog *mdlog = mds->mdlog;
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "snap_rollback_remove");

  if (straydn) {
    straydn->push_projected_linkage(in);
  }
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

  if (straydn) {
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(),
                                      PREDIRTY_PRIMARY|PREDIRTY_DIR, -1);
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, straydn->get_dir(),
                                      PREDIRTY_PRIMARY|PREDIRTY_DIR, 1);

    pi.inode->update_backtrace();
    le->metablob.add_primary_dentry(straydn, in, true, true);
  } else {
    // remote link: the inode stays where it is
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(),
                                      PREDIRTY_DIR, -1);
    mdcache->predirty_journal_parents(mdr, &le->metablob, in, nullptr,
                                      PREDIRTY_PRIMARY);
    mdcache->journal_dirty_inode(mdr.get(), &le->metablob, in);
  }

  mdcache->journal_cow_dentry(mdr.get(), &le->metablob, dn);
  le->metablob.add_null_dentry(dn, true);

  dn->push_projected_linkage();

  if (straydn) {
    ceph_assert(in->first <= straydn->first);
    in->first = straydn->first;
  }

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
  CInode *strayin = nullptr;
  bool hadrealm = false;
  if (straydn) {
    strayin = dn->get_linkage()->get_inode();
    hadrealm = strayin->snaprealm ? true : false;
    strayin->early_pop_projected_snaprealm();
  }

  dn->get_dir()->unlink_inode(dn);
  dn->pop_projected_linkage();
  dn->mark_dirty(dnpv, mdr->ls);

  if (straydn) {
    straydn->pop_projected_linkage();
    mdcache->touch_dentry_bottom(straydn);
  }

  mdr->apply();

  mdcache->send_dentry_unlink(dn, straydn, mdr);
  if (straydn && strayin->snaprealm && !hadrealm) {
    mdcache->do_realm_invalidate_and_update_notify(strayin, CEPH_SNAP_OP_SPLIT, false);
  }

  finish_request(mdr, 0);

  dn->get_dir()->try_remove_unlinked_dn(dn);
  if (straydn && !straydn->get_projected_linkage()->is_null()) {
    mdcache->notify_stray(straydn);
  }
}

/*
 * The name does not exist in head and the head inode is gone (purged):
 * create the head inode again under its original inode number, so that
 * the data objects (rolled back from their snapshot clones) are reachable
 * again. If the head inode is alive it gets relinked or linked instead,
 * see relink_inode() and link_inode().
 *
 * An inode with more links in the snapshot comes back with this one; the
 * others are added when the rollback gets to them.
 */
void SnapRollback::reinstate_inode(const MDRequestRef& mdr, rollback_op_t *op)
{
  MDSRank *mds = mdcache->mds;
  CDir *dir = op->dir;
  snapid_t snapid = op->header->get_rollback_snapid();

  CDentry *sdn = dir->lookup(op->name, snapid);
  CDentry::linkage_t *sl = sdn->get_projected_linkage();

  // A purge in progress; a head inode in any other state means the action
  // changes (see classify()).
  if (mdcache->get_inode(linkage_ino(sl))) {
    wait_for_purge(mdr, op, "head inode being purged");
    return;
  }
  // classify() has an inode that isn't in cache loaded if a remote dentry
  // refers to it
  ceph_assert(sl->is_primary());
  CInode *sin = sl->get_inode();
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
  // The inode has snapshots older than its (new) first: a snaprealm that
  // it gets later, when it is unlinked or hard linked, has to inherit them.
  in->oldest_snap = sin->get_oldest_snap();
  auto _inode = in->_get_inode();
  *_inode = si;
  _inode->version = dn->pre_dirty();
  _inode->nlink = 1;
  _inode->stray_prior_path.clear();
  copy_snap_inode(*_inode, si, mdr->get_op_stamp());
  // a new inode: linking it accounts its rstat in the dirfrag in full
  _inode->accounted_rstat = _inode->rstat;

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

  // more links in the snapshot: they find it rolled back
  bool track = si.nlink > 1;

  mdr->committing = true;
  mdlog->submit_entry(le, new C_SnapRollback_Logged(mds, [this, mdr, dn, in, track]() {
    _reinstate_inode_logged(mdr, dn, in, track);
  }));
  mdlog->flush();
}

void SnapRollback::_reinstate_inode_logged(const MDRequestRef& mdr, CDentry *dn,
                                           CInode *in, bool track)
{
  auto op = static_cast<rollback_op_t*>(mdr->internal_op_private);

  dn->pop_projected_linkage();

  in->mark_dirty(mdr->ls);
  in->mark_dirty_parent(mdr->ls, true);

  mdr->apply();

  if (track) {
    // its data and metadata are the snapshot's
    op->header->set_rollback_done(in->ino());
  }

  mdcache->send_dentry_link(dn, MDRequestRef());
  finish_request(mdr, 0);
}

/*
 * The name does not exist in head but the head inode is alive and can be
 * moved: in stray (not purged yet, kept for its old_inodes, or its primary
 * link was removed) or at another name that isn't in the snapshot (renamed
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
  CDentry::linkage_t *sl = sdn->get_projected_linkage();

  // dispatch() just classified this entry, so the head inode is in cache
  CInode *in = mdcache->get_inode(linkage_ino(sl));
  ceph_assert(in);
  CDentry *srcdn = in->get_projected_parent_dn();
  bool from_stray = srcdn->get_dir()->get_inode()->is_stray();

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
    if (in->is_projected_snaprealm_global()) {
      sr_t *new_srnode = in->prepare_new_srnode(0);
      in->record_snaprealm_parent_dentry(new_srnode, src_realm, srcdn, true);
      mdr->more()->srci_srnode = new_srnode;
    } else {
      snapid_t follows = src_realm->get_newest_seq();
      if (src_realm != dest_realm &&
          (in->snaprealm || follows + 1 > in->get_oldest_snap())) {
        sr_t *new_srnode = in->prepare_new_srnode(follows);
        in->record_snaprealm_past_parent(new_srnode, dest_realm);
        mdr->more()->srci_srnode = new_srnode;
      }
    }
  }

  MDLog *mdlog = mds->mdlog;
  mdr->ls = mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mdlog, "snap_rollback_relink");

  mds->server->_rename_prepare(mdr, &le->metablob, nullptr, srcdn, destdn,
                               sdn->get_alternate_name(), nullptr);

  // An inode in stray gets this link on top of the remote links it has, if
  // any. The journal event refers to the projected inode, so this makes it
  // to the journal.
  auto pi = in->_get_projected_inode();
  if (from_stray) {
    pi->nlink++;
  }
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

/*
 * The name does not exist in head and the head inode is alive at a link
 * that may be in the snapshot as well (it is hardlinked, or was at the
 * snapshot): add a remote link to it at this name, the same as a link(2).
 * The entry then classifies as ACTION_ROLLBACK, unless the inode was
 * rolled back from another link already.
 */
void SnapRollback::link_inode(const MDRequestRef& mdr, rollback_op_t *op)
{
  MDSRank *mds = mdcache->mds;
  CDir *dir = op->dir;

  CDentry *sdn = dir->lookup(op->name, op->header->get_rollback_snapid());
  CDentry::linkage_t *sl = sdn->get_projected_linkage();

  CInode *in = mdcache->get_inode(linkage_ino(sl));
  if (!in) {
    // the action changes if it turns out to be in stray, or linked outside
    load_inode(mdr, op, linkage_ino(sl));
    return;
  }

  CDentry *dn = dir->lookup(op->name);
  if (!dn) {
    dn = dir->add_null_dentry(op->name, mdcache->get_global_snaprealm()->get_newest_seq() + 1);
    dn->mark_new();
  }
  if (!acquire_locks(mdr, op, dn, in, nullptr)) {
    return;
  }

  dout(10) << __func__ << " " << *dn << " to " << *in << dendl;

  dn->first = mdcache->get_global_snaprealm()->get_newest_seq() + 1;
  dn->set_alternate_name(sdn->get_alternate_name());

  MDLog *mdlog = mds->mdlog;
  mdr->ls = mdlog->get_current_segment();

  version_t dnpv = dn->pre_dirty();

  auto pi = in->project_inode(mdr);
  pi.inode->version = in->pre_dirty();
  pi.inode->nlink++;
  pi.inode->ctime = mdr->get_op_stamp();
  if (mdr->get_op_stamp() > pi.inode->rstat.rctime) {
    pi.inode->rstat.rctime = mdr->get_op_stamp();
  }
  pi.inode->change_attr++;

  // hardlinked now: unless in a subvolume, any snapshot may see it
  bool adjust_realm = false;
  SnapRealm *target_realm = in->get_projected_parent_dir()->inode->find_snaprealm();
  if (!target_realm->get_subvolume_ino() && !in->is_projected_snaprealm_global()) {
    sr_t *newsnap = in->project_snaprealm();
    in->mark_snaprealm_global(newsnap);
    in->record_snaprealm_parent_dentry(newsnap, target_realm,
                                       in->get_projected_parent_dn(), true);
    adjust_realm = true;
  }

  EUpdate *le = new EUpdate(mdlog, "snap_rollback_link");
  mdcache->predirty_journal_parents(mdr, &le->metablob, in, dn->get_dir(),
                                    PREDIRTY_DIR, 1);
  mdcache->predirty_journal_parents(mdr, &le->metablob, in, nullptr,
                                    PREDIRTY_PRIMARY);
  le->metablob.add_remote_dentry(dn, true, in->ino(), in->d_type());
  mdcache->journal_dirty_inode(mdr.get(), &le->metablob, in);

  dn->push_projected_linkage(in->ino(), in->d_type());

  mdr->committing = true;
  mdlog->submit_entry(le, new C_SnapRollback_Logged(mds, [this, mdr, dn, in, dnpv, adjust_realm]() {
    _link_inode_logged(mdr, dn, in, dnpv, adjust_realm);
  }));
  mdlog->flush();
}

void SnapRollback::_link_inode_logged(const MDRequestRef& mdr, CDentry *dn,
                                      CInode *in, version_t dnpv,
                                      bool adjust_realm)
{
  CDentry::linkage_t *dnl = dn->pop_projected_linkage();
  if (!dnl->get_inode()) {
    dn->link_remote(dnl, in);
  }
  dn->mark_dirty(dnpv, mdr->ls);

  mdr->apply();

  mdcache->send_dentry_link(dn, MDRequestRef());

  if (adjust_realm) {
    int snap_op = CEPH_SNAP_OP_SPLIT;
    mdcache->send_snap_update(in, 0, snap_op);
    mdcache->do_realm_invalidate_and_update_notify(in, snap_op);
  }

  finish_request(mdr, 0);
}
