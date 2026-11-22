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

#include "TrashManager.h"

#include "CDentry.h"
#include "CDir.h"
#include "CInode.h"
#include "Locker.h"
#include "MDCache.h"
#include "MDLog.h"
#include "MDSContext.h"
#include "MDSRank.h"
#include "Server.h"
#include "SnapRealm.h"
#include "events/EUpdate.h"
#include "messages/MClientRequest.h"

#include "common/debug.h"
#include "common/errno.h"
#include "include/ceph_assert.h"
#include "common/filepath.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << mds->get_nodeid() << ".trash "

struct C_TrashNotify : public MDSInternalContext {
  TrashManager *tm;
  explicit C_TrashNotify(TrashManager *t) : MDSInternalContext(t->mds), tm(t) {}
  void finish(int r) override {
    tm->kick();
  }
};

struct C_TrashKick : public MDSInternalContext {
  TrashManager *tm;
  explicit C_TrashKick(TrashManager *t) : MDSInternalContext(t->mds), tm(t) {}
  void finish(int r) override {
    tm->kick_scheduled = false;
    tm->advance();
  }
};

struct C_TrashWait : public MDSInternalContext {
  TrashManager *tm;
  inodeno_t ino;
  C_TrashWait(TrashManager *t, inodeno_t i) : MDSInternalContext(t->mds), tm(t), ino(i) {}
  void finish(int r) override {
    tm->wake(ino, r);
  }
};

struct C_TrashRecall : public MDSInternalContext {
  TrashManager *tm;
  inodeno_t root_ino;
  inodeno_t dir_ino;
  C_TrashRecall(TrashManager *t, inodeno_t root, inodeno_t dir)
    : MDSInternalContext(t->mds), tm(t), root_ino(root), dir_ino(dir) {}
  void finish(int r) override {
    tm->recall_finished(root_ino, dir_ino, r);
  }
};

struct C_TrashDiscover : public MDSInternalContext {
  TrashManager *tm;
  explicit C_TrashDiscover(TrashManager *t) : MDSInternalContext(t->mds), tm(t) {}
  void finish(int r) override {
    tm->discover_peer_step();
  }
};

struct C_TrashMarked : public MDSInternalContext {
  TrashManager *tm;
  inodeno_t ino;
  C_TrashMarked(TrashManager *t, inodeno_t i) : MDSInternalContext(t->mds), tm(t), ino(i) {}
  void finish(int r) override {
    if (r == 0)
      tm->rmsnaps_marked(ino);
  }
};

struct C_TrashMarkLogged : public MDSLogContextBase {
  TrashManager *tm;
  MDRequestRef mdr;
  CInode *in;
  C_TrashMarkLogged(TrashManager *t, const MDRequestRef& m, CInode *i)
    : tm(t), mdr(m), in(i) {}
  MDSRank *get_mds() override { return tm->mds; }
  void finish(int r) override {
    ceph_assert(r == 0);
    in->pop_and_dirty_projected_inode(mdr->ls, mdr);
    mdr->apply();
    tm->mds->server->respond_to_request(mdr, 0);
  }
};

struct TrashDiscoverFactory : public MDSContextFactory {
  TrashManager *tm;
  explicit TrashDiscoverFactory(TrashManager *t) : tm(t) {}
  MDSContext *build() override {
    return new C_TrashDiscover(tm);
  }
};

TrashManager::TrashManager(MDSRank *m) : mds(m) {}

TrashManager::TrashMetaRequest::TrashMetaRequest(int op, ceph_tid_t tid, mds_rank_t to,
                                                 CDentry *d, inodeno_t root, inodeno_t entry,
                                                 MDSContext *f, bool mig)
  : MDSMetaRequest(op, tid, to),
    dn(d), root_ino(root), entry_ino(entry), fin(f), migration(mig)
{
  if (dn)
    dn->get(CDentry::PIN_TRASHWALKER);
}

TrashManager::TrashMetaRequest::~TrashMetaRequest()
{
  if (dn)
    dn->put(CDentry::PIN_TRASHWALKER);
  if (fin) {
    MDSContext *c = fin;
    fin = nullptr;
    c->complete(-ECANCELED);
  }
}

MDCache *TrashManager::cache() const
{
  return mds->mdcache;
}

uint64_t TrashManager::max_inflight() const
{
  return g_conf().get_val<uint64_t>("mds_trash_max_inflight");
}

bool TrashManager::throttled() const
{
  uint64_t max = max_inflight();
  return max == 0 || global_inflight >= (int)max;
}

void TrashManager::activate()
{
  if (activated)
    return;
  activated = true;
  dout(10) << "activate" << dendl;
  kick();
}

void TrashManager::resume()
{
  paused = false;
  kick();
}

void TrashManager::arm_timer()
{
  if (kick_scheduled || !activated)
    return;
  kick_scheduled = true;
  mds->timer.add_event_after(0, new C_TrashKick(this));
}

void TrashManager::kick()
{
  if (in_advance) {
    need_advance = true;
    return;
  }
  arm_timer();
}

void TrashManager::note_trashed(CInode *in)
{
  dout(10) << "note_trashed " << *in << dendl;
  if (shutdown_stopped)
    return;
  kick();
}

void TrashManager::note_imported(CInode *in)
{
  if (in && (in->is_trash() || in->is_under_trash()) && !shutdown_stopped) {
    rescan_done = true;
    kick();
  }
}

void TrashManager::advance()
{
  if (!activated || shutdown_stopped)
    return;
  if (in_advance) {
    need_advance = true;
    return;
  }
  in_advance = true;
  int passes = 0;
  do {
    need_advance = false;
    advance_body();
  } while (need_advance && ++passes < 32);
  in_advance = false;
  if (need_advance)
    arm_timer();
}

void TrashManager::fetch_trash()
{
  CInode *trash = cache()->trash_inode;
  if (!trash)
    return;
  frag_vec_t leaves;
  trash->dirfragtree.get_leaves(leaves);
  for (auto fg : leaves) {
    CDir *dir = trash->get_dirfrag(fg);
    if (!dir || !dir->is_auth() || dir->is_complete() || dir->is_frozen())
      continue;
    dir->fetch(new C_TrashNotify(this));
  }
}

void TrashManager::advance_body()
{
  // Read the trash directory whether or not the walker may run. After a
  // failover its fragments are in the cache but not complete, and "trash ls"
  // on a paused rank would not know how many entries there are.
  fetch_trash();

  if (paused || g_conf().get_val<bool>("mds_trash_pause"))
    return;
  // Wait when the journal is halfway to the size that raises
  // MDS_HEALTH_TRIM. Trimming aims at mds_log_max_segments, so a journal of
  // that size is not behind, and waiting for less would leave this walk one
  // segment for every tick. Count what is left to trim and not what is in
  // the journal: an expired segment stays there until the next major one,
  // and on a rank that does nothing else that is until this walk adds to it.
  uint64_t unexpired = mds->mdlog->get_num_unexpired_segments();
  double warn = g_conf().get_val<double>("mds_log_warn_factor");
  uint64_t limit = mds->mdlog->get_max_segments() * (1 + (std::max(warn, 1.0) - 1) / 2);
  if (unexpired > limit) {
    dout(15) << "journal has " << unexpired << " segments to trim, waiting for "
             << limit << dendl;
    return;
  }
  if (cache()->cache_overfull()) {
    dout(15) << "cache overfull, waiting" << dendl;
    return;
  }

  discover_roots();
  cleanup_done();

  // One tree may use every slot. Several share them, so a tree that sorts
  // first cannot keep the others waiting.
  uint64_t active = 0;
  for (auto &p : roots) {
    if (p.second.state == State::ACTIVE)
      active++;
  }
  root_inflight_max = active ? std::max<uint64_t>(1, (max_inflight() + active - 1) / active) : 1;

  int budget = 64;
  std::vector<inodeno_t> ids;
  ids.reserve(roots.size());
  for (auto &p : roots)
    ids.push_back(p.first);

  bool more = false;
  for (auto ino : ids) {
    auto it = roots.find(ino);
    if (it == roots.end())
      continue;
    Step step = advance_root(it->second, budget);
    if (step == Step::Erase) {
      drop_pins(it->second);
      roots.erase(it);
      more = true;
    } else if (step == Step::Again) {
      more = true;
    }
    if (budget <= 0)
      break;
  }
  if (more && budget > 0)
    need_advance = true;
  else if (budget <= 0 && more)
    arm_timer();
}

bool TrashManager::has_snaps(CInode *in) const
{
  if (in->get_projected_inode()->rstat.rsnaps > 0)
    return true;
  if (in->snaprealm && !in->snaprealm->srnode.snaps.empty())
    return true;
  return false;
}

bool TrashManager::wants_rmsnaps(inodeno_t root_ino) const
{
  CInode *in = cache()->get_inode(root_ino);
  if (!in)
    return false;
  // The mark is on the trash entry. A root may be a subtree below it.
  in = top_under_trash(in);
  const auto& px = in->get_projected_xattrs();
  return px && px->count(mempool::mds_co::string(RMSNAPS_XATTR));
}

CInode *TrashManager::top_under_trash(CInode *in) const
{
  CInode *cur = in;
  while (cur && !cur->is_base()) {
    CDentry *dn = cur->get_parent_dn();
    if (!dn)
      break;
    CInode *parent = dn->get_dir()->inode;
    if (parent->is_trash())
      return cur;
    if (!parent->is_under_trash() && !parent->is_trash())
      break;
    cur = parent;
  }
  return in;
}

void TrashManager::ensure_root(CInode *in, bool remove_when_empty)
{
  if (!in || !in->is_dir())
    return;
  if (in->is_trash())
    return;
  auto it = roots.find(in->ino());
  if (it == roots.end()) {
    CInode *entry = top_under_trash(in);
    Root root;
    root.ino = in->ino();
    root.entry = entry->ino();
    root.pinned = true;
    root.remove_when_empty = remove_when_empty;
    root.prior_path = entry->get_projected_inode()->stray_prior_path;
    in->get(CInode::PIN_TRASH);
    root.stack.emplace_back();
    root.stack.back().ino = in->ino();
    dout(10) << "tracking " << *in << " remove_when_empty=" << remove_when_empty
             << " prior " << root.prior_path << dendl;
    roots.emplace(in->ino(), std::move(root));
    return;
  }
  if (remove_when_empty)
    it->second.remove_when_empty = true;
  if (it->second.prior_path.empty()) {
    CInode *entry = top_under_trash(in);
    it->second.entry = entry->ino();
    it->second.prior_path = entry->get_projected_inode()->stray_prior_path;
  }
  if (it->second.state == State::DONE && remove_when_empty) {
    it->second.state = State::ACTIVE;
    it->second.stack.clear();
    it->second.stack.emplace_back();
    it->second.stack.back().ino = in->ino();
  }
}

void TrashManager::discover_roots()
{
  CInode *trash = cache()->trash_inode;
  if (trash) {
    frag_vec_t leaves;
    trash->dirfragtree.get_leaves(leaves);
    for (auto fg : leaves) {
      CDir *dir = trash->get_dirfrag(fg);
      // fetch_trash() reads the fragments that are not complete yet.
      if (!dir || !dir->is_auth() || !dir->is_complete())
        continue;
      for (auto it = dir->begin(); it != dir->end(); ++it) {
        if (it->first.snapid != CEPH_NOSNAP)
          continue;
        auto *dnl = it->second->get_projected_linkage();
        if (!dnl->is_primary() || !dnl->get_inode()->is_dir())
          continue;
        ensure_root(dnl->get_inode(), true);
      }
    }
  }

  // A subtree of this rank inside a tree in trash, its own or another
  // rank's. Start at the subtree: the directories between it and the trash
  // entry belong to other ranks, and a walk from the entry stops at the
  // first of them. The rank above removes the directory once it is empty.
  std::vector<CDir*> subs;
  cache()->get_subtrees(subs);
  for (CDir *dir : subs) {
    CInode *in = dir->inode;
    if (!dir->is_auth() || !in->is_under_trash())
      continue;
    CDentry *pdn = in->get_parent_dn();
    if (pdn && pdn->get_dir()->is_auth()) {
      // The walk that covers the parent directory comes down to this one.
      // A trash entry was added above.
      continue;
    }
    ensure_root(in, false);
  }

  if (rescan_done) {
    // A subtree arrived from a rank that is stopping. It may belong to a
    // directory this rank had nothing left to do in.
    rescan_done = false;
    for (auto &p : roots) {
      Root &root = p.second;
      if (root.state != State::DONE)
        continue;
      root.state = State::ACTIVE;
      root.stack.clear();
      root.stack.emplace_back();
      root.stack.back().ino = root.ino;
    }
  }
}

void TrashManager::cleanup_done()
{
  std::vector<inodeno_t> drop;
  for (auto &p : roots) {
    if (p.second.state != State::DONE)
      continue;
    CInode *in = cache()->get_inode(p.first);
    if (!in || !in->is_under_trash())
      drop.push_back(p.first);
  }
  for (auto ino : drop) {
    auto it = roots.find(ino);
    if (it == roots.end())
      continue;
    drop_pins(it->second);
    roots.erase(it);
  }
}

void TrashManager::drop_pins(Root &root)
{
  if (root.pinned) {
    if (CInode *in = cache()->get_inode(root.ino))
      in->put(CInode::PIN_TRASH);
    root.pinned = false;
  }
  for (size_t i = 1; i < root.stack.size(); ++i) {
    if (CInode *in = cache()->get_inode(root.stack[i].ino))
      in->put(CInode::PIN_TRASH);
  }
  root.stack.clear();
}

void TrashManager::backoff(Root &root)
{
  if (root.backoff < 1)
    root.backoff = 1;
  root.backoff_until = ceph_clock_now() + utime_t((time_t)root.backoff, 0);
  root.backoff = std::min(root.backoff * 2, 30.0);
  dout(15) << "backoff root " << root.ino << " for " << root.backoff << "s" << dendl;
}

void TrashManager::park(Root &root)
{
  if (root.state != State::PARKED) {
    root.state = State::PARKED;
    root.parked_since = ceph_clock_now();
    if (cache()->logger)
      cache()->logger->inc(l_mdc_trash_parked);
    dout(5) << "parked " << root.ino << " (" << root.prior_path
            << ") because it has snapshots" << dendl;
    mds->clog->warn() << "trash entry " << root.prior_path
                      << " (" << root.entry << ") has snapshots and will not be purged"
                      << "; 'trash rmsnaps " << root.entry.val
                      << "' on the rank that has it in 'trash ls' removes them";
  }
  root.backoff_until = ceph_clock_now() + utime_t(60, 0);
}

void TrashManager::rewind_top(Root &root)
{
  root.drain = true;
  if (root.stack.empty())
    return;
  inodeno_t ino = root.stack.back().ino;
  root.stack.back().reset_scan();
  root.stack.back().ino = ino;
}

void TrashManager::wake(inodeno_t root_ino, int result)
{
  auto it = roots.find(root_ino);
  if (it == roots.end())
    return;
  it->second.waiting = false;
  if (result < 0)
    backoff(it->second);
  kick();
}

void TrashManager::begin_recall(Root &root, Frame &frame, CInode *diri)
{
  MDRequestRef mdr = cache()->request_start_internal(CEPH_MDS_OP_RMTREE);
  mdr->set_filepath(filepath(diri->ino()));
  mdr->set_filepath2(filepath(root.ino));
  mdr->internal_op_finish = new C_TrashRecall(this, root.ino, diri->ino());
  frame.recalling = true;
  frame.recall_req = mdr->reqid;
  dout(10) << "recall caps on " << *diri << " for root " << root.ino << dendl;
  cache()->dispatch_request(mdr);
}

void TrashManager::recall_finished(inodeno_t root_ino, inodeno_t dir_ino, int result)
{
  auto it = roots.find(root_ino);
  if (it == roots.end())
    return;
  for (auto &fr : it->second.stack) {
    if (fr.ino != dir_ino)
      continue;
    fr.recalling = false;
    fr.caps_recalled = (result == 0);
    if (result != 0)
      backoff(it->second);
  }
  kick();
}

void TrashManager::dispatch_internal(const boost::intrusive_ptr<MDRequestImpl>& mdr)
{
  if (mdr->has_more() && mdr->more()->trash_mark_rmsnaps)
    dispatch_mark_rmsnaps(mdr);
  else
    dispatch_recall(mdr);
}

int TrashManager::remove_snaps(inodeno_t ino, std::ostream& ss)
{
  CInode *in = cache()->get_inode(ino);
  CDentry *pdn = in ? in->get_projected_parent_dn() : nullptr;
  if (!in || !in->is_dir() || !pdn || !pdn->get_dir()->inode->is_trash()) {
    ss << "no trash entry with inode " << ino << " is known here; see 'trash ls'";
    return -ENOENT;
  }
  if (!in->is_auth()) {
    ss << "trash entry " << ino << " belongs to mds." << in->authority().first;
    return -EXDEV;
  }
  dout(5) << "remove_snaps asked for " << *in << dendl;
  MDRequestRef mdr = cache()->request_start_internal(CEPH_MDS_OP_RMTREE);
  mdr->set_filepath(filepath(ino));
  mdr->more()->trash_mark_rmsnaps = true;
  mdr->internal_op_finish = new C_TrashMarked(this, ino);
  cache()->dispatch_request(mdr);
  return 0;
}

void TrashManager::dispatch_mark_rmsnaps(const boost::intrusive_ptr<MDRequestImpl>& mdr)
{
  CInode *in = cache()->get_inode(mdr->get_filepath().get_ino());
  CDentry *pdn = in ? in->get_projected_parent_dn() : nullptr;
  if (!in || !in->is_auth() || !pdn || !pdn->get_dir()->inode->is_trash()) {
    mds->server->respond_to_request(mdr, -ENOENT);
    return;
  }
  if (!(mdr->locking_state & MutationImpl::ALL_LOCKED)) {
    MutationImpl::LockOpVec lov;
    lov.add_xlock(&in->xattrlock);
    if (!mds->locker->acquire_locks(mdr, lov))
      return;
    mdr->locking_state |= MutationImpl::ALL_LOCKED;
  }
  if (wants_rmsnaps(in->ino())) {
    mds->server->respond_to_request(mdr, 0);
    return;
  }

  // The walker keeps its state in memory. Journal the request in the inode
  // so a new rank, or rank 0 after a migration, picks it up again.
  auto pi = in->project_inode(mdr, true);
  pi.inode->version = in->pre_dirty();
  pi.inode->change_attr++;
  pi.inode->xattr_version++;
  (*pi.xattrs)[mempool::mds_co::string(RMSNAPS_XATTR)] = ceph::buffer::copy("1", 1);

  mdr->ls = mds->mdlog->get_current_segment();
  EUpdate *le = new EUpdate(mds->mdlog, "trash rmsnaps");
  cache()->predirty_journal_parents(mdr, &le->metablob, in, 0, PREDIRTY_PRIMARY);
  cache()->journal_dirty_inode(mdr.get(), &le->metablob, in);
  mds->mdlog->submit_entry(le, new C_TrashMarkLogged(this, mdr, in));
  mds->mdlog->flush();
}

void TrashManager::rmsnaps_marked(inodeno_t root_ino)
{
  auto it = roots.find(root_ino);
  if (it != roots.end())
    it->second.backoff_until = utime_t();
  kick();
}

void TrashManager::dispatch_recall(const boost::intrusive_ptr<MDRequestImpl>& mdr)
{
  inodeno_t dirino = mdr->get_filepath().get_ino();
  CInode *diri = cache()->get_inode(dirino);
  if (!diri || !diri->is_dir()) {
    mds->server->respond_to_request(mdr, -ENOENT);
    return;
  }
  if (!(mdr->locking_state & MutationImpl::ALL_LOCKED)) {
    MutationImpl::LockOpVec lov;
    lov.add_wrlock(&diri->filelock);
    lov.add_wrlock(&diri->nestlock);
    if (!mds->locker->acquire_locks(mdr, lov))
      return;
    // Leases on cached dentries, so clients drop names before we unlink them.
    // The filelock wrlock gathers directory caps (readdir / async dirop caches).
    for (CDir *dir : diri->get_dirfrags()) {
      for (auto it = dir->begin(); it != dir->end(); ++it) {
        if (it->first.snapid != CEPH_NOSNAP)
          continue;
        mds->locker->revoke_client_leases(&it->second->lock);
      }
    }
    mdr->locking_state |= MutationImpl::ALL_LOCKED;
  }
  mds->server->respond_to_request(mdr, 0);
}

bool TrashManager::send_request(int op, CDentry *dn, mds_rank_t to, inodeno_t root_ino,
                                inodeno_t entry_ino, bool migration, MDSContext *fin,
                                inodeno_t dest_dir)
{
  if (!dn || !mds->mdsmap->is_up(to)) {
    if (fin)
      fin->complete(-EAGAIN);
    return false;
  }
  ceph_tid_t tid = mds->issue_tid();
  auto msg = ceph::make_message<MClientRequest>(op);
  msg->set_tid(tid);
  if (op == CEPH_MDS_OP_RENAME) {
    msg->set_filepath2(filepath(dn->get_name(), dn->get_dir()->ino()));
    msg->set_filepath(filepath(std::string(dn->get_name()), dest_dir));
  } else {
    msg->set_filepath(filepath(dn->get_name(), dn->get_dir()->ino()));
  }

  return send_tracked(msg, dn, to, root_ino, entry_ino, migration, fin);
}

bool TrashManager::send_tracked(const ceph::ref_t<MClientRequest>& msg, CDentry *dn,
                                mds_rank_t to, inodeno_t root_ino,
                                inodeno_t entry_ino, bool migration, MDSContext *fin)
{
  int op = msg->get_op();
  ceph_tid_t tid = msg->get_tid();
  auto tracked = std::make_unique<TrashMetaRequest>(op, tid, to, dn, root_ino,
                                                    entry_ino, fin, migration);
  mds->internal_client_requests.emplace(tid, std::move(tracked));
  int r = mds->send_message_mds(msg, to);
  if (r != 0) {
    auto it = mds->internal_client_requests.find(tid);
    if (it != mds->internal_client_requests.end()) {
      auto owned = std::move(it->second);
      mds->internal_client_requests.erase(it);
      handle_reply(static_cast<TrashMetaRequest*>(owned.get()),
                   r > 0 ? -r : r);
    }
    return false;
  }
  auto *req = static_cast<TrashMetaRequest*>(mds->internal_client_requests[tid].get());
  req->counts_inflight = true;
  global_inflight++;
  dout(10) << "sent " << ceph_mds_op_name(op) << " " << msg->get_filepath()
           << " to mds." << to << " tid " << tid << dendl;
  return true;
}

bool TrashManager::submit_rmsnap(Root &root, CInode *diri)
{
  // One snapshot per request. The next pass finds the one after it.
  const auto& snaps = diri->snaprealm->srnode.snaps;
  ceph_assert(!snaps.empty());
  mds_rank_t to = diri->authority().first;
  if (throttled() || !mds->mdsmap->is_up(to))
    return false;
  auto msg = ceph::make_message<MClientRequest>(CEPH_MDS_OP_RMSNAP);
  msg->set_tid(mds->issue_tid());
  msg->set_filepath(filepath(snaps.begin()->second.name, diri->ino()));
  dout(5) << "removing snapshot " << snaps.begin()->second.name << " of " << *diri
          << " for root " << root.ino << dendl;
  if (!send_tracked(msg, nullptr, to, root.ino, diri->ino(), false, nullptr))
    return false;
  root.inflight++;
  return true;
}

bool TrashManager::submit_purge(Root &root, CDentry *dn, int op)
{
  if (throttled() || dn->is_projected())
    return false;
  mds_rank_t to = dn->authority().first;
  inodeno_t entry;
  auto *dnl = dn->get_projected_linkage();
  if (dnl->is_primary())
    entry = dnl->get_inode()->ino();
  if (!send_request(op, dn, to, root.ino, entry, false, nullptr))
    return false;
  root.inflight++;
  return true;
}

void TrashManager::finish_frame(Root &root, bool removed, bool &erase_root)
{
  if (root.stack.empty())
    return;
  Frame fr = std::move(root.stack.back());
  root.stack.pop_back();
  if (fr.ino != root.ino) {
    if (CInode *in = cache()->get_inode(fr.ino))
      in->put(CInode::PIN_TRASH);
  }
  if (!root.stack.empty())
    return;
  if (removed) {
    erase_root = true;
    return;
  }
  root.state = State::DONE;
  dout(10) << "root " << root.ino << " has no locally removable entries" << dendl;
}

TrashManager::Step TrashManager::advance_root(Root &root, int &budget)
{
  if (root.state == State::DONE)
    return Step::Wait;
  if (root.waiting)
    return Step::Wait;
  if (root.backoff_until != utime_t() && ceph_clock_now() < root.backoff_until)
    return Step::Wait;

  bool rmsnaps = wants_rmsnaps(root.ino);
  if (root.state == State::PARKED) {
    CInode *in = cache()->get_inode(root.ino);
    if (in && has_snaps(in) && !rmsnaps)
      return Step::Wait;
    dout(10) << "unparking " << root.ino << dendl;
    root.state = State::ACTIVE;
    root.parked_since = utime_t();
    rewind_top(root);
  }

  if (root.drain) {
    if (root.inflight)
      return Step::Wait;
    root.drain = false;
  }

  // Unlinks of the files in one directory run side by side. Everything
  // else, from here to the scan and after it, waits for them to finish.
  if (root.stack.empty()) {
    root.stack.emplace_back();
    root.stack.back().ino = root.ino;
  }

  Frame *frame = &root.stack.back();
  CInode *in = cache()->get_inode(frame->ino);
  if (!in) {
    root.waiting = true;
    cache()->open_ino(frame->ino, -1, new C_TrashWait(this, root.ino));
    return Step::Wait;
  }
  if (!in->is_dir()) {
    if (root.inflight)
      return Step::Wait;
    bool erase = false;
    finish_frame(root, true, erase);
    return erase ? Step::Erase : Step::Again;
  }
  // Two walks can cover one directory: a walk that starts at a subtree, and
  // the walk above it once this rank imports the directory in between. When
  // the other walk has removed this directory it is in a stray directory, and
  // its parent dentry is the stray dentry. That is not a name to rmdir.
  if (CDentry *pdn = in->get_projected_parent_dn();
      pdn && pdn->get_dir()->get_inode()->is_stray()) {
    if (root.inflight)
      return Step::Wait;
    dout(10) << "root " << root.ino << ": " << *in << " is removed already" << dendl;
    bool erase = false;
    finish_frame(root, true, erase);
    return erase ? Step::Erase : Step::Again;
  }
  if (has_snaps(in)) {
    if (!rmsnaps) {
      park(root);
      return Step::Wait;
    }
    // The count that comes up from below does not say where the snapshot
    // is. Remove this directory's own and walk on to find the others.
    if (in->is_auth() && in->snaprealm && !in->snaprealm->srnode.snaps.empty()) {
      if (root.inflight)
        return Step::Wait;
      if (!submit_rmsnap(root, in))
        backoff(root);
      return Step::Wait;
    }
  }

  if (frame->recalling) {
    if (!frame->recall_req.name.is_mds() || !cache()->have_request(frame->recall_req))
      frame->recalling = false;
    else
      return Step::Wait;
  }
  if (!frame->caps_recalled) {
    if (root.inflight)
      return Step::Wait;
    bool want = in->is_auth();
    if (!want) {
      for (CDir *dir : in->get_dirfrags()) {
        if (dir->is_auth()) {
          want = true;
          break;
        }
      }
    }
    if (want) {
      begin_recall(root, *frame, in);
      return Step::Wait;
    }
    frame->caps_recalled = true;
  }

  if (!frame->frags_ready) {
    frag_vec_t leaves;
    in->dirfragtree.get_leaves(leaves);
    frame->frags.assign(leaves.begin(), leaves.end());
    frame->frag_i = 0;
    frame->has_cursor = false;
    frame->frags_ready = true;
    frame->skipped_remote = false;
  }

  while (frame->frag_i < frame->frags.size()) {
    if (budget <= 0)
      return Step::Again;
    frag_t fg = frame->frags[frame->frag_i];
    if (!in->dirfragtree.is_leaf(fg)) {
      // A split or merge that was under way when the directory came to trash
      // has finished since the fragments were listed. Opening the old
      // fragment would read its object again and load every dentry twice.
      dout(10) << "root " << root.ino << ": " << fg << " is not a fragment of "
               << *in << " any more, listing them again" << dendl;
      frame->frags_ready = false;
      --budget;
      return Step::Again;
    }
    CDir *dir = in->get_dirfrag(fg);
    if (!dir) {
      if (!in->is_auth()) {
        frame->skipped_remote = true;
        frame->frag_i++;
        frame->has_cursor = false;
        continue;
      }
      dir = in->get_or_open_dirfrag(cache(), fg);
    }
    if (!dir->is_auth()) {
      frame->skipped_remote = true;
      frame->frag_i++;
      frame->has_cursor = false;
      continue;
    }
    if (dir->is_frozen() || dir->is_freezing()) {
      root.waiting = true;
      dir->add_waiter(CDir::WAIT_UNFREEZE, new C_TrashWait(this, root.ino));
      return Step::Wait;
    }
    if (!dir->is_complete()) {
      root.waiting = true;
      dir->fetch(new C_TrashWait(this, root.ino));
      return Step::Wait;
    }

    auto it = frame->has_cursor
      ? dir->upper_bound(dentry_key_t(frame->cursor_snap, frame->cursor_name, frame->cursor_hash))
      : dir->begin();
    for (; it != dir->end(); ++it) {
      if (it->first.snapid != CEPH_NOSNAP)
        continue;
      CDentry *dn = it->second;
      auto *dnl = dn->get_projected_linkage();
      if (dnl->is_null())
        continue;
      if (--budget < 0)
        return Step::Again;

      if (dnl->is_primary() && dnl->get_inode()->is_dir()) {
        if (root.inflight)
          return Step::Wait;
        frame->has_cursor = true;
        frame->cursor_name = std::string(dn->get_name());
        frame->cursor_hash = it->first.hash;
        frame->cursor_snap = it->first.snapid;
        CInode *child = dnl->get_inode();
        child->get(CInode::PIN_TRASH);
        Frame child_frame;
        child_frame.ino = child->ino();
        root.stack.push_back(std::move(child_frame));
        return Step::Again;
      }

      if ((uint64_t)root.inflight >= root_inflight_max ||
          throttled() || dn->is_projected())
        return Step::Wait;
      if (!submit_purge(root, dn, CEPH_MDS_OP_UNLINK)) {
        backoff(root);
        return Step::Wait;
      }
      // Sent. The cursor moves past it and the scan goes on to the next file.
      frame->has_cursor = true;
      frame->cursor_name = std::string(dn->get_name());
      frame->cursor_hash = it->first.hash;
      frame->cursor_snap = it->first.snapid;
    }
    frame->frag_i++;
    frame->has_cursor = false;
  }

  // Every head dentry in this directory's auth frags is gone or has an
  // unlink on its way.
  if (root.inflight)
    return Step::Wait;

  CDentry *pdn = in->get_parent_dn();
  bool top = root.stack.size() == 1;
  bool can_rmdir = false;
  if (pdn && pdn->is_auth() && pdn->last == CEPH_NOSNAP) {
    if (top)
      can_rmdir = root.remove_when_empty;
    else
      can_rmdir = true;
  }
  if (can_rmdir) {
    if (throttled() || pdn->is_projected())
      return Step::Wait;
    if (!submit_purge(root, pdn, CEPH_MDS_OP_RMDIR)) {
      backoff(root);
      return Step::Wait;
    }
    return Step::Wait;
  }

  bool erase = false;
  finish_frame(root, false, erase);
  return erase ? Step::Erase : Step::Again;
}

void TrashManager::on_walker_reply(inodeno_t root_ino, int op, int result)
{
  auto it = roots.find(root_ino);
  if (it == roots.end())
    return;
  Root &root = it->second;
  if (root.inflight > 0)
    root.inflight--;
  if (result == 0 || result == -ENOENT) {
    root.backoff = 1;
    if (op == CEPH_MDS_OP_RMSNAP)
      return;
    // ENOENT: another walk removed it, and counted it.
    if (result == 0 && cache()->logger) {
      cache()->logger->inc(op == CEPH_MDS_OP_RMDIR ? l_mdc_trash_dirs
                                                   : l_mdc_trash_files);
    }
    if (op == CEPH_MDS_OP_RMDIR) {
      bool erase = false;
      finish_frame(root, true, erase);
      if (erase) {
        drop_pins(root);
        roots.erase(it);
      }
    }
    return;
  }
  dout(10) << "root " << root_ino << " " << ceph_mds_op_name(op)
           << " -> " << cpp_strerror(result) << dendl;
  if (result == -ENOTEMPTY) {
    CInode *in = root.stack.empty() ? nullptr : cache()->get_inode(root.stack.back().ino);
    if (in && has_snaps(in) && !wants_rmsnaps(root.ino))
      park(root);
    else {
      rewind_top(root);
      backoff(root);
    }
    return;
  }
  rewind_top(root);
  backoff(root);
}

void TrashManager::handle_reply(TrashMetaRequest *req, int result)
{
  if (req->counts_inflight && global_inflight > 0)
    global_inflight--;
  req->counts_inflight = false;
  if (req->migration)
    migrating.erase(req->entry_ino);
  if (req->root_ino)
    on_walker_reply(req->root_ino, req->get_op(), result);
  if (req->fin) {
    MDSContext *fin = req->fin;
    req->fin = nullptr;
    fin->complete(result);
  }
  if (!shutdown_stopped)
    kick();
}

void TrashManager::drop_requests_to(mds_rank_t who)
{
  std::vector<std::unique_ptr<MDSMetaRequest>> dropped;
  auto &reqs = mds->internal_client_requests;
  for (auto it = reqs.begin(); it != reqs.end(); ) {
    auto *t = dynamic_cast<TrashMetaRequest*>(it->second.get());
    if (!t || t->get_target() != who) {
      ++it;
      continue;
    }
    dout(5) << "dropping trash request tid " << it->first << " to mds." << who << dendl;
    dropped.push_back(std::move(it->second));
    it = reqs.erase(it);
  }
  if (who == 0)
    peer_trash_ino = inodeno_t();
  for (auto &owned : dropped)
    handle_reply(static_cast<TrashMetaRequest*>(owned.get()), -EAGAIN);
}

void TrashManager::add_subtree_witnesses(const boost::intrusive_ptr<MDRequestImpl>& mdr,
                                        CInode *srci)
{
  std::vector<CDir*> subs;
  cache()->get_subtrees(subs);
  for (CDir *dir : subs) {
    if (dir->is_auth())
      continue;
    if (dir->inode != srci && !srci->is_ancestor_of(dir->inode))
      continue;
    mds_rank_t auth = dir->authority().first;
    if (auth == mds->get_nodeid() || auth < 0)
      continue;
    if (!mds->mdsmap->is_clientreplay_or_active_or_stopping(auth))
      continue;
    mdr->more()->extra_witnesses.insert(auth);
    dout(10) << "rmtree witness mds." << auth << " for " << *dir << dendl;
  }
}

void TrashManager::move_to_trash(CInode *in, MDSContext *fin)
{
  if (!in || !in->is_dir()) {
    if (fin)
      fin->complete(-EINVAL);
    return;
  }
  if (in->is_under_trash()) {
    if (fin)
      fin->complete(0);
    return;
  }
  CDentry *dn = in->get_parent_dn();
  if (!dn) {
    if (fin)
      fin->complete(-EINVAL);
    return;
  }
  mds_rank_t to = dn->authority().first;
  if (!mds->mdsmap->is_up(to)) {
    if (fin)
      fin->complete(-EAGAIN);
    return;
  }
  dout(10) << "move_to_trash " << *in << " via mds." << to << dendl;
  send_request(CEPH_MDS_OP_RMTREE, dn, to, inodeno_t(), in->ino(), false, fin);
}

void TrashManager::start_discover_peer()
{
  if (discovering_peer || peer_trash_ino || mds->get_nodeid() == 0)
    return;
  if (peer_discover_after != utime_t() && ceph_clock_now() < peer_discover_after)
    return;
  if (!mds->mdsmap->is_active(mds_rank_t(0)))
    return;
  discovering_peer = true;
  discover_peer_step();
}

void TrashManager::discover_peer_step()
{
  filepath path(TRASH_NAME, MDS_INO_MDSDIR(0));
  std::vector<CDentry*> trace;
  TrashDiscoverFactory factory(this);
  MDRequestRef none;
  // No MDRequest is available to drive a replica dentry rdlock. The trash
  // dentry stays LOCK'd on rank 0, so a replica can_read() is false and
  // path_traverse would wait for a transition nobody requests. The name is
  // stable; read the linkage that discover already delivered.
  int r = cache()->path_traverse(none, factory, path,
                                 MDS_TRAVERSE_DISCOVER | MDS_TRAVERSE_WANT_DENTRY |
                                 MDS_TRAVERSE_PATH_LOCKED,
                                 &trace);
  if (r > 0)
    return;
  discovering_peer = false;
  if (r == 0 && !trace.empty()) {
    auto *dnl = trace.back()->get_projected_linkage();
    if (dnl->is_primary() && dnl->get_inode()) {
      peer_trash_ino = dnl->get_inode()->ino();
      dout(10) << "rank 0 trash inode is " << peer_trash_ino << dendl;
      return;
    }
  }
  dout(5) << "rank 0 trash directory is not available yet (" << cpp_strerror(r) << ")" << dendl;
  if (!peer_missing_logged) {
    mds->clog->warn() << "mds." << mds->get_nodeid()
                      << " cannot migrate trash to rank 0; keeping the metadata until it can";
    peer_missing_logged = true;
  }
  peer_discover_after = ceph_clock_now() + utime_t(5, 0);
}

void TrashManager::migrate_some()
{
  if (!peer_trash_ino)
    return;
  if (migrate_backoff_until != utime_t() && ceph_clock_now() < migrate_backoff_until)
    return;
  if (!mds->mdsmap->is_active(mds_rank_t(0)))
    return;
  CInode *trash = cache()->trash_inode;
  if (!trash)
    return;
  frag_vec_t leaves;
  trash->dirfragtree.get_leaves(leaves);
  int budget = 8;
  for (auto fg : leaves) {
    CDir *dir = trash->get_dirfrag(fg);
    if (!dir || !dir->is_auth())
      continue;
    if (!dir->is_complete()) {
      dir->fetch(new C_TrashNotify(this));
      return;
    }
    for (auto it = dir->begin(); it != dir->end() && budget > 0; ++it) {
      if (it->first.snapid != CEPH_NOSNAP)
        continue;
      CDentry *dn = it->second;
      auto *dnl = dn->get_projected_linkage();
      if (dnl->is_null())
        continue;
      inodeno_t entry = dnl->is_primary() ? dnl->get_inode()->ino() : inodeno_t();
      if (entry && migrating.count(entry))
        continue;
      if (throttled() || dn->is_projected())
        return;
      if (!send_request(CEPH_MDS_OP_RENAME, dn, mds_rank_t(0), inodeno_t(), entry,
                        true, nullptr, peer_trash_ino)) {
        migrate_backoff_until = ceph_clock_now() + utime_t(2, 0);
        return;
      }
      if (entry)
        migrating.insert(entry);
      budget--;
    }
  }
}

void TrashManager::note_cache_released()
{
  // The pin drop is what lets the cache trim. Shutdown still has to cap
  // the log and come back for another pass; that pass must not look at the
  // trash directory again.
  cache_released = true;
  shutdown_stopped = true;
}

bool TrashManager::shutdown_pass()
{
  auto done = [this]() {
    if (cache_released)
      return true;
    bool incomplete = false;
    int entries = count_entries(incomplete);
    return !incomplete && entries == 0 && migrating.empty() && global_inflight == 0;
  };

  if (!shutdown_stopped) {
    for (auto &p : roots)
      drop_pins(p.second);
    roots.clear();
    shutdown_stopped = true;
    dout(5) << "stopped local trash purge for shutdown" << dendl;
  }

  if (mds->get_nodeid() == 0) {
    // Rank 0 has no rank to hand its trash to, and a large or parked tree
    // must not keep the file system from going down. Leave the entries on
    // disk, as strays are left. The walker is rebuilt from the trash
    // directory when the rank is next active. Only wait for the requests
    // that were already sent.
    return cache_released || global_inflight == 0;
  }

  // Nothing local to migrate. Looking up ~mds0/trash must not hold the rank
  // in up:stopping, including while that lookup waits on a replica lock.
  if (done())
    return true;
  start_discover_peer();
  if (discovering_peer)
    return false;
  // An older rank 0 has no trash directory. Keep retrying and do not drop
  // the tree.
  if (!peer_trash_ino)
    return false;
  migrate_some();
  return done();
}

int TrashManager::count_entries(bool &incomplete)
{
  incomplete = false;
  CInode *trash = cache()->trash_inode;
  if (!trash) {
    incomplete = true;
    return 0;
  }
  int entries = 0;
  frag_vec_t leaves;
  trash->dirfragtree.get_leaves(leaves);
  for (auto fg : leaves) {
    CDir *dir = trash->get_dirfrag(fg);
    if (!dir || !dir->is_complete()) {
      incomplete = true;
      continue;
    }
    for (auto it = dir->begin(); it != dir->end(); ++it) {
      if (it->first.snapid != CEPH_NOSNAP)
        continue;
      if (!it->second->get_projected_linkage()->is_null())
        entries++;
    }
  }
  return entries;
}

void TrashManager::dump(ceph::Formatter *f)
{
  bool incomplete = false;
  int entries = count_entries(incomplete);
  f->open_object_section("trash");
  f->dump_bool("paused", paused || g_conf().get_val<bool>("mds_trash_pause"));
  f->dump_int("entries", entries);
  f->dump_bool("incomplete", incomplete);
  f->dump_int("inflight", global_inflight);
  f->open_array_section("roots");
  for (auto &p : roots) {
    f->open_object_section("root");
    f->dump_unsigned("ino", p.first.val);
    f->dump_unsigned("entry", p.second.entry.val);
    const char *state = "active";
    if (p.second.state == State::PARKED)
      state = "parked";
    else if (p.second.state == State::DONE)
      state = "done";
    f->dump_string("state", state);
    f->dump_string("prior_path", p.second.prior_path);
    f->dump_bool("remove_when_empty", p.second.remove_when_empty);
    f->dump_bool("rmsnaps", wants_rmsnaps(p.first));
    f->dump_int("depth", p.second.stack.size());
    f->dump_int("inflight", p.second.inflight);
    f->close_section();
  }
  f->close_section();
  f->close_section();
}

void TrashManager::gather_health(std::vector<MDSHealthMetric>& metrics) const
{
  double after = g_conf().get_val<double>("mds_trash_parked_warn_after");
  utime_t now = ceph_clock_now();
  int parked = 0;
  for (auto &p : roots) {
    if (p.second.state != State::PARKED || p.second.parked_since == utime_t())
      continue;
    if (double(now - p.second.parked_since) < after)
      continue;
    parked++;
  }
  if (!parked)
    return;
  CachedStackStringStream css;
  *css << parked << " trash " << (parked == 1 ? "tree has" : "trees have")
       << " snapshots and cannot be purged on mds." << mds->get_nodeid();
  MDSHealthMetric m(MDS_HEALTH_TRASH_PARKED, HEALTH_WARN, css->strv());
  m.metadata["mds"] = stringify(mds->get_nodeid());
  m.metadata["count"] = stringify(parked);
  metrics.push_back(std::move(m));
}
