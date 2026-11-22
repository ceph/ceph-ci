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
  if (in && (in->is_trash() || in->is_under_trash()) && !shutdown_stopped)
    kick();
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

void TrashManager::advance_body()
{
  bool stopping = mds->is_stopping();
  if (!stopping && (paused || g_conf().get_val<bool>("mds_trash_pause")))
    return;
  if (!stopping &&
      mds->mdlog->get_num_segments() > mds->mdlog->get_max_segments()) {
    dout(15) << "journal has " << mds->mdlog->get_num_segments()
             << " segments, waiting" << dendl;
    return;
  }
  if (!stopping && cache()->cache_overfull()) {
    dout(15) << "cache overfull, waiting" << dendl;
    return;
  }

  discover_roots();
  cleanup_done();

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
    Root root;
    root.ino = in->ino();
    root.pinned = true;
    root.remove_when_empty = remove_when_empty;
    root.prior_path = in->get_projected_inode()->stray_prior_path;
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
  if (it->second.prior_path.empty())
    it->second.prior_path = in->get_projected_inode()->stray_prior_path;
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
      if (!dir || !dir->is_auth())
        continue;
      if (!dir->is_complete()) {
        if (!dir->is_frozen())
          dir->fetch(new C_TrashNotify(this));
        continue;
      }
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

  std::vector<CDir*> subs;
  cache()->get_subtrees(subs);
  for (CDir *dir : subs) {
    if (!dir->is_auth() || !dir->inode->is_under_trash())
      continue;
    CInode *top = top_under_trash(dir->inode);
    bool remove = false;
    if (CDentry *pdn = top->get_parent_dn())
      remove = pdn->is_auth() && pdn->get_dir()->inode->is_trash();
    ensure_root(top, remove);
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
                      << " (" << root.ino << ") has snapshots and will not be purged";
  }
  root.backoff_until = ceph_clock_now() + utime_t(60, 0);
}

void TrashManager::rewind_top(Root &root)
{
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
  auto msg = make_message<MClientRequest>(op);
  msg->set_tid(tid);
  if (op == CEPH_MDS_OP_RENAME) {
    msg->set_filepath2(filepath(dn->get_name(), dn->get_dir()->ino()));
    msg->set_filepath(filepath(std::string(dn->get_name()), dest_dir));
  } else {
    msg->set_filepath(filepath(dn->get_name(), dn->get_dir()->ino()));
  }

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
  dout(10) << "sent " << ceph_mds_op_name(op) << " " << *dn << " to mds." << to
           << " tid " << tid << dendl;
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
  if (root.waiting || root.inflight)
    return Step::Wait;
  if (root.backoff_until != utime_t() && ceph_clock_now() < root.backoff_until)
    return Step::Wait;

  if (root.state == State::PARKED) {
    CInode *in = cache()->get_inode(root.ino);
    if (in && has_snaps(in))
      return Step::Wait;
    dout(10) << "unparking " << root.ino << dendl;
    root.state = State::ACTIVE;
    root.parked_since = utime_t();
    rewind_top(root);
  }

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
    bool erase = false;
    finish_frame(root, true, erase);
    return erase ? Step::Erase : Step::Again;
  }
  if (has_snaps(in)) {
    park(root);
    return Step::Wait;
  }

  if (frame->recalling) {
    if (!frame->recall_req.name.is_mds() || !cache()->have_request(frame->recall_req))
      frame->recalling = false;
    else
      return Step::Wait;
  }
  if (!frame->caps_recalled) {
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

      if (throttled() || dn->is_projected())
        return Step::Wait;
      if (!submit_purge(root, dn, CEPH_MDS_OP_UNLINK)) {
        backoff(root);
        return Step::Wait;
      }
      frame->has_cursor = true;
      frame->cursor_name = std::string(dn->get_name());
      frame->cursor_hash = it->first.hash;
      frame->cursor_snap = it->first.snapid;
      return Step::Wait;
    }
    frame->frag_i++;
    frame->has_cursor = false;
  }

  // This directory's auth frags have no remaining head dentries.
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
    if (cache()->logger) {
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
    if (in && has_snaps(in))
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
  int r = cache()->path_traverse(none, factory, path,
                                 MDS_TRAVERSE_DISCOVER | MDS_TRAVERSE_WANT_DENTRY,
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

bool TrashManager::shutdown_pass()
{
  auto done = [this]() {
    bool incomplete = false;
    int entries = count_entries(incomplete);
    return !incomplete && entries == 0 && migrating.empty() && global_inflight == 0;
  };

  if (mds->get_nodeid() == 0) {
    advance();
    if (!done())
      return false;
    for (auto &p : roots) {
      if (p.second.state != State::DONE)
        return false;
    }
    return true;
  }

  if (!shutdown_stopped) {
    for (auto &p : roots)
      drop_pins(p.second);
    roots.clear();
    shutdown_stopped = true;
    dout(5) << "stopped local trash purge for shutdown" << dendl;
  }
  start_discover_peer();
  if (discovering_peer)
    return false;
  // An older rank 0 has no trash directory. Keep retrying and do not drop
  // the tree; an empty local trash can still shut down.
  if (!peer_trash_ino)
    return done();
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
    const char *state = "active";
    if (p.second.state == State::PARKED)
      state = "parked";
    else if (p.second.state == State::DONE)
      state = "done";
    f->dump_string("state", state);
    f->dump_string("prior_path", p.second.prior_path);
    f->dump_bool("remove_when_empty", p.second.remove_when_empty);
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
