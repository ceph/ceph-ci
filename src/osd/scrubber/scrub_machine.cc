// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <chrono>
#include <typeinfo>

#include <boost/core/demangle.hpp>

#include "osd/OSD.h"
#include "osd/OpRequest.h"

#include "ScrubStore.h"
#include "scrub_machine.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << " scrubberFSM "

using namespace std::chrono;
using namespace std::chrono_literals;

#define DECLARE_LOCALS                                           \
  auto& machine = context<ScrubMachine>();			 \
  std::ignore = machine;					 \
  ScrubMachineListener* scrbr = machine.m_scrbr;		 \
  std::ignore = scrbr;                                           \
  auto pg_id = machine.m_pg_id;					 \
  std::ignore = pg_id;

NamedSimply::NamedSimply(ScrubMachineListener* scrubber, const char* name)
{
  scrubber->set_state_name(name);
}

namespace Scrub {

// --------- trace/debug auxiliaries -------------------------------

void on_event_creation(std::string_view nm)
{
  dout(20) << " event: --vvvv---- " << nm << dendl;
}

void on_event_discard(std::string_view nm)
{
  dout(20) << " event: --^^^^---- " << nm << dendl;
}

void ScrubMachine::assert_not_active() const
{
  ceph_assert(state_cast<const NotActive*>());
}

bool ScrubMachine::is_reserving() const
{
  return state_cast<const ReservingReplicas*>();
}

bool ScrubMachine::is_accepting_updates() const
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  ceph_assert(scrbr->is_primary());

  return state_cast<const WaitLastUpdate*>();
}

// for the rest of the code in this file - we know what PG we are dealing with:
#undef dout_prefix
#define dout_prefix _prefix(_dout, this->context<ScrubMachine>())

template <class T>
static ostream& _prefix(std::ostream* _dout, T& t)
{
  return t.gen_prefix(*_dout);
}

std::ostream& ScrubMachine::gen_prefix(std::ostream& out) const
{
  return m_scrbr->gen_prefix(out) << "FSM: ";
}

// ////////////// the actual actions

// ----------------------- NotActive -----------------------------------------

NotActive::NotActive(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "NotActive")
{
  dout(10) << "-- state -->> NotActive" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->clear_queued_or_active();
}

sc::result NotActive::react(const StartScrub&)
{
  dout(10) << "NotActive::react(const StartScrub&)" << dendl;
  DECLARE_LOCALS;
  scrbr->set_scrub_begin_time();
  return transit<ReservingReplicas>();
}

sc::result NotActive::react(const AfterRepairScrub&)
{
  dout(10) << "NotActive::react(const AfterRepairScrub&)" << dendl;
  DECLARE_LOCALS;
  scrbr->set_scrub_begin_time();
  return transit<ReservingReplicas>();
}

// ----------------------- ReservingReplicas ---------------------------------

ReservingReplicas::ReservingReplicas(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "ReservingReplicas")
{
  dout(10) << "-- state -->> ReservingReplicas" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  // prevent the OSD from starting another scrub while we are trying to secure
  // replicas resources
  scrbr->set_reserving_now();
  scrbr->reserve_replicas();

  auto timeout = scrbr->get_cct()->_conf.get_val<
    std::chrono::milliseconds>("osd_scrub_reservation_timeout");
timeout = 500ms; // RRR: remove this line, it is for testing only
  if (timeout.count() > 0) {
    // Start a timer to handle case where the replicas take a long time to
    // ack the reservation.  See ReservationTimeout handler below.
    m_timeout_token = machine.schedule_timer_event_after<ReservationTimeout>(
      timeout);
  }
}

ReservingReplicas::~ReservingReplicas()
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->clear_reserving_now();
}

sc::result ReservingReplicas::react(const ReservationTimeout&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReservingReplicas::react(const ReservationTimeout&)" << dendl;

  dout(10)
    << "PgScrubber: " << scrbr->get_spgid()
    << " timeout on reserving replicas (since " << entered_at
    << ")" << dendl;
  scrbr->get_clog()->warn()
    << "osd." << scrbr->get_whoami()
    << " PgScrubber: " << scrbr->get_spgid()
    << " timeout on reserving replicsa (since " << entered_at
    << ")";

  scrbr->on_replica_reservation_timeout();
  return discard_event();
}

sc::result ReservingReplicas::react(const ReservationFailure&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReservingReplicas::react(const ReservationFailure&)" << dendl;

  // the Scrubber must release all resources and abort the scrubbing
  scrbr->clear_pgscrub_state();
  return transit<NotActive>();
}

/**
 * note: the event poster is handling the scrubber reset
 */
sc::result ReservingReplicas::react(const FullReset&)
{
  dout(10) << "ReservingReplicas::react(const FullReset&)" << dendl;
  return transit<NotActive>();
}

// ----------------------- ActiveScrubbing -----------------------------------

ActiveScrubbing::ActiveScrubbing(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "ActiveScrubbing")
{
  dout(10) << "-- state -->> ActiveScrubbing" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->on_init();
}

/**
 *  upon exiting the Active state
 */
ActiveScrubbing::~ActiveScrubbing()
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(15) << __func__ << dendl;
  scrbr->unreserve_replicas();
  scrbr->clear_queued_or_active();
}

/*
 * The only source of an InternalError event as of now is the BuildMap state,
 * when encountering a backend error.
 * We kill the scrub and reset the FSM.
 */
sc::result ActiveScrubbing::react(const InternalError&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << __func__ << dendl;
  scrbr->clear_pgscrub_state();
  return transit<NotActive>();
}

sc::result ActiveScrubbing::react(const FullReset&)
{
  dout(10) << "ActiveScrubbing::react(const FullReset&)" << dendl;
  // caller takes care of clearing the scrubber & FSM states
  return transit<NotActive>();
}

// ----------------------- RangeBlocked -----------------------------------

/*
 * Blocked. Will be released by kick_object_context_blocked() (or upon
 * an abort)
 *
 * Note: we are never expected to be waiting for long for a blocked object.
 * Unfortunately we know from experience that a bug elsewhere might result
 * in an indefinite wait in this state, for an object that is never released.
 * If that happens, all we can do is to issue a warning message to help
 * with the debugging.
 */
RangeBlocked::RangeBlocked(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Act/RangeBlocked")
{
  dout(10) << "-- state -->> Act/RangeBlocked" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  auto grace = scrbr->get_range_blocked_grace();
  if (grace == ceph::timespan{}) {
    // we will not be sending any alarms re the blocked object
    dout(10)
      << __func__
      << ": blocked-alarm disabled ('osd_blocked_scrub_grace_period' set to 0)"
      << dendl;
  } else {
    // Schedule an event to warn that the pg has been blocked for longer than
    // the timeout, see RangeBlockedAlarm handler below
    dout(20) << fmt::format(": timeout:{}",
			    std::chrono::duration_cast<seconds>(grace))
	     << dendl;

    m_timeout_token = machine.schedule_timer_event_after<RangeBlockedAlarm>(
      grace);
  }
}

sc::result RangeBlocked::react(const RangeBlockedAlarm&)
{
  DECLARE_LOCALS;
  char buf[50];
  std::time_t now_c = ceph::coarse_real_clock::to_time_t(entered_at);
  strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", std::localtime(&now_c));
  dout(10)
    << "PgScrubber: " << scrbr->get_spgid()
    << " blocked on an object for too long (since " << buf << ")" << dendl;
  scrbr->get_clog()->warn()
    << "osd." << scrbr->get_whoami()
    << " PgScrubber: " << scrbr->get_spgid()
    << " blocked on an object for too long (since " << buf
    << ")";

  scrbr->set_scrub_blocked(utime_t{now_c, 0});
  return discard_event();
}

// ----------------------- PendingTimer -----------------------------------

/**
 *  Sleeping till timer reactivation - or just requeuing
 */
PendingTimer::PendingTimer(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Act/PendingTimer")
{
  dout(10) << "-- state -->> Act/PendingTimer" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  auto sleep_time = scrbr->get_scrub_sleep_time();
  if (sleep_time.count()) {
    // the following log line is used by osd-scrub-test.sh
    dout(20) << __func__ << " scrub state is PendingTimer, sleeping" << dendl;

    dout(20) << "PgScrubber: " << scrbr->get_spgid()
	     << " sleeping for " << sleep_time << dendl;
    m_sleep_timer = machine.schedule_timer_event_after<SleepComplete>(
      sleep_time);
  } else {
    scrbr->queue_for_scrub_resched(Scrub::scrub_prio_t::high_priority);
  }
}

sc::result PendingTimer::react(const SleepComplete&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "PendingTimer::react(const SleepComplete&)" << dendl;

  auto slept_for = ceph::coarse_real_clock::now() - entered_at;
  dout(20) << "PgScrubber: " << scrbr->get_spgid()
	   << " slept for " << slept_for << dendl;

  scrbr->queue_for_scrub_resched(Scrub::scrub_prio_t::low_priority);
  return discard_event();
}

// ----------------------- NewChunk -----------------------------------

/**
 *  Preconditions:
 *  - preemption data was set
 *  - epoch start was updated
 */
NewChunk::NewChunk(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Act/NewChunk")
{
  dout(10) << "-- state -->> Act/NewChunk" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  scrbr->get_preemptor().adjust_parameters();

  //  choose range to work on
  //  select_range_n_notify() will signal either SelectedChunkFree or
  //  ChunkIsBusy. If 'busy', we transition to Blocked, and wait for the
  //  range to become available.
  scrbr->select_range_n_notify();
}

sc::result NewChunk::react(const SelectedChunkFree&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "NewChunk::react(const SelectedChunkFree&)" << dendl;

  scrbr->set_subset_last_update(scrbr->search_log_for_updates());
  return transit<WaitPushes>();
}

// ----------------------- WaitPushes -----------------------------------

WaitPushes::WaitPushes(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Act/WaitPushes")
{
  dout(10) << " -- state -->> Act/WaitPushes" << dendl;
  post_event(ActivePushesUpd{});
}

/*
 * Triggered externally, by the entity that had an update re pushes
 */
sc::result WaitPushes::react(const ActivePushesUpd&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10)
    << "WaitPushes::react(const ActivePushesUpd&) pending_active_pushes: "
    << scrbr->pending_active_pushes() << dendl;

  if (!scrbr->pending_active_pushes()) {
    // done waiting
    return transit<WaitLastUpdate>();
  }

  return discard_event();
}

// ----------------------- WaitLastUpdate -----------------------------------

WaitLastUpdate::WaitLastUpdate(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Act/WaitLastUpdate")
{
  dout(10) << " -- state -->> Act/WaitLastUpdate" << dendl;
  post_event(UpdatesApplied{});
}

/**
 *  Note:
 *  Updates are locally readable immediately. Thus, on the replicas we do need
 *  to wait for the update notifications before scrubbing. For the Primary it's
 *  a bit different: on EC (and only there) rmw operations have an additional
 *  read roundtrip. That means that on the Primary we need to wait for
 *  last_update_applied (the replica side, even on EC, is still safe
 *  since the actual transaction will already be readable by commit time.
 */
void WaitLastUpdate::on_new_updates(const UpdatesApplied&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitLastUpdate::on_new_updates(const UpdatesApplied&)" << dendl;

  if (scrbr->has_pg_marked_new_updates()) {
    post_event(InternalAllUpdates{});
  } else {
    // will be requeued by op_applied
    dout(10) << "wait for EC read/modify/writes to queue" << dendl;
  }
}

/*
 *  request maps from the replicas in the acting set
 */
sc::result WaitLastUpdate::react(const InternalAllUpdates&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitLastUpdate::react(const InternalAllUpdates&)" << dendl;

  scrbr->get_replicas_maps(scrbr->get_preemptor().is_preemptable());
  return transit<BuildMap>();
}

// ----------------------- BuildMap -----------------------------------

BuildMap::BuildMap(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Act/BuildMap")
{
  dout(10) << " -- state -->> Act/BuildMap" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  // no need to check for an epoch change, as all possible flows that brought
  // us here have a check_interval() verification of their final event.

  if (scrbr->get_preemptor().was_preempted()) {

    // we were preempted, either directly or by a replica
    dout(10) << __func__ << " preempted!!!" << dendl;
    scrbr->mark_local_map_ready();
    post_event(IntBmPreempted{});

  } else {

    auto ret = scrbr->build_primary_map_chunk();

    if (ret == -EINPROGRESS) {
      // must wait for the backend to finish. No specific event provided.
      // build_primary_map_chunk() has already requeued us.
      dout(20) << "waiting for the backend..." << dendl;

    } else if (ret < 0) {

      dout(10) << "BuildMap::BuildMap() Error! Aborting. Ret: " << ret << dendl;
      post_event(InternalError{});

    } else {

      // the local map was created
      post_event(IntLocalMapDone{});
    }
  }
}

sc::result BuildMap::react(const IntLocalMapDone&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "BuildMap::react(const IntLocalMapDone&)" << dendl;

  scrbr->mark_local_map_ready();
  return transit<WaitReplicas>();
}

// ----------------------- DrainReplMaps -----------------------------------

DrainReplMaps::DrainReplMaps(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Act/DrainReplMaps")
{
  dout(10) << "-- state -->> Act/DrainReplMaps" << dendl;
  // we may have got all maps already. Send the event that will make us check.
  post_event(GotReplicas{});
}

sc::result DrainReplMaps::react(const GotReplicas&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "DrainReplMaps::react(const GotReplicas&)" << dendl;

  if (scrbr->are_all_maps_available()) {
    // NewChunk will handle the preemption that brought us to this state
    return transit<PendingTimer>();
  }

  dout(15) << "DrainReplMaps::react(const GotReplicas&): still draining "
	      "incoming maps: "
	   << scrbr->dump_awaited_maps() << dendl;
  return discard_event();
}

// ----------------------- WaitReplicas -----------------------------------

WaitReplicas::WaitReplicas(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Act/WaitReplicas")
{
  dout(10) << "-- state -->> Act/WaitReplicas" << dendl;
  post_event(GotReplicas{});
}

/**
 * note: now that maps_compare_n_cleanup() is "futurized"(*), and we remain in
 * this state for a while even after we got all our maps, we must prevent
 * are_all_maps_available() (actually - the code after the if()) from being
 * called more than once.
 * This is basically a separate state, but it's too transitory and artificial
 * to justify the cost of a separate state.

 * (*) "futurized" - in Crimson, the call to maps_compare_n_cleanup() returns
 * immediately after initiating the process. The actual termination of the
 * maps comparing etc' is signalled via an event. As we share the code with
 * "classic" OSD, here too maps_compare_n_cleanup() is responsible for
 * signalling the completion of the processing.
 */
sc::result WaitReplicas::react(const GotReplicas&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitReplicas::react(const GotReplicas&)" << dendl;

  if (!all_maps_already_called && scrbr->are_all_maps_available()) {
    dout(10) << "WaitReplicas::react(const GotReplicas&) got all" << dendl;

    all_maps_already_called = true;

    // were we preempted?
    if (scrbr->get_preemptor().disable_and_test()) {  // a test&set


      dout(10) << "WaitReplicas::react(const GotReplicas&) PREEMPTED!" << dendl;
      return transit<PendingTimer>();

    } else {
      scrbr->maps_compare_n_cleanup();
      return transit<WaitDigestUpdate>();
    }
  } else {
    return discard_event();
  }
}

sc::result WaitReplicas::react(const DigestUpdate&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  auto warn_msg =
    "WaitReplicas::react(const DigestUpdate&): Unexpected DigestUpdate event"s;
  dout(10) << warn_msg << dendl;
  scrbr->log_cluster_warning(warn_msg);
  return discard_event();
}

// ----------------------- WaitDigestUpdate -----------------------------------

WaitDigestUpdate::WaitDigestUpdate(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "Act/WaitDigestUpdate")
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "-- state -->> Act/WaitDigestUpdate" << dendl;

  // perform an initial check: maybe we already
  // have all the updates we need:
  // (note that DigestUpdate is usually an external event)
  post_event(DigestUpdate{});
}

sc::result WaitDigestUpdate::react(const DigestUpdate&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitDigestUpdate::react(const DigestUpdate&)" << dendl;

  // on_digest_updates() will either:
  // - do nothing - if we are still waiting for updates, or
  // - finish the scrubbing of the current chunk, and:
  //  - send NextChunk, or
  //  - send ScrubFinished
  scrbr->on_digest_updates();
  return discard_event();
}

sc::result WaitDigestUpdate::react(const ScrubFinished&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "WaitDigestUpdate::react(const ScrubFinished&)" << dendl;
  scrbr->set_scrub_duration();
  scrbr->scrub_finish();
  return transit<NotActive>();
}

ScrubMachine::ScrubMachine(PG* pg, ScrubMachineListener* pg_scrub)
    : m_pg_id{pg->pg_id}
    , m_scrbr{pg_scrub}
{}

ScrubMachine::~ScrubMachine() = default;

// -------- for replicas -----------------------------------------------------

// ----------------------- ReplicaWaitUpdates --------------------------------

ReplicaWaitUpdates::ReplicaWaitUpdates(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "ReplicaWaitUpdates")
{
  dout(10) << "-- state -->> ReplicaWaitUpdates" << dendl;
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->on_replica_init();
}

/*
 * Triggered externally, by the entity that had an update re pushes
 */
sc::result ReplicaWaitUpdates::react(const ReplicaPushesUpd&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ReplicaWaitUpdates::react(const ReplicaPushesUpd&): "
	   << scrbr->pending_active_pushes() << dendl;

  if (scrbr->pending_active_pushes() == 0) {

    // done waiting
    return transit<ActiveReplica>();
  }

  return discard_event();
}

/**
 * the event poster is handling the scrubber reset
 */
sc::result ReplicaWaitUpdates::react(const FullReset&)
{
  dout(10) << "ReplicaWaitUpdates::react(const FullReset&)" << dendl;
  return transit<NotActive>();
}

// ----------------------- ActiveReplica -----------------------------------

ActiveReplica::ActiveReplica(my_context ctx)
    : my_base(ctx)
    , NamedSimply(context<ScrubMachine>().m_scrbr, "ActiveReplica")
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "-- state -->> ActiveReplica" << dendl;
  // and as we might have skipped ReplicaWaitUpdates:
  scrbr->on_replica_init();
  post_event(SchedReplica{});
}

sc::result ActiveReplica::react(const SchedReplica&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  dout(10) << "ActiveReplica::react(const SchedReplica&). is_preemptable? "
	   << scrbr->get_preemptor().is_preemptable() << dendl;

  if (scrbr->get_preemptor().was_preempted()) {
    dout(10) << "replica scrub job preempted" << dendl;

    scrbr->send_preempted_replica();
    scrbr->replica_handling_done();
    return transit<NotActive>();
  }

  // start or check progress of build_replica_map_chunk()
  auto ret_init = scrbr->build_replica_map_chunk();
  if (ret_init != -EINPROGRESS) {
    return transit<NotActive>();
  }

  return discard_event();
}

/**
 * the event poster is handling the scrubber reset
 */
sc::result ActiveReplica::react(const FullReset&)
{
  dout(10) << "ActiveReplica::react(const FullReset&)" << dendl;
  return transit<NotActive>();
}

}  // namespace Scrub
