#pragma once

#include <seastar/core/future.hh>
#include "crimson/common/errorator.h"
#include "crimson/osd/object_context.h"
#include "crimson/osd/pg_backend.h"

namespace crimson::osd {
class ObjectContextLoader {
public:
  using obc_accessing_list_t = boost::intrusive::list<
    ObjectContext,
    ObjectContext::obc_accessing_option_t>;

  ObjectContextLoader(
    ObjectContextRegistry& _obc_services,
    PGBackend& _backend,
    DoutPrefixProvider& dpp)
    : obc_registry{_obc_services},
      backend{_backend},
      dpp{dpp}
    {}

  using load_obc_ertr = crimson::errorator<
    crimson::ct_error::enoent,
    crimson::ct_error::object_corrupted>;
  using load_obc_iertr =
    ::crimson::interruptible::interruptible_errorator<
      ::crimson::osd::IOInterruptCondition,
      load_obc_ertr>;

  using interruptor = ::crimson::interruptible::interruptor<
    ::crimson::osd::IOInterruptCondition>;

  using with_obc_func_t =
    std::function<load_obc_iertr::future<> (ObjectContextRef, ObjectContextRef)>;

  // Use this variant by default
  // If oid is a clone object, the clone obc *and* it's
  // matching head obc will be locked and can be used in func.
  // resolve_clone: For SnapTrim, it may be possible that it
  //                won't be possible to resolve the clone.
  // See SnapTrimObjSubEvent::remove_or_update - in_removed_snaps_queue usage.
  template<RWState::State State>
  load_obc_iertr::future<> with_obc(hobject_t oid,
                                    with_obc_func_t&& func,
                                    bool resolve_clone = true);

  // Use this variant in the case where the head object
  // obc is already locked and only the clone obc is needed.
  // Avoid nesting with_head_obc() calls by using with_clone_obc()
  // with an already locked head.
  template<RWState::State State>
  load_obc_iertr::future<> with_clone_obc_only(ObjectContextRef head,
                                               hobject_t clone_oid,
                                               with_obc_func_t&& func,
                                               bool resolve_clone = true);

  void notify_on_change(bool is_primary);

private:
  ObjectContextRegistry& obc_registry;
  PGBackend& backend;
  DoutPrefixProvider& dpp;
  obc_accessing_list_t obc_set_accessing;

  template<RWState::State State>
  load_obc_iertr::future<> with_clone_obc(const hobject_t& oid,
                                          with_obc_func_t&& func,
                                          bool resolve_clone);

  template<RWState::State State>
  load_obc_iertr::future<> with_head_obc(const hobject_t& oid,
                                         with_obc_func_t&& func);

  template<RWState::State State, bool track, typename Func>
  load_obc_iertr::future<> with_locked_obc(const hobject_t& oid,
					   Func&& func);

  template<RWState::State State>
  load_obc_iertr::future<ObjectContextRef>
  get_or_load_obc(ObjectContextRef obc,
                  bool existed);

  load_obc_iertr::future<> load_obc(ObjectContextRef obc);
};
}
