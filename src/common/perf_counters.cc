// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 * Copyright (C) 2017 OVH
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/perf_counters.h"
#include "common/perf_counters_key.h"
#include "common/dout.h"
#include "common/valgrind.h"
#include "include/common_fwd.h"

using std::ostringstream;
using std::make_pair;
using std::pair;

namespace TOPNSPC::common {
PerfCountersCollectionImpl::PerfCountersCollectionImpl()
{
}

PerfCountersCollectionImpl::~PerfCountersCollectionImpl()
{
  clear();
}

void PerfCountersCollectionImpl::add(PerfCounters *l)
{
  // make sure the name is unique
  perf_counters_set_t::iterator i;
  i = m_loggers.find(l);
  while (i != m_loggers.end()) {
    ostringstream ss;
    ss << l->get_name() << "-" << (void*)l;
    l->set_name(ss.str());
    i = m_loggers.find(l);
  }

  m_loggers.insert(l);

  for (unsigned int i = 0; i < l->m_data.size(); ++i) {
    PerfCounters::perf_counter_data_any_d &data = l->m_data[i];

    std::string path = l->get_name();
    path += ".";
    path += data.name;

    by_path[path] = {&data, l};
  }
}

void PerfCountersCollectionImpl::remove(PerfCounters *l)
{
  for (unsigned int i = 0; i < l->m_data.size(); ++i) {
    PerfCounters::perf_counter_data_any_d &data = l->m_data[i];

    std::string path = l->get_name();
    path += ".";
    path += data.name;

    by_path.erase(path);
  }

  perf_counters_set_t::iterator i = m_loggers.find(l);
  ceph_assert(i != m_loggers.end());
  m_loggers.erase(i);
}

void PerfCountersCollectionImpl::clear()
{
  perf_counters_set_t::iterator i = m_loggers.begin();
  perf_counters_set_t::iterator i_end = m_loggers.end();
  for (; i != i_end; ) {
    delete *i;
    m_loggers.erase(i++);
  }

  by_path.clear();
}

bool PerfCountersCollectionImpl::reset(const std::string &name)
{
  bool result = false;
  perf_counters_set_t::iterator i = m_loggers.begin();
  perf_counters_set_t::iterator i_end = m_loggers.end();

  if (!strcmp(name.c_str(), "all"))  {
    while (i != i_end) {
      (*i)->reset();
      ++i;
    }
    result = true;
  } else {
    while (i != i_end) {
      if (!name.compare((*i)->get_name())) {
	(*i)->reset();
	result = true;
	break;
      }
      ++i;
    }
  }

  return result;
}


/**
 * Serialize current values of performance counters.  Optionally
 * output the schema instead, or filter output to a particular
 * PerfCounters or particular named counter.
 *
 * @param logger name of subsystem logger, e.g. "mds_cache", may be empty
 * @param counter name of counter within subsystem, e.g. "num_strays",
 *                may be empty.
 * @param schema if true, output schema instead of current data.
 * @param histograms if true, dump histogram values,
 *                   if false dump all non-histogram counters
 */
void PerfCountersCollectionImpl::dump_formatted_generic(
    Formatter *f,
    bool schema,
    bool histograms,
    bool dump_labeled,
    const std::string &logger,
    const std::string &counter) const
{
  f->open_object_section("perfcounter_collection");
  
  if (dump_labeled) {
    std::string prev_key_name;
    for (auto l = m_loggers.begin(); l != m_loggers.end(); ++l) {
      std::string_view key_name = ceph::perf_counters::key_name((*l)->get_name());
      if (key_name != prev_key_name) {
        // close previous set of counters before dumping new one
        if (!prev_key_name.empty()) {
          f->close_section(); // array section
        }
        prev_key_name = key_name;

        f->open_array_section(key_name);
        (*l)->dump_formatted_generic(f, schema, histograms, true, "");
      } else {
        (*l)->dump_formatted_generic(f, schema, histograms, true, "");
      }
    }
    if (!m_loggers.empty()) {
      f->close_section(); // final array section
    }
  } else {
    for (auto l = m_loggers.begin(); l != m_loggers.end(); ++l) {
      // Optionally filter on logger name, pass through counter filter
      if (logger.empty() || (*l)->get_name() == logger) {
        (*l)->dump_formatted_generic(f, schema, histograms, false, counter);
      }
    }
  }
  f->close_section();
}

void PerfCountersCollectionImpl::with_counters(std::function<void(
      const PerfCountersCollectionImpl::CounterMap &)> fn) const
{
  fn(by_path);
}

// ---------------------------

PerfCounters::~PerfCounters()
{
}

void PerfCounters::inc(int idx, uint64_t amt)
{
#ifndef WITH_SEASTAR
  if (!m_cct->_conf->perf)
    return;
#endif

  ceph_assert(idx > m_lower_bound);
  ceph_assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_U64))
    return;
  if (data.type & PERFCOUNTER_LONGRUNAVG) {
    data.avgcount++;
    data.u64 += amt;
    data.avgcount2++;
  } else {
    data.u64 += amt;
  }
}

void PerfCounters::dec(int idx, uint64_t amt)
{
#ifndef WITH_SEASTAR
  if (!m_cct->_conf->perf)
    return;
#endif

  ceph_assert(idx > m_lower_bound);
  ceph_assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  ceph_assert(!(data.type & PERFCOUNTER_LONGRUNAVG));
  if (!(data.type & PERFCOUNTER_U64))
    return;
  data.u64 -= amt;
}

void PerfCounters::set(int idx, uint64_t amt)
{
#ifndef WITH_SEASTAR
  if (!m_cct->_conf->perf)
    return;
#endif

  ceph_assert(idx > m_lower_bound);
  ceph_assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_U64))
    return;

  ANNOTATE_BENIGN_RACE_SIZED(&data.u64, sizeof(data.u64),
                             "perf counter atomic");
  if (data.type & PERFCOUNTER_LONGRUNAVG) {
    data.avgcount++;
    data.u64 = amt;
    data.avgcount2++;
  } else {
    data.u64 = amt;
  }
}

uint64_t PerfCounters::get(int idx) const
{
#ifndef WITH_SEASTAR
  if (!m_cct->_conf->perf)
    return 0;
#endif

  ceph_assert(idx > m_lower_bound);
  ceph_assert(idx < m_upper_bound);
  const perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_U64))
    return 0;
  return data.u64;
}

void PerfCounters::tinc(int idx, utime_t amt)
{
#ifndef WITH_SEASTAR
  if (!m_cct->_conf->perf)
    return;
#endif

  ceph_assert(idx > m_lower_bound);
  ceph_assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_TIME))
    return;
  if (data.type & PERFCOUNTER_LONGRUNAVG) {
    data.avgcount++;
    data.u64 += amt.to_nsec();
    data.avgcount2++;
  } else {
    data.u64 += amt.to_nsec();
  }
}

void PerfCounters::tinc(int idx, ceph::timespan amt)
{
#ifndef WITH_SEASTAR
  if (!m_cct->_conf->perf)
    return;
#endif

  ceph_assert(idx > m_lower_bound);
  ceph_assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_TIME))
    return;
  if (data.type & PERFCOUNTER_LONGRUNAVG) {
    data.avgcount++;
    data.u64 += amt.count();
    data.avgcount2++;
  } else {
    data.u64 += amt.count();
  }
}

void PerfCounters::tset(int idx, utime_t amt)
{
#ifndef WITH_SEASTAR
  if (!m_cct->_conf->perf)
    return;
#endif

  ceph_assert(idx > m_lower_bound);
  ceph_assert(idx < m_upper_bound);
  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_TIME))
    return;
  data.u64 = amt.to_nsec();
  if (data.type & PERFCOUNTER_LONGRUNAVG)
    ceph_abort();
}

utime_t PerfCounters::tget(int idx) const
{
#ifndef WITH_SEASTAR
  if (!m_cct->_conf->perf)
    return utime_t();
#endif

  ceph_assert(idx > m_lower_bound);
  ceph_assert(idx < m_upper_bound);
  const perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_TIME))
    return utime_t();
  uint64_t v = data.u64;
  return utime_t(v / 1000000000ull, v % 1000000000ull);
}

void PerfCounters::hinc(int idx, int64_t x, int64_t y)
{
#ifndef WITH_SEASTAR
  if (!m_cct->_conf->perf)
    return;
#endif

  ceph_assert(idx > m_lower_bound);
  ceph_assert(idx < m_upper_bound);

  perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  ceph_assert(data.type == (PERFCOUNTER_HISTOGRAM | PERFCOUNTER_COUNTER | PERFCOUNTER_U64));
  ceph_assert(data.histogram);

  data.histogram->inc(x, y);
}

pair<uint64_t, uint64_t> PerfCounters::get_tavg_ns(int idx) const
{
#ifndef WITH_SEASTAR
  if (!m_cct->_conf->perf)
    return make_pair(0, 0);
#endif

  ceph_assert(idx > m_lower_bound);
  ceph_assert(idx < m_upper_bound);
  const perf_counter_data_any_d& data(m_data[idx - m_lower_bound - 1]);
  if (!(data.type & PERFCOUNTER_TIME))
    return make_pair(0, 0);
  if (!(data.type & PERFCOUNTER_LONGRUNAVG))
    return make_pair(0, 0);
  pair<uint64_t,uint64_t> a = data.read_avg();
  return make_pair(a.second, a.first);
}

void PerfCounters::reset()
{
  perf_counter_data_vec_t::iterator d = m_data.begin();
  perf_counter_data_vec_t::iterator d_end = m_data.end();

  while (d != d_end) {
    d->reset();
    ++d;
  }
}

static std::string get_metric_type(const PerfCounters::perf_counter_data_any_d &d) {
  if (d.type & PERFCOUNTER_COUNTER) {
    return "counter";
  } else {
    return "gauge";
  }
}

static std::string get_metric_name(const PerfCounters::perf_counter_data_any_d &d) {
  return d.name;
}

static std::string get_units(const PerfCounters::perf_counter_data_any_d &d) {
  if (d.unit == UNIT_NONE) {
    return "none";
  } else if (d.unit == UNIT_BYTES) {
    return "bytes";
  }

  return "";
}

static std::string get_value_type(const PerfCounters::perf_counter_data_any_d &d) {
  if (d.type & PERFCOUNTER_LONGRUNAVG) {
    if (d.type & PERFCOUNTER_TIME) {
      return "real-integer-pair";
    } else {
      return "integer-integer-pair";
    }
  } else if (d.type & PERFCOUNTER_HISTOGRAM) {
    if (d.type & PERFCOUNTER_TIME) {
      return "real-2d-histogram";
    } else {
      return "integer-2d-histogram";
    }
  } else {
    if (d.type & PERFCOUNTER_TIME) {
      return "real";
    } else {
      return "integer";
    }
  }
}

static uint64_t get_value(const PerfCounters::perf_counter_data_any_d &d) {
  return d.u64;
}

static std::pair<uint64_t,uint64_t> get_average(const PerfCounters::perf_counter_data_any_d &d) {
  return d.read_avg();
}
static PerfHistogram<> *get_histogram(const PerfCounters::perf_counter_data_any_d &d) {
  return d.histogram.get();
}

static std::string get_nick(const PerfCounters::perf_counter_data_any_d &d) {
  return d.nick ? d.nick : "";
}

static std::string get_description(const PerfCounters::perf_counter_data_any_d &d) {
  return d.description ? d.description : "";
}

static uint8_t get_priority(const PerfCounters::perf_counter_data_any_d &d) {
  return d.prio;
}

void PerfCounters::for_each_unlabeled_counter(const std::function<
                                              void (perfcounter_type_d, std::string_view,
                                                    std::string_view, std::string_view,
                                                    std::string_view, std::string_view,
                                                    std::string_view, int, const PerfType&)> &fn) const {
  for (auto &data : m_data) {
    PerfType perf_type = UnknownType();
    auto perf_name = get_metric_name(data);

    if (data.type & PERFCOUNTER_LONGRUNAVG) {
      auto avg = get_average(data);
      if (data.type & PERFCOUNTER_U64) {
        perf_type = LongRunAverageType(perf_name, avg);
      } else if (data.type & PERFCOUNTER_TIME) {
        perf_type = LongRunTimeAverageType(perf_name, avg);
      }
    } else if (data.type & PERFCOUNTER_HISTOGRAM) {
      auto h = get_histogram(data);
      perf_type = HistogramType(perf_name, h);
    } else {
      auto value = get_value(data);
      if (data.type & PERFCOUNTER_U64) {
        perf_type = ValueType(perf_name, value);
      } else if (data.type & PERFCOUNTER_TIME) {
        perf_type = TimeType(perf_name, value);
      }
    }

    fn(data.type,
       perf_name,
       get_metric_type(data),
       get_value_type(data),
       get_nick(data),
       get_description(data),
       get_units(data),
       get_adjusted_priority(get_priority(data)), perf_type);
  }
}

void PerfCounters::get_unlabeled_perf_counters(struct perf_counters *pc) const {
  auto fn = [&](perfcounter_type_d type, std::string_view name,
                std::string_view mtype, std::string_view vtype,
                std::string_view nick, std::string_view description,
                std::string_view units, int priority, const PerfType &perf_type) {
    boost::apply_visitor(UnformattedDumpTypeVisitor(name, description, priority, pc), perf_type);
  };

  for_each_unlabeled_counter(fn);
}

void PerfCounters::dump_formatted_generic(Formatter *f, bool schema,
    bool histograms, bool dump_labeled, const std::string &counter) const
{
  if (dump_labeled) {
    f->open_object_section(""); // should be enclosed by array
    f->open_object_section("labels");
    for (auto label : ceph::perf_counters::key_labels(m_name)) {
      // don't dump labels with empty label names
      if (!label.first.empty()) {
        f->dump_string(label.first, label.second);
      }
    }
    f->close_section(); // labels
    f->open_object_section("counters");
  } else {
    auto labels = ceph::perf_counters::key_labels(m_name);
    // do not dump counters when counter instance is labeled and dump_labeled is not set
    if (labels.begin() != labels.end()) {
      return;
    }

    f->open_object_section(m_name.c_str());
  }

  auto fn = [&](perfcounter_type_d type, std::string_view name,
                std::string_view mtype, std::string_view vtype,
                std::string_view nick, std::string_view description,
                std::string_view units, int priority, const PerfType &perf_type) {
    if (!counter.empty() && counter != name) {
      return;
    }

    // Switch between normal and histogram view
    bool is_histogram = (type & PERFCOUNTER_HISTOGRAM) != 0;
    if (is_histogram != histograms) {
      return;
    }

    if (schema) {
      f->open_object_section(name);
      // we probably should not have exposed this raw field (with bit
      // values), but existing plugins rely on it so we're stuck with
      // it.
      f->dump_int("type", type);
      f->dump_string("metric_type", mtype);
      f->dump_string("value_type", vtype);
      f->dump_string("description", description);
      f->dump_string("nick", nick);
      f->dump_int("priority", priority);
      f->dump_string("units", units);
      f->close_section();
    } else {
      boost::apply_visitor(DumpTypeVisitor(f), perf_type);
    }
    if (dump_labeled) {
      f->close_section(); // counters
    }
    f->close_section();
  };

  for_each_unlabeled_counter(fn);
}

const std::string &PerfCounters::get_name() const
{
  return m_name;
}

PerfCounters::PerfCounters(CephContext *cct, const std::string &name,
	   int lower_bound, int upper_bound)
  : m_cct(cct),
    m_lower_bound(lower_bound),
    m_upper_bound(upper_bound),
    m_name(name)
#if !defined(WITH_SEASTAR) || defined(WITH_ALIEN)
    ,
    m_lock_name(std::string("PerfCounters::") + name.c_str()),
    m_lock(ceph::make_mutex(m_lock_name))
#endif
{
  m_data.resize(upper_bound - lower_bound - 1);
}

PerfCountersBuilder::PerfCountersBuilder(CephContext *cct, const std::string &name,
                  int first, int last)
  : m_perf_counters(new PerfCounters(cct, name, first, last))
{
}

PerfCountersBuilder::~PerfCountersBuilder()
{
  if (m_perf_counters)
    delete m_perf_counters;
  m_perf_counters = NULL;
}

void PerfCountersBuilder::add_u64_counter(
  int idx, const char *name,
  const char *description, const char *nick, int prio, int unit)
{
  add_impl(idx, name, description, nick, prio,
	   PERFCOUNTER_U64 | PERFCOUNTER_COUNTER, unit);
}

void PerfCountersBuilder::add_u64(
  int idx, const char *name,
  const char *description, const char *nick, int prio, int unit)
{
  add_impl(idx, name, description, nick, prio, PERFCOUNTER_U64, unit);
}

void PerfCountersBuilder::add_u64_avg(
  int idx, const char *name,
  const char *description, const char *nick, int prio, int unit)
{
  add_impl(idx, name, description, nick, prio,
	   PERFCOUNTER_U64 | PERFCOUNTER_LONGRUNAVG, unit);
}

void PerfCountersBuilder::add_time(
  int idx, const char *name,
  const char *description, const char *nick, int prio)
{
  add_impl(idx, name, description, nick, prio, PERFCOUNTER_TIME);
}

void PerfCountersBuilder::add_time_avg(
  int idx, const char *name,
  const char *description, const char *nick, int prio)
{
  add_impl(idx, name, description, nick, prio,
	   PERFCOUNTER_TIME | PERFCOUNTER_LONGRUNAVG);
}

void PerfCountersBuilder::add_u64_counter_histogram(
  int idx, const char *name,
  PerfHistogramCommon::axis_config_d x_axis_config,
  PerfHistogramCommon::axis_config_d y_axis_config,
  const char *description, const char *nick, int prio, int unit)
{
  add_impl(idx, name, description, nick, prio,
	   PERFCOUNTER_U64 | PERFCOUNTER_HISTOGRAM | PERFCOUNTER_COUNTER, unit,
           std::unique_ptr<PerfHistogram<>>{new PerfHistogram<>{x_axis_config, y_axis_config}});
}

void PerfCountersBuilder::add_impl(
  int idx, const char *name,
  const char *description, const char *nick, int prio, int ty, int unit,
  std::unique_ptr<PerfHistogram<>> histogram)
{
  ceph_assert(idx > m_perf_counters->m_lower_bound);
  ceph_assert(idx < m_perf_counters->m_upper_bound);
  PerfCounters::perf_counter_data_vec_t &vec(m_perf_counters->m_data);
  PerfCounters::perf_counter_data_any_d
    &data(vec[idx - m_perf_counters->m_lower_bound - 1]);
  ceph_assert(data.type == PERFCOUNTER_NONE);
  data.name = name;
  data.description = description;
  // nick must be <= 4 chars
  if (nick) {
    ceph_assert(strlen(nick) <= 4);
  }
  data.nick = nick;
  data.prio = prio ? prio : prio_default;
  data.type = (enum perfcounter_type_d)ty;
  data.unit = (enum unit_t) unit;
  data.histogram = std::move(histogram);
}

PerfCounters *PerfCountersBuilder::create_perf_counters()
{
  PerfCounters::perf_counter_data_vec_t::const_iterator d = m_perf_counters->m_data.begin();
  PerfCounters::perf_counter_data_vec_t::const_iterator d_end = m_perf_counters->m_data.end();
  for (; d != d_end; ++d) {
    ceph_assert(d->type != PERFCOUNTER_NONE);
    ceph_assert(d->type & (PERFCOUNTER_U64 | PERFCOUNTER_TIME));
  }

  PerfCounters *ret = m_perf_counters;
  m_perf_counters = NULL;
  return ret;
}

}
