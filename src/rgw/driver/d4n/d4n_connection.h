/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 International Business Machines Corp. (IBM)
 *      
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
*/

#pragma once

#ifdef WITH_RADOSGW_FDB
#include "rgw/ceph_fdb.h"
#endif
#include <boost/redis/connection.hpp>

#ifdef WITH_RADOSGW_FDB
namespace lfdb = ceph::libfdb;
#endif

using boost::redis::connection;

namespace rgw::d4n {

struct DirectoryConnection {
public:
    virtual ~DirectoryConnection() = default;
    virtual std::shared_ptr<void> get_conn() = 0;
};


class RedisConnection : public DirectoryConnection {
private:
    std::shared_ptr<connection> conn;

public:
    RedisConnection(std::shared_ptr<connection> c) : conn(c) {}

    std::shared_ptr<void> get_conn() override {
        return conn;
    }

    std::shared_ptr<connection> get_redis_conn() {
        return conn;
    }
};

#ifdef WITH_RADOSGW_FDB
class FDBConnection : public DirectoryConnection {
private:
    lfdb::database_handle conn;

public:
    explicit FDBConnection(lfdb::database_handle c)
      : conn(c)
    {
    }

    std::shared_ptr<void> get_conn() override
    {
        return conn;
    }

    const lfdb::database_handle& get_fdb_conn() const
    {
        return conn;
    }
};
#endif

} //namespace rgw::d4n
