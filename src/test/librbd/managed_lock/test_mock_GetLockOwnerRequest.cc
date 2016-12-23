// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "cls/lock/cls_lock_ops.h"
#include "cls/lock/cls_lock_types.h"
#include "librbd/managed_lock/AcquireRequest.h"
#include "librbd/managed_lock/BreakLockRequest.h"
#include "librbd/managed_lock/GetLockOwnerRequest.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <arpa/inet.h>
#include <list>


namespace librbd {
namespace watcher {
template <>
struct Traits<MockImageCtx> {
  typedef librbd::MockImageWatcher Watcher;
};
} // namespace watcher

namespace managed_lock {
template <>
struct BreakLockRequest<librbd::MockImageCtx> {
  static BreakLockRequest *create(librados::IoCtx& ioctx,
                                  ContextWQ *work_queue, const std::string& oid,
                                  const typename ManagedLock<librbd::MockImageCtx>::LockOwner &owner,
                                  bool blacklist_lock_owner,
                                  Context *on_finish) {
    return nullptr;
  }

  MOCK_METHOD0(send, void());
};

} // namespace managed_lock

} // namespace librbd

// template definitions
#include "librbd/managed_lock/GetLockOwnerRequest.cc"
template class librbd::managed_lock::GetLockOwnerRequest<librbd::MockImageCtx>;

#include "librbd/ManagedLock.cc"
template class librbd::ManagedLock<librbd::MockImageCtx>;

namespace librbd {
namespace managed_lock {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockManagedLockGetLockOwnerRequest : public TestMockFixture {
public:
  typedef GetLockOwnerRequest<MockImageCtx> MockGetLockOwnerRequest;
  typedef ManagedLock<MockImageCtx>::LockOwner LockOwner;
  typedef ManagedLock<MockImageCtx> MockManagedLock;

  void expect_get_lock_info(MockImageCtx &mock_image_ctx, int r,
                            const entity_name_t &locker_entity,
                            const std::string &locker_address,
                            const std::string &locker_cookie,
                            const std::string &lock_tag,
                            ClsLockType lock_type) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               exec(mock_image_ctx.header_oid, _, StrEq("lock"),
                               StrEq("get_info"), _, _, _));
    if (r < 0 && r != -ENOENT) {
      expect.WillOnce(Return(r));
    } else {
      entity_name_t entity(locker_entity);
      entity_addr_t entity_addr;
      entity_addr.parse(locker_address.c_str(), NULL);

      cls_lock_get_info_reply reply;
      if (r != -ENOENT) {
        reply.lockers = decltype(reply.lockers) {
          {rados::cls::lock::locker_id_t(entity, locker_cookie),
           rados::cls::lock::locker_info_t(utime_t(), entity_addr, "")}};
        reply.tag = lock_tag;
        reply.lock_type = lock_type;
      }

      bufferlist bl;
      ::encode(reply, bl, CEPH_FEATURES_SUPPORTED_DEFAULT);

      std::string str(bl.c_str(), bl.length());
      expect.WillOnce(DoAll(WithArg<5>(CopyInBufferlist(str)), Return(0)));
    }
  }

  void expect_list_watchers(MockImageCtx &mock_image_ctx, int r,
                            const std::string &address, uint64_t watch_handle) {
    auto &expect = EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                               list_watchers(mock_image_ctx.header_oid, _));
    if (r < 0) {
      expect.WillOnce(Return(r));
    } else {
      obj_watch_t watcher;
      strcpy(watcher.addr, (address + ":0/0").c_str());
      watcher.cookie = watch_handle;

      std::list<obj_watch_t> watchers;
      watchers.push_back(watcher);

      expect.WillOnce(DoAll(SetArgPointee<1>(watchers), Return(0)));
    }
  }
};

TEST_F(TestMockManagedLockGetLockOwnerRequest, GetLockersError) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, -EINVAL, entity_name_t::CLIENT(1), "",
                       "", MockManagedLock::WATCHER_LOCK_TAG, LOCK_EXCLUSIVE);

  LockOwner lock_owner;
  C_SaferCond ctx;
  MockGetLockOwnerRequest *req = MockGetLockOwnerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, &lock_owner, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockGetLockOwnerRequest, Empty) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, -ENOENT, entity_name_t::CLIENT(1), "",
                       "", MockManagedLock::WATCHER_LOCK_TAG, LOCK_EXCLUSIVE);

  LockOwner lock_owner;
  C_SaferCond ctx;
  MockGetLockOwnerRequest *req = MockGetLockOwnerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, &lock_owner, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockManagedLockGetLockOwnerRequest, ExternalTag) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", "external tag", LOCK_EXCLUSIVE);

  LockOwner lock_owner;
  C_SaferCond ctx;
  MockGetLockOwnerRequest *req = MockGetLockOwnerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, &lock_owner, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockManagedLockGetLockOwnerRequest, Shared) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_SHARED);

  LockOwner lock_owner;
  C_SaferCond ctx;
  MockGetLockOwnerRequest *req = MockGetLockOwnerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, &lock_owner, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockManagedLockGetLockOwnerRequest, GetWatchersError) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_EXCLUSIVE);
  expect_list_watchers(mock_image_ctx, -EINVAL, "", 0);

  LockOwner lock_owner;
  C_SaferCond ctx;
  MockGetLockOwnerRequest *req = MockGetLockOwnerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, &lock_owner, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockGetLockOwnerRequest, Dead) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_EXCLUSIVE);
  expect_list_watchers(mock_image_ctx, 0, "dead client", 123);

  LockOwner lock_owner;
  C_SaferCond ctx;
  MockGetLockOwnerRequest *req = MockGetLockOwnerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, &lock_owner, &ctx);
  req->send();
  ASSERT_EQ(-ENOTCONN, ctx.wait());
}

TEST_F(TestMockManagedLockGetLockOwnerRequest, Alive) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_get_lock_info(mock_image_ctx, 0, entity_name_t::CLIENT(1), "1.2.3.4",
                       "auto 123", MockManagedLock::WATCHER_LOCK_TAG,
                       LOCK_EXCLUSIVE);
  expect_list_watchers(mock_image_ctx, 0, "1.2.3.4", 123);

  LockOwner lock_owner;
  C_SaferCond ctx;
  MockGetLockOwnerRequest *req = MockGetLockOwnerRequest::create(
    mock_image_ctx.md_ctx, mock_image_ctx.header_oid, &lock_owner, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

} // namespace managed_lock
} // namespace librbd
