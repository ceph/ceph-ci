// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librbd/test_mock_fixture.h"
#include "test/librbd/test_support.h"
#include "test/librados_test_stub/MockTestMemIoCtxImpl.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "cls/lock/cls_lock_ops.h"
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
}

namespace managed_lock {

template <>
struct BreakLockRequest<librbd::MockImageCtx> {
  bool blacklist_lock_owner;
  Context *on_finish;

  static BreakLockRequest *s_instance;
  static BreakLockRequest *create(librados::IoCtx& ioctx,
                                  ContextWQ *work_queue, const std::string& oid,
                                  const typename ManagedLock<librbd::MockImageCtx>::LockOwner &owner,
                                  bool blacklist_lock_owner,
                                  Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->blacklist_lock_owner = blacklist_lock_owner;
    s_instance->on_finish = on_finish;
        return s_instance;
  }

  BreakLockRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

BreakLockRequest<librbd::MockImageCtx> *BreakLockRequest<librbd::MockImageCtx>::s_instance = nullptr;

template <>
struct GetLockOwnerRequest<librbd::MockImageCtx> {
  ManagedLock<librbd::MockImageCtx>::LockOwner *lock_owner;
  Context *on_finish;

  static GetLockOwnerRequest *s_instance;
  static GetLockOwnerRequest *create(librados::IoCtx& ioctx,
                                     const std::string& oid,
                                     typename ManagedLock<librbd::MockImageCtx>::LockOwner *owner,
                                     Context *on_finish) {
    assert(s_instance != nullptr);
    s_instance->lock_owner = owner;
    s_instance->on_finish = on_finish;
        return s_instance;
  }

  GetLockOwnerRequest() {
    s_instance = this;
  }

  MOCK_METHOD0(send, void());
};

GetLockOwnerRequest<librbd::MockImageCtx> *GetLockOwnerRequest<librbd::MockImageCtx>::s_instance = nullptr;

} // namespace managed_lock
} // namespace librbd

// template definitions
#include "librbd/managed_lock/AcquireRequest.cc"
template class librbd::managed_lock::AcquireRequest<librbd::MockImageCtx>;

namespace librbd {
namespace managed_lock {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

static const std::string TEST_COOKIE("auto 123");

class TestMockManagedLockAcquireRequest : public TestMockFixture {
public:
  typedef AcquireRequest<MockImageCtx> MockAcquireRequest;
  typedef GetLockOwnerRequest<MockImageCtx> MockGetLockOwnerRequest;
  typedef BreakLockRequest<MockImageCtx> MockBreakLockRequest;
  typedef ManagedLock<MockImageCtx>::LockOwner LockOwner;

  void expect_lock(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"), StrEq("lock"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_unlock(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"), StrEq("unlock"), _, _, _))
                  .WillOnce(Return(r));
  }

  void expect_get_lock_owner(MockGetLockOwnerRequest &mock_get_lock_owner_request,
                             const LockOwner &lock_owner, int r) {
    EXPECT_CALL(mock_get_lock_owner_request, send())
        .WillOnce(Invoke([&mock_get_lock_owner_request, &lock_owner, r]() {
                  if (r == 0) {
                    *mock_get_lock_owner_request.lock_owner = lock_owner;
                  }
                  mock_get_lock_owner_request.on_finish->complete(r);
                }));
  }

  void expect_break_lock(MockImageCtx &mock_image_ctx,
                         MockBreakLockRequest &mock_break_lock_request, int r) {
    CephContext *cct = reinterpret_cast<CephContext *>(
      mock_image_ctx.md_ctx.cct());
    EXPECT_CALL(mock_break_lock_request, send())
        .WillOnce(Invoke([cct, &mock_break_lock_request, r]() {
                  ASSERT_EQ(mock_break_lock_request.blacklist_lock_owner,
                            cct->_conf->rbd_blacklist_on_break_lock);
                  mock_break_lock_request.on_finish->complete(r);
                }));
  }
};

TEST_F(TestMockManagedLockAcquireRequest, Success) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_lock(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, LockBusy) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockOwnerRequest mock_get_lock_owner_request;
  MockBreakLockRequest mock_break_lock_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);

  LockOwner lock_owner;
  lock_owner.entity = entity_name_t::CLIENT(1);
  lock_owner.cookie = "auto 123";
  lock_owner.address = "1.2.3.4";

  expect_get_lock_owner(mock_get_lock_owner_request, lock_owner, -ENOTCONN);
  expect_break_lock(mock_image_ctx, mock_break_lock_request, 0);
  expect_lock(mock_image_ctx, -ENOENT);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-ENOENT, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetLockOwnerError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockOwnerRequest mock_get_lock_owner_request;

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_owner(mock_get_lock_owner_request, LockOwner(), -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetLockOwnerEmpty) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockOwnerRequest mock_get_lock_owner_request;

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);

  LockOwner lock_owner;
  lock_owner.entity = entity_name_t::CLIENT(1);

  expect_get_lock_owner(mock_get_lock_owner_request, lock_owner, -ENOENT);
  expect_lock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, GetLockOwnerBusy) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockOwnerRequest mock_get_lock_owner_request;

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);
  expect_get_lock_owner(mock_get_lock_owner_request, LockOwner(), -EBUSY);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EBUSY, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, BreakLockMissing) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockOwnerRequest mock_get_lock_owner_request;
  MockBreakLockRequest mock_break_lock_request;
  expect_op_work_queue(mock_image_ctx);

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);

  LockOwner lock_owner;
  lock_owner.entity = entity_name_t::CLIENT(1);
  lock_owner.cookie = "auto 123";
  lock_owner.address = "1.2.3.4";

  expect_get_lock_owner(mock_get_lock_owner_request, lock_owner, -ENOTCONN);
  expect_break_lock(mock_image_ctx, mock_break_lock_request, -ENOENT);
  expect_lock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockAcquireRequest, BreakLockError) {
  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);
  MockGetLockOwnerRequest mock_get_lock_owner_request;
  MockBreakLockRequest mock_break_lock_request;

  InSequence seq;
  expect_lock(mock_image_ctx, -EBUSY);

  LockOwner lock_owner;
  lock_owner.entity = entity_name_t::CLIENT(1);
  lock_owner.cookie = "auto 123";
  lock_owner.address = "1.2.3.4";

  expect_get_lock_owner(mock_get_lock_owner_request, lock_owner, -ENOTCONN);
  expect_break_lock(mock_image_ctx, mock_break_lock_request, -EINVAL);

  C_SaferCond ctx;
  MockAcquireRequest *req = MockAcquireRequest::create(mock_image_ctx.md_ctx,
     mock_image_ctx.image_watcher, ictx->op_work_queue, mock_image_ctx.header_oid,
     TEST_COOKIE, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace managed_lock
} // namespace librbd
