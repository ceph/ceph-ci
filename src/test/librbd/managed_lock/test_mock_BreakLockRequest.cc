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


} // namespace librbd

// template definitions
#include "librbd/managed_lock/BreakLockRequest.cc"
template class librbd::managed_lock::BreakLockRequest<librbd::MockImageCtx>;

namespace librbd {
namespace managed_lock {

using ::testing::_;
using ::testing::DoAll;
using ::testing::InSequence;
using ::testing::Return;
using ::testing::SetArgPointee;
using ::testing::StrEq;
using ::testing::WithArg;

class TestMockManagedLockBreakLockRequest : public TestMockFixture {
public:
  typedef BreakLockRequest<MockImageCtx> MockBreakLockRequest;
  typedef ManagedLock<MockImageCtx>::LockOwner LockOwner;

  void expect_blacklist_add(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_rados_client(), blacklist_add(_, _))
                  .WillOnce(Return(r));
  }

  void expect_break_lock(MockImageCtx &mock_image_ctx, int r) {
    EXPECT_CALL(get_mock_io_ctx(mock_image_ctx.md_ctx),
                exec(mock_image_ctx.header_oid, _, StrEq("lock"),
                     StrEq("break_lock"), _, _, _)).WillOnce(Return(r));
  }
};

TEST_F(TestMockManagedLockBreakLockRequest, Success) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockBreakLockRequest *req = MockBreakLockRequest::create(
    mock_image_ctx.md_ctx, ictx->op_work_queue, mock_image_ctx.header_oid,
    LockOwner(), true, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockBreakLockRequest, SuccessNoBlacklist) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_break_lock(mock_image_ctx, 0);

  C_SaferCond ctx;
  MockBreakLockRequest *req = MockBreakLockRequest::create(
    mock_image_ctx.md_ctx, ictx->op_work_queue, mock_image_ctx.header_oid,
    LockOwner(), false, &ctx);
  req->send();
  ASSERT_EQ(0, ctx.wait());
}

TEST_F(TestMockManagedLockBreakLockRequest, ErrorBlacklist) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_blacklist_add(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockBreakLockRequest *req = MockBreakLockRequest::create(
    mock_image_ctx.md_ctx, ictx->op_work_queue, mock_image_ctx.header_oid,
    LockOwner(), true, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

TEST_F(TestMockManagedLockBreakLockRequest, ErrorBreak) {

  librbd::ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  MockImageCtx mock_image_ctx(*ictx);

  InSequence seq;
  expect_blacklist_add(mock_image_ctx, 0);
  expect_break_lock(mock_image_ctx, -EINVAL);

  C_SaferCond ctx;
  MockBreakLockRequest *req = MockBreakLockRequest::create(
    mock_image_ctx.md_ctx, ictx->op_work_queue, mock_image_ctx.header_oid,
    LockOwner(), true, &ctx);
  req->send();
  ASSERT_EQ(-EINVAL, ctx.wait());
}

} // namespace managed_lock
} // namespace librbd
