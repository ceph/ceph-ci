// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "test/librados/test_cxx.h"
#include "test/librbd/test_fixture.h"
#include "test/librbd/test_support.h"
#include "common/Cond.h"
#include "include/neorados/RADOS.hpp"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/api/Image.h"
#include "librbd/api/Io.h"
#include "librbd/api/Namespace.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/io/ReadResult.h"
#include <boost/scope_exit.hpp>
#include <thread>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <shared_mutex> // for std::shared_lock

void register_test_image_ctx() {
}

namespace librbd {

namespace {

struct DummyContext : public Context {
  void finish(int r) override {
  }
};

struct QuiesceWatcher : public librbd::QuiesceWatchCtx {
  ImageCtx *image_ctx;
  uint64_t handle = 0;

  std::mutex lock;
  std::condition_variable cond;
  size_t quiesce_count = 0;

  explicit QuiesceWatcher(ImageCtx *image_ctx) : image_ctx(image_ctx) {
  }

  void handle_quiesce() override {
    {
      std::lock_guard locker{lock};
      ++quiesce_count;
      cond.notify_all();
    }
    image_ctx->state->quiesce_complete(handle, 0);
  }

  void handle_unquiesce() override {
  }

  bool wait_for_quiesce(size_t count) {
    std::unique_lock locker{lock};
    return cond.wait_for(locker, std::chrono::seconds(30),
                         [this, count] { return quiesce_count >= count; });
  }
};

struct UpdateWatcher : public librbd::UpdateWatchCtx {
  std::mutex lock;
  std::condition_variable cond;
  size_t notify_count = 0;

  void handle_notify() override {
    std::lock_guard locker{lock};
    ++notify_count;
    cond.notify_all();
  }

  bool wait_for_notify(size_t count) {
    std::unique_lock locker{lock};
    return cond.wait_for(locker, std::chrono::seconds(30),
                         [this, count] { return notify_count >= count; });
  }
};

} // anonymous namespace

class TestImageCtx : public TestFixture {
public:
  static void SetUpTestCase() {
    TestFixture::SetUpTestCase();

    _other_pool_name = get_temp_pool_name("test-librbd-");
    ASSERT_EQ(0, _rados.pool_create(_other_pool_name.c_str()));
  }

  static void TearDownTestCase() {
    ASSERT_EQ(0, _rados.pool_delete(_other_pool_name.c_str()));

    TestFixture::TearDownTestCase();
  }

  void SetUp() override {
    TestFixture::SetUp();

    ASSERT_EQ(0, _rados.ioctx_create(_other_pool_name.c_str(),
                                     m_other_pool_ioctx));
  }

  void TearDown() override {
    m_other_pool_ioctx.close();

    TestFixture::TearDown();
  }

  int reopen(ImageCtx *ictx, librados::IoCtx& io_ctx,
             const std::string &image_name, const std::string &image_id = "",
             const char *snap_name = nullptr) {
    return ictx->state->reopen(io_ctx, image_name, image_id, snap_name);
  }

  std::string create_image(librados::IoCtx& io_ctx, uint64_t size) {
    auto image_name = get_temp_image_name();
    EXPECT_EQ(0, create_image_pp(m_rbd, io_ctx, image_name, size));
    return image_name;
  }

  void write_pattern(librados::IoCtx& io_ctx, const std::string &image_name,
                     char c) {
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(io_ctx, image, image_name.c_str()));

    bufferlist bl;
    bl.append(std::string(4096, c));
    ASSERT_EQ(4096, image.write(0, bl.length(), bl));
  }

  void assert_pattern(ImageCtx *ictx, char c) {
    bufferlist expected_bl;
    expected_bl.append(std::string(4096, c));

    bufferlist read_bl;
    read_bl.push_back(bufferptr(4096));
    ASSERT_EQ(4096, api::Io<>::read(*ictx, 0, 4096,
                                    io::ReadResult{&read_bl}, 0));
    ASSERT_TRUE(expected_bl.contents_equal(read_bl));
  }

  static std::string _other_pool_name;
  librados::IoCtx m_other_pool_ioctx;
};

std::string TestImageCtx::_other_pool_name;

TEST_F(TestImageCtx, ReopenSamePool) {
  auto dst_image_name = create_image(m_ioctx, m_image_size * 2);
  write_pattern(m_ioctx, m_image_name, '1');
  write_pattern(m_ioctx, dst_image_name, '2');

  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_NO_FATAL_FAILURE(assert_pattern(ictx, '1'));

  auto src_id = ictx->id;
  auto src_header_oid = ictx->header_oid;
  auto src_object_prefix = ictx->object_prefix;

  ASSERT_EQ(0, reopen(ictx, m_ioctx, dst_image_name, "", nullptr));

  ASSERT_EQ(dst_image_name, ictx->name);
  if (!ictx->old_format) {
    // format 1 images have no id at all
    ASSERT_NE(src_id, ictx->id);
  }
  ASSERT_NE(src_header_oid, ictx->header_oid);
  ASSERT_NE(src_object_prefix, ictx->object_prefix);
  ASSERT_EQ(m_image_size * 2, ictx->size);

  // reads and writes go to the image the context was re-targeted at
  ASSERT_NO_FATAL_FAILURE(assert_pattern(ictx, '2'));

  bufferlist bl;
  bl.append(std::string(4096, '3'));
  ASSERT_EQ(4096, api::Io<>::write(*ictx, 0, bl.length(), bufferlist{bl}, 0));
  ASSERT_NO_FATAL_FAILURE(assert_pattern(ictx, '3'));

  // ... and the image it was re-targeted away from is untouched
  ImageCtx *src_ictx;
  ASSERT_EQ(0, open_image(m_image_name, &src_ictx));
  ASSERT_NO_FATAL_FAILURE(assert_pattern(src_ictx, '1'));
}

TEST_F(TestImageCtx, ReopenOtherPool) {
  auto dst_image_name = create_image(m_other_pool_ioctx, m_image_size);
  write_pattern(m_ioctx, m_image_name, '1');
  write_pattern(m_other_pool_ioctx, dst_image_name, '2');

  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  auto src_pool_id = ictx->md_ctx.get_id();
  ASSERT_NE(src_pool_id, m_other_pool_ioctx.get_id());

  ASSERT_EQ(0, reopen(ictx, m_other_pool_ioctx, dst_image_name, "", nullptr));

  ASSERT_EQ(m_other_pool_ioctx.get_id(), ictx->md_ctx.get_id());
  ASSERT_EQ(m_other_pool_ioctx.get_id(), ictx->data_ctx.get_id());
  ASSERT_EQ(m_other_pool_ioctx.get_id(), ictx->layout.pool_id);
  ASSERT_EQ(m_other_pool_ioctx.get_id(),
            ictx->get_data_io_context()->get_pool());

  ASSERT_NO_FATAL_FAILURE(assert_pattern(ictx, '2'));
}

TEST_F(TestImageCtx, ReopenOtherNamespace) {
  REQUIRE_FORMAT_V2();

  ASSERT_EQ(0, librbd::api::Namespace<>::create(m_ioctx, "ns1"));

  librados::IoCtx ns_ioctx;
  ns_ioctx.dup(m_ioctx);
  ns_ioctx.set_namespace("ns1");
  BOOST_SCOPE_EXIT(&ns_ioctx) {
    ns_ioctx.close();
  } BOOST_SCOPE_EXIT_END;

  auto dst_image_name = create_image(ns_ioctx, m_image_size);
  write_pattern(m_ioctx, m_image_name, '1');
  write_pattern(ns_ioctx, dst_image_name, '2');

  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ("", ictx->md_ctx.get_namespace());

  ASSERT_EQ(0, reopen(ictx, ns_ioctx, dst_image_name, "", nullptr));

  ASSERT_EQ("ns1", ictx->md_ctx.get_namespace());
  ASSERT_EQ("ns1", ictx->data_ctx.get_namespace());
  ASSERT_EQ("ns1", ictx->get_data_io_context()->get_ns());

  ASSERT_NO_FATAL_FAILURE(assert_pattern(ictx, '2'));
}

TEST_F(TestImageCtx, ReopenById) {
  REQUIRE_FORMAT_V2();

  auto dst_image_name = create_image(m_ioctx, m_image_size);
  write_pattern(m_ioctx, dst_image_name, '2');

  std::string dst_id;
  {
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, dst_image_name.c_str()));
    ASSERT_EQ(0, get_image_id(image, &dst_id));
  }

  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  // the name is looked up from the id, just like a fresh open by id
  ASSERT_EQ(0, reopen(ictx, m_ioctx, "", dst_id, nullptr));

  ASSERT_EQ(dst_id, ictx->id);
  ASSERT_EQ(dst_image_name, ictx->name);
  ASSERT_NO_FATAL_FAILURE(assert_pattern(ictx, '2'));
}

TEST_F(TestImageCtx, ReopenToSnapshot) {
  auto dst_image_name = create_image(m_ioctx, m_image_size);
  write_pattern(m_ioctx, dst_image_name, '2');

  {
    ImageCtx *dst_ictx;
    ASSERT_EQ(0, open_image(dst_image_name, &dst_ictx));
    ASSERT_EQ(0, snap_create(*dst_ictx, "snap1"));
    close_image(dst_ictx);
  }

  // the snapshot content diverges from HEAD so the two can't be confused
  write_pattern(m_ioctx, dst_image_name, '3');

  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(0, reopen(ictx, m_ioctx, dst_image_name, "", "snap1"));

  ASSERT_EQ("snap1", ictx->snap_name);
  ASSERT_NE(CEPH_NOSNAP, ictx->snap_id);
  ASSERT_EQ(ictx->snap_id, ictx->get_data_io_context()->get_read_snap());
  ASSERT_NO_FATAL_FAILURE(assert_pattern(ictx, '2'));
}

TEST_F(TestImageCtx, ReopenFromSnapshot) {
  auto dst_image_name = create_image(m_ioctx, m_image_size);
  write_pattern(m_ioctx, m_image_name, '1');
  write_pattern(m_ioctx, dst_image_name, '2');

  {
    ImageCtx *src_ictx;
    ASSERT_EQ(0, open_image(m_image_name, &src_ictx));
    ASSERT_EQ(0, snap_create(*src_ictx, "snap1"));
    close_image(src_ictx);
  }

  // pinned to a snapshot of the image being re-targeted away from
  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, "snap1", &ictx));
  ASSERT_NE(CEPH_NOSNAP, ictx->snap_id);

  ASSERT_EQ(0, reopen(ictx, m_ioctx, dst_image_name, "", nullptr));

  // the snapshot the context was pinned to is gone -- the re-targeted context
  // tracks the new image's HEAD and is writeable
  ASSERT_TRUE(ictx->snap_name.empty());
  ASSERT_EQ(CEPH_NOSNAP, ictx->snap_id);
  ASSERT_FALSE(ictx->read_only);
  ASSERT_EQ(CEPH_NOSNAP, ictx->get_data_io_context()->get_read_snap());

  bufferlist bl;
  bl.append(std::string(4096, '3'));
  ASSERT_EQ(4096, api::Io<>::write(*ictx, 0, bl.length(), bufferlist{bl}, 0));
  ASSERT_NO_FATAL_FAILURE(assert_pattern(ictx, '3'));
}

TEST_F(TestImageCtx, ReopenPreservesReadOnly) {
  auto dst_image_name = create_image(m_ioctx, m_image_size);
  write_pattern(m_ioctx, dst_image_name, '2');

  auto ictx = new ImageCtx(m_image_name, "", nullptr, m_ioctx, true);
  m_ictxs.insert(ictx);
  ASSERT_EQ(0, ictx->state->open(0));
  ASSERT_TRUE(ictx->read_only);

  ASSERT_EQ(0, reopen(ictx, m_ioctx, dst_image_name, "", nullptr));

  // the read-only mode the caller opened the handle with survives
  ASSERT_TRUE(ictx->read_only);
  ASSERT_NO_FATAL_FAILURE(assert_pattern(ictx, '2'));

  bufferlist bl;
  bl.append(std::string(4096, '3'));
  ASSERT_EQ(-EROFS, api::Io<>::write(*ictx, 0, bl.length(), bufferlist{bl}, 0));
}

TEST_F(TestImageCtx, ReopenPreservesQuiesceWatchers) {
  REQUIRE_FORMAT_V2();

  auto dst_image_name = create_image(m_ioctx, m_image_size);

  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  QuiesceWatcher watcher(ictx);
  ASSERT_EQ(0, ictx->state->register_quiesce_watcher(&watcher,
                                                     &watcher.handle));

  ASSERT_EQ(0, reopen(ictx, m_ioctx, dst_image_name, "", nullptr));

  // the handle the caller holds is still registered, and now quiesces for the
  // image the context was re-targeted at
  {
    ImageCtx *dst_ictx;
    ASSERT_EQ(0, open_image(dst_image_name, &dst_ictx));
    ASSERT_EQ(0, snap_create(*dst_ictx, "snap1"));
  }
  ASSERT_TRUE(watcher.wait_for_quiesce(1));

  ASSERT_EQ(0, ictx->state->unregister_quiesce_watcher(watcher.handle));
}

TEST_F(TestImageCtx, ReopenResetsConfigOverrides) {
  REQUIRE_FORMAT_V2();

  auto dst_image_name = create_image(m_ioctx, m_image_size);

  // an override that only the image being re-targeted away from carries
  {
    librbd::Image image;
    ASSERT_EQ(0, m_rbd.open(m_ioctx, image, m_image_name.c_str()));
    ASSERT_EQ(0, image.metadata_set("conf_rbd_sparse_read_threshold_bytes",
                                    "8192"));
  }

  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_EQ(8192U, ictx->sparse_read_threshold_bytes);
  ASSERT_EQ(1U, ictx->config_overrides.count(
                  "rbd_sparse_read_threshold_bytes"));

  uint64_t global_value = ictx->cct->_conf.get_val<Option::size_t>(
    "rbd_sparse_read_threshold_bytes");
  ASSERT_NE(8192U, global_value);

  ASSERT_EQ(0, reopen(ictx, m_ioctx, dst_image_name, "", nullptr));

  ASSERT_TRUE(ictx->config_overrides.empty());
  uint64_t image_value = ictx->config.get_val<Option::size_t>(
    "rbd_sparse_read_threshold_bytes");
  ASSERT_EQ(global_value, image_value);
}

TEST_F(TestImageCtx, ReopenRepeatedly) {
  std::vector<std::string> image_names = {m_image_name};
  for (int i = 0; i < 3; i++) {
    image_names.push_back(create_image(m_ioctx, m_image_size));
  }
  for (size_t i = 0; i < image_names.size(); i++) {
    write_pattern(m_ioctx, image_names[i], '0' + i);
  }

  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(image_names[0], &ictx));
  ASSERT_NO_FATAL_FAILURE(assert_pattern(ictx, '0'));

  for (size_t i = 1; i < image_names.size(); i++) {
    ASSERT_EQ(0, reopen(ictx, m_ioctx, image_names[i]));

    ASSERT_EQ(image_names[i], ictx->name);
    ASSERT_NO_FATAL_FAILURE(assert_pattern(ictx, '0' + i));
  }
}

TEST_F(TestImageCtx, ReopenMissingImage) {
  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  ASSERT_EQ(-ENOENT, reopen(ictx, m_ioctx, "missing-image"));

  // unlike a failed initial open, a failed reopen must leave the image context
  // alive: the caller still holds a handle to it and closes it itself
  close_image(ictx);
}

TEST_F(TestImageCtx, ReopenPreservesUpdateWatchers) {
  REQUIRE_FORMAT_V2();

  auto dst_image_name = create_image(m_ioctx, m_image_size);

  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  UpdateWatcher watcher;
  uint64_t handle;
  ASSERT_EQ(0, ictx->state->register_update_watcher(&watcher, &handle));

  ASSERT_EQ(0, reopen(ictx, m_ioctx, dst_image_name));

  // the handle the caller holds is still registered, and now tracks the image
  // the context was re-targeted at
  {
    ImageCtx *dst_ictx;
    ASSERT_EQ(0, open_image(dst_image_name, &dst_ictx));
    ASSERT_EQ(0, resize(dst_ictx, m_image_size / 2));
  }
  ASSERT_TRUE(watcher.wait_for_notify(1));

  ASSERT_EQ(0, ictx->state->unregister_update_watcher(handle));
}

TEST_F(TestImageCtx, BlockIoParksRequests) {
  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));
  ASSERT_FALSE(ictx->io_image_dispatcher->io_blocked());

  C_SaferCond on_blocked;
  ictx->io_image_dispatcher->block_io(&on_blocked);
  ASSERT_EQ(0, on_blocked.wait());
  ASSERT_TRUE(ictx->io_image_dispatcher->io_blocked());

  bufferlist read_bl;
  read_bl.push_back(bufferptr(4096));
  Context *read_ctx = new DummyContext();
  auto read_comp = io::AioCompletion::create(read_ctx);
  read_comp->get();
  api::Io<>::aio_read(*ictx, read_comp, 0, 4096, io::ReadResult{&read_bl}, 0,
                      true);

  bufferlist bl;
  bl.append(std::string(4096, '1'));
  Context *write_ctx = new DummyContext();
  auto write_comp = io::AioCompletion::create(write_ctx);
  write_comp->get();
  api::Io<>::aio_write(*ictx, write_comp, 0, bl.length(), bufferlist{bl}, 0,
                       true);

  // neither may make progress while IO is parked
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_FALSE(read_comp->is_complete());
  ASSERT_FALSE(write_comp->is_complete());

  ictx->io_image_dispatcher->unblock_io(0);
  ASSERT_FALSE(ictx->io_image_dispatcher->io_blocked());

  ASSERT_EQ(0, read_comp->wait_for_complete());
  ASSERT_EQ(4096, read_comp->get_return_value());
  read_comp->put();

  ASSERT_EQ(0, write_comp->wait_for_complete());
  ASSERT_EQ(0, write_comp->get_return_value());
  write_comp->put();
}

TEST_F(TestImageCtx, BlockIoFailsParkedRequests) {
  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  C_SaferCond on_blocked;
  ictx->io_image_dispatcher->block_io(&on_blocked);
  ASSERT_EQ(0, on_blocked.wait());

  bufferlist bl;
  bl.append(std::string(4096, '1'));
  Context *write_ctx = new DummyContext();
  auto write_comp = io::AioCompletion::create(write_ctx);
  write_comp->get();
  api::Io<>::aio_write(*ictx, write_comp, 0, bl.length(), bufferlist{bl}, 0,
                       true);

  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  ASSERT_FALSE(write_comp->is_complete());

  // a re-target that could not open its image has nowhere to dispatch the
  // requests it parked
  ictx->io_image_dispatcher->unblock_io(-ENOENT);

  ASSERT_EQ(0, write_comp->wait_for_complete());
  ASSERT_EQ(-ENOENT, write_comp->get_return_value());
  write_comp->put();
}

TEST_F(TestImageCtx, ReopenWithConcurrentIo) {
  auto dst_image_name = create_image(m_ioctx, m_image_size);

  ImageCtx *ictx;
  ASSERT_EQ(0, open_image(m_image_name, &ictx));

  std::atomic<bool> done = false;
  std::atomic<int> failed = 0;
  std::atomic<uint64_t> completed = 0;

  // hammer the image while it is re-targeted underneath: every request has to
  // land on whichever image the context points at when it is dispatched, and
  // none may be lost
  std::thread io_thread([&]() {
    bufferlist bl;
    bl.append(std::string(4096, 'x'));
    while (!done) {
      ssize_t r = api::Io<>::write(*ictx, 0, bl.length(), bufferlist{bl}, 0);
      if (r != 4096) {
        ++failed;
        break;
      }

      bufferlist read_bl;
      read_bl.push_back(bufferptr(4096));
      r = api::Io<>::read(*ictx, 0, 4096, io::ReadResult{&read_bl}, 0);
      if (r != 4096) {
        ++failed;
        break;
      }
      ++completed;
    }
  });

  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(0, reopen(ictx, m_ioctx, dst_image_name));
    ASSERT_EQ(0, reopen(ictx, m_ioctx, m_image_name));
  }

  done = true;
  io_thread.join();

  ASSERT_EQ(0, failed);
  ASSERT_GT(completed, 0U);
}

} // namespace librbd
