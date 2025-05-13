// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "cls/blake3/client.h"
#include "test/librados/test_cxx.h"
#include "gtest/gtest.h"
#include "include/encoding.h"
#include "common/errno.h"
#include "common/ceph_crypto.h"
#include <optional>
#include "BLAKE3/c/blake3.h"
#include <climits>
#include <cstdlib>
#include <iostream>
#include <cmath>
#include <iomanip>
#include <random>
#include <cstdlib>
#include <ctime>

// create/destroy a pool that's shared by all tests in the process
struct RadosEnv : public ::testing::Environment {
  static std::optional<std::string> pool_name;
public:
  static librados::Rados rados;
  static librados::IoCtx ioctx;

  void SetUp() override {
    // create pool
    std::string name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(name, rados));
    pool_name = name;
    ASSERT_EQ(rados.ioctx_create(name.c_str(), ioctx), 0);
  }
  void TearDown() override {
    ioctx.close();
    if (pool_name) {
      ASSERT_EQ(destroy_one_pool_pp(*pool_name, rados), 0);
    }
  }
};

std::optional<std::string> RadosEnv::pool_name;
librados::Rados RadosEnv::rados;
librados::IoCtx RadosEnv::ioctx;
auto *const rados_env = ::testing::AddGlobalTestEnvironment(new RadosEnv);

namespace cls::blake3_hash {

  // test fixture with helper functions
  class Blake3Test : public ::testing::Test {
  protected:
    librados::IoCtx& ioctx = RadosEnv::ioctx;

    //---------------------------------------------------------------------------
    bool do_blake3_hash(const std::string& oid)
    {
      bufferlist bl;
      librados::ObjectReadOperation op;
      int ret = ioctx.operate(oid, &op, &bl, 0);
      return ret;
    }

  };

  //---------------------------------------------------------------------------
  static void print_hash(const char* name, const uint8_t *p_hash)
  {
    uint64_t *p = (uint64_t*)p_hash;
    std::cerr << name << std::hex << *p << *(p+1) << *(p+2) << *(p+3) << std::endl;
  }

  char buff[4*1024*1024];

  //---------------------------------------------------------------------------
  void blake3_cls(const std::vector<std::string> &vec,
                  librados::IoCtx &ioctx,
                  uint8_t *p_hash)
  {
    blake3_hasher hmac;
    const unsigned first_part_idx = 0;
    const unsigned last_part_idx = (vec.size() - 1);
    unsigned idx = 0;
    for (const auto& oid : vec ) {
      librados::ObjectReadOperation op;
      cls_blake3_flags_t flags;
      bufferlist out_bl;
      bufferlist blake3_state_bl;
      if (idx == first_part_idx) {
        flags.set_first_part();
      }
      else {
        blake3_state_bl.append((const char*)&hmac, sizeof(hmac));
      }
      if (idx == last_part_idx) {
        flags.set_last_part();
      }
      ASSERT_EQ(0, blake3_hash_data(op, &blake3_state_bl, &out_bl, flags));

      int ret = ioctx.operate(oid, &op, nullptr, 0);
      if (ret != 0) {
        std::cerr << "ERROR: ret=" << ret << "::" << cpp_strerror(-ret) << std::endl;
      }
      ASSERT_EQ(0, ret);

      const char *p_bl = out_bl.c_str();
      if (idx == last_part_idx) {
        ASSERT_EQ(out_bl.length(), BLAKE3_OUT_LEN);
        memcpy((char*)p_hash, p_bl, out_bl.length());
      }
      else {
        ASSERT_EQ(out_bl.length(), sizeof(hmac));
        memcpy((char*)&hmac, p_bl, out_bl.length());
      }

      idx++;
    }
  }

  //---------------------------------------------------------------------------
  void blake3_lcl(const std::vector<std::string> &vec,
                  const std::vector<unsigned> &size_vec,
                  librados::IoCtx &ioctx,
                  uint8_t *p_hash)
  {
    blake3_hasher hmac;
    blake3_hasher_init(&hmac);
    unsigned idx = 0;
    for (const auto& oid : vec ) {
      bufferlist bl;
      int ret = ioctx.read(oid, bl, 0, 0);
      ASSERT_EQ(ret, size_vec[idx++]);
      for (const auto& bptr : bl.buffers()) {
        blake3_hasher_update(&hmac, (const unsigned char *)bptr.c_str(), bptr.length());
      }
    }

    memset(p_hash, 0, BLAKE3_OUT_LEN);
    blake3_hasher_finalize(&hmac, p_hash, BLAKE3_OUT_LEN);
  }

  //---------------------------------------------------------------------------
  void fill_buff_with_rand_data(char *buff, uint32_t size)
  {
    // Seed with a real random value, if available
    std::random_device r;
    // Choose a random mean between 1 ULLONG_MAX
    std::default_random_engine e1(r());
    std::uniform_int_distribution<uint64_t> uniform_dist(1, std::numeric_limits<uint64_t>::max());
    uint64_t *p_start = (uint64_t*)buff;
    uint64_t *p_end   = (uint64_t*)(buff + size);
    for (auto p = p_start; p < p_end; p++) {
      *p = uniform_dist(e1);
    }
  }

  //---------------------------------------------------------------------------
  void write_obj(const std::string &oid, librados::IoCtx &ioctx, char *buff, uint32_t size)
  {
    bufferlist bl ;
    bl.append(buff, size);
    int ret = ioctx.write_full(oid, bl);
    ASSERT_EQ(ret, (int)bl.length());
  }

  //---------------------------------------------------------------------------
  void write_obj_rand_data(const std::string &oid, librados::IoCtx &ioctx, uint32_t size)
  {
    fill_buff_with_rand_data(buff, size);
    write_obj(oid, ioctx, buff, size);
  }

  //---------------------------------------------------------------------------
  TEST_F(Blake3Test, hash_single_object)
  {
    const std::string funcname(__PRETTY_FUNCTION__);
    std::vector<std::string> vec;
    std::vector<unsigned> size_vec;
    const unsigned MAX_OBJS = 17;

    uint8_t hash1[BLAKE3_OUT_LEN];
    uint8_t hash2[BLAKE3_OUT_LEN];
    uint8_t hash3[BLAKE3_OUT_LEN];

    blake3_hasher hmac3;
    blake3_hasher_init(&hmac3);
    memset(hash3, 0, sizeof(hash3));

    std::srand(std::time({})); // use current time as seed for random generator
    for (unsigned i = 0; i < MAX_OBJS; i++) {
      const std::string oid = funcname + std::to_string(i);
      vec.push_back(oid);
      uint32_t size = sizeof(buff) - (std::rand() % (sizeof(buff)/2));
      //std::cerr << "size=" << size << std::endl;
      size_vec.push_back(size);
      fill_buff_with_rand_data(buff, size);
      blake3_hasher_update(&hmac3, (const unsigned char *)buff, size);
      write_obj(oid, ioctx, buff, size);
    }

    blake3_cls(vec, ioctx, hash1);
    print_hash("XCLS::", hash1);
    blake3_lcl(vec, size_vec, ioctx, hash2);
    print_hash("READ::", hash2);
    blake3_hasher_finalize(&hmac3, hash3, BLAKE3_OUT_LEN);
    print_hash("BUFF::", hash3);

    ASSERT_EQ(memcmp(hash1, hash2, sizeof(hash1)), 0);
    ASSERT_EQ(memcmp(hash1, hash3, sizeof(hash1)), 0);
  }

  //---------------------------------------------------------------------------
  TEST_F(Blake3Test, hash_single_object_single_part)
  {
    const std::string funcname(__PRETTY_FUNCTION__);
    std::vector<std::string> vec;
    std::vector<unsigned> size_vec;
    const unsigned MAX_OBJS = 1;

    uint8_t hash1[BLAKE3_OUT_LEN];
    uint8_t hash2[BLAKE3_OUT_LEN];
    uint8_t hash3[BLAKE3_OUT_LEN];

    blake3_hasher hmac3;
    blake3_hasher_init(&hmac3);
    memset(hash3, 0, sizeof(hash3));

    std::srand(std::time({})); // use current time as seed for random generator
    for (unsigned i = 0; i < MAX_OBJS; i++) {
      const std::string oid = funcname + std::to_string(i);
      vec.push_back(oid);
      uint32_t size = sizeof(buff) - (std::rand() % (sizeof(buff)/2));
      //std::cerr << "size=" << size << std::endl;
      size_vec.push_back(size);
      fill_buff_with_rand_data(buff, size);
      blake3_hasher_update(&hmac3, (const unsigned char *)buff, size);
      write_obj(oid, ioctx, buff, size);
    }

    blake3_cls(vec, ioctx, hash1);
    print_hash("XCLS::", hash1);
    blake3_lcl(vec, size_vec, ioctx, hash2);
    print_hash("READ::", hash2);
    blake3_hasher_finalize(&hmac3, hash3, BLAKE3_OUT_LEN);
    print_hash("BUFF::", hash3);

    ASSERT_EQ(memcmp(hash1, hash2, sizeof(hash1)), 0);
    ASSERT_EQ(memcmp(hash1, hash3, sizeof(hash1)), 0);
  }

  //---------------------------------------------------------------------------
  TEST_F(Blake3Test, hash_2_identical_objects)
  {
    const std::string funcname(__PRETTY_FUNCTION__);
    std::vector<std::string> vec1, vec2;
    const unsigned MAX_OBJS = 3;

    uint8_t hash1[BLAKE3_OUT_LEN];
    uint8_t hash2[BLAKE3_OUT_LEN];

    std::srand(std::time({})); // use current time as seed for random generator
    for (unsigned i = 0; i < MAX_OBJS; i++) {
      const std::string oid1 = funcname + std::to_string(i);
      const std::string oid2 = funcname + std::to_string(i) + "_b";
      vec1.push_back(oid1);
      vec2.push_back(oid2);
      uint32_t size = sizeof(buff) - (std::rand() % (sizeof(buff)/2));
      write_obj(oid1, ioctx, buff, size);
      write_obj(oid2, ioctx, buff, size);
    }

    blake3_cls(vec1, ioctx, hash1);
    print_hash("VEC1::", hash1);
    blake3_cls(vec2, ioctx, hash2);
    print_hash("VEC2::", hash2);

    ASSERT_EQ(memcmp(hash1, hash2, sizeof(hash1)), 0);
  }

  //---------------------------------------------------------------------------
  TEST_F(Blake3Test, hash_2_identical_objects_single_part)
  {
    const std::string funcname(__PRETTY_FUNCTION__);
    std::vector<std::string> vec1, vec2;
    const unsigned MAX_OBJS = 1;

    uint8_t hash1[BLAKE3_OUT_LEN];
    uint8_t hash2[BLAKE3_OUT_LEN];

    std::srand(std::time({})); // use current time as seed for random generator
    for (unsigned i = 0; i < MAX_OBJS; i++) {
      const std::string oid1 = funcname + std::to_string(i);
      const std::string oid2 = funcname + std::to_string(i) + "_b";
      vec1.push_back(oid1);
      vec2.push_back(oid2);
      uint32_t size = sizeof(buff) - (std::rand() % (sizeof(buff)/2));
      write_obj(oid1, ioctx, buff, size);
      write_obj(oid2, ioctx, buff, size);
    }

    blake3_cls(vec1, ioctx, hash1);
    print_hash("VEC1::", hash1);
    blake3_cls(vec2, ioctx, hash2);
    print_hash("VEC2::", hash2);

    ASSERT_EQ(memcmp(hash1, hash2, sizeof(hash1)), 0);
  }

  //---------------------------------------------------------------------------
  TEST_F(Blake3Test, hash_2_non_identical_objects)
  {
    const std::string funcname(__PRETTY_FUNCTION__);
    std::vector<std::string> vec1, vec2;
    const unsigned MAX_OBJS = 3;

    uint8_t hash1[BLAKE3_OUT_LEN];
    uint8_t hash2[BLAKE3_OUT_LEN];

    std::srand(std::time({})); // use current time as seed for random generator
    for (unsigned i = 0; i < MAX_OBJS; i++) {
      const std::string oid1 = funcname + std::to_string(i);
      const std::string oid2 = funcname + std::to_string(i) + "_b";
      vec1.push_back(oid1);
      vec2.push_back(oid2);
      uint32_t size = sizeof(buff) - (std::rand() % (sizeof(buff)/2));
      write_obj(oid1, ioctx, buff, size);
      // change one byte in the second object
      if (i == MAX_OBJS - 1) {
        buff[0]++;
      }
      write_obj(oid2, ioctx, buff, size);
    }

    blake3_cls(vec1, ioctx, hash1);
    print_hash("VEC1::", hash1);
    blake3_cls(vec2, ioctx, hash2);
    print_hash("VEC2::", hash2);

    ASSERT_NE(memcmp(hash1, hash2, sizeof(hash1)), 0);
  }

  //---------------------------------------------------------------------------
  TEST_F(Blake3Test, bad_input_overflow_first_part)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    write_obj_rand_data(oid, ioctx, sizeof(buff));

    bufferlist out_bl;
    bufferlist blake3_state_bl;
    char junk[16];
    blake3_state_bl.append((const char*)&junk, sizeof(junk));
    librados::ObjectReadOperation op;
    cls_blake3_flags_t flags;
    flags.set_first_part();
    ASSERT_EQ(0, blake3_hash_data(op, &blake3_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EINVAL, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(Blake3Test, bad_input_overflow)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    write_obj_rand_data(oid, ioctx, sizeof(buff));

    blake3_hasher hmac;
    blake3_hasher_init(&hmac);
    bufferlist out_bl;
    bufferlist blake3_state_bl;
    blake3_state_bl.append((const char*)&hmac, sizeof(hmac));
    char junk[16];
    blake3_state_bl.append((const char*)&junk, sizeof(junk));
    librados::ObjectReadOperation op;
    cls_blake3_flags_t flags;
    ASSERT_EQ(0, blake3_hash_data(op, &blake3_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EINVAL, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(Blake3Test, bad_input_underflow)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    write_obj_rand_data(oid, ioctx, sizeof(buff));

    blake3_hasher hmac;
    blake3_hasher_init(&hmac);
    bufferlist out_bl;
    bufferlist blake3_state_bl;
    blake3_state_bl.append((const char*)&hmac, sizeof(hmac)-16);
    librados::ObjectReadOperation op;
    cls_blake3_flags_t flags;
    ASSERT_EQ(0, blake3_hash_data(op, &blake3_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EINVAL, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(Blake3Test, bad_input_corrupted_stack_len)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    write_obj_rand_data(oid, ioctx, sizeof(buff));

    blake3_hasher hmac;
    blake3_hasher_init(&hmac);
    hmac.cv_stack_len = (BLAKE3_MAX_DEPTH + 2);
    bufferlist out_bl;
    bufferlist blake3_state_bl;
    blake3_state_bl.append((const char*)&hmac, sizeof(hmac));
    librados::ObjectReadOperation op;
    cls_blake3_flags_t flags;
    ASSERT_EQ(0, blake3_hash_data(op, &blake3_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EINVAL, ret);
  }

  //---------------------------------------------------------------------------
  TEST_F(Blake3Test, bad_input_corrupted_chunk_buf_len)
  {
    const std::string oid = __PRETTY_FUNCTION__;
    write_obj_rand_data(oid, ioctx, sizeof(buff));

    blake3_hasher hmac;
    blake3_hasher_init(&hmac);
    hmac.chunk.buf_len = (BLAKE3_BLOCK_LEN + 1);
    bufferlist out_bl;
    bufferlist blake3_state_bl;
    blake3_state_bl.append((const char*)&hmac, sizeof(hmac));
    librados::ObjectReadOperation op;
    cls_blake3_flags_t flags;
    ASSERT_EQ(0, blake3_hash_data(op, &blake3_state_bl, &out_bl, flags));
    int ret = ioctx.operate(oid, &op, nullptr, 0);
    ASSERT_EQ(-EINVAL, ret);
  }

} // namespace cls::blake3_hash
