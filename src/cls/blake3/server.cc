// -*- mode:C++; tab-width:8; c-basic-offset:2
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM Corp.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "objclass/objclass.h"
#include "ops.h"
#include "common/ceph_crypto.h"
#include "BLAKE3/c/blake3.h"
//#include "../rgw/rgw_blake3_digest.h"
//using namespace rgw::digest;
#include "common/errno.h"
CLS_VER(1,0)
CLS_NAME(blake3)

using namespace cls::blake3_hash;

//---------------------------------------------------------------------------
[[maybe_unused]]static std::string stringToHex(const std::string& input)
{
  std::stringstream ss;
  for (char c : input) {
    ss << std::hex << std::setw(2) << std::setfill('0')
       << static_cast<int>(static_cast<unsigned char>(c));
  }
  return ss.str();
}

//------------------------------------------------------------------------------
static int hash_data(cls_method_context_t hctx, bufferlist *in, bufferlist *out)
{
  CLS_LOG(0, "%s: was called", __func__);
  cls_blake3_op op;
  try {
    auto p = in->cbegin();
    decode(op, p);
  } catch (const buffer::error&) {
    CLS_LOG(0, "ERROR: %s: failed to decode input", __func__);
    return -EINVAL;
  }
  CLS_LOG(0, "%s: op.flags=0x%X", __func__, op.flags.get_flags());

  if (op.flags.is_first_part()) {
    if (unlikely(op.blake3_state_bl.length() > 0)) {
      CLS_LOG(0, "ERROR: %s: Non empty blake3_state_bl on first chunk (%u)",
              __func__, op.blake3_state_bl.length());
      return -EINVAL;
    }
  }
  else if (unlikely(op.blake3_state_bl.length() != sizeof(blake3_hasher))) {
    CLS_LOG(0, "ERROR: %s: bad blake3_state_bl len (%u/%lu)",
            __func__, op.blake3_state_bl.length(), sizeof(blake3_hasher));
    return -EINVAL;
  }

  // TBD: Should we follow deep scrub behavior and bypass ObjectStore cache using
  // CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE ???
  uint32_t read_flags = CEPH_OSD_OP_FLAG_FADVISE_NOCACHE;
  int ofs = 0, len = 0;
  ceph::buffer::list bl;
  int ret = cls_cxx_read2(hctx, ofs, len, &bl, read_flags);
  CLS_LOG(0, "%s: cls_cxx_read2() = %d", __func__, ret);
  if (ret < 0) {
    CLS_LOG(0, "%s:: failed cls_cxx_read2() ret=%d (%s)",
            __func__, ret, cpp_strerror(-ret).c_str());
    return ret;
  }

  blake3_hasher hmac;
  if (op.flags.is_first_part()) {
    blake3_hasher_init(&hmac);
  }
  else {
    const char *p_bl = op.blake3_state_bl.c_str();
    memcpy((char*)&hmac, p_bl, op.blake3_state_bl.length());
    if (hmac.cv_stack_len > (BLAKE3_MAX_DEPTH + 1) ||
        hmac.chunk.buf_len > BLAKE3_BLOCK_LEN) {
      CLS_LOG(0, "ERROR: %s: bad blake3_state_bl", __func__);
      return -EINVAL;
    }
  }


  //Blake3 hmac(op.blake3_state_bl);
  for (const auto& bptr : bl.buffers()) {
    //hmac.Update((const unsigned char *)bptr.c_str(), bptr.length());
    blake3_hasher_update(&hmac, (const unsigned char *)bptr.c_str(), bptr.length());
  }

  //set the results in the returned bl
  if (op.flags.is_last_part()) {
    uint8_t hash[BLAKE3_OUT_LEN];
    blake3_hasher_finalize(&hmac, hash, BLAKE3_OUT_LEN);
    CLS_LOG(0, "%s: last part hash=%s", __func__,
            stringToHex(std::string((const char*)hash, BLAKE3_OUT_LEN)).c_str());
    out->append((const char *)hash, BLAKE3_OUT_LEN);
  }
  else {
    const char *p_hmac = (const char *)&hmac;
    CLS_LOG(0, "%s: out.length()=%d, sizeof(hmac)=%ld", __func__,
            out->length(), sizeof(hmac));
    out->append(p_hmac, sizeof(blake3_hasher));
    //hmac.Serialize(*out);
  }

  return 0;
}

//------------------------------------------------------------------------------
CLS_INIT(blake3)
{
  CLS_LOG(0, "Loaded hash class (blake3)!");
  cls_handle_t h_class;
  cls_method_handle_t h_hash_data;
  cls_register("blake3", &h_class);
  cls_register_cxx_method(h_class, "hash_data", CLS_METHOD_RD, hash_data,
                          &h_hash_data);
}
