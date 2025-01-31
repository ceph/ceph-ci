// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 * 
 */

#include "common/debug.h"
#include "ErasureCodeJerasure.h"


extern "C" {
#include "jerasure.h"
#include "reed_sol.h"
#include "galois.h"
#include "cauchy.h"
#include "liberation.h"
}

#define LARGEST_VECTOR_WORDSIZE 16

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout)

using std::ostream;
using std::map;
using std::set;

using ceph::bufferlist;
using ceph::ErasureCodeProfile;

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ErasureCodeJerasure: ";
}


int ErasureCodeJerasure::init(ErasureCodeProfile& profile, ostream *ss)
{
  int err = 0;
  dout(10) << "technique=" << technique << dendl;
  profile["technique"] = technique;
  err |= parse(profile, ss);
  if (err)
    return err;
  prepare();
  return ErasureCode::init(profile, ss);
}

int ErasureCodeJerasure::parse(ErasureCodeProfile &profile,
			       ostream *ss)
{
  int err = ErasureCode::parse(profile, ss);
  err |= to_int("k", profile, &k, DEFAULT_K, ss);
  err |= to_int("m", profile, &m, DEFAULT_M, ss);
  err |= to_int("w", profile, &w, DEFAULT_W, ss);
  if (chunk_mapping.size() > 0 && (int)chunk_mapping.size() != k + m) {
    *ss << "mapping " << profile.find("mapping")->second
	<< " maps " << chunk_mapping.size() << " chunks instead of"
	<< " the expected " << k + m << " and will be ignored" << std::endl;
    chunk_mapping.clear();
    err = -EINVAL;
  }
  err |= sanity_check_k_m(k, m, ss);
  return err;
}

unsigned int ErasureCodeJerasure::get_chunk_size(unsigned int stripe_width) const
{
  unsigned alignment = get_alignment();
  if (per_chunk_alignment) {
    unsigned chunk_size = stripe_width / k;
    if (stripe_width % k)
      chunk_size++;
    dout(20) << "get_chunk_size: chunk_size " << chunk_size
	     << " must be modulo " << alignment << dendl; 
    ceph_assert(alignment <= chunk_size);
    unsigned modulo = chunk_size % alignment;
    if (modulo) {
      dout(10) << "get_chunk_size: " << chunk_size
	       << " padded to " << chunk_size + alignment - modulo << dendl;
      chunk_size += alignment - modulo;
    }
    return chunk_size;
  } else {
    unsigned tail = stripe_width % alignment;
    unsigned padded_length = stripe_width + (tail ? (alignment - tail) : 0);
    ceph_assert(padded_length % k == 0);
    return padded_length / k;
  }
}

int ErasureCodeJerasure::encode_chunks(const shard_id_map<bufferptr> &in, 
                                       shard_id_map<bufferptr> &out)
{
  char *chunks[k + m]; //TODO don't use variable length arrays
  memset(chunks, 0, sizeof(char*) * (k + m));
  uint64_t size = 0;

  for (auto &&[shard, ptr] : in) {
    if (size == 0) size = ptr.length();
    else ceph_assert(size == ptr.length());
    chunks[shard] = const_cast<char*>(ptr.c_str());
  }

  for (auto &&[shard, ptr] : out) {
    if (size == 0) size = ptr.length();
    else ceph_assert(size == ptr.length());
    chunks[shard] = ptr.c_str();
  }

  char *zeros = nullptr;

  for (int i = 0; i < k + m; i++) {
    if (in.contains(i) || out.contains(i)) continue;

    if (zeros == nullptr) {
      zeros = (char*)malloc(size);
      memset(zeros, 0, size);
    }

    chunks[i] = zeros;
  }

  jerasure_encode(&chunks[0], &chunks[k], size);

  if (zeros != nullptr) free(zeros);

  return 0;
}

int ErasureCodeJerasure::decode_chunks(const shard_id_set &want_to_read,
				       const shard_id_map<bufferlist> &chunks,
				       shard_id_map<bufferlist> *decoded)
{
  unsigned blocksize = (*chunks.begin()).second.length();
  int erasures[k + m + 1];
  int erasures_count = 0;
  char *data[k];
  char *coding[m];
  for (int i =  0; i < k + m; i++) {
    if (chunks.find(i) == chunks.end()) {
      erasures[erasures_count] = i;
      erasures_count++;
    }
    if (i < k)
      data[i] = (*decoded)[i].c_str();
    else
      coding[i - k] = (*decoded)[i].c_str();
  }
  erasures[erasures_count] = -1;

  ceph_assert(erasures_count > 0);
  return jerasure_decode(erasures, data, coding, blocksize);
}

void ErasureCodeJerasure::encode_delta(const bufferptr &old_data,
                                       const bufferptr &new_data,
                                       bufferptr *delta)
{
  //TODO handle old_data or new_data being a special zero buffer.
  if (&old_data != delta) {
    memcpy(delta->c_str(), old_data.c_str(), delta->length());
  }
  char * new_data_p = const_cast<char*>(new_data.c_str());
  char * delta_p = delta->c_str();
  galois_region_xor(new_data_p, delta_p, delta->length());

  //TODO return a special zero buffer if delta is all zeroes
}

bool ErasureCodeJerasure::is_prime(int value)
{
  int prime55[] = {
    2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,
    73,79,83,89,97,101,103,107,109,113,127,131,137,139,149,
    151,157,163,167,173,179,
    181,191,193,197,199,211,223,227,229,233,239,241,251,257
  };
  int i;
  for (i = 0; i < 55; i++)
    if (value == prime55[i])
      return true;
  return false;
}

void ErasureCodeJerasure::matrix_apply_delta(const shard_id_map<bufferptr> &in,
                                             shard_id_map<bufferptr> &out,
                                             int k, int w, int *matrix)
{
  auto first = in.begin();
  const unsigned blocksize = first->second.length();

  for (auto const& [datashard, databuf] : in) {
    if (datashard < k) {
      for (auto const& [codingshard, codingbuf] : out) {
        if (codingshard >= k) {
          ceph_assert(codingbuf.length() == blocksize);
          char* input_data = const_cast<char*>(databuf.c_str());
          char* output_data = const_cast<char*>(codingbuf.c_str());
          if (codingshard == k) {
            galois_region_xor(input_data, output_data, blocksize);
          }
          else {
            switch (w) {
              case 8:  
                galois_w08_region_multiply(input_data, matrix[datashard + (k * (codingshard - k))], blocksize, output_data, 1);
                break;
              case 16: 
                galois_w16_region_multiply(input_data, matrix[datashard + (k * (codingshard - k))], blocksize, output_data, 1);
                break;
              case 32: 
                galois_w32_region_multiply(input_data, matrix[datashard + (k * (codingshard - k))], blocksize, output_data, 1);
                break;
            }
          }
          //TODO return a special zero buffer if coding buffer is all zeroes
        }
      }
    }
  }
}

void ErasureCodeJerasure::do_scheduled_ops(char **ptrs, int **operations, int packetsize, int s, int d)
{
  char *sptr;
  char *dptr;
  int op;

  for (op = 0; operations[op][0] >= 0; op++) {    
    if (operations[op][0] == s && operations[op][2] == d) {
      sptr = ptrs[0] + operations[op][1]*packetsize;
      dptr = ptrs[1] + operations[op][3]*packetsize;
      galois_region_xor(sptr, dptr, packetsize);
    }
  }
}

void ErasureCodeJerasure::schedule_apply_delta(const shard_id_map<bufferptr> &in,
                                               shard_id_map<bufferptr> &out,
                                               int k, int w, int packetsize,
                                               int ** simple_schedule)
{
  auto first = in.begin();
  unsigned int blocksize = first->second.length();

  for (auto const& [datashard, databuf] : in) {
    if (datashard < k) {
      for (auto const& [codingshard, codingbuf] : out) {
        if (codingshard >= k) {
          ceph_assert(codingbuf.length() == blocksize);
          char * ptr_copy[2];
          ptr_copy[0] = const_cast<char*>(databuf.c_str());
          ptr_copy[1] = const_cast<char*>(codingbuf.c_str());
          unsigned int done;
          for (done = 0; done < blocksize; done += (packetsize*w)) {
            do_scheduled_ops(ptr_copy, simple_schedule, packetsize, datashard, codingshard);
            ptr_copy[0] += (packetsize*w);
            ptr_copy[1] += (packetsize*w);
          }
        }
      }
    }
  }
    //TODO return a special zero buffer if coding buffer is all zeroes
}

// 
// ErasureCodeJerasureReedSolomonVandermonde
//
void ErasureCodeJerasureReedSolomonVandermonde::jerasure_encode(char **data,
                                                                char **coding,
                                                                int blocksize)
{
  jerasure_matrix_encode(k, m, w, matrix, data, coding, blocksize);
}

int ErasureCodeJerasureReedSolomonVandermonde::jerasure_decode(int *erasures,
                                                                char **data,
                                                                char **coding,
                                                                int blocksize)
{
  return jerasure_matrix_decode(k, m, w, matrix, 1,
				erasures, data, coding, blocksize);
}

void ErasureCodeJerasureReedSolomonVandermonde::apply_delta(const shard_id_map<bufferptr> &in,
                                                            shard_id_map<bufferptr> &out)
{
  matrix_apply_delta(in, out, k, w, matrix);
}

unsigned ErasureCodeJerasureReedSolomonVandermonde::get_alignment() const
{
  if (per_chunk_alignment) {
    return w * LARGEST_VECTOR_WORDSIZE;
  } else {
    unsigned alignment = k*w*sizeof(int);
    if ( ((w*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
      alignment = k*w*LARGEST_VECTOR_WORDSIZE;
    return alignment;
  }
}

int ErasureCodeJerasureReedSolomonVandermonde::parse(ErasureCodeProfile &profile,
						     ostream *ss)
{
  int err = 0;
  err |= ErasureCodeJerasure::parse(profile, ss);
  if (w != 8 && w != 16 && w != 32) {
    *ss << "ReedSolomonVandermonde: w=" << w
	<< " must be one of {8, 16, 32} : revert to " << DEFAULT_W << std::endl;
    err = -EINVAL;
  }
  err |= to_bool("jerasure-per-chunk-alignment", profile,
		 &per_chunk_alignment, "false", ss);
  return err;
}

void ErasureCodeJerasureReedSolomonVandermonde::prepare()
{
  matrix = reed_sol_vandermonde_coding_matrix(k, m, w);
}

// 
// ErasureCodeJerasureReedSolomonRAID6
//
void ErasureCodeJerasureReedSolomonRAID6::jerasure_encode(char **data,
                                                                char **coding,
                                                                int blocksize)
{
  reed_sol_r6_encode(k, w, data, coding, blocksize);
}

int ErasureCodeJerasureReedSolomonRAID6::jerasure_decode(int *erasures,
							 char **data,
							 char **coding,
							 int blocksize)
{
  return jerasure_matrix_decode(k, m, w, matrix, 1, erasures, data, coding, blocksize);
}

void ErasureCodeJerasureReedSolomonRAID6::apply_delta(const shard_id_map<bufferptr> &in,
                                                      shard_id_map<bufferptr> &out)
{
  matrix_apply_delta(in, out, k, w, matrix);
}

unsigned ErasureCodeJerasureReedSolomonRAID6::get_alignment() const
{
  if (per_chunk_alignment) {
    return w * LARGEST_VECTOR_WORDSIZE;
  } else {
    unsigned alignment = k*w*sizeof(int);
    if ( ((w*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
      alignment = k*w*LARGEST_VECTOR_WORDSIZE;
    return alignment;
  }
}

int ErasureCodeJerasureReedSolomonRAID6::parse(ErasureCodeProfile &profile,
					       ostream *ss)
{
  int err = ErasureCodeJerasure::parse(profile, ss);
  if (m != stoi(DEFAULT_M)) {
    *ss << "ReedSolomonRAID6: m=" << m
        << " must be 2 for RAID6: revert to 2" << std::endl;
    err = -EINVAL;
  }
  if (w != 8 && w != 16 && w != 32) {
    *ss << "ReedSolomonRAID6: w=" << w
	<< " must be one of {8, 16, 32} : revert to 8 " << std::endl;
    err = -EINVAL;
  }
  return err;
}

void ErasureCodeJerasureReedSolomonRAID6::prepare()
{
  matrix = reed_sol_r6_coding_matrix(k, w);
}

// 
// ErasureCodeJerasureCauchy
//
void ErasureCodeJerasureCauchy::jerasure_encode(char **data,
						char **coding,
						int blocksize)
{
  jerasure_schedule_encode(k, m, w, schedule,
			   data, coding, blocksize, packetsize);
}

int ErasureCodeJerasureCauchy::jerasure_decode(int *erasures,
					       char **data,
					       char **coding,
					       int blocksize)
{
  return jerasure_schedule_decode_lazy(k, m, w, bitmatrix,
				       erasures, data, coding, blocksize, packetsize, 1);
}

void ErasureCodeJerasureCauchy::apply_delta(const shard_id_map<bufferptr> &in,
                                            shard_id_map<bufferptr> &out)
{
  schedule_apply_delta(in, out, k, w, packetsize, simple_schedule);
}

unsigned ErasureCodeJerasureCauchy::get_alignment() const
{
  if (per_chunk_alignment) {
    unsigned alignment = w * packetsize;
    unsigned modulo = alignment % LARGEST_VECTOR_WORDSIZE;
    if (modulo)
      alignment += LARGEST_VECTOR_WORDSIZE - modulo;
    return alignment;
  } else {
    unsigned alignment = k*w*packetsize*sizeof(int);
    if ( ((w*packetsize*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
      alignment = k*w*packetsize*LARGEST_VECTOR_WORDSIZE;
    return alignment;
  }  
}

int ErasureCodeJerasureCauchy::parse(ErasureCodeProfile &profile,
				     ostream *ss)
{
  int err = ErasureCodeJerasure::parse(profile, ss);
  err |= to_int("packetsize", profile, &packetsize, DEFAULT_PACKETSIZE, ss);
  err |= to_bool("jerasure-per-chunk-alignment", profile,
		 &per_chunk_alignment, "false", ss);
  return err;
}

void ErasureCodeJerasureCauchy::prepare_schedule(int *matrix)
{
  bitmatrix = jerasure_matrix_to_bitmatrix(k, m, w, matrix);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
  simple_schedule = jerasure_dumb_bitmatrix_to_schedule(k, m, w, bitmatrix);
}

ErasureCodeJerasureCauchy::~ErasureCodeJerasureCauchy() 
{
  if (bitmatrix)
    free(bitmatrix);
  if (schedule)
    jerasure_free_schedule(schedule);
  if (simple_schedule)
    jerasure_free_schedule(simple_schedule);
}

// 
// ErasureCodeJerasureCauchyOrig
//
void ErasureCodeJerasureCauchyOrig::prepare()
{
  int *matrix = cauchy_original_coding_matrix(k, m, w);
  prepare_schedule(matrix);
  free(matrix);
}

// 
// ErasureCodeJerasureCauchyGood
//
void ErasureCodeJerasureCauchyGood::prepare()
{
  int *matrix = cauchy_good_general_coding_matrix(k, m, w);
  prepare_schedule(matrix);
  free(matrix);
}

// 
// ErasureCodeJerasureLiberation
//
ErasureCodeJerasureLiberation::~ErasureCodeJerasureLiberation()
{
  if (bitmatrix)
    free(bitmatrix);
  if (schedule)
    jerasure_free_schedule(schedule);
  if (simple_schedule)
    jerasure_free_schedule(simple_schedule);
}

void ErasureCodeJerasureLiberation::jerasure_encode(char **data,
                                                    char **coding,
                                                    int blocksize)
{
  jerasure_schedule_encode(k, m, w, schedule, data,
			   coding, blocksize, packetsize);
}

int ErasureCodeJerasureLiberation::jerasure_decode(int *erasures,
                                                    char **data,
                                                    char **coding,
                                                    int blocksize)
{
  return jerasure_schedule_decode_lazy(k, m, w, bitmatrix, erasures, data,
				       coding, blocksize, packetsize, 1);
}

void ErasureCodeJerasureLiberation::apply_delta(const shard_id_map<bufferptr> &in,
                                                shard_id_map<bufferptr> &out)
{
  schedule_apply_delta(in, out, k, w, packetsize, simple_schedule);
}

unsigned ErasureCodeJerasureLiberation::get_alignment() const
{
  unsigned alignment = k*w*packetsize*sizeof(int);
  if ( ((w*packetsize*sizeof(int))%LARGEST_VECTOR_WORDSIZE) )
    alignment = k*w*packetsize*LARGEST_VECTOR_WORDSIZE;
  return alignment;
}

bool ErasureCodeJerasureLiberation::check_k(ostream *ss) const
{
  if (k > w) {
    *ss << "k=" << k << " must be less than or equal to w=" << w << std::endl;
    return false;
  } else {
    return true;
  }
}

bool ErasureCodeJerasureLiberation::check_w(ostream *ss) const
{
  if (w <= 2 || !is_prime(w)) {
    *ss <<  "w=" << w << " must be greater than two and be prime" << std::endl;
    return false;
  } else {
    return true;
  }
}

bool ErasureCodeJerasureLiberation::check_packetsize_set(ostream *ss) const
{
  if (packetsize == 0) {
    *ss << "packetsize=" << packetsize << " must be set" << std::endl;
    return false;
  } else {
    return true;
  }
}

bool ErasureCodeJerasureLiberation::check_packetsize(ostream *ss) const
{
  if ((packetsize%(sizeof(int))) != 0) {
    *ss << "packetsize=" << packetsize
	<< " must be a multiple of sizeof(int) = " << sizeof(int) << std::endl;
    return false;
  } else {
    return true;
  }
}

int ErasureCodeJerasureLiberation::revert_to_default(ErasureCodeProfile &profile,
						     ostream *ss)
{
  int err = 0;
  *ss << "reverting to k=" << DEFAULT_K << ", w="
      << DEFAULT_W << ", packetsize=" << DEFAULT_PACKETSIZE << std::endl;
  profile["k"] = DEFAULT_K;
  err |= to_int("k", profile, &k, DEFAULT_K, ss);
  profile["w"] = DEFAULT_W;
  err |= to_int("w", profile, &w, DEFAULT_W, ss);
  profile["packetsize"] = DEFAULT_PACKETSIZE;
  err |= to_int("packetsize", profile, &packetsize, DEFAULT_PACKETSIZE, ss);
  return err;
}

int ErasureCodeJerasureLiberation::parse(ErasureCodeProfile &profile,
					 ostream *ss)
{
  int err = ErasureCodeJerasure::parse(profile, ss);
  err |= to_int("packetsize", profile, &packetsize, DEFAULT_PACKETSIZE, ss);

  bool error = false;
  if (!check_k(ss))
    error = true;
  if (!check_w(ss))
    error = true;
  if (!check_packetsize_set(ss) || !check_packetsize(ss))
    error = true;
  if (error) {
    revert_to_default(profile, ss);
    err = -EINVAL;
  }
  return err;
}

void ErasureCodeJerasureLiberation::prepare()
{
  bitmatrix = liberation_coding_bitmatrix(k, w);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
  simple_schedule = jerasure_dumb_bitmatrix_to_schedule(k, m, w, bitmatrix);
}

// 
// ErasureCodeJerasureBlaumRoth
//
bool ErasureCodeJerasureBlaumRoth::check_w(ostream *ss) const
{
  // back in Firefly, w = 7 was the default and produced usable
  // chunks. Tolerate this value for backward compatibility.
  if (w == 7)
    return true;
  if (w <= 2 || !is_prime(w+1)) {
    *ss <<  "w=" << w << " must be greater than two and "
	<< "w+1 must be prime" << std::endl;
    return false;
  } else {
    return true;
  }
}

void ErasureCodeJerasureBlaumRoth::prepare()
{
  bitmatrix = blaum_roth_coding_bitmatrix(k, w);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
  simple_schedule = jerasure_dumb_bitmatrix_to_schedule(k, m, w, bitmatrix);
}

// 
// ErasureCodeJerasureLiber8tion
//
int ErasureCodeJerasureLiber8tion::parse(ErasureCodeProfile &profile,
					 ostream *ss)
{
  int err = ErasureCodeJerasure::parse(profile, ss);
  if (m != stoi(DEFAULT_M)) {
    *ss << "liber8tion: m=" << m << " must be " << DEFAULT_M
        << " for liber8tion: revert to " << DEFAULT_M << std::endl;
    err = -EINVAL;
  }
  if (w != stoi(DEFAULT_W)) {
    *ss << "liber8tion: w=" << w << " must be " << DEFAULT_W
        << " for liber8tion: revert to " << DEFAULT_W << std::endl;
    err = -EINVAL;
  }
  err |= to_int("packetsize", profile, &packetsize, DEFAULT_PACKETSIZE, ss);

  bool error = false;
  if (!check_k(ss))
    error = true;
  if (!check_packetsize_set(ss))
    error = true;
  if (error) {
    revert_to_default(profile, ss);
    err = -EINVAL;
  }
  return err;
}

void ErasureCodeJerasureLiber8tion::prepare()
{
  bitmatrix = liber8tion_coding_bitmatrix(k);
  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
  simple_schedule = jerasure_dumb_bitmatrix_to_schedule(k, m, w, bitmatrix);
}
