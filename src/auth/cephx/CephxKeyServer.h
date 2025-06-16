// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_KEYSSERVER_H
#define CEPH_KEYSSERVER_H

#include <boost/intrusive_ptr.hpp>

#include "auth/KeyRing.h"
#include "CephxProtocol.h"
#include "common/ceph_mutex.h"
#include "include/common_fwd.h"

struct KeyServerData {
  version_t version{0};

  /* for each entity */
  std::map<EntityName, EntityAuth> secrets;
  KeyRing *extra_secrets = nullptr;

  /* for each service type */
  version_t rotating_ver{0};
  std::map<uint32_t, RotatingSecrets> rotating_secrets;
  KeyServerData() {}

  explicit KeyServerData(KeyRing *extra)
    : version(0),
      extra_secrets(extra),
      rotating_ver(0) {}

  void encode(ceph::buffer::list& bl) const {
     __u8 struct_v = 1;
    using ceph::encode;
    encode(struct_v, bl);
    encode(version, bl);
    encode(rotating_ver, bl);
    encode(secrets, bl);
    encode(rotating_secrets, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    using ceph::decode;
    __u8 struct_v;
    decode(struct_v, bl);
    decode(version, bl);
    decode(rotating_ver, bl);
    decode(secrets, bl);
    decode(rotating_secrets, bl);
  }

  void encode_rotating(ceph::buffer::list& bl) const {
    using ceph::encode;
     __u8 struct_v = 1;
    encode(struct_v, bl);
    encode(rotating_ver, bl);
    encode(rotating_secrets, bl);
  }
  void decode_rotating(ceph::buffer::list& rotating_bl) {
    using ceph::decode;
    auto iter = rotating_bl.cbegin();
    __u8 struct_v;
    decode(struct_v, iter);
    decode(rotating_ver, iter);
    decode(rotating_secrets, iter);
  }
  void dump(ceph::Formatter *f) const {
    f->dump_unsigned("version", version);
    f->dump_unsigned("rotating_version", rotating_ver);
    f->open_array_section("secrets");
    for (auto const& [name, auth] : secrets) {
      f->open_object_section("secret");
      f->dump_object("entity", name);
      f->dump_object("auth", auth);
      f->close_section();
    }
    f->close_section();
    f->open_array_section("rotating_secrets");
    for (auto const& [entity_type, secrets] : rotating_secrets) {
      f->open_object_section("rotating_secret");
      auto name = EntityName(entity_type);
      f->dump_object("entity", name);
      f->dump_object("secrets", secrets);
      f->close_section();
    }
    f->close_section();
  }
  static void generate_test_instances(std::list<KeyServerData*>& ls) {
    ls.push_back(new KeyServerData);
    ls.push_back(new KeyServerData);
    ls.back()->version = 1;
  }
  bool contains(const EntityName& name) const {
    return (secrets.find(name) != secrets.end());
  }

  void clear_secrets() {
    version = 0;
    secrets.clear();
    rotating_ver = 0;
    rotating_secrets.clear();
  }

  void add_auth(const EntityName& name, const EntityAuth& auth) {
    secrets[name] = auth;
  }

  void remove_secret(const EntityName& name) {
    auto iter = secrets.find(name);
    if (iter == secrets.end())
      return;
    secrets.erase(iter);
  }

  bool get_service_secret(CephContext *cct, uint32_t service_id,
			  CryptoKey& secret, uint64_t& secret_id,
			  double& ttl) const;
  bool get_service_secret(CephContext *cct, uint32_t service_id,
			  uint64_t secret_id, CryptoKey& secret) const;
  bool get_auth(CephContext *cct, const EntityName& name, EntityAuth& auth) const;
  bool get_secret(CephContext *cct, const EntityName& name, CryptoKey& secret) const;
  bool get_caps(CephContext *cct, const EntityName& name,
		const std::string& type, AuthCapsInfo& caps) const;

  auto const& get_secrets() const { return secrets; }
  std::map<EntityName, EntityAuth>::iterator secrets_begin()
  { return secrets.begin(); }
  std::map<EntityName, EntityAuth>::const_iterator secrets_begin() const 
  { return secrets.begin(); }
  std::map<EntityName, EntityAuth>::iterator secrets_end()
  { return secrets.end(); }
  std::map<EntityName, EntityAuth>::const_iterator secrets_end() const
  { return secrets.end(); }
  std::map<EntityName, EntityAuth>::iterator find_name(const EntityName& name)
  { return secrets.find(name); }
  std::map<EntityName, EntityAuth>::const_iterator find_name(const EntityName& name) const
  { return secrets.find(name); }


  // -- incremental updates --
  typedef enum {
    AUTH_INC_NOP,
    AUTH_INC_ADD,
    AUTH_INC_DEL,
    AUTH_INC_SET_ROTATING,
  } IncrementalOp;

  struct Incremental {
    IncrementalOp op;
    ceph::buffer::list rotating_bl;  // if SET_ROTATING.  otherwise,
    EntityName name;
    EntityAuth auth;

    void encode(ceph::buffer::list& bl) const {
      using ceph::encode;
      __u8 struct_v = 1;
      encode(struct_v, bl);
     __u32 _op = (__u32)op;
      encode(_op, bl);
      if (op == AUTH_INC_SET_ROTATING) {
	encode(rotating_bl, bl);
      } else {
	encode(name, bl);
	encode(auth, bl);
      }
    }
    void decode(ceph::buffer::list::const_iterator& bl) {
      using ceph::decode;
      __u8 struct_v;
      decode(struct_v, bl);
      __u32 _op;
      decode(_op, bl);
      op = (IncrementalOp)_op;
      ceph_assert(op >= AUTH_INC_NOP && op <= AUTH_INC_SET_ROTATING);
      if (op == AUTH_INC_SET_ROTATING) {
	decode(rotating_bl, bl);
      } else {
	decode(name, bl);
	decode(auth, bl);
      }
    }
    void dump(ceph::Formatter *f) const {
      f->dump_unsigned("op", op);
      f->dump_object("name", name);
      f->dump_object("auth", auth);
    }
    static void generate_test_instances(std::list<Incremental*>& ls) {
      ls.push_back(new Incremental);
      ls.back()->op = AUTH_INC_DEL;
      ls.push_back(new Incremental);
      ls.back()->op = AUTH_INC_ADD;
      ls.push_back(new Incremental);
      ls.back()->op = AUTH_INC_SET_ROTATING;
    }
  };
  
  void apply_incremental(Incremental& inc) {
    switch (inc.op) {
    case AUTH_INC_ADD:
      add_auth(inc.name, inc.auth);
      break;
      
    case AUTH_INC_DEL:
      remove_secret(inc.name);
      break;

    case AUTH_INC_SET_ROTATING:
      decode_rotating(inc.rotating_bl);
      break;

    case AUTH_INC_NOP:
      break;

    default:
      ceph_abort();
    }
  }

};
WRITE_CLASS_ENCODER(KeyServerData)
WRITE_CLASS_ENCODER(KeyServerData::Incremental)


class KeyServer : public KeyStore {
  boost::intrusive_ptr<CephContext> kscct;
  KeyServerData data;
  std::map<EntityName, CryptoKey> used_pending_keys;
  mutable ceph::mutex lock;

  int _rotate_secret(uint32_t service_id, KeyServerData &pending_data);
  void _dump_rotating_secrets();
  int _build_session_auth_info(uint32_t service_id, 
			       const AuthTicket& parent_ticket,
                               std::optional<int> key_type,
			       CephXSessionAuthInfo& info,
			       double ttl);
  bool _get_service_caps(const EntityName& name, uint32_t service_id,
	AuthCapsInfo& caps) const;
public:
  KeyServer() : lock{ceph::make_mutex("KeyServer::lock")} {}
  KeyServer(CephContext *cct_, KeyRing *extra_secrets);
  KeyServer& operator=(const KeyServer&) = delete;
  bool generate_secret(CryptoKey& secret, std::optional<int> type = std::nullopt);

  auto const& get_rotating_secrets() const {
    return data.rotating_secrets;
  }

  bool get_secret(const EntityName& name, CryptoKey& secret) const override;
  bool get_auth(const EntityName& name, EntityAuth& auth) const;
  bool get_caps(const EntityName& name, const std::string& type, AuthCapsInfo& caps) const;
  bool get_active_rotating_secret(const EntityName& name, CryptoKey& secret) const;

  void note_used_pending_key(const EntityName& name, const CryptoKey& key);
  void clear_used_pending_keys();
  std::map<EntityName,CryptoKey> get_used_pending_keys();

  int start_server();
  void rotate_timeout(double timeout);

  void dump();
  
  int build_session_auth_info(uint32_t service_id,
			      const AuthTicket& parent_ticket,
                              std::optional<int> key_type,
			      CephXSessionAuthInfo& info);
  int build_session_auth_info(uint32_t service_id,
			      const AuthTicket& parent_ticket,
			      const CryptoKey& service_secret,
			      uint64_t secret_id,
                              std::optional<int> key_type,
			      CephXSessionAuthInfo& info);

  /* get current secret for specific service type */
  bool get_service_secret(uint32_t service_id, CryptoKey& secret,
			  uint64_t& secret_id, double& ttl) const;
  bool get_service_secret(uint32_t service_id, uint64_t secret_id,
			  CryptoKey& secret) const override;

  void encode(ceph::buffer::list& bl) const {
    using ceph::encode;
    encode(data, bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    std::scoped_lock l{lock};
    using ceph::decode;
    decode(data, bl);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<KeyServer*>& ls);
  bool contains(const EntityName& name) const;
  int encode_secrets(ceph::Formatter *f, std::stringstream *ds) const;
  void encode_formatted(std::string label, ceph::Formatter *f, ceph::buffer::list &bl);
  void encode_plaintext(ceph::buffer::list &bl);
  int list_secrets(std::stringstream& ds) const {
    return encode_secrets(NULL, &ds);
  }
  version_t get_ver() const {
    std::scoped_lock l{lock};
    return data.version;
  }

  void clear_secrets() {
    std::scoped_lock l{lock};
    data.clear_secrets();
  }

  void apply_data_incremental(KeyServerData::Incremental& inc) {
    std::scoped_lock l{lock};
    data.apply_incremental(inc);
  }
  void set_ver(version_t ver) {
    std::scoped_lock l{lock};
    data.version = ver;
  }

  void add_auth(const EntityName& name, const EntityAuth& auth) {
    std::scoped_lock l{lock};
    data.add_auth(name, auth);
  }

  void remove_secret(const EntityName& name) {
    std::scoped_lock l{lock};
    data.remove_secret(name);
  }

  bool has_secrets() {
    auto b = data.secrets_begin();
    return (b != data.secrets_end());
  }
  int get_num_secrets() {
    std::scoped_lock l{lock};
    return data.secrets.size();
  }

  void clone_to(KeyServerData& dst) const {
    std::scoped_lock l{lock};
    dst = data;
  }
  void export_keyring(KeyRing& keyring) {
    std::scoped_lock l{lock};
    for (auto p = data.secrets.begin(); p != data.secrets.end(); ++p) {
      keyring.add(p->first, p->second);
    }
  }

  bool prepare_rotating_update(ceph::buffer::list& rotating_bl, bool wipe);

  bool get_rotating_encrypted(const EntityName& name, ceph::buffer::list& enc_bl) const;

  ceph::mutex& get_lock() const { return lock; }
  bool get_service_caps(const EntityName& name, uint32_t service_id,
			AuthCapsInfo& caps) const;

  auto const& get_secrets() const { return data.get_secrets(); }
  std::map<EntityName, EntityAuth>::iterator secrets_begin()
  { return data.secrets_begin(); }
  std::map<EntityName, EntityAuth>::iterator secrets_end()
  { return data.secrets_end(); }

  virtual int get_service_cipher() const {
    return CEPH_CRYPTO_AES256KRB5;
  }
  virtual bool is_cipher_allowed(int cipher) const {
    return cipher == CEPH_CRYPTO_AES256KRB5;
  }
  virtual std::vector<int> get_ciphers_allowed() const {
    return {CEPH_CRYPTO_AES256KRB5};
  }
};
WRITE_CLASS_ENCODER(KeyServer)


#endif
