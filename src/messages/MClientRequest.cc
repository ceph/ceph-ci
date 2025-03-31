#include "common/debug.h"

#include "messages/MClientRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds." << node_id << ".server "

std::atomic<int> MClientRequest::counter = 0;

MClientRequest::MClientRequest()
  : MMDSOp(CEPH_MSG_CLIENT_REQUEST, HEAD_VERSION, COMPAT_VERSION) {
  memset(&head, 0, sizeof(head));
  head.owner_uid = -1;
  head.owner_gid = -1;
  int c = counter.fetch_add(1);
  id = ++c;
  dout(20) << __func__ << " ctor(" << id << ")" << dendl;
}

MClientRequest::MClientRequest(int op, feature_bitset_t features /* = 0 */)
  : MMDSOp(CEPH_MSG_CLIENT_REQUEST, HEAD_VERSION, COMPAT_VERSION) {
  memset(&head, 0, sizeof(head));
  head.op = op;
  mds_features = features;
  head.owner_uid = -1;
  head.owner_gid = -1;
  int c = counter.fetch_add(1);
  id = ++c;
  dout(20) << __func__ << " ctor(" << id << ")" << dendl;
}

MClientRequest::~MClientRequest()
{
  dout(20) << __func__ << " dtor(" << id << ") "
	   << " (" << std::hex << this << std::dec << "): "
	   << *this
	   << dendl;
}

void MClientRequest::decode_payload()
{
  using ceph::decode;
  auto p = payload.cbegin();

  if (header.version >= 4) {
    decode(head, p);
  } else {
    struct ceph_mds_request_head_legacy old_mds_head;

    decode(old_mds_head, p);
    copy_from_legacy_head(&head, &old_mds_head);
    head.version = 0;

    head.ext_num_retry = head.num_retry;
    head.ext_num_fwd = head.num_fwd;

    head.owner_uid = head.caller_uid;
    head.owner_gid = head.caller_gid;

    /* Can't set the btime from legacy struct */
    if (head.op == CEPH_MDS_OP_SETATTR) {
      int localmask = head.args.setattr.mask;

      localmask &= ~CEPH_SETATTR_BTIME;

      head.args.setattr.btime = { ceph_le32(0), ceph_le32(0) };
      head.args.setattr.mask = localmask;
    }
  }

  decode(path, p);
  decode(path2, p);
  ceph::decode_nohead(head.num_releases, releases, p);
  if (header.version >= 2)
    decode(stamp, p);
  if (header.version >= 4) // epoch 3 was for a ceph_mds_request_args change
    decode(gid_list, p);
  if (header.version >= 5)
    decode(alternate_name, p);
  if (header.version >= 6) {
    decode(fscrypt_auth, p);
    decode(fscrypt_file, p);
  }
}

void MClientRequest::encode_payload(uint64_t features)
{
  using ceph::encode;
  head.num_releases = releases.size();
  /*
   * If the peer is old version, we must skip all the
   * new members, because the old version of MDS or
   * client will just copy the 'head' memory and isn't
   * that smart to skip them.
   */
  if (!mds_features.test(CEPHFS_FEATURE_32BITS_RETRY_FWD)) {
    head.version = 1;
  } else if (!mds_features.test(CEPHFS_FEATURE_HAS_OWNER_UIDGID)) {
    head.version = 2;
  } else {
    head.version = CEPH_MDS_REQUEST_HEAD_VERSION;
  }

  if (features & CEPH_FEATURE_FS_BTIME) {
    encode(head, payload);
  } else {
    struct ceph_mds_request_head_legacy old_mds_head;

    copy_to_legacy_head(&old_mds_head, &head);
    encode(old_mds_head, payload);
  }

  encode(path, payload);
  encode(path2, payload);
  ceph::encode_nohead(releases, payload);
  encode(stamp, payload);
  encode(gid_list, payload);
  encode(alternate_name, payload);
  encode(fscrypt_auth, payload);
  encode(fscrypt_file, payload);
}

std::string_view MClientRequest::get_type_name() const
{
  return "creq";
}

void MClientRequest::print(std::ostream& out) const
{
  out << "client_request(" << get_orig_source()
      << ":" << get_tid()
      << " " << ceph_mds_op_name(get_op());
  if (IS_CEPH_MDS_OP_NEWINODE(head.op)) {
    out << " owner_uid=" << head.owner_uid
        << ", owner_gid=" << head.owner_gid;
  }
  if (head.op == CEPH_MDS_OP_GETATTR)
    out << " " << ccap_string(head.args.getattr.mask);
  if (head.op == CEPH_MDS_OP_SETATTR) {
    if (head.args.setattr.mask & CEPH_SETATTR_MODE)
      out << " mode=0" << std::oct << head.args.setattr.mode << std::dec;
    if (head.args.setattr.mask & CEPH_SETATTR_UID)
      out << " uid=" << head.args.setattr.uid;
    if (head.args.setattr.mask & CEPH_SETATTR_GID)
      out << " gid=" << head.args.setattr.gid;
    if (head.args.setattr.mask & CEPH_SETATTR_SIZE)
      out << " size=" << head.args.setattr.size;
    if (head.args.setattr.mask & CEPH_SETATTR_MTIME)
      out << " mtime=" << utime_t(head.args.setattr.mtime);
    if (head.args.setattr.mask & CEPH_SETATTR_ATIME)
      out << " atime=" << utime_t(head.args.setattr.atime);
  }
  if (head.op == CEPH_MDS_OP_SETFILELOCK ||
      head.op == CEPH_MDS_OP_GETFILELOCK) {
    out << " rule " << (int)head.args.filelock_change.rule
        << ", type " << (int)head.args.filelock_change.type
        << ", owner " << head.args.filelock_change.owner
        << ", pid " << head.args.filelock_change.pid
        << ", start " << head.args.filelock_change.start
        << ", length " << head.args.filelock_change.length
        << ", wait " << (int)head.args.filelock_change.wait;
  }
  //if (!get_filepath().empty()) 
  out << " " << get_filepath();
  if (alternate_name.size())
    out << " (" << alternate_name << ") ";
  if (!get_filepath2().empty())
    out << " " << get_filepath2();
  if (stamp != utime_t())
    out << " " << stamp;
  if (head.ext_num_fwd)
    out << " FWD=" << (int)head.ext_num_fwd;
  if (head.ext_num_retry)
    out << " RETRY=" << (int)head.ext_num_retry;
  if (is_async())
    out << " ASYNC";
  if (is_replay())
    out << " REPLAY";
  if (queued_for_replay)
    out << " QUEUED_FOR_REPLAY";
  out << " caller_uid=" << head.caller_uid
      << ", caller_gid=" << head.caller_gid
      << '{';
  for (auto i = gid_list.begin(); i != gid_list.end(); ++i)
    out << *i << ',';
  out << '}'
      << ")";
  out << " nref:" << get_nref();
}
