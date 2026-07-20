// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/root_block.h"
#include "crimson/os/seastore/lba/lba_btree_node.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"
#include "crimson/os/seastore/linked_tree_node.h"

namespace crimson::os::seastore {

// NOTE: RootBlock never takes the no-conflict publish path (see
// should_publish_extent).  If the LBA root node itself publishes under a
// user transaction, its scratch NEXT never links into any RootBlock
// (FixedKV{Internal,Leaf}Node::on_replace_prior skip root relinking for
// published extents), so the "!lba_root_node" arm below correctly links
// the prior's root node -- which stays canonical.
void RootBlock::on_replace_prior(Transaction &t) {
  if (!lba_root_node ||
      // for rewrite transactions, we keep the prior extents instead of
      // the new ones.
      is_rewrite_transaction(t.get_src())) {
    auto &prior = static_cast<RootBlock&>(*get_prior_instance());
    if (prior.lba_root_node) {
      RootBlockRef this_ref = this;
      auto lba_root = static_cast<
	lba::LBANode*>(prior.lba_root_node);
      if (likely(lba_root->range.depth > 1)) {
	TreeRootLinker<RootBlock, lba::LBAInternalNode>::link_root(
	  this_ref,
	  static_cast<lba::LBAInternalNode*>(prior.lba_root_node)
	);
      } else {
	assert(lba_root->range.depth == 1);
	TreeRootLinker<RootBlock, lba::LBALeafNode>::link_root(
	  this_ref,
	  static_cast<lba::LBALeafNode*>(prior.lba_root_node)
	);
      }
    }
  }
  if (!backref_root_node ||
      // for rewrite transactions, we keep the prior extents instead of
      // the new ones.
      is_rewrite_transaction(t.get_src())) {
    auto &prior = static_cast<RootBlock&>(*get_prior_instance());
    if (prior.backref_root_node) {
      RootBlockRef this_ref = this;
      auto backref_root = static_cast<
	backref::BackrefNode*>(prior.backref_root_node);
      if (likely(backref_root->range.depth > 1)) {
	TreeRootLinker<RootBlock, backref::BackrefInternalNode>::link_root(
	  this_ref,
	  static_cast<backref::BackrefInternalNode*>(prior.backref_root_node)
	);
      } else {
	assert(backref_root->range.depth == 1);
	TreeRootLinker<RootBlock, backref::BackrefLeafNode>::link_root(
	  this_ref,
	  static_cast<backref::BackrefLeafNode*>(prior.backref_root_node)
	);
      }
    }
  }
}

} // namespace crimson::os::seastore
