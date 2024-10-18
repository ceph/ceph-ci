# Patching Ceph Cheatsheet

Upstream Ceph patches need to frequently be backported to our `bb/squid` branch. Here is a quick reference for how that can be done.

1.  From Ceph main branch, either our fork of Ceph or upstream main, find the relevent commit(s). The commit details can be verified using `git show <commit-id>`.

2. Save the commit to a patch file using: `git format-patch -1 <commit-id> --stdout > ~/ceph.patch`.

3. [Optional] Due to a major version mismatch between branches, the RGW folder structure between `bb/squid` and upstream `main` branch is different. To make patching easier, the recommendation is to go into the patch file from the previous step and change instances of `rgw/driver` to `rgw/store`.

4. To check how the current branch will be affected by the patch, the following command can be run: `git apply --check ~/ceph.patch`. Frequently, there will not be a perfect match so the patch can be forced using `git apply --reject ~/ceph.patch` and then the rejections can be manually addressed.
