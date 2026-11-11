"""
https://tracker.ceph.com/issues/81523

Dropping the last reference on an inode queues a CapRelease and sends it on
a later flush. A lookup in between puts the same cap back. The queued
release still names that cap, so once it is sent the MDS drops the cap
while ceph-fuse keeps it. The next readdir issues a new cap id and
Client::add_update_cap aborts.

client_debug_hold_cap_releases keeps the batch queued across the drop and
the lookup. The pass condition is that the cap id is unchanged. An unfixed
client dies in add_update_cap when the readdir reply arrives.
"""

import logging
import time

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)


class TestStaleCapRelease(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    def _cache_entries(self):
        dumped = self.mount_a.admin_socket(['dump_cache'])
        if isinstance(dumped, dict) and 'cache' in dumped:
            return dumped['cache']
        if isinstance(dumped, dict) and isinstance(dumped.get('result'), dict):
            return dumped['result']['cache']
        self.fail("dump_cache returned no cache array: {0}".format(
            list(dumped) if isinstance(dumped, dict) else type(dumped)))

    def _ino_value(self, value):
        if isinstance(value, int):
            return value
        return int(value, 0)

    def _cached_inode(self, ino):
        for item in self._cache_entries():
            if not isinstance(item, dict) or 'caps' not in item or 'ino' not in item:
                continue
            if self._ino_value(item['ino']) == ino:
                return item
        return None

    def _cap_id(self, ino):
        inode = self._cached_inode(ino)
        self.assertIsNotNone(inode, "inode {0:#x} is not cached".format(ino))
        caps = inode['caps']
        self.assertTrue(caps, "inode {0:#x} has no caps".format(ino))
        for cap in caps:
            if cap.get('auth'):
                return int(cap['cap_id'])
        return int(caps[0]['cap_id'])

    def _wait_until_uncached(self, ino, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._cached_inode(ino) is None:
                return
            time.sleep(0.5)
        self.fail("inode {0:#x} stayed cached; cap release was never queued".format(ino))

    def test_readdir_after_late_cap_release(self):
        """
        Re-acquire a cap while its CapRelease is still queued, then flush
        and readdir. The cap id must stay the same.
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("requires ceph-fuse; the crash is in Client::add_update_cap")

        dirname = "caps_stale"
        filename = "file"
        othername = "other"
        self.mount_a.run_shell(["mkdir", dirname])
        self.mount_a.run_shell(["touch", "{0}/{1}".format(dirname, filename)])
        self.mount_a.run_shell(["touch", "{0}/{1}".format(dirname, othername)])

        ino = self.mount_a.path_to_ino("{0}/{1}".format(dirname, filename))
        dir_ino = self.mount_a.path_to_ino(dirname)
        cap_id = self._cap_id(ino)
        log.info("inode {0:#x} cap_id {1}".format(ino, cap_id))
        self.assertGreater(cap_id, 0)

        # Hold releases before the kernel forgets the inode. cache_size 0
        # lets the client drop the inode once fuse delivers the forget.
        self.mount_a.admin_socket(['config', 'set', 'client_debug_hold_cap_releases', 'true'])
        self.mount_a.admin_socket(['config', 'set', 'client_tick_interval', '1'])
        self.mount_a.set_cache_size(0)
        try:
            self.mount_a.run_shell(["sync"])
            self.mount_a.run_shell_payload(
                "echo 3 > /proc/sys/vm/drop_caches", sudo=True)
            self._wait_until_uncached(ino)

            # Lookup puts the same cap back. The queued release is still held.
            self.assertEqual(ino, self.mount_a.path_to_ino(
                "{0}/{1}".format(dirname, filename)))
            self.assertEqual(
                cap_id, self._cap_id(ino),
                "lookup issued a new cap; the CapRelease was flushed before lookup")

            # Unlink a sibling so the directory is no longer a complete cache.
            self.mount_a.run_shell(["rm", "{0}/{1}".format(dirname, othername)])
            directory = self._cached_inode(dir_ino)
            if directory is not None:
                self.assertFalse(
                    directory.get('complete') and directory.get('ordered'),
                    "readdir would be served from the client cache")

            self.mount_a.admin_socket(
                ['config', 'set', 'client_debug_hold_cap_releases', 'false'])
            self.mount_a.admin_socket(['flush_cap_releases'])

            names = self.mount_a.ls(dirname)
            self.assertIn(filename, names)
            cap_after = self._cap_id(ino)
            self.assertEqual(
                cap_id, cap_after,
                "cap {0} was reissued as {1}; a stale CapRelease dropped the MDS cap".format(
                    cap_id, cap_after))
        finally:
            try:
                self.mount_a.admin_socket(
                    ['config', 'set', 'client_debug_hold_cap_releases', 'false'])
            except Exception:
                log.debug("could not clear client_debug_hold_cap_releases", exc_info=True)
