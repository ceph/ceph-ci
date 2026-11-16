"""
https://tracker.ceph.com/issues/81523

A CapRelease queued for a cap the client still holds is honored by the MDS
(_do_cap_release matches cap_id and issue_seq). The next readdir issues a
new cap id at seq 1. add_update_cap aborts when that id does not match the
cap the client kept.

A lookup cannot build this state. encode_inodestat() calls set_last_issue(),
so a release captured before that reply has an older issue_seq and the MDS
ignores it. The client also has to keep the inode: a trim that drops it
installs the readdir cap on a fresh inode, and the assert is never reached.
New inodes are issued cap id 1 without advancing last_cap_id, so a reissue
while last_cap_id is still 0 is also cap id 1 and the assert passes.

This test pins the inode with an open fd, advances last_cap_id on another
file, queues a release for the cap the client still holds, flushes it, and
readdirs. An unfixed client dies in add_update_cap. A client that accepts
the reissue stays up and reports the new cap id.
"""

import logging
import time

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)


class TestStaleCapRelease(CephFSTestCase):
    CLIENTS_REQUIRED = 2
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

    def _drop_caches(self):
        self.mount_a.run_shell_payload(
            "echo 3 > /proc/sys/vm/drop_caches", sudo=True)

    def _reissue_cap(self, path):
        """Drop path from the client cache, flush its release, and look it up.

        The lookup runs only after the release is sent, so the MDS has no cap
        and add_client_cap() assigns ++last_cap_id.
        """
        ino = self.mount_a.path_to_ino(path)
        self._drop_caches()
        self._wait_until_uncached(ino)
        self.mount_a.admin_socket(['flush_cap_releases'])
        self.assertEqual(ino, self.mount_a.path_to_ino(path))
        cap_id = self._cap_id(ino)
        log.info("reissued {0} ino {1:#x} cap_id {2}".format(path, ino, cap_id))
        return cap_id

    def test_readdir_after_late_cap_release(self):
        """
        Flush a CapRelease for a cap this client still holds, then readdir.
        """
        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("requires ceph-fuse; the crash is in Client::add_update_cap")

        dirname = "caps_stale"
        filename = "file"
        bumper = "bumper"
        extra = "extra"
        self.mount_a.run_shell(["mkdir", dirname])
        self.mount_a.run_shell(["touch", "{0}/{1}".format(dirname, filename)])
        self.mount_a.run_shell(["touch", "{0}/{1}".format(dirname, bumper)])

        ino = self.mount_a.path_to_ino("{0}/{1}".format(dirname, filename))
        dir_ino = self.mount_a.path_to_ino(dirname)
        holder = self.mount_a.run_shell([
            "python3", "-c",
            "import os, time\n"
            "os.open('{0}/{1}', os.O_RDONLY)\n"
            "open('/tmp/caps_stale_holder_ready', 'w').close()\n"
            "time.sleep(600)\n".format(dirname, filename),
        ], wait=False)
        try:
            ready = False
            for _ in range(50):
                result = self.mount_a.run_shell(
                    ["test", "-f", "/tmp/caps_stale_holder_ready"],
                    check_status=False)
                if result.exitstatus == 0:
                    ready = True
                    break
                time.sleep(0.2)
            self.assertTrue(ready, "holder did not open the file")

            cap_id = self._cap_id(ino)
            log.info("inode {0:#x} cap_id {1}".format(ino, cap_id))
            self.assertGreater(cap_id, 0)
            pinned = self._cached_inode(ino)
            self.assertGreater(
                pinned.get('ll_ref', 0), 0,
                "open fd did not pin the inode")

            # Trim unused inodes, but the open file stays in cache.
            self.mount_a.admin_socket(['config', 'set', 'client_tick_interval', '1'])
            self.mount_a.set_cache_size(0)

            # new_inode caps are issued as cap id 1 and do not move
            # last_cap_id. Reissue another file until a new id is past ours,
            # so the victim's reissue cannot collide with cap id 1.
            bumper_path = "{0}/{1}".format(dirname, bumper)
            advanced = None
            for _ in range(6):
                advanced = self._reissue_cap(bumper_path)
                self.assertEqual(
                    cap_id, self._cap_id(ino),
                    "pinned inode lost its cap while advancing last_cap_id")
                if advanced > cap_id:
                    break
            else:
                self.fail(
                    "MDS last_cap_id did not move past cap {0} (last reissue {1})".format(
                        cap_id, advanced))

            # A create on the other client revokes this client's directory
            # caps. Looking the new name up clears I_DIR_ORDERED, so readdir
            # is not served from the client cache.
            self.mount_b.run_shell(["touch", "{0}/{1}".format(dirname, extra)])
            self.mount_a.path_to_ino("{0}/{1}".format(dirname, extra))
            directory = self._cached_inode(dir_ino)
            if directory is not None:
                self.assertFalse(
                    directory.get('complete') and directory.get('ordered'),
                    "readdir would be served from the client cache")

            self.mount_a.admin_socket(
                ['queue_held_cap_release', 'ino={0}'.format(ino)])
            self.mount_a.admin_socket(['flush_cap_releases'])

            names = self.mount_a.ls(dirname)
            self.assertIn(filename, names)
            cap_after = self._cap_id(ino)
            log.info("inode {0:#x} cap_id {1} after readdir".format(ino, cap_after))
            self.assertNotEqual(
                cap_id, cap_after,
                "MDS still has cap {0}; the queued release was not applied".format(
                    cap_id))
        finally:
            try:
                holder.kill()
            except Exception:
                log.debug("could not stop cap holder", exc_info=True)
            try:
                self.mount_a.run_shell(
                    ["rm", "-f", "/tmp/caps_stale_holder_ready"],
                    check_status=False, cwd="/")
            except Exception:
                log.debug("could not remove holder ready file", exc_info=True)
