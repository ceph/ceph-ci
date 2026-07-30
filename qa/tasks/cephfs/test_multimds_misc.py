import logging
import errno

from time import sleep

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.io_workload_manager import IoWorkloadManager, WriterThreadConf

from teuthology.contextutil import safe_while
from teuthology.exceptions import CommandFailedError


log = logging.getLogger(__name__)


class TestReducingMaxMds(CephFSTestCase):
    '''
    Contains tests for verifying that MDSs successfully transition from
    up:active to up:standby when max_mds is reduced from 3 to 1 while
    metadata-heavy IO workload is on.

    NOTE: IO workload with plenty MD ops is preferred so that MDS is busy while
    its state transitions.
    '''
    MDSS_REQUIRED = 3
    CLIENTS_REQUIRED = 3

    def setUp(self):
        super(TestReducingMaxMds, self).setUp()

        self.wr_th_conf = WriterThreadConf(depth=4, br_factor=3, wr_th_lim=12,
                                           wr_th_file_size=4096,
                                           wr_th_file_lim=20000, wr_th_sleep=0)

        self.iow_man = IoWorkloadManager(
            mounts=(self.mount_a, self.mount_b, self.mount_c),
            wr_th_conf=self.wr_th_conf)

    def tearDown(self):
        self.iow_man.teardown()
        super(TestReducingMaxMds, self).tearDown()

    def wait_till_MDSs_are(self, active, standby, sleep, tries):
        logmsg = 'waiting for cluster to have 1 active and 2 standby MDSs...'
        with safe_while(sleep=sleep, tries=tries, action=logmsg) as proceed:
            while proceed():
                cs = self.get_ceph_status()
                active = cs['fsmap']['in']
                standby = cs['fsmap']['up:standby']
                log.info(f'MDSs: {active} active, {standby} standby')
                if active == 1 and standby == 2:
                    return

    def test_with_manually_pinned_dirs(self):
        '''
        Test that 2 MDS ranks successfully transition from up:active to
        up:standby when max_mds is reduced from 3 to 1 while metadata-heavy IO
        is going on on all three manually pinned directories; each pinned to a
        different rank.
        '''
        def setup_for_this_test():
            self.run_ceph_cmd(f'fs set {self.fs.name} max_mds 3')

            self.mount_a.run_shell('mkdir dir1 dir2 dir3')
            self.mount_a.run_shell('setfattr -n ceph.dir.pin -v 0 dir1')
            self.mount_a.run_shell('setfattr -n ceph.dir.pin -v 1 dir2')
            self.mount_a.run_shell('setfattr -n ceph.dir.pin -v 2 dir3')

            self.mount_a.remount(cephfs_mntpt='/dir1')
            self.mount_b.remount(cephfs_mntpt='/dir2')
            self.mount_c.remount(cephfs_mntpt='/dir3')

            cs = self.get_ceph_status()
            self.assertEqual(cs['fsmap']['up'], 3)
            self.assertEqual(cs['fsmap']['in'], 3)
            self.assertEqual(cs['fsmap']['up:standby'], 0)

        setup_for_this_test()

        self.iow_man.start_io_workload()
        log.info('letting IO workload run undisturbed...')
        for i in range(60):
            sleep(30)
            assert self.iow_man.are_all_io_procs_alive()
            log.info('IO workload is on... ')

        # NOTE: pass confirmation option to avoid failure due to health
        # warnings.
        self.run_ceph_cmd(f'fs set {self.fs.name} max_mds 1 '
                           '--yes-i-really-mean-it')
        assert self.iow_man.are_all_io_procs_alive()
        self.wait_till_MDSs_are(active=1, standby=2, sleep=30, tries=120)


class TestScrub2(CephFSTestCase):
    MDSS_REQUIRED = 3
    CLIENTS_REQUIRED = 1

    def _check_scrub_status(self, result=None, reverse=False):
        self.assertEqual(self.fs.wait_until_scrub_complete(result=result, rank=1,
                                                           sleep=5, timeout=30,
                                                           reverse=reverse), True)
        self.assertEqual(self.fs.wait_until_scrub_complete(result=result, rank=2,
                                                           sleep=5, timeout=30,
                                                           reverse=reverse), True)
        self.assertEqual(self.fs.wait_until_scrub_complete(result=result, rank=0,
                                                           sleep=5, timeout=30,
                                                           reverse=reverse), True)

    def _check_task_status_na(self, timo=120):
        """ check absence of scrub status in ceph status """
        with safe_while(sleep=1, tries=120, action='wait for task status') as proceed:
            while proceed():
                active = self.fs.get_active_names()
                log.debug("current active={0}".format(active))
                task_status = self.fs.get_task_status("scrub status")
                if not active[0] in task_status:
                    return True

    def _check_task_status(self, expected_status, timo=120):
        """ check scrub status for current active mds in ceph status """
        with safe_while(sleep=1, tries=120, action='wait for task status') as proceed:
            while proceed():
                active = self.fs.get_active_names()
                log.debug("current active={0}".format(active))
                task_status = self.fs.get_task_status("scrub status")
                try:
                    if task_status[active[0]].startswith(expected_status):
                        return True
                except KeyError:
                    pass

    def _find_path_inos(self, root_path):
        inos = []
        p = self.mount_a.run_shell(["find", root_path])
        paths = p.stdout.getvalue().strip().split()
        for path in paths:
            inos.append(self.mount_a.path_to_ino(path))
        return inos

    def _setup_subtrees(self):
        self.fs.set_max_mds(3)
        self.fs.wait_for_daemons()
        status = self.fs.status()

        path = 'd1/d2/d3/d4/d5/d6/d7/d8'
        self.mount_a.run_shell(['mkdir', '-p', path])
        self.mount_a.run_shell(['sync', path])

        self.mount_a.setfattr("d1/d2", "ceph.dir.pin", "0")
        self.mount_a.setfattr("d1/d2/d3/d4", "ceph.dir.pin", "1")
        self.mount_a.setfattr("d1/d2/d3/d4/d5/d6", "ceph.dir.pin", "2")
        
        self._wait_subtrees([('/d1/d2', 0), ('/d1/d2/d3/d4', 1)], status, 0)
        self._wait_subtrees([('/d1/d2/d3/d4', 1), ('/d1/d2/d3/d4/d5/d6', 2)], status, 1)
        self._wait_subtrees([('/d1/d2/d3/d4', 1), ('/d1/d2/d3/d4/d5/d6', 2)], status, 2)

        for rank in range(3):
            self.fs.rank_tell(["flush", "journal"], rank=rank)

    def test_apply_tag(self):
        self._setup_subtrees()
        inos = self._find_path_inos('d1/d2/d3/')

        tag = "tag123"
        out_json = self.fs.rank_tell(["tag", "path", "/d1/d2/d3", tag], rank=0)
        self.assertNotEqual(out_json, None)
        self.assertEqual(out_json["return_code"], 0)
        self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

        def assertTagged(ino):
            file_obj_name = "{0:x}.00000000".format(ino)
            self.fs.radosm(["getxattr", file_obj_name, "scrub_tag"])

        for ino in inos:
            assertTagged(ino)

    def test_scrub_backtrace(self):
        self._setup_subtrees()
        inos = self._find_path_inos('d1/d2/d3/')

        for ino in inos:
            file_obj_name = "{0:x}.00000000".format(ino)
            self.fs.radosm(["rmxattr", file_obj_name, "parent"])

        out_json = self.fs.run_scrub(["start", "/d1/d2/d3", "recursive,force"], 0)
        self.assertNotEqual(out_json, None)
        self.assertEqual(out_json["return_code"], 0)
        self.assertEqual(self.fs.wait_until_scrub_complete(tag=out_json["scrub_tag"]), True)

        def _check_damage(mds_rank, inos):
            all_damage = self.fs.rank_tell(["damage", "ls"], rank=mds_rank)
            damage = [d for d in all_damage if d['ino'] in inos and d['damage_type'] == "backtrace"]
            return len(damage) >= len(inos)

        self.assertTrue(_check_damage(0, inos[0:2]))
        self.assertTrue(_check_damage(1, inos[2:4]))
        self.assertTrue(_check_damage(2, inos[4:6]))

    def test_scrub_non_mds0(self):
        self._setup_subtrees()

        def expect_exdev(cmd, mds):
            try:
                self.run_ceph_cmd('tell', 'mds.{0}'.format(mds), *cmd)
            except CommandFailedError as e:
                if e.exitstatus == errno.EXDEV:
                    pass
                else:
                    raise
            else:
                raise RuntimeError("expected failure")

        rank1 = self.fs.get_rank(rank=1)
        expect_exdev(["scrub", "start", "/d1/d2/d3"], rank1["name"])
        expect_exdev(["scrub", "abort"], rank1["name"])
        expect_exdev(["scrub", "pause"], rank1["name"])
        expect_exdev(["scrub", "resume"], rank1["name"])

    def test_scrub_abort_mds0(self):
        self._setup_subtrees()

        inos = self._find_path_inos('d1/d2/d3/')

        for ino in inos:
            file_obj_name = "{0:x}.00000000".format(ino)
            self.fs.radosm(["rmxattr", file_obj_name, "parent"])

        out_json = self.fs.run_scrub(["start", "/d1/d2/d3", "recursive,force"], 0)
        self.assertNotEqual(out_json, None)
        
        res = self.fs.run_scrub(["abort"])
        self.assertEqual(res['return_code'], 0)

        # Abort and verify in both mdss. We also check the status in rank 0 mds because
        # it is supposed to gather the scrub status from other mdss.
        self._check_scrub_status()

        # sleep enough to fetch updated task status
        checked = self._check_task_status_na()
        self.assertTrue(checked)

    def test_scrub_pause_and_resume_mds0(self):
        self._setup_subtrees()

        inos = self._find_path_inos('d1/d2/d3/')

        for ino in inos:
            file_obj_name = "{0:x}.00000000".format(ino)
            self.fs.radosm(["rmxattr", file_obj_name, "parent"])

        out_json = self.fs.run_scrub(["start", "/d1/d2/d3", "recursive,force"], 0)
        self.assertNotEqual(out_json, None)

        res = self.fs.run_scrub(["pause"])
        self.assertEqual(res['return_code'], 0)

        self._check_scrub_status(result="PAUSED")

        checked = self._check_task_status("paused")
        self.assertTrue(checked)

        # resume and verify
        res = self.fs.run_scrub(["resume"])
        self.assertEqual(res['return_code'], 0)
        
        self._check_scrub_status(result="PAUSED", reverse=True)

        checked = self._check_task_status_na()
        self.assertTrue(checked)

    def test_scrub_pause_and_resume_with_abort_mds0(self):
        self._setup_subtrees()

        inos = self._find_path_inos('d1/d2/d3/')

        for ino in inos:
            file_obj_name = "{0:x}.00000000".format(ino)
            self.fs.radosm(["rmxattr", file_obj_name, "parent"])

        out_json = self.fs.run_scrub(["start", "/d1/d2/d3", "recursive,force"], 0)
        self.assertNotEqual(out_json, None)

        res = self.fs.run_scrub(["pause"])
        self.assertEqual(res['return_code'], 0)

        self._check_scrub_status(result="PAUSED")

        checked = self._check_task_status("paused")
        self.assertTrue(checked)

        res = self.fs.run_scrub(["abort"])
        self.assertEqual(res['return_code'], 0)

        self._check_scrub_status(result="PAUSED")
        self._check_scrub_status(result="0 inodes")

        # scrub status should still be paused...
        checked = self._check_task_status("paused")
        self.assertTrue(checked)

        # resume and verify
        res = self.fs.run_scrub(["resume"])
        self.assertEqual(res['return_code'], 0)

        self._check_scrub_status(result="PAUSED", reverse=True)

        checked = self._check_task_status_na()
        self.assertTrue(checked)
