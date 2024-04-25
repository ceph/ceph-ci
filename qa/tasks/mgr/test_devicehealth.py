from io import StringIO
import logging

from teuthology.exceptions import CommandFailedError

from .mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class TestDeviceHealth(MgrTestCase):
    MGRS_REQUIRED = 1

    def setUp(self):
        super(TestDeviceHealth, self).setUp()
        self.setup_mgrs()

    def tearDown(self):
        self.mgr_cluster.set_down()
        self.remove_mgr_pool()
        self.mgr_cluster.set_down(yes='false')
        return super(TestDeviceHealth, self).tearDown()

    def remove_mgr_pool(self):
        self.config_set('mon', 'mon_allow_pool_delete', 'true')
        self.mgr_cluster.mon_manager.raw_cluster_cmd('osd', 'pool', 'rm', '.mgr', '.mgr', '--yes-i-really-really-mean-it-not-faking')

    def test_legacy_upgrade_snap(self):
        """
        """

        o = "ABC_DEADB33F_FA"
        self.mon_manager.do_rados(["put", o, "-"], pool=".mgr", stdin=StringIO("junk"))
        self.mon_manager.do_rados(["mksnap", "foo"], pool=".mgr")
        self.mon_manager.do_rados(["rm", o], pool=".mgr")
        self.mgr_cluster.mgr_fail()

        with self.assert_cluster_log("Unhandled exception from module 'devicehealth' while running", present=False):
            self.wait_until_true(lambda: self.mgr_cluster.get_active_id() is not None, timeout=60)

    def test_sql_commit(self):
        """
        That commits work.
        """

        self.mgr_cluster.set_down()
        self.config_set('mgr', 'mgr/devicehealth/sqlite3_killpoint', 3)
        self.remove_mgr_pool()
        self.mgr_cluster.set_down(yes='false')

        # wait for killpoint trigger kills all mgrs
        def all_dead():
            for mgr_id, mgr_daemon in self.mgr_cluster.mgr_daemons.items():
                log.info(f"{mgr_id}")
                try:
                    s = mgr_daemon.check_status()
                    if s is None:
                        return False
                    log.info(f"{s}")
                except CommandFailedError as e:
                    log.info(f"{e}")
                    pass
            return True

        self.wait_until_true(all_dead, timeout=30)

        script = """
            export CEPH_ARGS='--id admin --no-log-to-stderr'
            sqlite3 -cmd '.load libcephsqlite.so' -cmd '.open file:///.mgr:devicehealth/main.db?vfs=ceph' <<<'.schema'
        """
        p = self.mon_manager.controller.run(args=['bash'], stdin=StringIO(script), stdout=StringIO())
        schema = p.stdout.getvalue().strip()
        self.assertIn("TABLE MgrModuleKV", schema)
        self.assertIn("TABLE Device", schema)

    def _test_sql_autocommit(self, kv):
        """
        That autocommit transactions is off.
        """

        self.mgr_cluster.set_down()
        self.config_set('mgr', 'mgr/devicehealth/sqlite3_killpoint', kv)
        self.remove_mgr_pool()
        self.mgr_cluster.set_down(yes='false')

        # wait for killpoint trigger kills all mgrs
        def all_dead():
            for mgr_id, mgr_daemon in self.mgr_cluster.mgr_daemons.items():
                log.info(f"{mgr_id}")
                try:
                    s = mgr_daemon.check_status()
                    if s is None:
                        return False
                    log.info(f"{s}")
                except CommandFailedError as e:
                    log.info(f"{e}")
                    pass
            return True

        self.wait_until_true(all_dead, timeout=30)

        script = """
            export CEPH_ARGS='--id admin --no-log-to-stderr'
            sqlite3 -cmd '.load libcephsqlite.so' -cmd '.open file:///.mgr:devicehealth/main.db?vfs=ceph' <<<'.schema'
        """
        p = self.mon_manager.controller.run(args=['bash'], stdin=StringIO(script), stdout=StringIO())
        schema = p.stdout.getvalue().strip()
        self.assertEqual("", schema)

    def test_sql_autocommit1(self):
        return self._test_sql_autocommit(1)

    def test_sql_autocommit2(self):
        return self._test_sql_autocommit(2)
