import json
import logging

from tasks.mgr.mgr_test_case import MgrTestCase

log = logging.getLogger(__name__)


class TestMgrPoolCreate(MgrTestCase):
    """
    Test that the .mgr pool is created with the correct CRUSH rule
    based on OSD device classes and pool configuration.
    """

    MGR_POOL = '.mgr'
    POOL_TIMEOUT = 60

    def setUp(self):
        super(TestMgrPoolCreate, self).setUp()
        if self._osd_count() < 3:
            self.skipTest("Not enough OSDS!")

         # Remove any filesystems so that we can remove their pools
        if self.mds_cluster:
            self.mds_cluster.mds_stop()
            self.mds_cluster.mds_fail()
            self.mds_cluster.delete_all_filesystems()

        self._mgr_id = self.mgr_cluster.get_active_id()

    def tearDown(self):
        """
        Clean up the cluster after the test.
        """
        for pool in ['data0', 'data1', 'data2', self.MGR_POOL]:
            if pool in self.mgr_cluster.mon_manager.pools:
                self.mgr_cluster.mon_manager.remove_pool(pool)
        for rule in ['replicated_hdd', 'replicated_ssd']:
            self._remove_rule_if_exists(rule)
        self._remove_rule_if_exists(self.MGR_POOL)
        # Reset all OSD device classes back to their original
        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for osd in osd_map['osds']:
            osd_id = osd['osd']
            self._raw('osd', 'crush', 'rm-device-class', f'osd.{osd_id}')
        self._raw('config', 'rm', 'mgr', 'mgr_pool_device_class')
        super(TestMgrPoolCreate, self).tearDown()


    def _raw(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(*args)

    def _osd_count(self):
        """
        Get the number of OSDs in the cluster.
        """
        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        return len(osd_map['osds'])

    def _pool_exists(self, name):
        pools = self.mgr_cluster.mon_manager.get_osd_dump_json()['pools']
        return any(p['pool_name'] == name for p in pools)

    def _rule_exists(self, name):
        out = self._raw('osd', 'crush', 'rule', 'ls')
        return name in out.split()

    def _remove_pool_if_exists(self, name):
        if self._pool_exists(name):
            self._raw('osd', 'pool', 'rm', name, name,
                      '--yes-i-really-really-mean-it')

    def _remove_rule_if_exists(self, name):
        if self._rule_exists(name):
            self._raw('osd', 'crush', 'rule', 'rm', name)

    def _set_all_osd_class(self, device_class):
        """
        Remove and re-set device class on all OSDs
        """
        osd_map = self.mgr_cluster.mon_manager.get_osd_dump_json()
        for osd in osd_map['osds']:
            osd_id = osd['osd']
            self._raw('osd', 'crush', 'rm-device-class', f'osd.{osd_id}')
            if device_class:
                self._raw('osd', 'crush', 'set-device-class',
                          device_class, f'osd.{osd_id}')

    def _set_osd_class(self, osd_id, device_class):
        self._raw('osd', 'crush', 'rm-device-class', f'osd.{osd_id}')
        if device_class:
            self._raw('osd', 'crush', 'set-device-class',
                      device_class, f'osd.{osd_id}')

    def _restart_mgr_and_wait(self):
        initial_gid = self.mgr_cluster.get_active_gid()
        self.mgr_cluster.mgr_restart(self._mgr_id)
        self.wait_until_true(
            lambda: (self.mgr_cluster.get_active_gid() != initial_gid
                     and self.mgr_cluster.get_mgr_map()['available']),
            timeout=30
        )

    def _wait_for_mgr_pool(self):
        """
        Poll until the .mgr pool appears (created by open_db on restart)
        """
        self.wait_until_true(
            lambda: self._pool_exists(self.MGR_POOL),
            timeout=self.POOL_TIMEOUT
        )

    def _get_mgr_crush_rule(self):
        """
        Return the JSON of the .mgr CRUSH rule
        """
        out = self._raw('osd', 'crush', 'rule', 'dump', self.MGR_POOL,
                        '--format=json')
        return json.loads(out)

    def _mgr_rule_item_name(self):
        """
        Return the 'item_name' from the first 'take' step of the .mgr rule
        """
        rule = self._get_mgr_crush_rule()
        for step in rule.get('steps', []):
            if step.get('op') == 'take':
                return step['item_name']
        return None

    def _reset_mgr_pool(self):
        """
        Delete .mgr pool and rule to force fresh creation on next restart
        """
        self._remove_pool_if_exists(self.MGR_POOL)
        self._remove_rule_if_exists(self.MGR_POOL)

    def test_all_ssd(self):
        """All OSDs ssd — rule should target default~ssd."""
        self._set_all_osd_class('ssd')
        self._reset_mgr_pool()
        self._restart_mgr_and_wait()
        self._wait_for_mgr_pool()
        self.assertEqual(self._mgr_rule_item_name(), 'default~ssd')

    def test_no_device_class(self):
        """No device class on any OSD — rule should target plain default."""
        self._set_all_osd_class(None)
        self._reset_mgr_pool()
        self._restart_mgr_and_wait()
        self._wait_for_mgr_pool()
        self.assertEqual(self._mgr_rule_item_name(), 'default')

    def test_majority_hdd(self):
        """2 hdd, 1 ssd — rule should target default~hdd (most OSDs)."""
        self._set_osd_class(0, 'hdd')
        self._set_osd_class(1, 'hdd')
        self._set_osd_class(2, 'ssd')
        self._reset_mgr_pool()
        self._restart_mgr_and_wait()
        self._wait_for_mgr_pool()
        self.assertEqual(self._mgr_rule_item_name(), 'default~hdd')

    def test_bulk_pool_majority(self):
        """2 bulk ssd pools, 1 bulk hdd pool — rule should follow bulk majority (ssd)."""
        self._set_osd_class(0, 'hdd')
        self._set_osd_class(1, 'hdd')
        self._set_osd_class(2, 'ssd')
        self._raw('osd', 'crush', 'rule', 'create-replicated',
                  'replicated_hdd', 'default', 'osd', 'hdd')
        self._raw('osd', 'crush', 'rule', 'create-replicated',
                  'replicated_ssd', 'default', 'osd', 'ssd')
        self._raw('osd', 'pool', 'create', 'data0', '32', '32',
                  'replicated', 'replicated_hdd', '--bulk')
        self._raw('osd', 'pool', 'create', 'data1', '32', '32',
                  'replicated', 'replicated_ssd', '--bulk')
        self._raw('osd', 'pool', 'create', 'data2', '32', '32',
                  'replicated', 'replicated_ssd', '--bulk')
        self._reset_mgr_pool()
        self._restart_mgr_and_wait()
        self._wait_for_mgr_pool()
        self.assertEqual(self._mgr_rule_item_name(), 'default~ssd')

    def test_config_override(self):
        """mgr_pool_device_class=ssd overrides bulk-pool heuristic."""
        self._set_osd_class(0, 'hdd')
        self._set_osd_class(1, 'hdd')
        self._set_osd_class(2, 'ssd')
        self._raw('osd', 'crush', 'rule', 'create-replicated',
                  'replicated_hdd', 'default', 'osd', 'hdd')
        self._raw('osd', 'crush', 'rule', 'create-replicated',
                  'replicated_ssd', 'default', 'osd', 'ssd')
        self._raw('osd', 'pool', 'create', 'data0', '32', '32',
                  'replicated', 'replicated_hdd', '--bulk')
        self._raw('osd', 'pool', 'create', 'data1', '32', '32',
                  'replicated', 'replicated_hdd', '--bulk')
        self._raw('osd', 'pool', 'create', 'data2', '32', '32',
                  'replicated', 'replicated_ssd', '--bulk')
        self._raw('config', 'set', 'mgr', 'mgr_pool_device_class', 'ssd')
        self._reset_mgr_pool()
        self._restart_mgr_and_wait()
        self._wait_for_mgr_pool()
        self.assertEqual(self._mgr_rule_item_name(), 'default~ssd')

    def test_invalid_config_falls_back(self):
        """mgr_pool_device_class=nvme (nonexistent) falls back to most OSDs (hdd)."""
        self._set_osd_class(0, 'hdd')
        self._set_osd_class(1, 'hdd')
        self._set_osd_class(2, 'ssd')
        self._raw('config', 'set', 'mgr', 'mgr_pool_device_class', 'nvme')
        self._reset_mgr_pool()
        self._restart_mgr_and_wait()
        self._wait_for_mgr_pool()
        self.assertEqual(self._mgr_rule_item_name(), 'default~hdd')
