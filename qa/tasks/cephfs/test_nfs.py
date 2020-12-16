# NOTE: these tests are not yet compatible with vstart_runner.py.
import errno
import json
import time
import logging
from io import BytesIO

from tasks.mgr.mgr_test_case import MgrTestCase
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


# TODO Add test for cluster update when ganesha can be deployed on multiple ports.
class TestNFS(MgrTestCase):
    def _cmd(self, *args):
        return self.mgr_cluster.mon_manager.raw_cluster_cmd(*args)

    def _nfs_cmd(self, *args):
        return self._cmd("nfs", *args)

    def _orch_cmd(self, *args):
        return self._cmd("orch", *args)

    def _sys_cmd(self, cmd):
        cmd[0:0] = ['sudo']
        ret = self.ctx.cluster.run(args=cmd, check_status=False, stdout=BytesIO(), stderr=BytesIO())
        stdout = ret[0].stdout
        if stdout:
            return stdout.getvalue()

    def setUp(self):
        super(TestNFS, self).setUp()
        self.cluster_id = "test"
        self.export_type = "cephfs"
        self.pseudo_path = "/cephfs"
        self.path = "/"
        self.fs_name = "nfs-cephfs"
        self.expected_name = "nfs.ganesha-test"
        self.sample_export = {
         "export_id": 1,
         "path": self.path,
         "cluster_id": self.cluster_id,
         "pseudo": self.pseudo_path,
         "access_type": "RW",
         "squash": "no_root_squash",
         "security_label": True,
         "protocols": [
           4
         ],
         "transports": [
           "TCP"
         ],
         "fsal": {
           "name": "CEPH",
           "user_id": "test1",
           "fs_name": self.fs_name,
           "sec_label_xattr": ''
         },
         "clients": []
        }

    def _check_nfs_server_status(self):
        res = self._sys_cmd(['systemctl', 'status', 'nfs-server'])
        if isinstance(res, bytes) and b'Active: active' in res:
            self._disable_nfs()

    def _disable_nfs(self):
        log.info("Disabling NFS")
        self._sys_cmd(['systemctl', 'disable', 'nfs-server', '--now'])

    def _fetch_nfs_status(self):
        return self._orch_cmd('ps', f'--service_name={self.expected_name}')

    def _check_nfs_cluster_status(self, expected_status, fail_msg):
        '''
        Tests if nfs cluster created or deleted successfully
        :param expected_status: Status to be verified
        :param fail_msg: Message to be printed if test failed
        '''
        # Wait for few seconds as ganesha daemon takes few seconds to be deleted/created
        wait_time = 10
        while wait_time <= 60:
            time.sleep(wait_time)
            if expected_status in self._fetch_nfs_status():
                return
            wait_time += 10
        self.fail(fail_msg)

    def _check_auth_ls(self, export_id=1, check_in=False):
        '''
        Tests export user id creation or deletion.
        :param export_id: Denotes export number
        :param check_in: Check specified export id
        '''
        output = self._cmd('auth', 'ls')
        if check_in:
            self.assertIn(f'client.{self.cluster_id}{export_id}', output)
        else:
            self.assertNotIn(f'client-{self.cluster_id}', output)

    def _test_idempotency(self, cmd_func, cmd_args):
        '''
        Test idempotency of commands. It first runs the TestNFS test method
        for a command and then checks the result of command run again. TestNFS
        test method has required checks to verify that command works.
        :param cmd_func: TestNFS method
        :param cmd_args: nfs command arguments to be run
        '''
        cmd_func()
        ret = self.mgr_cluster.mon_manager.raw_cluster_cmd_result(*cmd_args)
        if ret != 0:
            self.fail("Idempotency test failed")

    def _test_create_cluster(self):
        '''
        Test single nfs cluster deployment.
        '''
        # Disable any running nfs ganesha daemon
        self._check_nfs_server_status()
        self._nfs_cmd('cluster', 'create', self.export_type, self.cluster_id)
        # Check for expected status and daemon name (nfs.ganesha-<cluster_id>)
        self._check_nfs_cluster_status('running', 'NFS Ganesha cluster deployment failed')

    def _test_delete_cluster(self):
        '''
        Test deletion of a single nfs cluster.
        '''
        self._nfs_cmd('cluster', 'delete', self.cluster_id)
        self._check_nfs_cluster_status('No daemons reported',
                                       'NFS Ganesha cluster could not be deleted')

    def _test_list_cluster(self, empty=False):
        '''
        Test listing of deployed nfs clusters. If nfs cluster is deployed then
        it checks for expected cluster id. Otherwise checks nothing is listed.
        :param empty: If true it denotes no cluster is deployed.
        '''
        if empty:
            cluster_id = ''
        else:
            cluster_id = self.cluster_id
        nfs_output = self._nfs_cmd('cluster', 'ls')
        self.assertEqual(cluster_id, nfs_output.strip())

    def _create_export(self, export_id, create_fs=False, extra_cmd=None):
        '''
        Test creation of a single export.
        :param export_id: Denotes export number
        :param create_fs: If false filesytem exists. Otherwise create it.
        :param extra_cmd: List of extra arguments for creating export.
        '''
        if create_fs:
            self._cmd('fs', 'volume', 'create', self.fs_name)
        export_cmd = ['nfs', 'export', 'create', 'cephfs', self.fs_name, self.cluster_id]
        if isinstance(extra_cmd, list):
            export_cmd.extend(extra_cmd)
        else:
            export_cmd.append(self.pseudo_path)
        # Runs the nfs export create command
        self._cmd(*export_cmd)
        # Check if user id for export is created
        self._check_auth_ls(export_id, check_in=True)
        res = self._sys_cmd(['rados', '-p', 'nfs-ganesha', '-N', self.cluster_id, 'get',
                             f'export-{export_id}', '-'])
        # Check if export object is created
        if res == b'':
            self.fail("Export cannot be created")

    def _create_default_export(self):
        '''
        Deploy a single nfs cluster and create export with default options.
        '''
        self._test_create_cluster()
        self._create_export(export_id='1', create_fs=True)

    def _delete_export(self):
        '''
        Delete an export.
        '''
        self._nfs_cmd('export', 'delete', self.cluster_id, self.pseudo_path)
        self._check_auth_ls()

    def _test_list_export(self):
        '''
        Test listing of created exports.
        '''
        nfs_output = json.loads(self._nfs_cmd('export', 'ls', self.cluster_id))
        self.assertIn(self.pseudo_path, nfs_output)

    def _test_list_detailed(self, sub_vol_path):
        '''
        Test listing of created exports with detailed option.
        :param sub_vol_path: Denotes path of subvolume
        '''
        nfs_output = json.loads(self._nfs_cmd('export', 'ls', self.cluster_id, '--detailed'))
        # Export-1 with default values (access type = rw and path = '\')
        self.assertDictEqual(self.sample_export, nfs_output[0])
        # Export-2 with r only
        self.sample_export['export_id'] = 2
        self.sample_export['pseudo'] = self.pseudo_path + '1'
        self.sample_export['access_type'] = 'RO'
        self.sample_export['fsal']['user_id'] = self.cluster_id + '2'
        self.assertDictEqual(self.sample_export, nfs_output[1])
        # Export-3 for subvolume with r only
        self.sample_export['export_id'] = 3
        self.sample_export['path'] = sub_vol_path
        self.sample_export['pseudo'] = self.pseudo_path + '2'
        self.sample_export['fsal']['user_id'] = self.cluster_id + '3'
        self.assertDictEqual(self.sample_export, nfs_output[2])
        # Export-4 for subvolume
        self.sample_export['export_id'] = 4
        self.sample_export['pseudo'] = self.pseudo_path + '3'
        self.sample_export['access_type'] = 'RW'
        self.sample_export['fsal']['user_id'] = self.cluster_id + '4'
        self.assertDictEqual(self.sample_export, nfs_output[3])

    def _test_get_export(self):
        '''
        Test fetching of created export.
        '''
        nfs_output = json.loads(self._nfs_cmd('export', 'get', self.cluster_id, self.pseudo_path))
        self.assertDictEqual(self.sample_export, nfs_output)

    def _check_export_obj_deleted(self, conf_obj=False):
        '''
        Test if export or config object are deleted successfully.
        :param conf_obj: It denotes config object needs to be checked
        '''
        rados_obj_ls = self._sys_cmd(['rados', '-p', 'nfs-ganesha', '-N', self.cluster_id, 'ls'])

        if b'export-' in rados_obj_ls or (conf_obj and b'conf-nfs' in rados_obj_ls):
            self.fail("Delete export failed")

    def _get_port_ip_info(self):
        '''
        Return port and ip for a cluster
        '''
        #{'test': [{'hostname': 'smithi068', 'ip': ['172.21.15.68'], 'port': 2049}]}
        info_output = json.loads(self._nfs_cmd('cluster', 'info', self.cluster_id))['test'][0]
        return info_output["port"], info_output["ip"][0]

    def _test_mnt(self, pseudo_path, port, ip, check=True):
        '''
        Test mounting of created exports
        :param pseudo_path: It is the pseudo root name
        :param port: Port of deployed nfs cluster
        :param ip: IP of deployed nfs cluster
        :param check: It denotes if i/o testing needs to be done
        '''
        try:
            self.ctx.cluster.run(args=['sudo', 'mount', '-t', 'nfs', '-o', f'port={port}',
                                       f'{ip}:{pseudo_path}', '/mnt'])
        except CommandFailedError as e:
            # Check if mount failed only when non existing pseudo path is passed
            if not check and e.exitstatus == 32:
                return
            raise

        try:
            self.ctx.cluster.run(args=['sudo', 'touch', '/mnt/test'])
            out_mnt = self._sys_cmd(['sudo', 'ls', '/mnt'])
            self.assertEqual(out_mnt,  b'test\n')
        finally:
            self.ctx.cluster.run(args=['sudo', 'umount', '/mnt'])

    def test_create_and_delete_cluster(self):
        '''
        Test successful creation and deletion of the nfs cluster.
        '''
        self._test_create_cluster()
        self._test_list_cluster()
        self._test_delete_cluster()
        # List clusters again to ensure no cluster is shown
        self._test_list_cluster(empty=True)

    def test_create_delete_cluster_idempotency(self):
        '''
        Test idempotency of cluster create and delete commands.
        '''
        self._test_idempotency(self._test_create_cluster, ['nfs', 'cluster', 'create', self.export_type,
                                                           self.cluster_id])
        self._test_idempotency(self._test_delete_cluster, ['nfs', 'cluster', 'delete', self.cluster_id])

    def test_create_cluster_with_invalid_cluster_id(self):
        '''
        Test nfs cluster deployment failure with invalid cluster id.
        '''
        try:
            invalid_cluster_id = '/cluster_test'  # Only [A-Za-z0-9-_.] chars are valid
            self._nfs_cmd('cluster', 'create', self.export_type, invalid_cluster_id)
            self.fail(f"Cluster successfully created with invalid cluster id {invalid_cluster_id}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.EINVAL:
                raise

    def test_create_cluster_with_invalid_export_type(self):
        '''
        Test nfs cluster deployment failure with invalid export type.
        '''
        try:
            invalid_export_type = 'rgw'  # Only cephfs is valid
            self._nfs_cmd('cluster', 'create', invalid_export_type, self.cluster_id)
            self.fail(f"Cluster successfully created with invalid export type {invalid_export_type}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.EINVAL:
                raise

    def test_create_and_delete_export(self):
        '''
        Test successful creation and deletion of the cephfs export.
        '''
        self._create_default_export()
        self._test_get_export()
        port, ip = self._get_port_ip_info()
        self._test_mnt(self.pseudo_path, port, ip)
        self._delete_export()
        # Check if rados export object is deleted
        self._check_export_obj_deleted()
        self._test_mnt(self.pseudo_path, port, ip, False)
        self._test_delete_cluster()

    def test_create_delete_export_idempotency(self):
        '''
        Test idempotency of export create and delete commands.
        '''
        self._test_idempotency(self._create_default_export, ['nfs', 'export', 'create', 'cephfs',
                                                             self.fs_name, self.cluster_id,
                                                             self.pseudo_path])
        self._test_idempotency(self._delete_export, ['nfs', 'export', 'delete', self.cluster_id,
                                                     self.pseudo_path])
        self._test_delete_cluster()

    def test_create_multiple_exports(self):
        '''
        Test creating multiple exports with different access type and path.
        '''
        # Export-1 with default values (access type = rw and path = '\')
        self._create_default_export()
        # Export-2 with r only
        self._create_export(export_id='2', extra_cmd=[self.pseudo_path+'1', '--readonly'])
        # Export-3 for subvolume with r only
        self._cmd('fs', 'subvolume', 'create', self.fs_name, 'sub_vol')
        fs_path = self._cmd('fs', 'subvolume', 'getpath', self.fs_name, 'sub_vol').strip()
        self._create_export(export_id='3', extra_cmd=[self.pseudo_path+'2', '--readonly', fs_path])
        # Export-4 for subvolume
        self._create_export(export_id='4', extra_cmd=[self.pseudo_path+'3', fs_path])
        # Check if exports gets listed
        self._test_list_detailed(fs_path)
        self._test_delete_cluster()
        # Check if rados ganesha conf object is deleted
        self._check_export_obj_deleted(conf_obj=True)
        self._check_auth_ls()

    def test_exports_on_mgr_restart(self):
        '''
        Test export availability on restarting mgr.
        '''
        self._create_default_export()
        # unload and load module will restart the mgr
        self._unload_module("cephadm")
        self._load_module("cephadm")
        self._orch_cmd("set", "backend", "cephadm")
        # Check if ganesha daemon is running
        self._check_nfs_cluster_status('running', 'Failed to redeploy NFS Ganesha cluster')
        # Checks if created export is listed
        self._test_list_export()
        port, ip = self._get_port_ip_info()
        self._test_mnt(self.pseudo_path, port, ip)
        self._delete_export()
        self._test_delete_cluster()

    def test_export_create_with_non_existing_fsname(self):
        '''
        Test creating export with non-existing filesystem.
        '''
        try:
            fs_name = 'nfs-test'
            self._test_create_cluster()
            self._nfs_cmd('export', 'create', 'cephfs', fs_name, self.cluster_id, self.pseudo_path)
            self.fail(f"Export created with non-existing filesystem {fs_name}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.ENOENT:
                raise
        finally:
            self._test_delete_cluster()

    def test_export_create_with_non_existing_clusterid(self):
        '''
        Test creating cephfs export with non-existing nfs cluster.
        '''
        try:
            cluster_id = 'invalidtest'
            self._nfs_cmd('export', 'create', 'cephfs', self.fs_name, cluster_id, self.pseudo_path)
            self.fail(f"Export created with non-existing cluster id {cluster_id}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.ENOENT:
                raise

    def test_export_create_with_relative_pseudo_path_and_root_directory(self):
        '''
        Test creating cephfs export with relative or '/' pseudo path.
        '''
        def check_pseudo_path(pseudo_path):
            try:
                self._nfs_cmd('export', 'create', 'cephfs', self.fs_name, self.cluster_id,
                              pseudo_path)
                self.fail(f"Export created for {pseudo_path}")
            except CommandFailedError as e:
                # Command should fail for test to pass
                if e.exitstatus != errno.EINVAL:
                    raise

        self._test_create_cluster()
        self._cmd('fs', 'volume', 'create', self.fs_name)
        check_pseudo_path('invalidpath')
        check_pseudo_path('/')
        check_pseudo_path('//')
        self._cmd('fs', 'volume', 'rm', self.fs_name, '--yes-i-really-mean-it')
        self._test_delete_cluster()

    def test_write_to_read_only_export(self):
        '''
        Test write to readonly export.
        '''
        self._test_create_cluster()
        self._create_export(export_id='1', create_fs=True, extra_cmd=[self.pseudo_path, '--readonly'])
        port, ip = self._get_port_ip_info()
        try:
            self._test_mnt(self.pseudo_path, port, ip)
        except CommandFailedError as e:
            # Write to cephfs export should fail for test to pass
            if e.exitstatus != errno.EPERM:
                raise
        self._test_delete_cluster()

    def test_cluster_info(self):
        '''
        Test cluster info outputs correct ip and hostname
        '''
        self._test_create_cluster()
        info_output = json.loads(self._nfs_cmd('cluster', 'info', self.cluster_id))
        info_ip = info_output[self.cluster_id][0].pop("ip")
        host_details = {self.cluster_id: [{
            "hostname": self._sys_cmd(['hostname']).decode("utf-8").strip(),
            "port": 2049
            }]}
        host_ip = self._sys_cmd(['hostname', '-I']).decode("utf-8").split()
        self.assertDictEqual(info_output, host_details)
        self.assertTrue(any([ip in info_ip for ip in host_ip]))
        self._test_delete_cluster()

    def test_cluster_set_reset_user_config(self):
        '''
        Test cluster is created using user config and reverts back to default
        config on reset.
        '''
        self._test_create_cluster()

        pool = 'nfs-ganesha'
        user_id = 'test'
        fs_name = 'user_test_fs'
        pseudo_path = '/ceph'
        self._cmd('fs', 'volume', 'create', fs_name)
        time.sleep(20)
        key = self._cmd('auth', 'get-or-create-key', f'client.{user_id}', 'mon',
            'allow r', 'osd',
            f'allow rw pool={pool} namespace={self.cluster_id}, allow rw tag cephfs data={fs_name}',
            'mds', f'allow rw path={self.path}').strip()
        config = f""" LOG {{
        Default_log_level = FULL_DEBUG;
        }}

        EXPORT {{
	        Export_Id = 100;
	        Transports = TCP;
	        Path = /;
	        Pseudo = {pseudo_path};
	        Protocols = 4;
	        Access_Type = RW;
	        Attr_Expiration_Time = 0;
	        Squash = None;
	        FSAL {{
	              Name = CEPH;
                      Filesystem = {fs_name};
                      User_Id = {user_id};
                      Secret_Access_Key = '{key}';
	        }}
        }}"""
        port, ip = self._get_port_ip_info()
        self.ctx.cluster.run(args=['sudo', 'ceph', 'nfs', 'cluster', 'config',
            'set', self.cluster_id, '-i', '-'], stdin=config)
        time.sleep(30)
        res = self._sys_cmd(['rados', '-p', pool, '-N', self.cluster_id, 'get',
                             f'userconf-nfs.ganesha-{user_id}', '-'])
        self.assertEqual(config, res.decode('utf-8'))
        self._test_mnt(pseudo_path, port, ip)
        self._nfs_cmd('cluster', 'config', 'reset', self.cluster_id)
        rados_obj_ls = self._sys_cmd(['rados', '-p', 'nfs-ganesha', '-N', self.cluster_id, 'ls'])
        if b'conf-nfs' not in rados_obj_ls and b'userconf-nfs' in rados_obj_ls:
            self.fail("User config not deleted")
        time.sleep(30)
        self._test_mnt(pseudo_path, port, ip, False)
        self._cmd('fs', 'volume', 'rm', fs_name, '--yes-i-really-mean-it')
        self._test_delete_cluster()

    def test_cluster_set_user_config_with_non_existing_clusterid(self):
        '''
        Test setting user config for non-existing nfs cluster.
        '''
        try:
            cluster_id = 'invalidtest'
            self.ctx.cluster.run(args=['sudo', 'ceph', 'nfs', 'cluster',
                'config', 'set', self.cluster_id, '-i', '-'], stdin='testing')
            self.fail(f"User config set for non-existing cluster {cluster_id}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.ENOENT:
                raise

    def test_cluster_reset_user_config_with_non_existing_clusterid(self):
        '''
        Test resetting user config for non-existing nfs cluster.
        '''
        try:
            cluster_id = 'invalidtest'
            self._nfs_cmd('cluster', 'config', 'reset', cluster_id)
            self.fail(f"User config reset for non-existing cluster {cluster_id}")
        except CommandFailedError as e:
            # Command should fail for test to pass
            if e.exitstatus != errno.ENOENT:
                raise
