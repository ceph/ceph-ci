'''
Test v3-specific features.
'''
from uuid import UUID
from errno import ENOENT
from os.path import basename, dirname,join
from logging import getLogger

from tasks.cephfs.helpers.misc import (gen_name, gen_subvol_name,
                                       gen_subvol_grp_name,
                                       gen_subvol_snap_name)
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.helpers.subvolumes import SubvolV3

from teuthology.exceptions import CommandFailedError


log = getLogger(__name__)


def verify_uuid(uuid):
    assert type(uuid) is str
    UUID(uuid, version=4)


class TestLayout(CephFSTestCase):
    '''
    Test v3 subvols have their directory layout as expected.
    '''

    def wait_for_trash_dir_to_be_empty(self, timeout=60):
        self.mount_a.wait_for_dir_empty('volumes/_deleting', timeout)

    def test_for_basic_subvol(self):
        sv = SubvolV3(self.mon_manager, self.mount_a, self.fs.name)
        sv.create()

        subvol_mnt_path = sv.getpath()
        self.assertEqual(dirname(dirname(dirname(sv.build_and_get_base_path()))),
                         sv.build_and_get_base_path())
        self.assertEqual(basename(subvol_mnt_path), sv.MNT_DIR)
        self.assertEqual(basename(dirname(dirname(subvol_mnt_path))),
                         sv.INCAR_BASE_DIR)
        self.assertEqual(sv.build_and_get_mnt_path(), sv.getpath())

        uuid = sv.get_uuid_from_mnt_path(subvol_mnt_path)
        verify_uuid(uuid)

        self.assertIn(sv.name, sv.ls()[0].values())

        # check that layout have been created.
        subvol_paths = (sv.meta_symlink_path, sv.meta_path, sv.roots_path,
                        sv.uuid_path, sv.subvol_mnt_path)
        for path in subvol_paths:
            self.mount_a.run_shell(f'ls ./{path}')
            file_type = self.mount_a.get_shell_cmd_stdout('stat -c "%F" '
                                                          f'{path}')
            if '.meta' in path:
                if '.meta.' in path:
                    self.assertEqual('regular file', file_type)
                else:
                    self.assertEqual('symbolic link', file_type)
            else:
                self.assertEqual('directory', file_type)

        # test writing files on this path.
        sv.write_files()
        for i in range(5):
            self.mount_a.run_shell(f'ls ./{join(subvol_mnt_path, f"file-{i}")}')

        sv.rm()
        self.mount_a.negtestcmd(f'ls ./{subvol_mnt_path}', retval=2)
        self.wait_for_trash_dir_to_be_empty()

    def test_for_snap(self):
        sv = SubvolV3(self.mon_manager, self.mount_a, self.fs.name)
        sv.create()
        subvol_mnt_path = sv.getpath()

        snap_name = sv.snap_create()
        snap_path = sv.snap_getpath(snap_name)

        self.mount_a.run_shell(f'ls ./{snap_path}')
        self.assertIn(snap_name, sv.ls_snap()[0].values())
        self.assertEqual(sv.build_and_get_snap_path(), sv.snap_getpath())

        sv.snap_rm(snap_name)
        sv.rm()
        self.mount_a.negtestcmd(f'ls ./{subvol_mnt_path}', retval=2)
        self.wait_for_trash_dir_to_be_empty()

    def test_for_snap_retained(self):
        sv = SubvolV3(self.mon_manager, self.mount_a, self.fs.name)
        sv.create()
        subvol_mnt_path = sv.getpath()
        snap_name = sv.snap_create()
        snap_path = sv.snap_getpath(snap_name)

        sv.rm(opts='--retain-snapshots')

        retained_path = join(dirname(subvol_mnt_path), '.retained')
        self.mount_a.run_shell(f'ls ./{retained_path}')

        # test that snap path is unaffected
        self.mount_a.run_shell(f'ls ./{snap_path}')

        # test that subvol ls and snap ls still report this subvol and snap
        self.assertIn(sv.name, sv.ls()[0].values())
        self.assertEqual(sv.build_and_get_mnt_path(), sv.getpath())
        self.assertIn(snap_name, sv.ls_snap()[0].values())
        self.assertEqual(sv.build_and_get_snap_path(), sv.snap_getpath())

        sv.rm_snap()
        self.mount_a.negtestcmd(f'ls ./{snap_path}', retval=2)
        self.mount_a.negtestcmd(f'ls ./{subvol_mnt_path}', retval=2)
        self.wait_for_trash_dir_to_be_empty()


class TestIncarnations(CephFSTestCase):
    '''
    Test only features specific to subvolumes v3 here, since usual operations like
    create, remove, ls, etc are tested for v3 too through usual tests.
    '''

    def wait_for_trash_dir_to_be_empty(self, timeout=60):
        self.mount_a.wait_for_dir_empty('volumes/_deleting', timeout)

    def test_retain_snap(self):
        '''
        Test that an incarnation is retained if it the subvlume was removed with
        --retain-snapshots.
        '''
        sv = SubvolV3(self.mon_manager, self.mount_a, self.fs.name)
        sv.create()
        subvol_path = sv.getpath()
        sv.write_files(num=5)
        snap_name = sv.create_snap()

        snap_path = sv.snap_getpath()
        sv.rm(opts='--retain-snapshots')

        self.assertIn(sv.name, sv.ls()[0].values())
        self.assertEqual(sv.build_and_get_mnt_path(), sv.getpath())

        self.assertIn(snap_name, sv.ls_snap())
        self.assertEqual(sv.build_and_get_snap_path(snap_name), snap_path)

        sv.rm()
        self.mount_a.negtestcmd(f'ls ./{subvol_mnt_path}', retval=2)
        self.wait_for_trash_dir_to_be_empty()

    def test_removing_retained_snap_removes_subvol(self):
        '''
        Test that an incarnation is retained if it the subvlume was removed but
        snapshots were retained.
        '''
        sv = SubvolV3(self.fs.name, gen_subvol_name())
        sv.create()
        subvol_path = sv.getpath()
        for i in range(5):
            file_path = f'{subvol_path}/file-{i}'
            self.mount_a.run_shell(f'echo abcd | tee {file_path}')
        snap_name = sv.create_snap()

        snap_path = sv.snap_getpath()
        sv.rm(opts='--retain-snapshots')
        self.mount_a.run_shell(f'ls ./{snap_path}')

        sv.snap_rm(snap_name)
        self.assertNotIn(sv.name, sv.ls())
        with self.assertRaise(CommandFailedError) as e:
            sv.getpath()
            self.assertEqual(e.exception.exitstatus, ENOENT)
        self.wait_for_trash_dir_to_be_empty()

    def _rm_incar_and_check(self, sv, snap_paths, index):
        self.mount_a.negtest_cmd(f'ls ./{snap_paths[-1]}', retval=22)
        snap_paths.pop(-1)
        self._check_snap_ls_and_getpath_output(sv, snap_paths)

    def _check_snap_ls_and_getpath_output(self, sv, snap_paths):
        all_snap_names = sv.ls_snap()
        for i in all_snap_names:
            self.assertIn(i, all_snap_names)
            path = sv.snap_getpath(i)
            self.assertIn(path, snap_paths)

    def test_create(self):
        '''
        Create a subvol with 7 incars and delete first, last and a middle
        subvolume among other things.
        '''
        sv = SubvolV3(self.fs.name, gen_subvol_name())
        subvol_path = None
        all_snap_paths = set()
        all_uuids = set()

        # create 7 incars
        for i in range(7):
            sv.create()
            sv.write_files(num=5, sfx=i)
            snap_name = sv.snap_create()

            uuid = get_uuid_from_snap_path(snap_path)
            if uuid in all_uuids:
                raise RuntimeError('UUIDs must be unique!')
            all_uuids.add(uuid)

            snap_path = sv.snap_getpath(snap_name)
            if snap_path in all_snap_paths:
                raise RuntimeError('subvol snap paths must be unique!')
            all_snap_paths.add(snap_path)

            sv.rm(opts='--retain-snapshots')
            self.mount_a.negtestcmd(f'ls ./{snap_path}')

    def test_rm(self):
        self._check_snap_ls_and_getpath_output(sv, snap_paths)
        self._rm_incar_and_check(sv, snap_paths, 0)
        self._rm_incar_and_check(sv, snap_paths, -1)
        self._rm_incar_and_check(sv, snap_paths, 3)

        for i in sv.snaps:
            sv.snap_rm(i)
            self.mount_a.negtest_cmd(f'ls ./{snap_paths[i]}', retval=22)

        self.mount_a.negtest_cmd(f'ls ./{subvol_path}', retval=22)
        self.assertNotIn(sv.name, sv.ls())
        self.wait_for_trash_dir_to_be_empty()
