'''
Helper code for operating on subvolume v3 and v2.
'''
from logging import getLogger
from os.path import join
from uuid import uuid4, UUID
from textwrap import dedent
from json import loads
from typing import Final

from tasks.ceph_manager import RunCephCmd


log = getLogger(__name__)


DEFAULT_GRP_NAME = '_nogroup'


def verify_uuid(uuid):
    assert type(uuid) is str

    try:
        UUID(uuid, version=4)
    except:
        # just being explicit about exception being raised here
        raise


class SubvolOps(RunCephCmd):
    '''
    Helper methods for running 'ceph fs subvolume' commands.
    '''

    def __init__(self, mon_manager):
        super().__init__(mon_manager)


    # ----- helpers for subvol ops -----


    def create_subvol(self, volname, subvol_name, grp_name=None, opts=None):
        cmd = f'fs subvolume create {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'
        if opts:
            cmd += f' {opts}'

        return self.run_ceph_cmd(cmd)

    def rm_subvol(self, volname, subvol_name, grp_name=None):
        cmd = f'fs subvolume rm {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return self.run_ceph_cmd(cmd)

    def get_subvol_path(self, volname, subvol_name, grp_name=None):
        cmd = f'fs subvolume getpath {volname} {subvol_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return self.get_ceph_cmd_stdout(cmd).strip()

    def ls_subvol(self, volname, grp_name=None):
        cmd = f'fs subvolume ls {volname}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return loads(self.get_ceph_cmd_stdout(cmd))

    def auth_subvol(self, volname, subvol_name, client_id, grp_name=None):
        cmd = f'fs subvolume authorize {volname} {subvol_name} {client_id}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        self.run_ceph_cmd(cmd)


    # ----- helpers for subvol snap ops -----


    def create_snap(self, volname, subvol_name, snap_name, grp_name=None):
        cmd = (f'fs subvolume snapshot create {volname} {subvol_name} '
               f'{snap_name}')
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return self.run_ceph_cmd(cmd)

    def rm_snap(self, volname, subvol_name, snap_name, grp_name=None):
        cmd = (f'fs subvolume snapshot remove {volname} {subvol_name} '
               f'{snap_name}')
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return self.run_ceph_cmd(cmd)

    def get_snap_path(self, volname, subvol_name, snap_name, grp_name=None):
        cmd = (f'fs subvolume snapshot getpath {volname} {subvol_name} '
               f'{snap_name}')
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return self.get_ceph_cmd_stdout(cmd).strip()

    def ls_snap(self, volname, subvol_name, snap_name, grp_name=None):
        cmd = f'fs subvolume snapshot ls {volname} {subvol_name} {snap_name}'
        if grp_name:
            cmd += f' --group-name {grp_name}'

        return loads(self.get_ceph_cmd_stdout(cmd))


class SubvolBase(SubvolOps):
    '''
    Base class for v0, v1, v2, and v3 subvol helper classes.
    '''


    # ----- init and related methods -----


    def __init__(self, tco=None, volname=None, name=None, uuid=None,
                 grp_name=None, snaps=[], retained=False):
        '''
        tco -> test class object

        - grp_name and snap_name can be str, None or True.
            - None implies no group and no snap
            - True implies gen random str and use it as name
            - str implies use it as the name

        - retained can be True or False
            - has subvol been removed and snap has been retained

        - rest can be str or None
        '''
        self.tco = tco
        self._get_tco_members()
        super().__init__(self.mon_manager)

        self.volname = volname if volname else self.tco.volname
        self.grp_name = self._gen_subvol_grp_name() if grp_name is True else grp_name
        self.name = name if name else self._gen_subvol_name()
        self.snaps = snaps
        self.retained = retained

        # NOTE subclasses should call these methods at end of their __init__()
        # self._reset_paths()
        # self._verify_attrs()
        return

    def _get_tco_members(self):
        '''
        Get (only) needed members. This make it easy to use them as they are
        located within a namespace within object namespace and "only" so that
        ths helper classes are not croweded with unnecessary members.
        '''
        # members from CephFSTestCase
        self.fs = self.tco.fs
        self.mount_a = self.tco.mount_a
        self.mon_manager = self.tco.mon_manager

        # members from VolumesHelper
        self._gen_name = self.tco._gen_name
        self._gen_subvol_name = self.tco._gen_subvol_name
        self._gen_subvol_snap_name = self.tco._gen_subvol_snap_name
        self._gen_subvol_grp_name = self.tco._gen_subvol_grp_name

        # members from unittest
        self.assertIn = self.tco.assertIn
        self.assertEqual = self.tco.assertEqual

    def _verify_attrs(self):
        assert type(self.volname) is str, \
            f'self.volname = {self.volname}'
        assert type(self.grp_name) in (type(None), str), \
            f'self.grp_name = {self.grp_name}'
        assert type(self.name) is str, \
            f'self.name = {self.name}'

        assert type(self.snaps) is list, \
            f'self.snaps = {self.snaps}'
        assert type(self.retained) is bool, \
            f'self.retained = {self.retained}'

    def _reset_paths(self):
        self._base_path = None
        self._meta_path = None


    # ----- subvol basics -----


    @property
    def version(self):
        raise NotImplementedError('should\'ve been overridden')

    @property
    def base_path(self):
        if not self._base_path:
            grp_name = self.grp_name if self.grp_name else DEFAULT_GRP_NAME
            self._base_path = join('/volumes', grp_name, self.name)
        return self._base_path

    @property
    def data_path(self):
        '''
        Path where subvol users are supposed to store data can be different
        depenidng on the subvol version. This method, through overriding, allows
        writing code for subvolumes regardless of different data path for subvol
        version.
        '''
        raise NotImplementedError('should have been overriden classes for '
                                  'specific subvol versions')


    # ----- helpers for subvol ops -----


    def create(self, opts=None):
        self.create_subvol(self.volname, self.name, self.grp_name, opts)

    def rm(self):
        self.rm_subvol(self.volname, self.name, self.grp_name)

    def getpath(self):
        return self.get_subvol_path(self.volname, self.name, self.grp_name).\
                strip()

    def ls(self):
        return self.ls_subvol(self.volname, self.grp_name)

    def auth(self, client_id):
        self.auth_subvol(self.volname, self.name, client_id, self.grp_name)


    # ----- helpers for subvol snap ops -----


    def create_snap(self, snap_name):
        return super().create_snap(self.volname, self.name, snap_name,
                                   self.grp_name)

    def rm_snap(self, snap_name):
        return super().rm_snap(self.volname, self.name, snap_name,
                               self.grp_name)

    def get_snap_path(self, snap_name):
        return super().get_snap_path(self.volnmae, self.name, snap_name,
                                     self.grp_name).strip()

    def ls_snap(self):
        return super().ls_snap(self.volname, self.name, self.grp_name)


    # ----- helpers for sanity testing ------


    # XXX: do _not_ open subvol using volumes plugin as it will trigger
    # auto-upgrade for the subvol.
    def sanity_test_custom_made_subvol(self):
        ls_output = self.ls()
        self.assertEqual(ls_output[0]['name'], self.name)

    # XXX: do _not_ open subvol using volumes plugin as it will trigger
    # auto-upgrade for the subvol.
    def sanity_test_custom_made_snap(self, snap_name):
        snap_path = self.get_snap_path_str(snap_name)
        self.mount_a.run_shell(f'stat {snap_path[1:]}')

    def sanity_test_subvol_ls(self):
        ls_output = self.ls()
        self.assertEqual(ls_output[0]['name'], self.name)

    def sanity_test_subvol_getpath(self):
        sv_path = self.getpath()
        self.assertEqual(sv_path, self.data_path)

    def sanity_test_snap_ls(self, snap_name):
        ls_output = self.ls_snap()
        self.assertEqual(ls_output[0]['name'], snap_name)

    def sanity_test_snap_getpath(self, snap_name):
        exp_snap_path = self.get_snap_path_str(snap_name)
        getpath = self.get_snap_path(snap_name)
        self.assertEqual(getpath, exp_snap_path)

    def sanity_test_retained_subvol(self):
        assert self.retained
        raise NotImplementedError()

    def sanity_test_retained_snap(self):
        assert self.retained
        raise NotImplementedError()


class SubvolV2(SubvolBase):
    '''
    Helper class for v2 subvolumes.
    '''

    def __init__(self, tco=None, volname=None, name=None, uuid=None, 
                 grp_name=None, snaps=[], retained=False):
        super().__init__(tco=tco, volname=volname, name=name, uuid=uuid,
                         grp_name=grp_name, snaps=snaps, retained=retained)

        self.uuid = uuid if uuid else str(uuid4())

        self._reset_paths()
        self._verify_attrs()

    def _reset_paths(self):
        super()._reset_paths()

        self._uuid_path = None
        self._snap_base_path = None

    def _verify_attrs(self):
        super()._verify_attrs()

        assert type(self.uuid) is str, \
            f'self.uuid = {self.uuid}'


    # ----- subvol basics -----


    @property
    def version(self):
        return 2

    @property
    def data_path(self):
        return self.uuid_path


    # ----- v2 helpers -----


    @property
    def uuid_path(self):
        if not self._uuid_path:
            self._uuid_path = join(self.base_path, self.uuid)
        return self._uuid_path

    @property
    def meta_path(self):
        if not self._meta_path:
            self._meta_path = join(self.base_path, '.meta')
        return self._meta_path

    def get_snap_base_path_str(self):
        if not self._snap_base_path:
            self._snap_base_path = join(self.uuid_path, '.snap')
        return self._snap_base_path

    def get_snap_path_str(self, snap_name):
        return join(self.get_snap_base_path_str(), snap_name)


    # ----- create stuff without create command ------


    def custom_create(self):
        '''
        For testing subvol auto-upgrade, Create v2 subvol without using
        'subvolume create' command.
        '''
        data_path = self.data_path[1:]
        self.mount_a.run_shell(f'mkdir -p {data_path}')

        self.mount_a.write_file(self.meta_path[1:], dedent(f'''\
            [GLOBAL]
            version = 2
            type = subvolume
            path = {self.data_path}
            state = complete'''), sudo=True)

        # 755 so that files can accessed without unnecessary hassles
        self.mount_a.run_shell(f'chmod -R 755 {data_path}')

        for i in range(1, 6):
            self.mount_a.run_shell(f'dd if=/dev/zero of={data_path}/file-{i} bs=1M count=1')

        if self.retained:
            raise NotImplementedError()

    def custom_snap_create(self, snap_name=None):
        snap_name = snap_name if snap_name else self._gen_subvol_snap_name()

        self.mount_a.run_libcephfs_pybind_code(dedent(f"""
        cephfs.mksnap('{self.get_snap_base_path_str()}', '{snap_name}', 0o755)
        """))

        snap_path = self.get_snap_path_str(snap_name)
        # verify that snap was created
        self.mount_a.run_shell(f'stat {snap_path[1:]}')

        self.snaps.append(snap_name)
        return snap_name


class SubvolV3(SubvolBase):
    '''
    Helper class for v3 subvolumes.
    '''

    INCAR_BASE_DIR = 'incars'
    MNT_DIR = 'mnt'


    # ----- init and its helpers -----


    def __init__(self, tco=None, volname=None, name=None, uuid=None,
                 grp_name=None, snaps=[], retained=False, v2=None):
        self._reset_paths()

        if v2:
            assert not (tco or volname or name or uuid or grp_name or snaps or
                        retained)
            assert type(v2) is SubvolV2

            super().__init__(tco=v2.tco, volname=v2.volname, name=v2.name,
                             uuid=v2.uuid, grp_name=v2.grp_name, snaps=v2.snaps,
                             retained=v2.retained)
            self.has_v2_snaps = True if v2.snaps else False
            self.uuid = self.fetch_uuid() if self.has_v2_snaps else v2.uuid
        else:
            super().__init__(tco=tco, volname=volname, name=name, uuid=uuid,
                             grp_name=grp_name, snaps=snaps, retained=retained)
            self.has_v2_snaps = False
            self.uuuid = str(uuid4())

        self._verify_attrs()

    def _reset_paths(self):
        super()._reset_paths()

        self._incar_base_path = None
        self._meta_symlink_path = None
        self._meta_file_name = None

    def _verify_attrs(self):
        super()._verify_attrs()

        assert type(self.uuid) is str, \
                f'self.uuid = {self.uuid}'
        assert type(self.has_v2_snaps) is bool, \
                f'self.has_v2_snaps = {self.has_v2_snaps}'


    # ----- subvol helper -----


    @property
    def version(self):
        return 3

    @property
    def data_path(self):
        return self.get_mnt_path_str()


    # ----- v3 helpers -----


    @property
    def incar_base_path(self):
        if not self._incar_base_path:
            self._incar_base_path = join(self.base_path, self.INCAR_BASE_DIR)
        return self._incar_base_path

    @property
    def meta_path(self):
        if not self._meta_path:
            self._meta_path = join(self.base_path, self.meta_file_name)
        return self._meta_path

    @property
    def meta_file_name(self):
        if not self._meta_file_name:
            self._meta_file_name = f'.meta.{self.uuid}'
        return self._meta_file_name

    @property
    def meta_symlink_path(self):
        if not self._meta_symlink_path:
            self._meta_symlink_path = join(self.base_path, '.meta')
        return self._meta_symlink_path

    def get_incar_path_str(self, uuid=None):
        uuid = uuid if uuid else self.uuid
        return join(self.incar_base_path, self.uuid)

    def get_mnt_path_str(self, uuid=None):
        return join(self.get_incar_path_str(uuid), self.MNT_DIR)

    def get_snap_base_path_str(self, uuid=None):
        return join(self.get_incar_path_str(uuid), '.snap')

    def get_snap_path_str(self, snap_name, uuid=None):
        return join(self.get_snap_base_path_str(uuid), snap_name)


    # ---- misc methods -----


    def fetch_uuid(self):
        curr_incar_meta_file_name = self.mount_a.get_shell_stdout(
                f'readlink {self.meta_symlink_path[1:]}').strip()
        uuid = curr_incar_meta_file_name.replace('.meta.', '')
        verify_uuid(uuid)
        return uuid

    def sanity_test_meta_file(self):
        meta_content = self.mount_a.get_shell_stdout('sudo cat '
                                                     f'{self.meta_path[1:]}')

        # splitting ensures we compare line by line, which prevents any
        # accidental matching due to edge cases
        meta_content = meta_content.split('\n')

        self.assertIn('version = 3', meta_content)
        self.assertIn(f'path = {self.get_mnt_path_str()}', meta_content)
        self.assertIn('state = complete', meta_content)
        self.assertIn('type = subvolume', meta_content)

        if self.has_v2_snaps:
            self.assertIn('has_v2_snaps = True', meta_content)
