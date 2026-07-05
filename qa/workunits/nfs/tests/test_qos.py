import pytest

from cephutil import ceph_cmd
import nfsutil


@pytest.mark.rate_limiting
class TestNFSQoS:
    def test_cluster_bandwidth_qos_enable(self, nfs_cfg):
        """Test cluster bandwidth QoS enable and get validation."""
        qos = nfsutil.enable_cluster_qos_bw(
            nfs_cfg['cluster_id'],
            nfs_cfg['bw_limit'],
            nfs_cfg['bw_limit'],
        )

        assert qos is not None
        nfsutil.assert_qos_fields(qos, {
            'enable_qos': True,
            'enable_bw_control': True,
            'combined_rw_bw_control': False,
            'qos_type': 'PerShare',
            'enable_iops_control': False,
            'max_export_write_bw': '2.0MiB',
            'max_export_read_bw': '2.0MiB',
        })

        show_qos = nfsutil.get_cluster_qos(nfs_cfg['cluster_id'])
        nfsutil.assert_qos_fields(show_qos, {
            'enable_qos': True,
            'enable_bw_control': True,
            'max_export_write_bw': '2.0MiB',
            'max_export_read_bw': '2.0MiB',
        })

        nfsutil.disable_cluster_qos_bw(nfs_cfg['cluster_id'])

    def test_cluster_cqos_msg_interval(self, nfs_cfg):
        """Test cluster QoS message interval set and get validation."""
        nfsutil.enable_cluster_qos_bw(
            nfs_cfg['cluster_id'],
            nfs_cfg['bw_limit'],
            nfs_cfg['bw_limit'],
        )
        qos = nfsutil.set_cluster_qos_interval(nfs_cfg['cluster_id'], 200)

        assert qos is not None
        nfsutil.assert_qos_fields(qos, {'cqos_msg_interval': 200})

        show_qos = nfsutil.get_cluster_qos(nfs_cfg['cluster_id'])
        nfsutil.assert_qos_fields(show_qos, {'cqos_msg_interval': 200})

        nfsutil.disable_cluster_qos_bw(nfs_cfg['cluster_id'])

    def test_cluster_bandwidth_qos_disable(self, nfs_cfg):
        """Test cluster bandwidth QoS disable and get validation."""
        nfsutil.enable_cluster_qos_bw(
            nfs_cfg['cluster_id'],
            nfs_cfg['bw_limit'],
            nfs_cfg['bw_limit'],
        )
        qos = nfsutil.disable_cluster_qos_bw(nfs_cfg['cluster_id'])

        assert qos is not None
        nfsutil.assert_qos_fields(qos, {
            'enable_qos': False,
            'enable_bw_control': False,
            'combined_rw_bw_control': False,
            'enable_iops_control': False,
        })

        show_qos = nfsutil.get_cluster_qos(nfs_cfg['cluster_id'])
        nfsutil.assert_qos_fields(show_qos, {
            'enable_qos': False,
            'enable_bw_control': False,
        })

    def test_cluster_iops_qos_enable(self, nfs_cfg):
        """Test cluster IOPS QoS enable and get validation."""
        qos = nfsutil.enable_cluster_qos_ops(
            nfs_cfg['cluster_id'],
            nfs_cfg['iops_limit'],
        )

        assert qos is not None
        nfsutil.assert_qos_fields(qos, {
            'enable_qos': True,
            'enable_bw_control': False,
            'combined_rw_bw_control': False,
            'qos_type': 'PerShare',
            'enable_iops_control': True,
            'max_export_iops': nfs_cfg['iops_limit'],
        })

        show_qos = nfsutil.get_cluster_qos(nfs_cfg['cluster_id'])
        nfsutil.assert_qos_fields(show_qos, {
            'enable_iops_control': True,
            'max_export_iops': nfs_cfg['iops_limit'],
        })

        nfsutil.disable_cluster_qos_ops(nfs_cfg['cluster_id'])

    def test_cluster_iops_qos_disable(self, nfs_cfg):
        """Test cluster IOPS QoS disable and get validation."""
        nfsutil.enable_cluster_qos_ops(
            nfs_cfg['cluster_id'],
            nfs_cfg['iops_limit'],
        )
        nfsutil.disable_cluster_qos_ops(nfs_cfg['cluster_id'])
        show_qos = nfsutil.get_cluster_qos(nfs_cfg['cluster_id'])
        assert show_qos.get('enable_iops_control') is False

    def test_export_bandwidth_qos(self, nfs_cfg):
        """Test export bandwidth QoS enable and get validation."""
        nfsutil.enable_cluster_qos_bw(
            nfs_cfg['cluster_id'],
            nfs_cfg['bw_cluster_default'],
            nfs_cfg['bw_cluster_default'],
        )
        qos = nfsutil.enable_export_qos_bw(
            nfs_cfg['cluster_id'],
            nfs_cfg['pseudo_path'],
            nfs_cfg['bw_limit'],
            nfs_cfg['bw_limit'],
        )

        assert qos is not None
        nfsutil.assert_qos_fields(qos, {
            'enable_qos': True,
            'enable_bw_control': True,
            'combined_rw_bw_control': False,
            'max_export_write_bw': '2.0MiB',
            'max_export_read_bw': '2.0MiB',
            'enable_iops_control': False,
            'global_enable_qos': True,
            'global_enable_bw_control': True,
            'global_enable_iops_control': False,
        })

        show_qos = nfsutil.get_export_qos(
            nfs_cfg['cluster_id'],
            nfs_cfg['pseudo_path'],
        )
        nfsutil.assert_qos_fields(show_qos, {
            'max_export_write_bw': '2.0MiB',
            'max_export_read_bw': '2.0MiB',
            'global_enable_bw_control': True,
        })

        nfsutil.disable_export_qos_bw(
            nfs_cfg['cluster_id'], nfs_cfg['pseudo_path'])
        nfsutil.disable_cluster_qos_bw(nfs_cfg['cluster_id'])

    def test_export_iops_qos(self, nfs_cfg):
        """Test export IOPS QoS enable and get validation."""
        nfsutil.enable_cluster_qos_ops(
            nfs_cfg['cluster_id'],
            nfs_cfg['iops_cluster_default'],
        )
        qos = nfsutil.enable_export_qos_ops(
            nfs_cfg['cluster_id'],
            nfs_cfg['pseudo_path'],
            nfs_cfg['iops_limit'],
        )

        assert qos is not None
        nfsutil.assert_qos_fields(qos, {
            'enable_qos': True,
            'enable_bw_control': False,
            'combined_rw_bw_control': False,
            'enable_iops_control': True,
            'max_export_iops': nfs_cfg['iops_limit'],
            'global_enable_qos': True,
            'global_enable_bw_control': False,
            'global_enable_iops_control': True,
        })

        show_qos = nfsutil.get_export_qos(
            nfs_cfg['cluster_id'],
            nfs_cfg['pseudo_path'],
        )
        nfsutil.assert_qos_fields(show_qos, {
            'max_export_iops': nfs_cfg['iops_limit'],
            'global_enable_iops_control': True,
        })

        nfsutil.disable_export_qos_ops(
            nfs_cfg['cluster_id'], nfs_cfg['pseudo_path'])
        nfsutil.disable_cluster_qos_ops(nfs_cfg['cluster_id'])

    def test_export_qos_without_cluster_fails(self, nfs_cfg):
        """Export QoS enable must fail when cluster QoS is disabled."""
        nfsutil.cleanup_qos(nfs_cfg)

        bw_result = ceph_cmd([
            'nfs', 'export', 'qos', 'enable', 'bandwidth_control',
            nfs_cfg['cluster_id'], nfs_cfg['pseudo_path'],
            '--max_export_write_bw', '50MiB',
            '--max_export_read_bw', '50MiB',
        ], check=False)
        assert bw_result.returncode != 0, (
            'export bandwidth QoS enable succeeded without cluster QoS'
        )

        ops_result = ceph_cmd([
            'nfs', 'export', 'qos', 'enable', 'ops_control',
            nfs_cfg['cluster_id'], nfs_cfg['pseudo_path'],
            '--max_export_iops', str(nfs_cfg['iops_limit']),
        ], check=False)
        assert ops_result.returncode != 0, (
            'export IOPS QoS enable succeeded without cluster QoS'
        )
