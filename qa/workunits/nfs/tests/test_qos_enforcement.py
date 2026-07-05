import socket

import pytest

import nfsutil


@pytest.mark.qos_enforcement
class TestNFSQoSEnforcement:
    def test_mount(self, nfs_cfg_with_qos_cleanup):
        """Verify default NFS mount before fio checks."""
        nfs_hostname = socket.gethostname()
        nfsutil.mount_and_write_test(
            nfs_hostname, nfs_cfg_with_qos_cleanup['data_port'],
            nfs_cfg_with_qos_cleanup['pseudo_path'],
            nfs_cfg_with_qos_cleanup['mount_point'], 'qos',
        )

    def test_cluster_bandwidth_qos_enforcement(self, nfs_cfg_with_qos_cleanup):
        """Verify cluster bandwidth QoS limits write throughput."""
        nfs_cfg = nfs_cfg_with_qos_cleanup
        limit_bytes = nfsutil.bw_limit_bytes(nfs_cfg['bw_limit'])
        baseline_bw = nfsutil.measure_write_bw(nfs_cfg)

        nfsutil.enable_cluster_qos_bw(
            nfs_cfg['cluster_id'],
            nfs_cfg['bw_limit'],
            nfs_cfg['bw_limit'],
        )
        cluster_bw = nfsutil.measure_write_bw(nfs_cfg)
        nfsutil.assert_at_most(
            cluster_bw, limit_bytes, nfs_cfg['tolerance_pct'])
        if baseline_bw > limit_bytes * 2:
            nfsutil.assert_less_than(
                cluster_bw, baseline_bw,
                'cluster QoS did not reduce write bandwidth below baseline',
            )
        nfsutil.disable_cluster_qos_bw(nfs_cfg['cluster_id'])

    def test_cluster_iops_qos_enforcement(self, nfs_cfg_with_qos_cleanup):
        """Verify cluster IOPS QoS limits write IOPS."""
        nfs_cfg = nfs_cfg_with_qos_cleanup
        baseline_iops = nfsutil.measure_write_iops(nfs_cfg)

        nfsutil.enable_cluster_qos_ops(
            nfs_cfg['cluster_id'], nfs_cfg['iops_limit'],
        )
        cluster_iops = nfsutil.measure_write_iops(nfs_cfg)
        nfsutil.assert_at_most(
            cluster_iops, nfs_cfg['iops_limit'], nfs_cfg['tolerance_pct'])
        if baseline_iops > nfs_cfg['iops_limit'] * 2:
            nfsutil.assert_less_than(
                cluster_iops, baseline_iops,
                'cluster QoS did not reduce write IOPS below baseline',
            )
        nfsutil.disable_cluster_qos_ops(nfs_cfg['cluster_id'])

    def test_export_bandwidth_qos_enforcement(self, nfs_cfg_with_qos_cleanup):
        """Verify export bandwidth QoS limits write throughput."""
        nfs_cfg = nfs_cfg_with_qos_cleanup
        limit_bytes = nfsutil.bw_limit_bytes(nfs_cfg['bw_limit'])

        nfsutil.enable_cluster_qos_bw(
            nfs_cfg['cluster_id'],
            nfs_cfg['bw_cluster_default'],
            nfs_cfg['bw_cluster_default'],
        )
        nfsutil.enable_export_qos_bw(
            nfs_cfg['cluster_id'],
            nfs_cfg['pseudo_path'],
            nfs_cfg['bw_limit'],
            nfs_cfg['bw_limit'],
        )
        export_bw = nfsutil.measure_write_bw(nfs_cfg)
        nfsutil.assert_at_most(
            export_bw, limit_bytes, nfs_cfg['tolerance_pct'])
        nfsutil.disable_export_qos_bw(
            nfs_cfg['cluster_id'], nfs_cfg['pseudo_path'])
        nfsutil.disable_cluster_qos_bw(nfs_cfg['cluster_id'])

    def test_export_iops_qos_enforcement(self, nfs_cfg_with_qos_cleanup):
        """Verify export IOPS QoS limits write IOPS."""
        nfs_cfg = nfs_cfg_with_qos_cleanup
        nfsutil.enable_cluster_qos_ops(
            nfs_cfg['cluster_id'], nfs_cfg['iops_cluster_default'],
        )
        nfsutil.enable_export_qos_ops(
            nfs_cfg['cluster_id'],
            nfs_cfg['pseudo_path'],
            nfs_cfg['iops_limit'],
        )
        export_iops = nfsutil.measure_write_iops(nfs_cfg)
        nfsutil.assert_at_most(
            export_iops, nfs_cfg['iops_limit'], nfs_cfg['tolerance_pct'])
        nfsutil.disable_export_qos_ops(
            nfs_cfg['cluster_id'], nfs_cfg['pseudo_path'])
        nfsutil.disable_cluster_qos_ops(nfs_cfg['cluster_id'])
