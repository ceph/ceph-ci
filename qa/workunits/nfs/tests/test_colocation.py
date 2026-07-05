import pytest

import nfsutil


@pytest.mark.colocation
class TestNFSColocation:
    def test_daemon_colocation_custom_ports(self, nfs_cfg):
        """Test colocated daemons with explicit colocation_ports."""
        cluster_id = nfs_cfg['cluster_id']
        nfs_hostname = nfsutil.get_nfs_hostname(cluster_id)
        nfs_addr = nfsutil.get_host_addr(nfs_hostname)
        colo_ports = [
            nfs_cfg['colo_data_port'],
            nfs_cfg['colo_mon_port'],
            nfs_cfg['colo_qos_port'],
        ]

        nfsutil.apply_nfs_colocation_spec(
            cluster_id, nfs_hostname, nfs_cfg,
            custom_colocation_ports=True,
        )
        nfsutil.assert_nfs_colocation_spec(
            cluster_id, nfs_hostname, nfs_cfg,
            custom_colocation_ports=True,
        )

        show_spec = nfsutil.export_service_spec(cluster_id)
        assert show_spec['placement']['count_per_host'] == nfs_cfg['count_per_host']
        assert show_spec['spec']['colocation_ports'][0]['data_port'] == (
            nfs_cfg['colo_data_port']
        )

        nfsutil.verify_colocated_daemons(
            cluster_id, nfs_hostname, nfs_addr, nfs_cfg, colo_ports,
        )

    def test_daemon_colocation_default_ports(self, nfs_cfg):
        """Test colocated daemons when colocation_ports is omitted (base + 1)."""
        cluster_id = nfs_cfg['cluster_id']
        nfs_hostname = nfsutil.get_nfs_hostname(cluster_id)
        nfs_addr = nfsutil.get_host_addr(nfs_hostname)
        colo_ports = nfsutil.default_colo_ports(nfs_cfg)

        nfsutil.apply_nfs_colocation_spec(
            cluster_id, nfs_hostname, nfs_cfg,
            custom_colocation_ports=False,
        )
        nfsutil.assert_nfs_colocation_spec(
            cluster_id, nfs_hostname, nfs_cfg,
            custom_colocation_ports=False,
        )

        nfsutil.verify_colocated_daemons(
            cluster_id, nfs_hostname, nfs_addr, nfs_cfg, colo_ports,
        )
