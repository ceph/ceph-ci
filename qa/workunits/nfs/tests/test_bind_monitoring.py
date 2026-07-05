import os

import pytest

import nfsutil


@pytest.mark.bind_monitoring
class TestNFSBindMonitoring:
    @pytest.fixture(scope='class')
    def bind_cfg(self):
        cfg = nfsutil.nfs_config()
        cfg['data_port'] = int(os.environ.get('NFS_BIND_DATA_PORT', '12049'))
        cfg['mon_port'] = int(os.environ.get('NFS_BIND_MON_PORT', '10587'))
        return cfg

    def test_bind_and_monitoring(self, bind_cfg):
        """Test bind/monitoring ip_addrs, ports, mount, and metrics endpoint."""
        cluster_id = bind_cfg['cluster_id']
        nfs_hostname = nfsutil.get_nfs_hostname(cluster_id)
        bind_addr = nfsutil.get_host_addr(nfs_hostname)
        mon_addr = bind_addr

        nfsutil.apply_nfs_bind_spec(
            cluster_id, nfs_hostname, bind_addr, mon_addr,
            bind_cfg['data_port'], bind_cfg['mon_port'],
        )
        nfsutil.assert_nfs_bind_spec(
            cluster_id, nfs_hostname, bind_addr, mon_addr,
            bind_cfg['data_port'], bind_cfg['mon_port'],
        )
        nfsutil.verify_bind_monitoring_runtime(
            cluster_id, bind_addr, mon_addr, bind_cfg,
        )

    def test_networks(self, bind_cfg):
        """Test networks/monitoring_networks, ports, mount, and metrics endpoint."""
        cluster_id = bind_cfg['cluster_id']
        nfs_hostname = nfsutil.get_nfs_hostname(cluster_id)
        bind_addr = nfsutil.get_host_addr(nfs_hostname)
        mon_addr = bind_addr
        network_cidr = nfsutil.cidr_for_ip(bind_addr)

        nfsutil.apply_nfs_networks_spec(
            cluster_id, nfs_hostname, network_cidr,
            bind_cfg['data_port'], bind_cfg['mon_port'],
        )
        nfsutil.assert_nfs_networks_spec(
            cluster_id, nfs_hostname, network_cidr,
            bind_cfg['data_port'], bind_cfg['mon_port'],
        )
        nfsutil.verify_bind_monitoring_runtime(
            cluster_id, bind_addr, mon_addr, bind_cfg,
        )
