import pytest

import nfsutil


@pytest.fixture(scope='module')
def nfs_cfg():
    """Shared NFS configuration fixture."""
    return nfsutil.nfs_config()


@pytest.fixture(scope='module')
def nfs_cfg_with_qos_cleanup():
    """NFS configuration fixture with QoS cleanup before and after tests."""
    config = nfsutil.nfs_config()
    nfsutil.cleanup_qos(config)
    yield config
    nfsutil.cleanup_qos(config)
