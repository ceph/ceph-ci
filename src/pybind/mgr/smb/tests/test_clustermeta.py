import smb.clustermeta


def _entry(pnn, identity, node, state):
    return {'pnn': pnn, 'identity': identity, 'node': node, 'state': state}


def _daemon(name, host_ip):
    return {
        'daemon_type': 'smb',
        'daemon_id': name,
        'hostname': f'host-{host_ip}',
        'host_ip': host_ip,
    }


def test_sync_ranks():
    cmeta = smb.clustermeta.ClusterMeta()
    rank_map = {0: {0: 'cls1.host1.d1'}}
    daemon_map = {'smb.cls1.host1.d1': _daemon('d1', '10.0.0.21')}

    # new rank is added as "new"
    cmeta.sync_ranks(rank_map, daemon_map)
    assert cmeta.to_simplified()['nodes'] == [
        _entry(0, 'smb.cls1.host1.d1', '10.0.0.21', 'new')
    ]

    # nothing moved, so don't touch sambacc's own state value
    cmeta.to_simplified()['nodes'][0]['state'] = 'replaced'
    cmeta.sync_ranks(rank_map, daemon_map)
    assert cmeta.to_simplified()['nodes'][0]['state'] == 'replaced'

    # marked "changed" when a daemon moves to a new node
    rank_map = {0: {1: 'cls1.host3.d3'}}
    daemon_map = {'smb.cls1.host3.d3': _daemon('d3', '10.0.0.23')}
    cmeta.sync_ranks(rank_map, daemon_map)
    assert cmeta.to_simplified()['nodes'] == [
        _entry(0, 'smb.cls1.host3.d3', '10.0.0.23', 'changed')
    ]


def test_sync_ranks_scale_down():
    cmeta = smb.clustermeta.ClusterMeta()
    cmeta.load(
        {
            'nodes': [
                _entry(0, 'smb.cls1.host1.d1', '10.0.0.21', 'ready'),
                _entry(1, 'smb.cls1.host2.d2', '10.0.0.22', 'ready'),
            ]
        }
    )
    # scale down rank 1
    rank_map = {0: {0: 'cls1.host1.d1'}}
    daemon_map = {'smb.cls1.host1.d1': _daemon('d1', '10.0.0.21')}

    cmeta.sync_ranks(rank_map, daemon_map)

    nodes = {n['pnn']: n['state'] for n in cmeta.to_simplified()['nodes']}
    assert nodes == {0: 'ready', 1: 'gone'}
