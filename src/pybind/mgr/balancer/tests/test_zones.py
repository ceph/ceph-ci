import errno

from tests import mock

from balancer.module import Mode, Module, crush_compat_refused

ZONE_TYPE = 8


class FakeCrush:
    def __init__(self, buckets):
        self.buckets = {b['id']: b for b in buckets}

    def find_takes(self):
        return [-1]

    def get_item_name(self, item):
        return self.buckets[item]['name']

    def get_take_weight_osd_map(self, root):
        osds = []
        stack = [root]
        while stack:
            for i in self.buckets[stack.pop()]['items']:
                if i['id'] < 0:
                    stack.append(i['id'])
                else:
                    osds.append(i['id'])
        return {osd: 1.0 / len(osds) for osd in osds}


class FakeOSDMap:
    def get_pools_by_take(self, rootid):
        return [1]


class FakeMappingState:
    # zone dc-a has OSDs 0-2 and dc-b OSDs 3-4; every PG has one copy in each
    def __init__(self):
        buckets = [
            {'id': -1, 'name': 'default', 'type_id': 11,
             'items': [{'id': -2}, {'id': -3}]},
            {'id': -2, 'name': 'dc-a', 'type_id': ZONE_TYPE,
             'items': [{'id': 0}, {'id': 1}, {'id': 2}]},
            {'id': -3, 'name': 'dc-b', 'type_id': ZONE_TYPE,
             'items': [{'id': 3}, {'id': 4}]},
        ]
        self.crush = FakeCrush(buckets)
        self.crush_dump = {'buckets': buckets}
        self.osdmap = FakeOSDMap()
        self.osdmap_dump = {
            'pools': [{'pool': 1, 'pool_name': 'stretch', 'crush_rule': 0,
                       'type': 1, 'num_zones': 2,
                       'peering_crush_bucket_barrier': ZONE_TYPE}],
            'osds': [{'osd': osd, 'weight': 1.0} for osd in range(5)],
        }
        self.poolids = {1}
        up = {'1.%x' % ps: [ps % 3, 3 + ps % 2] for ps in range(6)}
        self.pg_up_by_poolid = {1: up}
        self.pg_stat = {pg: {'num_objects': 10, 'num_bytes': 1000} for pg in up}


def calc_eval(by_zone):
    m = mock.Mock()
    m.get_module_option.return_value = 'pgs,objects,bytes'
    with mock.patch('balancer.module.CRUSHMap') as crushmap:
        crushmap.ITEM_NONE = 0x7fffffff
        return Module.calc_eval(m, FakeMappingState(), [], by_zone=by_zone)


def test_eval_by_zone():
    pe = calc_eval(True)
    assert sorted(pe.target_by_root) == ['dc-a', 'dc-b']
    assert pe.pool_roots['stretch'] == ['dc-a', 'dc-b']
    assert pe.score == 0.0


def test_eval_by_root():
    pe = calc_eval(False)
    assert list(pe.target_by_root) == ['default']
    assert pe.score > 0.0


def test_crush_compat_refused():
    pools = [
        {'pool_name': 'rep', 'type': 1, 'num_zones': 2},
        {'pool_name': 'ec1', 'type': 3, 'num_zones': 1},
    ]
    assert crush_compat_refused({'pools': pools}) is None
    pools.append({'pool_name': 'ec2', 'type': 3, 'num_zones': 2})
    refused = crush_compat_refused({'pools': pools})
    assert refused is not None
    assert 'ec2' in refused
    assert 'rep' not in refused


MULTI_ZONE_EC = {'require_min_compat_client': 'luminous',
                 'pools': [{'pool_name': 'ec', 'type': 3, 'num_zones': 2}]}


def test_set_mode_crush_compat_refused():
    m = mock.Mock()
    m.get_osdmap.return_value.dump.return_value = MULTI_ZONE_EC
    r, _, err = Module.set_mode(m, Mode.crush_compat)
    assert r == -errno.EOPNOTSUPP
    assert 'ec' in err
    m.set_module_option.assert_not_called()


def test_do_crush_compat_refused():
    m = mock.Mock()
    m.get_module_option.side_effect = lambda key: {
        'crush_compat_max_iterations': 25,
        'crush_compat_step': 0.5,
    }[key]
    m.get_ceph_option.return_value = 0.05
    plan = mock.Mock()
    plan.initial.osdmap_dump = MULTI_ZONE_EC
    r, detail = Module.do_crush_compat(m, plan)
    assert r == -errno.EOPNOTSUPP
    assert 'ec' in detail
    m.calc_eval.assert_not_called()
