# flake8: noqa
import json
import yaml

import pytest

from ceph.deployment.hostspec import HostSpec, SpecValidationError


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ({"hostname": "foo"}, HostSpec('foo')),
        ({"hostname": "foo", "labels": "l1"}, HostSpec('foo', labels=['l1'])),
        ({"hostname": "foo", "labels": ["l1", "l2"]}, HostSpec('foo', labels=['l1', 'l2'])),
        ({"hostname": "foo", "location": {"rack": "foo"}}, HostSpec('foo', location={'rack': 'foo'})),
    ]
)
def test_parse_host_specs(test_input, expected):
    hs = HostSpec.from_json(test_input)
    assert hs == expected


@pytest.mark.parametrize(
    "bad_input",
    [
        ({"hostname": "foo", "labels": 124}),
        ({"hostname": "foo", "labels": {"a", "b"}}),
        ({"hostname": "foo", "labels": {"a", "b"}}),
        ({"hostname": "foo", "labels": ["a", 2]}),
        ({"hostname": "foo", "location": "rack=bar"}),
        ({"hostname": "foo", "location": ["a"]}),
        ({"hostname": "foo", "location": {"rack", 1}}),
        ({"hostname": "foo", "location": {1: "rack"}}),
    ]
)
def test_parse_host_specs(bad_input):
    with pytest.raises(SpecValidationError):
        hs = HostSpec.from_json(bad_input)


def test_hostname_case_insensitive():
    # Test hostname is lowercased
    hs = HostSpec(hostname="Ceph-Node-00")
    assert hs.hostname == "ceph-node-00"
    
    hs2 = HostSpec.from_json({"hostname": "MyHost"})
    assert hs2.hostname == "myhost"


def test_parse_topological_labels():
    hs_dict = HostSpec(hostname='foo', topological_labels={'foo': 'bar', 'foobar': 'bazfoo'})
    hs_str = HostSpec(hostname='foo', topological_labels='foo=bar,foobar=bazfoo')
    hs_list_str = HostSpec(hostname='foo', topological_labels=['foo=bar', 'foobar=bazfoo'])
    hs_list_str2 = HostSpec(hostname='foo', topological_labels=['foo=bar,foobar=bazfoo'])

    assert hs_dict == hs_str
    assert hs_dict == hs_list_str
    assert hs_str == hs_list_str
    assert hs_dict == hs_list_str2
    assert hs_dict.topological_labels == {'foo': 'bar', 'foobar': 'bazfoo'}
    assert hs_str.topological_labels == {'foo': 'bar', 'foobar': 'bazfoo'}
    assert hs_list_str.topological_labels == {'foo': 'bar', 'foobar': 'bazfoo'}
    assert hs_list_str2.topological_labels == {'foo': 'bar', 'foobar': 'bazfoo'}
    assert hs_dict.to_json()['topological_labels'] == hs_str.to_json()['topological_labels']
    assert hs_dict.to_json()['topological_labels'] == hs_list_str.to_json()['topological_labels']
    assert hs_str.to_json()['topological_labels'] == hs_list_str.to_json()['topological_labels']
    assert hs_dict.topological_labels == hs_dict.to_json()['topological_labels']
    assert hs_str.topological_labels == hs_str.to_json()['topological_labels']
    assert hs_list_str.topological_labels == hs_list_str.to_json()['topological_labels']

    with pytest.raises(SpecValidationError):
        HostSpec.parse_topological_labels('NoEqualSign')
    with pytest.raises(SpecValidationError):
        HostSpec.parse_topological_labels('Two=Equal=Sign')
    with pytest.raises(SpecValidationError):
        HostSpec.parse_topological_labels(['normal=label', 'NoEqualSign'])
    with pytest.raises(SpecValidationError):
        HostSpec.parse_topological_labels(['normal=label', 'Two=Equal=Sign'])
    with pytest.raises(SpecValidationError):
        HostSpec.parse_topological_labels(['normal=label', 6])
    with pytest.raises(SpecValidationError):
        HostSpec.parse_topological_labels(['normal=label', 'normal=AnotherLabel'])
    with pytest.raises(SpecValidationError):
        HostSpec.parse_topological_labels(6)
    with pytest.raises(SpecValidationError):
        HostSpec.parse_topological_labels({6: 'foo'})
    with pytest.raises(SpecValidationError):
        HostSpec.parse_topological_labels({'foo': 6})

def test_match_topological_labels():
    hs = HostSpec(hostname='foo', topological_labels={'foo': 'bar', 'foobar': 'bazfoo'})
    assert hs.matches_topological_labels({'foo': 'bar'})
    assert hs.matches_topological_labels({'foobar': 'bazfoo'})
    assert hs.matches_topological_labels({'foo': 'bar', 'foobar': 'bazfoo'})
    # We already test parse_topological_labels in another test.
    # Just adding one case to make sure we're calling it
    assert hs.matches_topological_labels('foo=bar')
    assert not hs.matches_topological_labels({'foo': 'bar', 'other': 'thing'})
    assert not hs.matches_topological_labels({'random': 'label'})
    assert not hs.matches_topological_labels({})
    assert not HostSpec(hostname='foo', topological_labels=None).matches_topological_labels({})
