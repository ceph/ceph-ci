# -*- coding: utf-8 -*-
# pylint: disable=protected-access,unused-argument,redefined-outer-name

from typing import Any, Callable, List, Optional
from unittest.mock import MagicMock, patch

import grpc
import pytest

from ..controllers.nvmeof import NVMeoFConnection, NVMeoFGateway, NVMeoFHost, \
    NVMeoFListener, NVMeoFNamespace, NVMeoFSubsystem
from ..exceptions import DashboardException
from ..services.nvmeof_client import NVMeoFClient
from ..services.nvmeof_failover import FailoverNVMeoFClient, FailoverPolicy, \
    check_channel_connectivity, clear_down_gateways, \
    discover_fallback_gateways, failover, get_current_policy, is_gateway_down, \
    mark_gateway_down, order_candidates, resolve_failover_policy


class FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str = 'rpc failed'):
        super().__init__()
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


class FakeChannel:
    def __init__(self, target_state: Optional[grpc.ChannelConnectivity] = None):
        self.target_state = target_state
        self.subscribed_cbs: List[Callable[[Any], None]] = []
        self.unsubscribed_cbs: List[Callable[[Any], None]] = []

    def subscribe(self, callback: Callable[[Any], None], try_to_connect: bool = False):
        self.subscribed_cbs.append(callback)
        if self.target_state is not None:
            callback(self.target_state)

    def unsubscribe(self, callback: Callable[[Any], None]):
        self.unsubscribed_cbs.append(callback)


@pytest.fixture(autouse=True)
def clean_down_records():
    clear_down_gateways()
    yield
    clear_down_gateways()


class TestPolicyAndDecorator:
    def test_default_policy_is_never(self):
        assert get_current_policy() == FailoverPolicy.NEVER

    def test_decorator_sets_policy_in_context(self):
        @failover(FailoverPolicy.ALWAYS)
        def func():
            return get_current_policy()

        assert get_current_policy() == FailoverPolicy.NEVER
        assert func() == FailoverPolicy.ALWAYS
        assert get_current_policy() == FailoverPolicy.NEVER

    def test_decorator_resets_policy_on_exception(self):
        @failover(FailoverPolicy.IF_NOT_SENT)
        def faulty_func():
            assert get_current_policy() == FailoverPolicy.IF_NOT_SENT
            raise ValueError('oops')

        with pytest.raises(ValueError):
            faulty_func()
        assert get_current_policy() == FailoverPolicy.NEVER

    def test_decorator_preserves_metadata(self):
        @failover(FailoverPolicy.ALWAYS)
        def my_sample_func(a, b=1):
            """Sample docstring."""
            return a + b

        assert my_sample_func.__name__ == 'my_sample_func'
        assert my_sample_func.__doc__ == 'Sample docstring.'
        assert my_sample_func(2, 3) == 5

    def test_resolve_policy_precedence(self):
        # 1. server_address forces NEVER regardless of context or explicit override
        assert resolve_failover_policy(
            server_address='10.0.0.1:5500',
            failover_override=FailoverPolicy.ALWAYS
        ) == FailoverPolicy.NEVER

        # 2. explicit override overrides context
        @failover(FailoverPolicy.ALWAYS)
        def check_override():
            return resolve_failover_policy(failover_override=FailoverPolicy.NEVER)

        assert check_override() == FailoverPolicy.NEVER

        # 3. context policy used when no override or server_address
        @failover(FailoverPolicy.IF_NOT_SENT)
        def check_context():
            return resolve_failover_policy()

        assert check_context() == FailoverPolicy.IF_NOT_SENT

        # 4. default is NEVER
        assert resolve_failover_policy() == FailoverPolicy.NEVER


class TestDownRecord:
    def test_mark_and_check_down_gateway(self):
        url = '10.0.0.1:5500'
        assert not is_gateway_down(url)
        mark_gateway_down(url, ttl=100.0)
        assert is_gateway_down(url)

    def test_down_gateway_expires(self):
        url = '10.0.0.1:5500'
        mark_gateway_down(url, ttl=-1.0)  # already expired
        assert not is_gateway_down(url)

    def test_order_candidates_sorts_up_before_down(self):
        c1 = {'service_url': '10.0.0.1:5500'}
        c2 = {'service_url': '10.0.0.2:5500'}
        c3 = {'service_url': '10.0.0.3:5500'}
        candidates = [c1, c2, c3]

        mark_gateway_down('10.0.0.1:5500')
        ordered = order_candidates(candidates)
        assert ordered == [c2, c3, c1]

    def test_order_candidates_all_down_preserves_all(self):
        c1 = {'service_url': '10.0.0.1:5500'}
        c2 = {'service_url': '10.0.0.2:5500'}
        candidates = [c1, c2]

        mark_gateway_down('10.0.0.1:5500')
        mark_gateway_down('10.0.0.2:5500')
        ordered = order_candidates(candidates)
        assert ordered == [c1, c2]


class TestChannelConnectivityCheck:
    def test_channel_ready(self):
        ch = FakeChannel(target_state=grpc.ChannelConnectivity.READY)
        assert check_channel_connectivity(ch, timeout=0.1)
        assert len(ch.unsubscribed_cbs) == 1

    def test_channel_transient_failure(self):
        ch = FakeChannel(target_state=grpc.ChannelConnectivity.TRANSIENT_FAILURE)
        assert not check_channel_connectivity(ch, timeout=0.1)
        assert len(ch.unsubscribed_cbs) == 1

    def test_channel_timeout(self):
        ch = FakeChannel(target_state=None)  # Never fires state callback
        assert not check_channel_connectivity(ch, timeout=0.01)
        assert len(ch.unsubscribed_cbs) == 1

    def test_none_channel(self):
        assert not check_channel_connectivity(None)


class TestFallbackDiscovery:
    _CONFIG = {
        'gateways': {
            'nvmeof.mygroup': [
                {'service_url': '10.0.0.1:5500', 'group': 'mygroup', 'daemon_name': 'd1'},
                {'service_url': '10.0.0.2:5500', 'group': 'mygroup', 'daemon_name': 'd2'},
                {'service_url': '10.0.0.3:5500', 'group': 'mygroup', 'daemon_name': 'd3'},
                {'service_url': '10.0.0.4:5500', 'group': 'mygroup'},  # No daemon_name
            ]
        }
    }

    def test_discover_fallbacks_running_only(self):
        mock_orch = MagicMock()
        mock_orch.available.return_value = True
        mock_orch.services.get.return_value = [MagicMock(spec=MagicMock(group='mygroup'))]

        with patch('dashboard.services.nvmeof_failover.NvmeofGatewaysConfig.get_gateways_config',
                   return_value=self._CONFIG), \
             patch('dashboard.services.nvmeof_failover.OrchClient.instance',
                   return_value=mock_orch), \
             patch('dashboard.services.nvmeof_failover._get_running_daemons',
                   return_value=['d1', 'd2']):  # d3 is stopped, d4 has no daemon_name
            fallbacks = discover_fallback_gateways(
                gw_group='mygroup',
                primary_daemon_name='d1',
                primary_service_url='10.0.0.1:5500'
            )
            # Only d2 is running and not the primary
            assert len(fallbacks) == 1
            assert fallbacks[0]['daemon_name'] == 'd2'
            assert fallbacks[0]['service_url'] == '10.0.0.2:5500'


class TestFailoverNVMeoFClientRouting:
    _GW1_ADDR = '10.0.0.1:5500'
    _GW2_ADDR = '10.0.0.2:5500'
    _GW1_DAEMON = 'd1'
    _GW2_DAEMON = 'd2'

    def _create_mock_client(self, addr: str, daemon: str, healthy: bool = True):
        mock_client = MagicMock(spec=NVMeoFClient)
        mock_client.gateway_addr = addr
        mock_client.daemon_name = daemon
        mock_client.channel = FakeChannel(
            target_state=grpc.ChannelConnectivity.READY if healthy
            else grpc.ChannelConnectivity.TRANSIENT_FAILURE
        )
        mock_client.stub = MagicMock()
        return mock_client

    def test_never_policy_calls_primary_directly(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON)
        c1.stub.list_subsystems.return_value = 'result_gw1'

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', return_value=c1):
            client = FailoverNVMeoFClient(gw_group='mygroup')
            assert client.policy == FailoverPolicy.NEVER
            res = client.stub.list_subsystems()
            assert res == 'result_gw1'
            c1.stub.list_subsystems.assert_called_once()

    def test_single_gateway_group_bypasses_check_and_fallbacks(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON)
        c1.stub.list_subsystems.return_value = 'result_gw1'

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', return_value=c1), \
             patch('dashboard.services.nvmeof_failover.discover_fallback_gateways',
                   return_value=[]), \
             patch('dashboard.services.nvmeof_failover.check_channel_connectivity') as mock_check:
            client = FailoverNVMeoFClient(gw_group='mygroup', failover=FailoverPolicy.ALWAYS)
            res = client.stub.list_subsystems()
            assert res == 'result_gw1'
            mock_check.assert_not_called()

    def test_pinned_server_address_no_failover(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON)
        c1.stub.list_subsystems.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', return_value=c1), \
             patch('dashboard.services.nvmeof_failover.discover_fallback_gateways') as mock_disc:
            client = FailoverNVMeoFClient(
                gw_group='mygroup',
                server_address='10.0.0.1:5500',
                failover=FailoverPolicy.ALWAYS
            )
            assert client.policy == FailoverPolicy.NEVER
            with pytest.raises(FakeRpcError):
                client.stub.list_subsystems()
            mock_disc.assert_not_called()

    def test_always_policy_connect_failure_fails_over(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON, healthy=False)
        c2 = self._create_mock_client(self._GW2_ADDR, self._GW2_DAEMON, healthy=True)
        c2.stub.list_subsystems.return_value = 'result_gw2'

        def client_factory(gw_group=None, server_address=None):
            if server_address == self._GW2_ADDR:
                return c2
            return c1

        fallbacks = [{'service_url': self._GW2_ADDR, 'daemon_name': self._GW2_DAEMON}]

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', side_effect=client_factory), \
             patch('dashboard.services.nvmeof_failover.discover_fallback_gateways',
                   return_value=fallbacks):
            client = FailoverNVMeoFClient(gw_group='mygroup', failover=FailoverPolicy.ALWAYS)
            res = client.stub.list_subsystems()
            assert res == 'result_gw2'
            assert is_gateway_down(self._GW1_ADDR)
            assert client.gateway_addr == self._GW2_ADDR

    def test_always_policy_unavailable_after_connect_fails_over(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON, healthy=True)
        c1.stub.list_subsystems.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
        c2 = self._create_mock_client(self._GW2_ADDR, self._GW2_DAEMON, healthy=True)
        c2.stub.list_subsystems.return_value = 'result_gw2'

        def client_factory(gw_group=None, server_address=None):
            if server_address == self._GW2_ADDR:
                return c2
            return c1

        fallbacks = [{'service_url': self._GW2_ADDR, 'daemon_name': self._GW2_DAEMON}]

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', side_effect=client_factory), \
             patch('dashboard.services.nvmeof_failover.discover_fallback_gateways',
                   return_value=fallbacks):
            client = FailoverNVMeoFClient(gw_group='mygroup', failover=FailoverPolicy.ALWAYS)
            res = client.stub.list_subsystems()
            assert res == 'result_gw2'
            assert is_gateway_down(self._GW1_ADDR)

    def test_always_policy_non_unavailable_error_raises_immediately(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON, healthy=True)
        c1.stub.list_subsystems.side_effect = FakeRpcError(grpc.StatusCode.INVALID_ARGUMENT)
        c2 = self._create_mock_client(self._GW2_ADDR, self._GW2_DAEMON, healthy=True)

        fallbacks = [{'service_url': self._GW2_ADDR, 'daemon_name': self._GW2_DAEMON}]

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', return_value=c1), \
             patch('dashboard.services.nvmeof_failover.discover_fallback_gateways',
                   return_value=fallbacks):
            client = FailoverNVMeoFClient(gw_group='mygroup', failover=FailoverPolicy.ALWAYS)
            with pytest.raises(FakeRpcError) as exc_info:
                client.stub.list_subsystems()
            assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT
            c2.stub.list_subsystems.assert_not_called()

    def test_if_not_sent_policy_connect_failure_fails_over(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON, healthy=False)
        c2 = self._create_mock_client(self._GW2_ADDR, self._GW2_DAEMON, healthy=True)
        c2.stub.create_subsystem.return_value = 'created_gw2'

        def client_factory(gw_group=None, server_address=None):
            if server_address == self._GW2_ADDR:
                return c2
            return c1

        fallbacks = [{'service_url': self._GW2_ADDR, 'daemon_name': self._GW2_DAEMON}]

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', side_effect=client_factory), \
             patch('dashboard.services.nvmeof_failover.discover_fallback_gateways',
                   return_value=fallbacks):
            client = FailoverNVMeoFClient(gw_group='mygroup', failover=FailoverPolicy.IF_NOT_SENT)
            res = client.stub.create_subsystem()
            assert res == 'created_gw2'
            assert is_gateway_down(self._GW1_ADDR)

    def test_if_not_sent_policy_unavailable_after_connect_raises_without_retry(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON, healthy=True)
        c1.stub.create_subsystem.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE)
        c2 = self._create_mock_client(self._GW2_ADDR, self._GW2_DAEMON, healthy=True)

        fallbacks = [{'service_url': self._GW2_ADDR, 'daemon_name': self._GW2_DAEMON}]

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', return_value=c1), \
             patch('dashboard.services.nvmeof_failover.discover_fallback_gateways',
                   return_value=fallbacks):
            client = FailoverNVMeoFClient(gw_group='mygroup', failover=FailoverPolicy.IF_NOT_SENT)
            with pytest.raises(FakeRpcError) as exc_info:
                client.stub.create_subsystem()
            assert exc_info.value.code() == grpc.StatusCode.UNAVAILABLE
            c2.stub.create_subsystem.assert_not_called()

    def test_all_candidates_failed_connect_checks_raises_dashboard_exception(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON, healthy=False)
        c2 = self._create_mock_client(self._GW2_ADDR, self._GW2_DAEMON, healthy=False)

        def client_factory(gw_group=None, server_address=None):
            if server_address == self._GW2_ADDR:
                return c2
            return c1

        fallbacks = [{'service_url': self._GW2_ADDR, 'daemon_name': self._GW2_DAEMON}]

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', side_effect=client_factory), \
             patch('dashboard.services.nvmeof_failover.discover_fallback_gateways',
                   return_value=fallbacks):
            client = FailoverNVMeoFClient(gw_group='mygroup', failover=FailoverPolicy.ALWAYS)
            with pytest.raises(DashboardException) as exc_info:
                client.stub.list_subsystems()
            assert exc_info.value.status == 504
            assert exc_info.value.component == 'nvmeof'

    def test_all_candidates_fail_rpc_error_reraised(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON, healthy=True)
        c1.stub.list_subsystems.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE, 'gw1 dead')
        c2 = self._create_mock_client(self._GW2_ADDR, self._GW2_DAEMON, healthy=True)
        c2.stub.list_subsystems.side_effect = FakeRpcError(grpc.StatusCode.UNAVAILABLE, 'gw2 dead')

        def client_factory(gw_group=None, server_address=None):
            if server_address == self._GW2_ADDR:
                return c2
            return c1

        fallbacks = [{'service_url': self._GW2_ADDR, 'daemon_name': self._GW2_DAEMON}]

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', side_effect=client_factory), \
             patch('dashboard.services.nvmeof_failover.discover_fallback_gateways',
                   return_value=fallbacks):
            client = FailoverNVMeoFClient(gw_group='mygroup', failover=FailoverPolicy.ALWAYS)
            with pytest.raises(FakeRpcError) as exc_info:
                client.stub.list_subsystems()
            assert exc_info.value.details() == 'gw2 dead'

    def test_sticky_failover_subsequent_calls(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON, healthy=False)
        c2 = self._create_mock_client(self._GW2_ADDR, self._GW2_DAEMON, healthy=True)
        c2.stub.list_subsystems.return_value = 'result_gw2'
        c2.stub.get_subsystem.return_value = 'subsys_gw2'

        def client_factory(gw_group=None, server_address=None):
            if server_address == self._GW2_ADDR:
                return c2
            return c1

        fallbacks = [{'service_url': self._GW2_ADDR, 'daemon_name': self._GW2_DAEMON}]

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', side_effect=client_factory), \
             patch('dashboard.services.nvmeof_failover.discover_fallback_gateways',
                   return_value=fallbacks):
            client = FailoverNVMeoFClient(gw_group='mygroup', failover=FailoverPolicy.ALWAYS)
            res1 = client.stub.list_subsystems()
            assert res1 == 'result_gw2'
            # Second call on same instance should call c2 directly
            res2 = client.stub.get_subsystem()
            assert res2 == 'subsys_gw2'
            c2.stub.get_subsystem.assert_called_once()

    def test_delegated_properties_and_attributes(self):
        c1 = self._create_mock_client(self._GW1_ADDR, self._GW1_DAEMON)
        c1.service_name = 'svc1'

        with patch('dashboard.services.nvmeof_failover.NVMeoFClient', return_value=c1):
            client = FailoverNVMeoFClient(gw_group='mygroup')
            assert client.gateway_addr == self._GW1_ADDR
            assert client.daemon_name == self._GW1_DAEMON
            assert client.service_name == 'svc1'
            assert client.channel is not None
            assert client.pb2 == NVMeoFClient.pb2

            # Unknown dunder/regular attr on stub
            with pytest.raises(AttributeError):
                _ = client.stub.__nonexistent__


class TestControllersDecorationGuard:
    def test_controller_classes_instantiation_and_methods(self):
        # Verify controller classes instantiate and expose methods
        gateway_controller = NVMeoFGateway()
        assert getattr(gateway_controller, 'list', None) is not None
        assert getattr(gateway_controller, 'get_gw_stats', None) is not None

        subsystem_controller = NVMeoFSubsystem()
        assert getattr(subsystem_controller, 'list', None) is not None
        assert getattr(subsystem_controller, 'create', None) is not None

        listener_controller = NVMeoFListener()
        assert getattr(listener_controller, 'create', None) is not None
        assert getattr(listener_controller, 'delete', None) is not None

        namespace_controller = NVMeoFNamespace()
        assert getattr(namespace_controller, 'list', None) is not None
        assert getattr(namespace_controller, 'create', None) is not None

        host_controller = NVMeoFHost()
        assert getattr(host_controller, 'list', None) is not None
        assert getattr(host_controller, 'create', None) is not None

        conn_controller = NVMeoFConnection()
        assert getattr(conn_controller, 'list', None) is not None
        assert getattr(conn_controller, 'reset_io_stats', None) is not None
