# -*- coding: utf-8 -*-
# pylint: disable=unexpected-keyword-arg,protected-access

import contextvars
import functools
import logging
import threading
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

from orchestrator import OrchestratorError

from ..exceptions import DashboardException
from .nvmeof_conf import NvmeofGatewaysConfig, _get_running_daemons
from .orchestrator import OrchClient

logger = logging.getLogger("nvmeof_failover")

try:
    import os
    os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"

    import grpc  # noqa: F401 # pylint: disable=unused-import
    import grpc._channel  # noqa: F401 # pylint: disable=unused-import

    from .nvmeof_client import NVMeoFClient
    from .proto import gateway_pb2 as pb2  # type: ignore
except ImportError:
    grpc = None
    NVMeoFClient = None  # type: ignore
    pb2 = None  # type: ignore


class FailoverPolicy(Enum):
    NEVER = 'NEVER'
    ALWAYS = 'ALWAYS'
    IF_NOT_SENT = 'IF_NOT_SENT'


_CURRENT_POLICY: contextvars.ContextVar[FailoverPolicy] = contextvars.ContextVar(
    '_CURRENT_POLICY', default=FailoverPolicy.NEVER
)


def get_current_policy() -> FailoverPolicy:
    return _CURRENT_POLICY.get()


def failover(policy: FailoverPolicy):
    """
    Decorator to set the failover policy for NVMe-oF client instances
    created within the execution scope of the decorated function.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            token = _CURRENT_POLICY.set(policy)
            try:
                return fn(*args, **kwargs)
            finally:
                _CURRENT_POLICY.reset(token)
        return wrapper
    return decorator


def resolve_failover_policy(
    server_address: Optional[str] = None,
    failover_override: Optional[FailoverPolicy] = None
) -> FailoverPolicy:
    """
    Resolve the effective failover policy based on precedence:
    1. server_address given -> NEVER (pinned to a specific gateway)
    2. failover_override explicitly provided on client -> failover_override
    3. decorator policy from context
    4. NEVER (default)
    """
    if server_address is not None:
        return FailoverPolicy.NEVER
    if failover_override is not None:
        return failover_override
    return get_current_policy()


# Process-wide down record with TTL
DEFAULT_DOWN_TTL_SECONDS: float = 10.0

_DOWN_RECORD_LOCK = threading.Lock()
_DOWN_GATEWAYS: Dict[str, float] = {}  # service_url -> expiry monotonic time


def mark_gateway_down(service_url: str, ttl: float = DEFAULT_DOWN_TTL_SECONDS) -> None:
    """Mark a gateway address as down for the specified TTL."""
    if not service_url:
        return
    with _DOWN_RECORD_LOCK:
        _DOWN_GATEWAYS[service_url] = time.monotonic() + ttl
        logger.debug("Marked gateway %s down for %s seconds", service_url, ttl)


def is_gateway_down(service_url: str) -> bool:
    """Check if a gateway address is currently marked down."""
    if not service_url:
        return False
    with _DOWN_RECORD_LOCK:
        expiry = _DOWN_GATEWAYS.get(service_url)
        if expiry is None:
            return False
        if time.monotonic() > expiry:
            del _DOWN_GATEWAYS[service_url]
            return False
        return True


def clear_down_gateways() -> None:
    """Clear all down gateway records (primarily for testing)."""
    with _DOWN_RECORD_LOCK:
        _DOWN_GATEWAYS.clear()


def order_candidates(
    candidates: List[Any],
    key: Optional[Callable[[Any], str]] = None
) -> List[Any]:
    """
    Order candidate gateways so that addresses not marked down come first.
    If all candidates are marked down, return them in their original order.
    """
    if not candidates:
        return []

    if key is None:
        def _get_url(c: Any) -> str:
            if isinstance(c, str):
                return c
            if isinstance(c, dict):
                return c.get('service_url', '')
            return getattr(c, 'gateway_addr', '')
        key = _get_url

    up_candidates: List[Any] = []
    down_candidates: List[Any] = []
    for c in candidates:
        url = key(c)
        if is_gateway_down(url):
            down_candidates.append(c)
        else:
            up_candidates.append(c)

    if not up_candidates:
        # If all candidates are marked down, preserve all in original order
        return list(candidates)
    return up_candidates + down_candidates


def _is_valid_fallback(
    gw: Dict[str, str],
    running_daemons: Set[str],
    primary_daemon_name: Optional[str],
    primary_service_url: Optional[str],
    gw_group: Optional[str]
) -> bool:
    d_name = gw.get('daemon_name')
    s_url = gw.get('service_url')
    if not d_name or not s_url:
        return False
    if d_name not in running_daemons:
        return False
    if primary_daemon_name and d_name == primary_daemon_name:
        return False
    if primary_service_url and s_url == primary_service_url:
        return False
    if gw_group and gw.get('group') and gw.get('group') != gw_group:
        return False
    return True


def discover_fallback_gateways(
    gw_group: Optional[str] = None,
    primary_daemon_name: Optional[str] = None,
    primary_service_url: Optional[str] = None
) -> List[Dict[str, str]]:
    """
    Discover running fallback gateways in the same group as the primary gateway,
    excluding the primary gateway and skipping any entries without a daemon_name.
    """
    try:
        config = NvmeofGatewaysConfig.get_gateways_config()
        gateways = config.get('gateways', {})
        if not gateways:
            return []

        orch = OrchClient.instance()
        if not orch.available():
            return []

        for service_name, svc_config in gateways.items():
            try:
                svcs = orch.services.get(service_name)
                group_name_from_svc = svcs[0].spec.group if svcs else None
            except (OrchestratorError, IndexError, AttributeError):
                group_name_from_svc = None

            if gw_group:
                if group_name_from_svc != gw_group:
                    if not any(gw.get('group') == gw_group for gw in svc_config):
                        continue
            else:
                if len(gateways) > 1:
                    continue

            try:
                running_daemons = set(_get_running_daemons(orch, service_name))
            except OrchestratorError:
                continue

            return [
                gw for gw in svc_config
                if _is_valid_fallback(
                    gw, running_daemons, primary_daemon_name, primary_service_url, gw_group
                )
            ]

        return []
    except (OrchestratorError, Exception) as e:  # pylint: disable=broad-except
        logger.debug("Error discovering fallback gateways: %s", e)
        return []


# Connectivity check
CONNECT_CHECK_TIMEOUT_SECONDS: float = 2.0


def check_channel_connectivity(
    channel: Any,
    timeout: float = CONNECT_CHECK_TIMEOUT_SECONDS
) -> bool:
    """
    Perform a bounded connectivity check on a gRPC channel using subscribe(try_to_connect=True).
    Returns True if channel reached READY, False if TRANSIENT_FAILURE or timeout.
    """
    if channel is None:
        return False
    if not hasattr(channel, 'subscribe') or not hasattr(channel, 'unsubscribe'):
        return True

    ready_event = threading.Event()
    result = {'ready': False}

    def _state_cb(state: Any) -> None:
        if grpc and state == grpc.ChannelConnectivity.READY:
            result['ready'] = True
            ready_event.set()
        elif grpc and state == grpc.ChannelConnectivity.TRANSIENT_FAILURE:
            result['ready'] = False
            ready_event.set()

    try:
        channel.subscribe(_state_cb, try_to_connect=True)
        ready_event.wait(timeout)
    except Exception as e:  # pylint: disable=broad-except
        logger.debug("Exception during channel connectivity check: %s", e)
        return False
    finally:
        try:
            channel.unsubscribe(_state_cb)
        except Exception:  # pylint: disable=broad-except
            pass

    return result['ready']


class _FailoverStub(object):
    """
    Proxy for the gRPC GatewayStub that handles failover routing per method call.
    """
    def __init__(self, client: 'FailoverNVMeoFClient'):
        self._client = client

    def __getattr__(self, method_name: str) -> Callable[..., Any]:
        if method_name.startswith('_'):
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{method_name}'"
            )
        return functools.partial(self._invoke, method_name)

    def _invoke(self, method_name: str, *args, **kwargs) -> Any:
        client = self._client
        policy = client.policy

        if policy == FailoverPolicy.NEVER or client.server_address is not None:
            return getattr(client._primary.stub, method_name)(*args, **kwargs)

        fallbacks = discover_fallback_gateways(
            gw_group=client.gw_group,
            primary_daemon_name=client._primary.daemon_name,
            primary_service_url=client._primary.gateway_addr
        )

        if not fallbacks:
            return getattr(client._primary.stub, method_name)(*args, **kwargs)

        candidates = client._build_candidates(fallbacks)
        ordered_candidates = order_candidates(
            candidates,
            key=lambda c: c['service_url']
        )

        last_rpc_error: Optional[grpc.RpcError] = None
        all_connect_checks_failed = True

        for candidate in ordered_candidates:
            candidate_client = client._get_or_create_candidate_client(candidate)
            if candidate_client is None:
                continue

            if not check_channel_connectivity(candidate_client.channel):
                mark_gateway_down(candidate_client.gateway_addr)
                logger.info(
                    "Connect check failed for gateway %s (%s); skipping to next candidate",
                    getattr(candidate_client, 'daemon_name', ''),
                    getattr(candidate_client, 'gateway_addr', '')
                )
                continue

            all_connect_checks_failed = False
            try:
                stub = getattr(candidate_client, 'stub', None)
                if stub is None:
                    logger.warning(
                        "Gateway %s has no stub available; skipping",
                        candidate_client.gateway_addr
                    )
                    continue
                response = getattr(stub, method_name)(*args, **kwargs)
                client._active_client = candidate_client
                return response
            except grpc.RpcError as rpc_err:
                last_rpc_error = rpc_err
                if rpc_err.code() == grpc.StatusCode.UNAVAILABLE:
                    mark_gateway_down(candidate_client.gateway_addr)
                    if policy == FailoverPolicy.ALWAYS:
                        logger.info(
                            "Gateway %s returned UNAVAILABLE under policy ALWAYS; retrying",
                            candidate_client.gateway_addr
                        )
                        continue
                    if policy == FailoverPolicy.IF_NOT_SENT:
                        logger.info(
                            "Gateway %s returned UNAVAILABLE after connect under IF_NOT_SENT; "
                            "raising to avoid duplicate execution",
                            candidate_client.gateway_addr
                        )
                        raise rpc_err
                raise rpc_err

        if last_rpc_error is not None:
            raise last_rpc_error

        if all_connect_checks_failed:
            raise DashboardException(
                msg="All NVMe-oF gateways are unavailable",
                code=grpc.StatusCode.UNAVAILABLE if grpc else "UNAVAILABLE",
                http_status_code=504,
                component="nvmeof"
            )

        raise DashboardException(
            msg="No NVMe-oF gateway was able to serve the request",
            code=grpc.StatusCode.UNAVAILABLE if grpc else "UNAVAILABLE",
            http_status_code=504,
            component="nvmeof"
        )


class FailoverNVMeoFClient(object):
    """
    Failover layer above NVMeoFClient.
    Manages primary and fallback gateway clients, dispatching calls based on FailoverPolicy.
    """
    pb2 = pb2

    def __init__(
        self,
        gw_group: Optional[str] = None,
        server_address: Optional[str] = None,
        failover: Optional[FailoverPolicy] = None  # pylint: disable=redefined-outer-name
    ):
        self.gw_group = gw_group
        self.server_address = server_address
        self.policy = resolve_failover_policy(
            server_address=server_address,
            failover_override=failover
        )
        self._primary = NVMeoFClient(gw_group=gw_group, server_address=server_address)
        self._active_client = self._primary
        self._fallback_clients: Dict[str, NVMeoFClient] = {}
        self.stub = _FailoverStub(self)

    @property
    def gateway_addr(self) -> str:
        return self._active_client.gateway_addr

    @property
    def daemon_name(self) -> str:
        return self._active_client.daemon_name

    @property
    def service_name(self) -> str:
        return getattr(self._active_client, 'service_name', None)

    @property
    def channel(self) -> Any:
        return self._active_client.channel

    def _build_candidates(self, fallbacks: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        primary_desc = {
            'type': 'primary',
            'client': self._primary,
            'service_url': self._primary.gateway_addr,
            'daemon_name': self._primary.daemon_name
        }
        fallback_descs = [
            {
                'type': 'fallback',
                'info': gw,
                'service_url': gw['service_url'],
                'daemon_name': gw.get('daemon_name')
            }
            for gw in fallbacks
        ]

        if self._active_client != self._primary:
            active_desc = {
                'type': 'active',
                'client': self._active_client,
                'service_url': self._active_client.gateway_addr,
                'daemon_name': self._active_client.daemon_name
            }
            remaining = [
                d for d in [primary_desc] + fallback_descs
                if d['service_url'] != self._active_client.gateway_addr
            ]
            return [active_desc] + remaining

        return [primary_desc] + fallback_descs

    def _get_or_create_candidate_client(
        self,
        candidate: Dict[str, Any]
    ) -> Optional[NVMeoFClient]:
        if 'client' in candidate:
            return candidate['client']
        s_url = candidate['service_url']
        if s_url not in self._fallback_clients:
            try:
                self._fallback_clients[s_url] = NVMeoFClient(
                    gw_group=self.gw_group,
                    server_address=s_url
                )
            except Exception as e:  # pylint: disable=broad-except
                logger.warning(
                    "Failed to create fallback client for %s: %s", s_url, e
                )
                return None
        return self._fallback_clients[s_url]
