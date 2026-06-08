# -*- coding: utf-8 -*-
import logging
import re
from typing import Any, Dict, List, Optional, Set, Union, Hashable, Protocol

from .secret_store import SecretRecord
from ceph_secrets_types import (
    CephSecretException,
    CephSecretNotFoundError,
    SecretRef,
    BadSecretURI,
    SecretScope,
    parse_secret_uri,
    SECRET_SCHEME
)


_SECRET_URI_PREFIX = f'{SECRET_SCHEME}:/'
_SECRET_URI_RE = re.compile(rf"{re.escape(_SECRET_URI_PREFIX)}(?!/)[^\s\"']*")


logger = logging.getLogger(__name__)


class SecretURI(Protocol, Hashable):
    def to_uri(self) -> str:
        ...


def _coerce_scope(scope: Union[SecretScope, str]) -> SecretScope:
    if isinstance(scope, SecretScope):
        return scope
    return SecretScope.from_str(str(scope))


class SecretMgr:
    """
    Phase 1: Mon-store backend only.

    Resolution rule:
      - If secret.data has exactly one key, return that single value.
      - Otherwise, return the dict as-is.
    """

    def __init__(self, store: Any) -> None:
        self.store = store

    def make_ref(
        self,
        namespace: str,
        scope: Union[SecretScope, str],
        target: str = '',
        name: str = '',
    ) -> SecretRef:
        try:
            return SecretRef(
                namespace=namespace,
                scope=_coerce_scope(scope),
                target=target or '',
                name=name,
            )
        except ValueError as e:
            raise CephSecretException(str(e)) from e

    def get(self, ref: SecretRef) -> SecretRecord:
        rec = self.store.get(ref.namespace, ref.scope, ref.target, ref.name)
        if rec is None:
            raise CephSecretNotFoundError(f"Secret not found: {ref.to_uri()}")
        return rec

    def get_value(self, ref: SecretRef) -> Any:
        # If exactly one entry exists, return the single value; otherwise return
        # the full dict. Field-level selection is intentionally not supported.
        rec = self.get(ref)
        if len(rec.data) == 1:
            return next(iter(rec.data.values()))

        return rec.data

    def set(
        self,
        namespace: str,
        scope: Union[SecretScope, str],
        target: str,
        name: str,
        data: Dict[str, Any],
        user_made: bool = True,
        editable: bool = True,
    ) -> SecretRecord:
        if not isinstance(data, dict):
            raise CephSecretException('Secret data must be a JSON object')

        ref = self.make_ref(namespace, scope, target, name)
        return self.store.set(
            ref.namespace,
            ref.scope,
            ref.target,
            ref.name,
            data,
            user_made,
            editable,
        )

    def rm(
        self,
        namespace: str,
        scope: Union[SecretScope, str],
        target: str,
        name: str,
    ) -> bool:
        ref = self.make_ref(namespace, scope, target, name)
        return self.store.rm(ref.namespace, ref.scope, ref.target, ref.name)

    def ls(
        self,
        namespace: Optional[str] = None,
        scope: Optional[Union[SecretScope, str]] = None,
        target: Optional[str] = None,
    ) -> List[SecretRecord]:
        sc = _coerce_scope(scope) if scope else None
        return self.store.ls(namespace=namespace, scope=sc, target=target)

    def scan_unresolved_refs(self, obj: Any, namespace: str) -> Set[SecretURI]:
        """
        Return secret refs found in `obj` that cannot be fetched.
        """
        unresolved: Set[SecretURI] = set()
        for ref in self.scan_refs(obj, namespace):
            if isinstance(ref, SecretRef):
                try:
                    self.get_value(ref)
                except CephSecretException:
                    unresolved.add(ref)
            else:
                unresolved.add(ref)
        return unresolved

    def scan_refs(self, obj: Any, namespace: str) -> Set[SecretURI]:
        refs: Set[SecretURI] = set()

        def _scan(v: Any) -> None:
            if isinstance(v, dict):
                for vv in v.values():
                    _scan(vv)
            elif isinstance(v, (list, tuple)):
                for vv in v:
                    _scan(vv)
            elif isinstance(v, str) and _SECRET_URI_PREFIX in v:
                for m in _SECRET_URI_RE.finditer(v):
                    uri = m.group(0)
                    try:
                        refs.add(parse_secret_uri(uri))
                    except Exception as e:
                        logger.warning("Failed to parse secret uri %r: %s", uri, e)
                        refs.add(BadSecretURI(raw=uri, namespace=namespace, error=str(e)))

        _scan(obj)
        return refs

    def _resolve_secret_uri(self, uri: str) -> Any:
        """Resolve a single secret URI to its value."""
        try:
            parsed_secret = parse_secret_uri(uri)
        except CephSecretException as e:
            raise CephSecretException(f"Invalid secret URI {uri!r}: {e}") from e
        if not isinstance(parsed_secret, SecretRef):
            raise CephSecretException(f"Invalid secret URI {uri!r}")
        return self.get_value(parsed_secret)

    def _resolve(self, v: Any) -> Any:
        """Recursively resolve secret URIs within a nested structure."""
        if isinstance(v, dict):
            return {k: self._resolve(vv) for k, vv in v.items()}
        if isinstance(v, list):
            return [self._resolve(vv) for vv in v]
        if isinstance(v, tuple):
            return tuple(self._resolve(vv) for vv in v)
        if isinstance(v, str):
            s = v.strip()
            if s.startswith(_SECRET_URI_PREFIX):
                return self._resolve_secret_uri(s)
        return v

    def resolve_object(self, obj: Any) -> Any:
        """Resolve secret references within nested dict/list/tuple structures.
        Strings that are exactly a secret URI are replaced by their referenced
        value.

        Note: Embedded secret URIs within larger strings are not supported,
        the entire string value must be a secret URI.
        """
        return self._resolve(obj)
