# -*- coding: utf-8 -*-
"""Tests for ceph_secrets.secret_mgr (SecretMgr)."""
from __future__ import annotations

import pytest

from ceph_secrets.secret_mgr import SecretMgr
from ceph_secrets_types import (
    SecretScope, SecretRef,
    CephSecretException, CephSecretNotFoundError,
    BadSecretURI,
)


# ============================================================
# make_ref
# ============================================================

class TestMakeRef:
    def test_basic(self, secret_mgr):
        ref = secret_mgr.make_ref("ns", SecretScope.GLOBAL, "", "key")
        assert isinstance(ref, SecretRef)
        assert ref.namespace == "ns"

    def test_scope_as_string(self, secret_mgr):
        ref = secret_mgr.make_ref("ns", "host", "node1", "ssh_key")
        assert ref.scope == SecretScope.HOST

    def test_bad_scope_raises(self, secret_mgr):
        with pytest.raises(CephSecretException):
            secret_mgr.make_ref("ns", "badscope", "", "key")

    def test_bad_namespace_raises(self, secret_mgr):
        with pytest.raises(CephSecretException):
            secret_mgr.make_ref("bad ns!", SecretScope.GLOBAL, "", "key")


# ============================================================
# get / get_value
# ============================================================

class TestGet:
    def test_get_existing(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", {"value": "s3cr3t"})
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "pw")
        rec = secret_mgr.get(ref)
        assert rec.data["value"] == "s3cr3t"

    def test_get_missing_raises(self, secret_mgr):
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "ghost")
        with pytest.raises(CephSecretNotFoundError):
            secret_mgr.get(ref)

    def test_get_value_single_key(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", {"value": "s3cr3t"})
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "pw")
        assert secret_mgr.get_value(ref) == "s3cr3t"

    def test_get_value_multiple_keys_returns_dict(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "creds", {"u": "admin", "p": "pw"})
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "creds")
        val = secret_mgr.get_value(ref)
        assert isinstance(val, dict)
        assert val["u"] == "admin"

    def test_get_value_missing_raises(self, secret_mgr):
        ref = SecretRef("ns", SecretScope.GLOBAL, "", "ghost")
        with pytest.raises(CephSecretNotFoundError):
            secret_mgr.get_value(ref)


# ============================================================
# set
# ============================================================

class TestSet:
    def test_set_global(self, secret_mgr):
        rec = secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", {"v": "1"})
        assert rec.metadata.version == 1
        assert rec.data["v"] == "1"

    def test_set_service(self, secret_mgr):
        rec = secret_mgr.set("ns", SecretScope.SERVICE, "prom", "auth", {"u": "a"})
        assert rec.target == "prom"

    def test_set_custom(self, secret_mgr):
        rec = secret_mgr.set("ns", SecretScope.CUSTOM, "", "a/b/c", {"t": "tok"})
        assert rec.name == "a/b/c"

    def test_set_non_dict_raises(self, secret_mgr):
        with pytest.raises(CephSecretException, match="JSON object"):
            secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", "not-a-dict")  # type: ignore[arg-type]

    def test_set_increments_version(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", {"v": "1"})
        rec = secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", {"v": "2"})
        assert rec.metadata.version == 2

    def test_set_scope_string(self, secret_mgr):
        rec = secret_mgr.set("ns", "global", "", "k", {"v": "x"})
        assert rec.scope == SecretScope.GLOBAL


# ============================================================
# rm
# ============================================================

class TestRm:
    def test_rm_existing(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "k", {"v": "x"})
        assert secret_mgr.rm("ns", SecretScope.GLOBAL, "", "k") is True

    def test_rm_nonexistent(self, secret_mgr):
        assert secret_mgr.rm("ns", SecretScope.GLOBAL, "", "ghost") is False


# ============================================================
# ls
# ============================================================

class TestLs:
    def test_ls_empty(self, secret_mgr):
        assert secret_mgr.ls(namespace="ns") == []

    def test_ls_returns_records(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "a", {"v": "1"})
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "b", {"v": "2"})
        recs = secret_mgr.ls(namespace="ns")
        assert len(recs) == 2

    def test_ls_scope_filter(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "g", {"v": "1"})
        secret_mgr.set("ns", SecretScope.SERVICE, "prom", "auth", {"v": "2"})
        recs = secret_mgr.ls(namespace="ns", scope="service")
        assert len(recs) == 1
        assert recs[0].scope == SecretScope.SERVICE


# ============================================================
# scan_refs / scan_unresolved_refs
# ============================================================

class TestScanRefs:
    def test_scan_simple_string(self, secret_mgr):
        obj = "secret:/ns/global/pw"
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        uris = {r.to_uri() for r in refs}
        assert "secret:/ns/global/pw" in uris

    def test_scan_in_dict(self, secret_mgr):
        obj = {"key": "secret:/ns/global/pw"}
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        assert len(refs) == 1

    def test_scan_in_list(self, secret_mgr):
        obj = ["secret:/ns/global/pw", "secret:/ns/host/node1/ssh"]
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        assert len(refs) == 2

    def test_scan_nested(self, secret_mgr):
        obj = {"a": {"b": "secret:/ns/global/pw"}}
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        assert len(refs) == 1

    def test_scan_no_refs(self, secret_mgr):
        obj = {"plain": "value"}
        assert secret_mgr.scan_refs(obj, namespace="ns") == set()

    def test_scan_bad_uri_yields_bad_secret_uri(self, secret_mgr):
        obj = "secret:/ns/badscope/key"
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        bad = [r for r in refs if isinstance(r, BadSecretURI)]
        assert len(bad) == 1

    def test_scan_unresolved_all_exist(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", {"v": "x"})
        obj = "secret:/ns/global/pw"
        unresolved = secret_mgr.scan_unresolved_refs(obj, namespace="ns")
        assert len(unresolved) == 0

    def test_scan_unresolved_missing(self, secret_mgr):
        obj = "secret:/ns/global/ghost"
        unresolved = secret_mgr.scan_unresolved_refs(obj, namespace="ns")
        assert len(unresolved) == 1

    def test_scan_unresolved_bad_uri_is_unresolved(self, secret_mgr):
        obj = "secret:/ns/badscope/key"
        unresolved = secret_mgr.scan_unresolved_refs(obj, namespace="ns")
        assert len(unresolved) == 1


# ============================================================
# resolve_object
# ============================================================

class TestResolveObject:
    def test_resolve_single_value_secret(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", {"value": "s3cr3t"})
        result = secret_mgr.resolve_object("secret:/ns/global/pw")
        assert result == "s3cr3t"

    def test_resolve_multi_value_secret_returns_dict(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "creds", {"u": "a", "p": "b"})
        result = secret_mgr.resolve_object("secret:/ns/global/creds")
        assert isinstance(result, dict)

    def test_resolve_in_dict(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "pw", {"value": "s3cr3t"})
        result = secret_mgr.resolve_object({"password": "secret:/ns/global/pw"})
        assert result["password"] == "s3cr3t"

    def test_resolve_in_list(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "a", {"value": "x"})
        result = secret_mgr.resolve_object(["secret:/ns/global/a", "plain"])
        assert result[0] == "x"
        assert result[1] == "plain"

    def test_resolve_in_tuple(self, secret_mgr):
        secret_mgr.set("ns", SecretScope.GLOBAL, "", "t", {"value": "y"})
        result = secret_mgr.resolve_object(("secret:/ns/global/t",))
        assert result == ("y",)

    def test_resolve_non_secret_string_unchanged(self, secret_mgr):
        result = secret_mgr.resolve_object("just a normal string")
        assert result == "just a normal string"

    def test_resolve_non_string_unchanged(self, secret_mgr):
        assert secret_mgr.resolve_object(42) == 42
        assert secret_mgr.resolve_object(None) is None

    def test_resolve_missing_secret_raises(self, secret_mgr):
        with pytest.raises(CephSecretException):
            secret_mgr.resolve_object("secret:/ns/global/ghost")

    def test_resolve_invalid_uri_raises(self, secret_mgr):
        with pytest.raises(CephSecretException):
            secret_mgr.resolve_object("secret:/ns/badscope/key")


# ============================================================
# scan_refs — edge cases
# ============================================================

class TestScanRefsEdgeCases:
    def test_multiple_refs_in_one_string(self, secret_mgr):
        obj = "use secret:/ns/global/a and secret:/ns/global/b"
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        uris = {r.to_uri() for r in refs}
        assert "secret:/ns/global/a" in uris
        assert "secret:/ns/global/b" in uris

    def test_duplicate_refs_deduped(self, secret_mgr):
        obj = ["secret:/ns/global/pw", "secret:/ns/global/pw"]
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        uris = [r.to_uri() for r in refs]
        assert uris.count("secret:/ns/global/pw") == 1

    def test_ref_followed_by_punctuation(self, secret_mgr):
        # The regex greedily matches up to the comma since comma is not a valid
        # URI character, so "secret:/ns/global/pw," is scanned as a BadSecretURI
        # (invalid name). This is correct product behaviour — the caller is
        # responsible for not embedding URIs inside punctuated strings without
        # separators.
        obj = "secret:/ns/global/pw,"
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        bad = [r for r in refs if isinstance(r, BadSecretURI)]
        assert len(bad) == 1

    def test_ref_inside_tuple(self, secret_mgr):
        obj = ("secret:/ns/global/pw",)
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        assert len(refs) == 1

    def test_cross_namespace_ref_is_scanned(self, secret_mgr):
        """scan_refs finds refs regardless of namespace — namespace arg does not filter."""
        obj = "secret:/other/global/pw"
        refs = secret_mgr.scan_refs(obj, namespace="ns")
        uris = {r.to_uri() for r in refs}
        assert "secret:/other/global/pw" in uris
