"""Catalog, migration, A/B substrate, lease, search, and SSE mixin for MembusClient."""

from __future__ import annotations

import base64
import http.client
import importlib.util
import pathlib
import sys
import urllib.parse
from types import ModuleType
from typing import Any, Iterator, Mapping, Sequence


def _load_sibling_module(mod_name: str) -> ModuleType:
    cache_key = f"_membus_v2_sibling_{mod_name}"
    existing = sys.modules.get(cache_key)
    if existing is not None:
        return existing
    path = pathlib.Path(__file__).resolve().parent / f"{mod_name}.py"
    spec = importlib.util.spec_from_file_location(cache_key, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"unable to load sibling module {mod_name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[cache_key] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(cache_key, None)
        raise
    return module


try:
    if __package__:
        from . import membus_protocol as _proto
    else:
        import membus_protocol as _proto
except ImportError:
    _proto = _load_sibling_module("membus_protocol")

CommitResult = _proto.CommitResult
LeaseGrant = _proto.LeaseGrant
LeaseInfo = _proto.LeaseInfo
MembusError = _proto.MembusError
SignalEvent = _proto.SignalEvent
SseMessage = _proto.SseMessage
_parse_http_error = _proto._parse_http_error
_quote_segment = _proto._quote_segment
_to_byte_list = _proto._to_byte_list
parse_signal_event = _proto.parse_signal_event
parse_sse_lines = _proto.parse_sse_lines


class MembusControlPlaneMixin:
    """Control-plane, lease, search, A/B substrate, and SSE streaming operations."""

    base_url: str
    timeout: float

    def _headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        raise NotImplementedError

    def _request_json(
        self,
        method: str,
        path: str,
        payload: Any | None = None,
        *,
        extra_headers: Mapping[str, str] | None = None,
    ) -> Any:
        raise NotImplementedError

    @staticmethod
    def _ab_headers(
        *,
        role_epoch: int | None = None,
        idempotency_key: str | None = None,
        route_origin: str | None = None,
    ) -> dict[str, str] | None:
        raise NotImplementedError

    # --- Collection Schema Catalog & Expand-Contract Migrations ---

    def register_collection(
        self,
        descriptor: Mapping[str, Any],
        *,
        expected_version: int | None = None,
        role_epoch: int | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v2/catalog/collections",
            {
                "expected_version": expected_version,
                "descriptor": dict(descriptor),
            },
            extra_headers=self._ab_headers(role_epoch=role_epoch),
        )

    def list_collections(self) -> list[dict[str, Any]]:
        res = self._request_json("GET", "/v2/catalog/collections")
        return list(res)

    def get_collection(self, namespace: str) -> dict[str, Any] | None:
        ns_enc = _quote_segment(namespace)
        try:
            return self._request_json("GET", f"/v2/catalog/collections/{ns_enc}")
        except MembusError as err:
            if err.status == 404:
                return None
            raise

    def get_collection_version(self, namespace: str, version: int) -> dict[str, Any] | None:
        ns_enc = _quote_segment(namespace)
        try:
            return self._request_json(
                "GET", f"/v2/catalog/collections/{ns_enc}/versions/{int(version)}"
            )
        except MembusError as err:
            if err.status == 404:
                return None
            raise

    def begin_migration(
        self,
        namespace: str,
        *,
        expected_version: int,
        target_version: int,
        target_sha256: Sequence[int] | bytes,
        target_schema_bytes: bytes | str,
        role_epoch: int | None = None,
    ) -> dict[str, Any]:
        ns_enc = _quote_segment(namespace)
        return self._request_json(
            "POST",
            f"/v2/catalog/collections/{ns_enc}/migrations/begin",
            {
                "expected_version": expected_version,
                "target_version": target_version,
                "target_sha256": list(target_sha256),
                "target_schema_bytes": _to_byte_list(target_schema_bytes),
            },
            extra_headers=self._ab_headers(role_epoch=role_epoch),
        )

    def advance_migration(
        self,
        namespace: str,
        *,
        target_version: int,
        verified_through: int,
        role_epoch: int | None = None,
    ) -> dict[str, Any]:
        ns_enc = _quote_segment(namespace)
        return self._request_json(
            "POST",
            f"/v2/catalog/collections/{ns_enc}/migrations/advance",
            {
                "target_version": target_version,
                "verified_through": verified_through,
            },
            extra_headers=self._ab_headers(role_epoch=role_epoch),
        )

    def enter_reverse_shadow(
        self,
        namespace: str,
        *,
        target_version: int,
        verified_through: int,
        role_epoch: int | None = None,
    ) -> dict[str, Any]:
        ns_enc = _quote_segment(namespace)
        return self._request_json(
            "POST",
            f"/v2/catalog/collections/{ns_enc}/migrations/reverse-shadow",
            {
                "target_version": target_version,
                "verified_through": verified_through,
            },
            extra_headers=self._ab_headers(role_epoch=role_epoch),
        )

    def rollback_migration(
        self,
        namespace: str,
        *,
        rollback_version: int,
        role_epoch: int | None = None,
    ) -> dict[str, Any]:
        ns_enc = _quote_segment(namespace)
        return self._request_json(
            "POST",
            f"/v2/catalog/collections/{ns_enc}/migrations/rollback",
            {"rollback_version": rollback_version},
            extra_headers=self._ab_headers(role_epoch=role_epoch),
        )

    def contract_migration(
        self,
        namespace: str,
        *,
        expected_version: int,
        role_epoch: int | None = None,
    ) -> dict[str, Any]:
        ns_enc = _quote_segment(namespace)
        return self._request_json(
            "POST",
            f"/v2/catalog/collections/{ns_enc}/migrations/contract",
            {"expected_version": expected_version},
            extra_headers=self._ab_headers(role_epoch=role_epoch),
        )

    def commit_migration(
        self,
        namespace: str,
        *,
        target_version: int,
        verified_through: int,
        role_epoch: int | None = None,
    ) -> dict[str, Any]:
        ns_enc = _quote_segment(namespace)
        return self._request_json(
            "POST",
            f"/v2/catalog/collections/{ns_enc}/migrations/commit",
            {
                "target_version": target_version,
                "verified_through": verified_through,
            },
            extra_headers=self._ab_headers(role_epoch=role_epoch),
        )

    def abort_migration(
        self,
        namespace: str,
        *,
        target_version: int,
        role_epoch: int | None = None,
    ) -> dict[str, Any]:
        ns_enc = _quote_segment(namespace)
        return self._request_json(
            "POST",
            f"/v2/catalog/collections/{ns_enc}/migrations/abort",
            {"target_version": target_version},
            extra_headers=self._ab_headers(role_epoch=role_epoch),
        )

    # --- A/B Substrate Primitives (Parity Digest, Slot Role, Replication Apply) ---

    def namespace_digest(
        self,
        namespace: str,
        *,
        mode: str = "full",
        prefix: bytes | str | None = None,
    ) -> dict[str, Any]:
        ns_enc = _quote_segment(namespace)
        body: dict[str, Any] = {"mode": mode}
        if isinstance(prefix, str):
            body["prefix_utf8"] = prefix
        elif isinstance(prefix, bytes):
            body["prefix_b64"] = base64.b64encode(prefix).decode("ascii")
        return self._request_json("POST", f"/v2/namespaces/{ns_enc}/digest", body)

    def transition_slot_role(
        self,
        *,
        expected_role: str,
        expected_role_epoch: int,
        new_role: str,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v2/admin/role",
            {
                "expected_role": expected_role,
                "expected_role_epoch": expected_role_epoch,
                "new_role": new_role,
            },
        )

    def apply_replicated_delta(
        self,
        delta: Mapping[str, Any],
        *,
        ack: str = "Fast",
        route_origin: str | None = None,
        role_epoch: int | None = None,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v2/replication/apply",
            {"delta": dict(delta), "ack": ack},
            extra_headers=self._ab_headers(
                role_epoch=role_epoch,
                route_origin=route_origin,
            ),
        )

    # --- Fenced Leases (`LeaseGrant` vs `LeaseInfo`) ---

    def acquire_lease(
        self,
        namespace: str,
        resource: str,
        holder: str,
        ttl_ms: int,
    ) -> LeaseGrant:
        res = self._request_json(
            "POST",
            "/v2/leases/acquire",
            {
                "namespace": namespace,
                "resource": resource,
                "holder": holder,
                "ttl_ms": ttl_ms,
            },
        )
        return LeaseGrant(
            namespace=res["namespace"],
            resource=res["resource"],
            holder=res["holder"],
            generation=int(res["generation"]),
            token=res["token"],
            expires_at_ms=int(res["expires_at_ms"]),
        )

    def renew_lease(self, grant: LeaseGrant, ttl_ms: int) -> LeaseGrant:
        res = self._request_json(
            "POST",
            "/v2/leases/renew",
            {
                "namespace": grant.namespace,
                "resource": grant.resource,
                "holder": grant.holder,
                "generation": grant.generation,
                "token": grant.token,
                "ttl_ms": ttl_ms,
            },
        )
        return LeaseGrant(
            namespace=res["namespace"],
            resource=res["resource"],
            holder=res["holder"],
            generation=int(res["generation"]),
            token=res["token"],
            expires_at_ms=int(res["expires_at_ms"]),
        )

    def release_lease(self, grant: LeaseGrant) -> bool:
        res = self._request_json(
            "POST",
            "/v2/leases/release",
            {
                "namespace": grant.namespace,
                "resource": grant.resource,
                "holder": grant.holder,
                "generation": grant.generation,
                "token": grant.token,
            },
        )
        return bool(res["ok"])

    def validate_lease(self, grant: LeaseGrant) -> bool:
        res = self._request_json(
            "POST",
            "/v2/leases/validate",
            {
                "namespace": grant.namespace,
                "resource": grant.resource,
                "holder": grant.holder,
                "generation": grant.generation,
                "token": grant.token,
            },
        )
        return bool(res["ok"])

    def inspect_lease(self, namespace: str, resource: str) -> LeaseInfo | None:
        ns_enc = _quote_segment(namespace)
        res_enc = _quote_segment(resource)
        try:
            res = self._request_json("GET", f"/v2/leases/{ns_enc}/{res_enc}")
        except MembusError as err:
            if err.status == 404:
                return None
            raise
        if "token" in res:
            raise MembusError(500, "protocol", "inspect_lease unexpectedly exposed secret token")
        return LeaseInfo(
            namespace=res["namespace"],
            resource=res["resource"],
            holder=res["holder"],
            generation=int(res["generation"]),
            expires_at_ms=int(res["expires_at_ms"]),
        )

    # --- Embedded Search (`Bm25` & `LiteralSubstring`) ---

    def index_search_document(
        self,
        *,
        namespace: str,
        doc_id: str,
        kind: str,
        title: str,
        body: str,
        tags: Sequence[str] = (),
        visibility: str = "Public",
        ack: str = "Fast",
    ) -> CommitResult:
        res = self._request_json(
            "POST",
            "/v2/search/documents",
            {
                "doc": {
                    "namespace": namespace,
                    "doc_id": doc_id,
                    "kind": kind,
                    "title": title,
                    "tags": list(tags),
                    "body": body,
                    "visibility": visibility,
                },
                "ack": ack,
            },
        )
        return CommitResult(
            seq=int(res["seq"]),
            epoch=int(res["epoch"]),
            versions=tuple(int(v) for v in res["versions"]),
        )

    def delete_search_document(
        self,
        namespace: str,
        doc_id: str,
        *,
        ack: str = "fast",
    ) -> CommitResult:
        ns_enc = _quote_segment(namespace)
        doc_enc = _quote_segment(doc_id)
        qs = urllib.parse.urlencode({"ack": ack})
        res = self._request_json("DELETE", f"/v2/search/documents/{ns_enc}/{doc_enc}?{qs}")
        return CommitResult(
            seq=int(res["seq"]),
            epoch=int(res["epoch"]),
            versions=tuple(int(v) for v in res["versions"]),
        )

    def revoke_search_prefix(
        self,
        namespace: str,
        prefix: str,
        *,
        ack: str = "Fast",
        role_epoch: int | None = None,
    ) -> CommitResult:
        res = self._request_json(
            "POST",
            "/v2/search/revoke-prefix",
            {"namespace": namespace, "prefix": prefix, "ack": ack},
            extra_headers=self._ab_headers(role_epoch=role_epoch),
        )
        return CommitResult(
            seq=int(res["seq"]),
            epoch=int(res["epoch"]),
            versions=tuple(int(v) for v in res["versions"]),
        )

    def search(
        self,
        query: str,
        *,
        mode: str = "Bm25",
        namespace: str | None = None,
        kind: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        return self._request_json(
            "POST",
            "/v2/search/query",
            {
                "query": query,
                "mode": mode,
                "namespace": namespace,
                "kind": kind,
                "limit": limit,
            },
        )

    # --- Live SSE Signal Streaming ---

    def stream_signals(
        self,
        namespace: str,
        *,
        mode: str = "each",
        prefix: str | None = None,
        since_seq: int | None = None,
        epoch: int | None = None,
        max_pending: int | None = None,
        timeout: float | None = None,
    ) -> Iterator[SseMessage]:
        """Yield parsed SSE messages (`subscribed`, `signal`, `error`) over `http.client`."""
        parsed = urllib.parse.urlsplit(self.base_url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        base_path = parsed.path.rstrip("/")
        params: dict[str, str] = {"mode": mode}
        if prefix is not None:
            params["prefix"] = prefix
        if since_seq is not None:
            params["since_seq"] = str(since_seq)
        if epoch is not None:
            params["epoch"] = str(epoch)
        if max_pending is not None:
            params["max_pending"] = str(max_pending)

        ns_enc = _quote_segment(namespace)
        path = f"{base_path}/v2/signals/{ns_enc}/stream?{urllib.parse.urlencode(params)}"
        effective_timeout = self.timeout if timeout is None else timeout
        conn_cls = (
            http.client.HTTPSConnection
            if parsed.scheme == "https"
            else http.client.HTTPConnection
        )
        conn = conn_cls(host, port, timeout=effective_timeout)
        try:
            headers = self._headers({"Accept": "text/event-stream"})
            conn.request("GET", path, headers=headers)
            resp = conn.getresponse()
            if resp.status != 200:
                raw = resp.read()
                raise _parse_http_error(resp.status, raw, default_code="sse_error")

            yield from self._parse_sse_lines(resp)
        except MembusError:
            raise
        except (http.client.HTTPException, OSError, UnicodeDecodeError) as err:
            raise MembusError(0, "transport", str(err)) from None
        finally:
            conn.close()

    @staticmethod
    def _parse_sse_lines(stream: Any) -> Iterator[SseMessage]:
        return parse_sse_lines(stream)

    @staticmethod
    def parse_signal_event(data: Mapping[str, Any]) -> SignalEvent:
        return parse_signal_event(data)
