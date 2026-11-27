"""Pure-stdlib Python 3.11+ client for the Membus v2 HTTP/REST + SSE surface.

Zero third-party dependencies: built strictly on `urllib`, `http.client`,
`json`, `base64`, and `dataclasses`.
"""

from __future__ import annotations

import base64
import http.client
import importlib.util
import json
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request
from types import ModuleType
from typing import Any, Mapping, Sequence


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
        from . import membus_control as _control
        from . import membus_protocol as _proto
    else:
        import membus_control as _control
        import membus_protocol as _proto
except ImportError:
    _proto = _load_sibling_module("membus_protocol")
    _control = _load_sibling_module("membus_control")

MembusControlPlaneMixin = _control.MembusControlPlaneMixin
CommitResult = _proto.CommitResult
LeaseGrant = _proto.LeaseGrant
LeaseInfo = _proto.LeaseInfo
MembusError = _proto.MembusError
RecordMeta = _proto.RecordMeta
SignalEvent = _proto.SignalEvent
SseMessage = _proto.SseMessage
StoredRecord = _proto.StoredRecord
_encode_expected = _proto._encode_expected
_from_byte_list = _proto._from_byte_list
_parse_http_error = _proto._parse_http_error
_quote_segment = _proto._quote_segment
_strip_sse_field_value = _proto._strip_sse_field_value
_to_byte_list = _proto._to_byte_list
_to_bytes = _proto._to_bytes

__all__ = [
    "CommitResult",
    "LeaseGrant",
    "LeaseInfo",
    "MembusClient",
    "MembusError",
    "RecordMeta",
    "SignalEvent",
    "SseMessage",
    "StoredRecord",
    "_encode_expected",
    "_from_byte_list",
    "_parse_http_error",
    "_quote_segment",
    "_strip_sse_field_value",
    "_to_byte_list",
    "_to_bytes",
]


class MembusClient(MembusControlPlaneMixin):
    """Synchronous pure-stdlib client targeting the unified Membus v2 HTTP/REST + SSE API."""

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout: float = 15.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def _headers(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self.token is not None:
            headers["Authorization"] = f"Bearer {self.token}"
        if extra:
            headers.update(extra)
        return headers

    def _request_raw(
        self,
        method: str,
        path: str,
        *,
        body: bytes | None = None,
        content_type: str | None = None,
        extra_headers: Mapping[str, str] | None = None,
    ) -> tuple[int, bytes]:
        url = f"{self.base_url}{path}"
        extra: dict[str, str] = dict(extra_headers) if extra_headers else {}
        if content_type is not None:
            extra["Content-Type"] = content_type
        req = urllib.request.Request(
            url,
            data=body,
            headers=self._headers(extra),
            method=method,
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as err:
            try:
                raw = err.read()
                status_code = err.code
            finally:
                err.close()
            raise _parse_http_error(status_code, raw) from None
        except (urllib.error.URLError, http.client.HTTPException, OSError) as err:
            raise MembusError(0, "transport", str(err)) from None

    def _request_json(
        self,
        method: str,
        path: str,
        payload: Any | None = None,
        *,
        extra_headers: Mapping[str, str] | None = None,
    ) -> Any:
        body: bytes | None = None
        content_type: str | None = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            content_type = "application/json"
        _status, raw = self._request_raw(
            method,
            path,
            body=body,
            content_type=content_type,
            extra_headers=extra_headers,
        )
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            raise MembusError(500, "protocol", f"invalid JSON response: {err}") from None

    @staticmethod
    def _ab_headers(
        *,
        role_epoch: int | None = None,
        idempotency_key: str | None = None,
        route_origin: str | None = None,
    ) -> dict[str, str] | None:
        hdrs: dict[str, str] = {}
        if role_epoch is not None:
            hdrs["X-Membus-Role-Epoch"] = str(role_epoch)
        if idempotency_key is not None:
            hdrs["X-Membus-Idempotency-Key"] = idempotency_key
        if route_origin is not None:
            hdrs["X-Membus-Route-Origin"] = route_origin
        return hdrs or None

    # --- Health & Checkpoint ---

    def health(self) -> dict[str, Any]:
        return self._request_json("GET", "/v2/health")

    def checkpoint(self, dest_dir: str) -> int:
        res = self._request_json("POST", "/v2/checkpoint", {"dest_dir": dest_dir})
        return int(res["seq"])

    # --- KV & Transactions ---

    def put(
        self,
        namespace: str,
        key: str,
        value: bytes | str,
        *,
        ack: str = "Fast",
        expected: str | int | Mapping[str, Any] = "Any",
        ordered_indexes: Sequence[Mapping[str, Any]] | None = None,
        graph_edges: Sequence[Mapping[str, Any]] | None = None,
        search_op: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        role_epoch: int | None = None,
    ) -> CommitResult:
        ns_enc = _quote_segment(namespace)
        key_enc = _quote_segment(key)
        body: dict[str, Any] = {
            "ack": ack,
            "expected": _encode_expected(expected),
        }
        if isinstance(value, str):
            body["value_utf8"] = value
        else:
            body["value_b64"] = base64.b64encode(value).decode("ascii")
        if ordered_indexes is not None or graph_edges is not None:
            body["indexes"] = {
                "ordered": [
                    {
                        "index_id": item["index_id"],
                        "sort_key": _to_byte_list(item["sort_key"]),
                        "summary": _to_byte_list(item.get("summary", b"")),
                    }
                    for item in (ordered_indexes or ())
                ],
                "edges": [
                    {
                        "index_id": edge["index_id"],
                        "src_id": _to_byte_list(edge["src_id"]),
                        "rel_label": _to_byte_list(edge["rel_label"]),
                        "dst_id": _to_byte_list(edge["dst_id"]),
                        "meta": _to_byte_list(edge.get("meta", b"")),
                    }
                    for edge in (graph_edges or ())
                ],
            }
        if search_op is not None:
            body["search_op"] = dict(search_op)
        res = self._request_json(
            "PUT",
            f"/v2/kv/{ns_enc}/{key_enc}",
            body,
            extra_headers=self._ab_headers(
                role_epoch=role_epoch,
                idempotency_key=idempotency_key,
            ),
        )
        return CommitResult(
            seq=int(res["seq"]),
            epoch=int(res["epoch"]),
            versions=tuple(int(v) for v in res["versions"]),
        )

    def get(self, namespace: str, key: str) -> StoredRecord | None:
        ns_enc = _quote_segment(namespace)
        key_enc = _quote_segment(key)
        try:
            res = self._request_json("GET", f"/v2/kv/{ns_enc}/{key_enc}")
        except MembusError as err:
            if err.status == 404:
                return None
            raise
        meta_raw = res["meta"]
        return StoredRecord(
            key=_from_byte_list(res["key"]),
            value=_from_byte_list(res["value"]),
            meta=RecordMeta(
                version=int(meta_raw["version"]),
                flags=int(meta_raw["flags"]),
                ts_ms=int(meta_raw["ts_ms"]),
                crc32=int(meta_raw["crc32"]),
            ),
        )

    def delete(
        self,
        namespace: str,
        key: str,
        *,
        ack: str = "Fast",
        expected: str | int | Mapping[str, Any] = "Any",
        search_op: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        role_epoch: int | None = None,
    ) -> CommitResult:
        ns_enc = _quote_segment(namespace)
        key_enc = _quote_segment(key)
        body: dict[str, Any] = {
            "ack": ack,
            "expected": _encode_expected(expected),
        }
        if search_op is not None:
            body["search_op"] = dict(search_op)
        res = self._request_json(
            "DELETE",
            f"/v2/kv/{ns_enc}/{key_enc}",
            body,
            extra_headers=self._ab_headers(
                role_epoch=role_epoch,
                idempotency_key=idempotency_key,
            ),
        )
        return CommitResult(
            seq=int(res["seq"]),
            epoch=int(res["epoch"]),
            versions=tuple(int(v) for v in res["versions"]),
        )

    def commit_tx(
        self,
        *,
        guards: Sequence[Mapping[str, Any]] = (),
        mutations: Sequence[Mapping[str, Any]],
        ack: str = "Fast",
        idempotency_key: str | None = None,
        role_epoch: int | None = None,
    ) -> CommitResult:
        enc_guards = [
            {
                "namespace": g["namespace"],
                "key": _to_byte_list(g["key"]),
                "expected": _encode_expected(g["expected"]),
            }
            for g in guards
        ]
        enc_mutations = []
        for m in mutations:
            if "value" in m and m["value"] is not None:
                op: Any = {"Put": {"value": _to_byte_list(m["value"])}}
            else:
                op = "Remove"
            indexes = None
            if m.get("ordered_indexes") is not None or m.get("graph_edges") is not None:
                indexes = {
                    "ordered": [
                        {
                            "index_id": item["index_id"],
                            "sort_key": _to_byte_list(item["sort_key"]),
                            "summary": _to_byte_list(item.get("summary", b"")),
                        }
                        for item in (m.get("ordered_indexes") or ())
                    ],
                    "edges": [
                        {
                            "index_id": edge["index_id"],
                            "src_id": _to_byte_list(edge["src_id"]),
                            "rel_label": _to_byte_list(edge["rel_label"]),
                            "dst_id": _to_byte_list(edge["dst_id"]),
                            "meta": _to_byte_list(edge.get("meta", b"")),
                        }
                        for edge in (m.get("graph_edges") or ())
                    ],
                }
            enc_mutations.append(
                {
                    "mutation": {
                        "namespace": m["namespace"],
                        "key": _to_byte_list(m["key"]),
                        "op": op,
                    },
                    "indexes": indexes,
                    "search_op": m.get("search_op"),
                }
            )
        res = self._request_json(
            "POST",
            "/v2/tx",
            {"ack": ack, "guards": enc_guards, "mutations": enc_mutations},
            extra_headers=self._ab_headers(
                role_epoch=role_epoch,
                idempotency_key=idempotency_key,
            ),
        )
        return CommitResult(
            seq=int(res["seq"]),
            epoch=int(res["epoch"]),
            versions=tuple(int(v) for v in res["versions"]),
        )

    # --- Ordered Secondary Indexes & Graph Traversal ---

    def query_ordered_index(
        self,
        namespace: str,
        index_id: str,
        *,
        prefix: str | None = None,
        start: str | None = None,
        end: str | None = None,
        after_cursor: bytes | None = None,
        limit: int = 256,
    ) -> dict[str, Any]:
        flt: dict[str, Any]
        if prefix is not None:
            flt = {"kind": "prefix", "prefix_utf8": prefix}
        elif start is not None or end is not None:
            flt = {
                "kind": "range",
                "start_utf8": start or "",
                "end_utf8": end or "",
            }
        else:
            flt = {"kind": "all"}
        req: dict[str, Any] = {
            "namespace": namespace,
            "index_id": index_id,
            "filter": flt,
            "limit": limit,
        }
        if after_cursor is not None:
            req["after_cursor_b64"] = base64.b64encode(after_cursor).decode("ascii")
        res = self._request_json("POST", "/v2/index/ordered/query", req)
        return {
            "entries": [
                {
                    "sort_key": _from_byte_list(e["sort_key"]),
                    "primary_key": _from_byte_list(e["primary_key"]),
                    "summary": _from_byte_list(e["summary"]),
                }
                for e in res["entries"]
            ],
            "next_cursor": (
                _from_byte_list(res["next_cursor"])
                if res.get("next_cursor") is not None
                else None
            ),
            "truncated": bool(res["truncated"]),
            "snapshot_seq": int(res["snapshot_seq"]),
        }

    def query_neighbors(
        self,
        namespace: str,
        index_id: str,
        node_id: str,
        *,
        direction: str = "Out",
        rel_filter: str | None = None,
        limit: int = 256,
    ) -> dict[str, Any]:
        req: dict[str, Any] = {
            "namespace": namespace,
            "index_id": index_id,
            "node_id_utf8": node_id,
            "direction": direction,
            "limit": limit,
        }
        if rel_filter is not None:
            req["rel_filter_utf8"] = rel_filter
        res = self._request_json("POST", "/v2/index/graph/neighbors", req)
        return {
            "edges": [
                {
                    "src_id": _from_byte_list(e["src_id"]),
                    "rel_label": _from_byte_list(e["rel_label"]),
                    "dst_id": _from_byte_list(e["dst_id"]),
                    "direction": e["direction"],
                    "meta": _from_byte_list(e["meta"]),
                }
                for e in res["edges"]
            ],
            "truncated": bool(res["truncated"]),
            "snapshot_seq": int(res["snapshot_seq"]),
        }

    def bfs_traverse(
        self,
        namespace: str,
        index_id: str,
        seed_node: str,
        *,
        direction: str = "Out",
        rel_filter: str | None = None,
        max_hops: int = 2,
        max_nodes: int = 1000,
    ) -> dict[str, Any]:
        req: dict[str, Any] = {
            "namespace": namespace,
            "index_id": index_id,
            "seed_node_utf8": seed_node,
            "direction": direction,
            "max_hops": max_hops,
            "max_nodes": max_nodes,
        }
        if rel_filter is not None:
            req["rel_filter_utf8"] = rel_filter
        res = self._request_json("POST", "/v2/index/graph/bfs", req)
        return {
            "visited_nodes": [_from_byte_list(n) for n in res["visited_nodes"]],
            "traversed_edges": [
                {
                    "src_id": _from_byte_list(e["src_id"]),
                    "rel_label": _from_byte_list(e["rel_label"]),
                    "dst_id": _from_byte_list(e["dst_id"]),
                    "direction": e["direction"],
                    "meta": _from_byte_list(e["meta"]),
                }
                for e in res["traversed_edges"]
            ],
            "frontier_nodes": [_from_byte_list(n) for n in res["frontier_nodes"]],
            "truncated": bool(res["truncated"]),
            "snapshot_seq": int(res["snapshot_seq"]),
        }

    # --- Content-Addressed Blobs ---

    def put_cas_blob(self, namespace: str, data: bytes, *, ack: str = "fast") -> str:
        ns_enc = _quote_segment(namespace)
        qs = urllib.parse.urlencode({"ack": ack})
        _status, raw = self._request_raw(
            "PUT",
            f"/v2/blobs/{ns_enc}?{qs}",
            body=data,
            content_type="application/octet-stream",
        )
        try:
            return str(json.loads(raw.decode("utf-8"))["sha256_hex"])
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError) as err:
            raise MembusError(500, "protocol", f"invalid blob response: {err}") from None

    def get_cas_blob(self, namespace: str, sha256_hex: str) -> bytes | None:
        ns_enc = _quote_segment(namespace)
        sha_enc = _quote_segment(sha256_hex)
        try:
            _status, raw = self._request_raw("GET", f"/v2/blobs/{ns_enc}/{sha_enc}")
            return raw
        except MembusError as err:
            if err.status == 404:
                return None
            raise

    def has_cas_blob(self, namespace: str, sha256_hex: str) -> bool:
        ns_enc = _quote_segment(namespace)
        sha_enc = _quote_segment(sha256_hex)
        try:
            status, _raw = self._request_raw("HEAD", f"/v2/blobs/{ns_enc}/{sha_enc}")
            return status == 200
        except MembusError as err:
            if err.status == 404:
                return False
            raise
