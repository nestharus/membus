"""Pure-stdlib Python 3.14+ client for the Membus v2 HTTP/REST + SSE surface.

Zero third-party dependencies: built strictly on `urllib`, `http.client`,
`json`, `base64`, and `dataclasses`.
"""

from __future__ import annotations

import base64
import http.client
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterator, Mapping, Sequence


class MembusError(RuntimeError):
    """Structured error returned by a Membus v2 HTTP endpoint or transport."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"[{status} {code}] {message}")
        self.status = status
        self.code = code
        self.message = message


@dataclass(frozen=True)
class RecordMeta:
    version: int
    flags: int
    ts_ms: int
    crc32: int


@dataclass(frozen=True)
class StoredRecord:
    key: bytes
    value: bytes
    meta: RecordMeta

    @property
    def key_text(self) -> str:
        return self.key.decode("utf-8")

    @property
    def value_text(self) -> str:
        return self.value.decode("utf-8")


@dataclass(frozen=True)
class CommitResult:
    seq: int
    epoch: int
    versions: tuple[int, ...]


@dataclass(frozen=True)
class LeaseGrant:
    namespace: str
    resource: str
    holder: str
    generation: int
    token: str
    expires_at_ms: int


@dataclass(frozen=True)
class LeaseInfo:
    """Public inspection view of an active lease (deliberately omits `token`)."""

    namespace: str
    resource: str
    holder: str
    generation: int
    expires_at_ms: int


@dataclass(frozen=True)
class SignalEvent:
    seq: int
    epoch: int
    namespace: str
    key: bytes
    version: int
    kind: str
    value_hint: bytes | None


@dataclass(frozen=True)
class SseMessage:
    event: str
    data: dict[str, Any]
    event_id: str | None = None


def _to_bytes(val: bytes | str) -> bytes:
    if isinstance(val, bytes):
        return val
    return val.encode("utf-8")


def _to_byte_list(val: bytes | str) -> list[int]:
    return list(_to_bytes(val))


def _from_byte_list(raw: Sequence[int]) -> bytes:
    return bytes(raw)


def _encode_expected(expected: str | int | Mapping[str, Any]) -> Any:
    if isinstance(expected, int):
        return {"Version": expected}
    return expected


def _quote_segment(seg: str) -> str:
    """Percent-encode a single path segment strictly (`safe=""`) to prevent injection/normalization."""
    return urllib.parse.quote(seg, safe="")


def _parse_http_error(status: int, raw: bytes, default_code: str = "http_error") -> MembusError:
    try:
        parsed = json.loads(raw.decode("utf-8"))
        if isinstance(parsed, dict):
            code = str(parsed.get("error", default_code))
            msg = str(parsed.get("message", raw.decode("utf-8", errors="replace")))
            return MembusError(status, code, msg)
    except Exception:
        pass
    return MembusError(status, default_code, raw.decode("utf-8", errors="replace"))


def _strip_sse_field_value(line: str, prefix: str) -> str:
    """Strip field prefix and at most ONE leading U+0020 space per the WHATWG SSE spec."""
    rest = line[len(prefix) :]
    if rest.startswith(" "):
        return rest[1:]
    return rest


class MembusClient:
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
    ) -> tuple[int, bytes]:
        url = f"{self.base_url}{path}"
        extra: dict[str, str] = {}
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
        )
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as err:
            raise MembusError(500, "protocol", f"invalid JSON response: {err}") from None

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
        res = self._request_json("PUT", f"/v2/kv/{ns_enc}/{key_enc}", body)
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
    ) -> CommitResult:
        ns_enc = _quote_segment(namespace)
        key_enc = _quote_segment(key)
        body: dict[str, Any] = {
            "ack": ack,
            "expected": _encode_expected(expected),
        }
        if search_op is not None:
            body["search_op"] = dict(search_op)
        res = self._request_json("DELETE", f"/v2/kv/{ns_enc}/{key_enc}", body)
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
        event_name = "message"
        event_id: str | None = None
        data_lines: list[str] = []

        while True:
            line_bytes = stream.readline()
            if not line_bytes:
                break
            line = line_bytes.decode("utf-8").rstrip("\r\n")
            if line == "":
                if data_lines:
                    raw_data = "\n".join(data_lines)
                    try:
                        data_obj = json.loads(raw_data)
                        if not isinstance(data_obj, dict):
                            data_obj = {"value": data_obj}
                    except json.JSONDecodeError:
                        data_obj = {"raw": raw_data}
                    yield SseMessage(
                        event=event_name,
                        data=data_obj,
                        event_id=event_id,
                    )
                event_name = "message"
                event_id = None
                data_lines = []
            elif line.startswith(":"):
                continue
            elif line.startswith("event:"):
                event_name = _strip_sse_field_value(line, "event:")
            elif line.startswith("id:"):
                event_id = _strip_sse_field_value(line, "id:")
            elif line.startswith("data:"):
                data_lines.append(_strip_sse_field_value(line, "data:"))

    @staticmethod
    def parse_signal_event(data: Mapping[str, Any]) -> SignalEvent:
        hint_raw = data.get("value_hint")
        return SignalEvent(
            seq=int(data["seq"]),
            epoch=int(data["epoch"]),
            namespace=str(data["namespace"]),
            key=_from_byte_list(data["key"]),
            version=int(data["version"]),
            kind=str(data["kind"]),
            value_hint=_from_byte_list(hint_raw) if hint_raw is not None else None,
        )
