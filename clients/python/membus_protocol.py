"""Shared dataclasses, JSON/URL codecs, and SSE framing helpers for Membus v2 Python client."""

from __future__ import annotations

import json
import urllib.parse
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


def parse_sse_lines(stream: Any) -> Iterator[SseMessage]:
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
