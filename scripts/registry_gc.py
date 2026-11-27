#!/usr/bin/env python3
"""Dual-slot anti-spam OCI registry garbage collector for Membus v2 A/B deployments.

Retains:
  1. Every active Slot A image digest pinned in Flux Kustomize manifests
  2. Every active Slot B image digest pinned in Flux Kustomize manifests
  3. Every active `ab-router` image digest pinned in Flux Kustomize manifests
  4. Last-known-good rollback digest(s) (preventing rollback-window image reaping)
  5. The most recent `N` tagged release digests
  6. Any manifest younger than `min_age_hours` (protecting in-flight builds)

Defaults to `--dry-run` (deletes NOTHING unless `--execute` is explicitly provided),
supports real OCI Distribution v2 / GHCR manifest listing (with RFC 8288 `Link` pagination)
and manifest `DELETE`, and fails closed on missing dual-slot+router pins, malformed digests,
symlinks/non-regular input files, or `--execute` without a live registry target.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import math
import os
import pathlib
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Iterable, Sequence

DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
OCI_REPO_RE = re.compile(r"^[a-z0-9]+([._/-][a-z0-9]+)*$")
INLINE_IMAGE_DIGEST_RE = re.compile(r"image:\s*['\"]?(\S+?)@([^\s'\"#]+)['\"]?")
LINK_NEXT_RE = re.compile(r'<([^>]+)>\s*;\s*rel="next"')
RELEASE_TAG_RE = re.compile(r"^v?\d+\.\d+\.\d+([+-][0-9A-Za-z.-]+)?$")
DEFAULT_MAX_PAGINATION_PAGES = 500

OCI_ACCEPT_MANIFESTS = ", ".join(
    [
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    ]
)


@dataclasses.dataclass(frozen=True)
class RegistryArtifact:
    digest: str
    tags: tuple[str, ...]
    pushed_at: dt.datetime

    @staticmethod
    def from_dict(raw: dict[str, object]) -> "RegistryArtifact":
        digest = str(raw["digest"]).strip()
        if not DIGEST_RE.match(digest):
            raise ValueError(f"invalid artifact digest: {digest!r}")
        raw_tags = raw.get("tags", [])
        if not isinstance(raw_tags, list):
            raise ValueError(f"artifact {digest} tags must be a list")
        tags = tuple(str(t).strip() for t in raw_tags if str(t).strip())
        pushed_raw = str(raw["pushed_at"]).strip()
        if pushed_raw.endswith("Z"):
            pushed_raw = pushed_raw[:-1] + "+00:00"
        pushed_at = dt.datetime.fromisoformat(pushed_raw)
        if pushed_at.tzinfo is None:
            pushed_at = pushed_at.replace(tzinfo=dt.timezone.utc)
        return RegistryArtifact(digest=digest, tags=tags, pushed_at=pushed_at)


@dataclasses.dataclass(frozen=True)
class PinnedDigests:
    slot_a: frozenset[str]
    slot_b: frozenset[str]
    ab_router: frozenset[str]
    rollback: frozenset[str]
    extra: frozenset[str]

    def reasons_for(self, digest: str) -> list[str]:
        reasons: list[str] = []
        if digest in self.slot_a:
            reasons.append("slot_a_pinned")
        if digest in self.slot_b:
            reasons.append("slot_b_pinned")
        if digest in self.ab_router:
            reasons.append("ab_router_pinned")
        if digest in self.rollback:
            reasons.append("rollback_pinned")
        if digest in self.extra:
            reasons.append("explicit_pin")
        return reasons


@dataclasses.dataclass(frozen=True)
class GcDecision:
    digest: str
    action: str  # "keep" or "delete"
    reasons: tuple[str, ...]
    tags: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class GcPlan:
    dry_run: bool
    decisions: tuple[GcDecision, ...]
    deleted_digests: tuple[str, ...]

    @property
    def kept(self) -> list[GcDecision]:
        return [d for d in self.decisions if d.action == "keep"]

    @property
    def candidates(self) -> list[GcDecision]:
        return [d for d in self.decisions if d.action == "delete"]

    def to_dict(self) -> dict[str, object]:
        return {
            "dry_run": self.dry_run,
            "kept_count": len(self.kept),
            "delete_candidate_count": len(self.candidates),
            "deleted_count": len(self.deleted_digests),
            "deleted_digests": list(self.deleted_digests),
            "decisions": [
                {
                    "digest": d.digest,
                    "action": d.action,
                    "reasons": list(d.reasons),
                    "tags": list(d.tags),
                }
                for d in self.decisions
            ],
        }


def _strip_yaml_comment(value: str) -> str:
    """Strip unquoted trailing `# ...` comments and surrounding quotes from a YAML scalar."""
    s = value.strip()
    if not s:
        return ""
    if s[0] in ("'", '"'):
        quote = s[0]
        end = s.find(quote, 1)
        if end == -1:
            raise ValueError(f"unterminated quoted YAML scalar: {value!r}")
        trailing = s[end + 1 :].strip()
        if trailing and not trailing.startswith("#"):
            raise ValueError(f"unexpected trailing characters after quoted scalar: {value!r}")
        return s[1:end].strip()
    hash_idx = s.find("#")
    if hash_idx != -1:
        s = s[:hash_idx].strip()
    return s


def _parse_yaml_kv(text: str) -> tuple[str, str] | None:
    if ":" not in text:
        return None
    key, raw_val = text.split(":", 1)
    key = key.strip()
    if not key or " " in key:
        return None
    return key, _strip_yaml_comment(raw_val)


def extract_kustomize_pins(text: str) -> dict[str, set[str]]:
    """Structurally parse Kustomize `images:` entries (arbitrary key order, quotes, comments)
    and inline `image: ...@sha256:...` references, failing closed on malformed digests."""
    pins: dict[str, set[str]] = {
        "slot_a": set(),
        "slot_b": set(),
        "ab_router": set(),
        "other": set(),
    }

    in_images = False
    images_indent = 0
    current_entry: dict[str, str] | None = None
    entry_indent = 0

    def flush_entry() -> None:
        nonlocal current_entry
        if current_entry is None:
            return
        digest = current_entry.get("digest")
        name = current_entry.get("name", "")
        if digest is not None:
            if not DIGEST_RE.match(digest):
                raise ValueError(f"malformed digest {digest!r} for image {name!r}")
            bucket = _classify_image_name(name)
            pins[bucket].add(digest)
        current_entry = None

    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))

        # Check inline `image: <ref>@<digest>` anywhere in manifest YAML.
        if not in_images:
            inline_match = INLINE_IMAGE_DIGEST_RE.search(stripped)
            if inline_match:
                img_ref, dig = inline_match.group(1), inline_match.group(2)
                if not DIGEST_RE.match(dig):
                    raise ValueError(f"malformed inline image digest {dig!r} for {img_ref!r}")
                pins[_classify_image_name(img_ref)].add(dig)

        if stripped.startswith("images:") and _strip_yaml_comment(stripped.split(":", 1)[1]) == "":
            flush_entry()
            in_images = True
            images_indent = indent
            continue

        if in_images:
            if indent <= images_indent and not stripped.startswith("-"):
                flush_entry()
                in_images = False
                continue

            if stripped.startswith("-"):
                flush_entry()
                current_entry = {}
                entry_indent = indent
                after_dash = stripped[1:].strip()
                if after_dash:
                    kv = _parse_yaml_kv(after_dash)
                    if kv is not None:
                        current_entry[kv[0]] = kv[1]
                continue

            if current_entry is not None and indent > entry_indent:
                kv = _parse_yaml_kv(stripped)
                if kv is not None:
                    current_entry[kv[0]] = kv[1]

    flush_entry()
    return pins


def _classify_image_name(name: str) -> str:
    lower = name.lower()
    if "slot-a" in lower or "slot_a" in lower:
        return "slot_a"
    if "slot-b" in lower or "slot_b" in lower:
        return "slot_b"
    if "ab-router" in lower or "ab_router" in lower:
        return "ab_router"
    return "other"


def _ensure_regular_non_symlink(path: pathlib.Path, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be an existing regular non-symlink file: {path}")


def load_pins(
    kustomization_paths: Sequence[pathlib.Path],
    rollback_digest_file: pathlib.Path | None,
    explicit_pins: Sequence[str],
    require_dual_slot_pins: bool,
) -> PinnedDigests:
    slot_a: set[str] = set()
    slot_b: set[str] = set()
    ab_router: set[str] = set()
    rollback: set[str] = set()
    extra: set[str] = set()

    for path in kustomization_paths:
        _ensure_regular_non_symlink(path, "kustomization file")
        raw = path.read_text(encoding="utf-8")
        parsed = extract_kustomize_pins(raw)
        slot_a.update(parsed["slot_a"])
        slot_b.update(parsed["slot_b"])
        ab_router.update(parsed["ab_router"])
        extra.update(parsed["other"])

    if rollback_digest_file is not None:
        _ensure_regular_non_symlink(rollback_digest_file, "rollback digest file")
        loaded_any = False
        for line in rollback_digest_file.read_text(encoding="utf-8").splitlines():
            trimmed = _strip_yaml_comment(line)
            if not trimmed:
                continue
            if not DIGEST_RE.match(trimmed):
                raise ValueError(
                    f"invalid rollback digest {trimmed!r} in {rollback_digest_file}"
                )
            rollback.add(trimmed)
            loaded_any = True
        if not loaded_any:
            raise ValueError(
                f"rollback digest file {rollback_digest_file} contains no digests (empty or comments only)"
            )

    for pin in explicit_pins:
        trimmed = pin.strip()
        if not trimmed:
            continue
        if not DIGEST_RE.match(trimmed):
            raise ValueError(f"invalid --pin-digest value: {trimmed!r}")
        extra.add(trimmed)

    if require_dual_slot_pins:
        missing: list[str] = []
        if not slot_a:
            missing.append("slot-a")
        if not slot_b:
            missing.append("slot-b")
        if not ab_router:
            missing.append("ab-router")
        if missing:
            raise ValueError(
                "fail-closed abort: missing required immutable @sha256: digest pins for "
                + ", ".join(missing)
            )

    return PinnedDigests(
        slot_a=frozenset(slot_a),
        slot_b=frozenset(slot_b),
        ab_router=frozenset(ab_router),
        rollback=frozenset(rollback),
        extra=frozenset(extra),
    )


def deduplicate_artifacts(artifacts: Sequence[RegistryArtifact]) -> list[RegistryArtifact]:
    """Deduplicate inventory entries by digest, merging tags and preserving newest pushed_at."""
    merged: dict[str, RegistryArtifact] = {}
    for art in artifacts:
        existing = merged.get(art.digest)
        if existing is None:
            merged[art.digest] = art
        else:
            combined_tags = tuple(dict.fromkeys((*existing.tags, *art.tags)))
            newest_ts = max(existing.pushed_at, art.pushed_at)
            merged[art.digest] = RegistryArtifact(
                digest=art.digest,
                tags=combined_tags,
                pushed_at=newest_ts,
            )
    return list(merged.values())


def plan_registry_gc(
    artifacts: Sequence[RegistryArtifact],
    pins: PinnedDigests,
    retain_releases: int = 5,
    min_age_hours: float = 24.0,
    now: dt.datetime | None = None,
    dry_run: bool = True,
    delete_fn: Callable[[str], None] | None = None,
) -> GcPlan:
    """Compute and optionally execute dual-slot safe registry garbage collection."""
    if retain_releases < 0:
        raise ValueError("retain_releases must be >= 0")
    if not math.isfinite(min_age_hours) or min_age_hours < 0:
        raise ValueError("min_age_hours must be a finite number >= 0")

    current_time = now if now is not None else dt.datetime.now(tz=dt.timezone.utc)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=dt.timezone.utc)

    deduped = deduplicate_artifacts(artifacts)

    release_artifacts = [
        a for a in deduped if any(RELEASE_TAG_RE.match(tag) for tag in a.tags)
    ]
    release_artifacts.sort(key=lambda a: (a.pushed_at, a.digest), reverse=True)
    recent_release_digests = {a.digest for a in release_artifacts[:retain_releases]}

    min_age_delta = dt.timedelta(hours=min_age_hours)
    decisions: list[GcDecision] = []

    for art in sorted(deduped, key=lambda a: (a.pushed_at, a.digest), reverse=True):
        reasons = pins.reasons_for(art.digest)
        if art.digest in recent_release_digests:
            reasons.append("recent_release")
        if current_time - art.pushed_at < min_age_delta:
            reasons.append("within_min_age")

        if reasons:
            decisions.append(
                GcDecision(
                    digest=art.digest,
                    action="keep",
                    reasons=tuple(reasons),
                    tags=art.tags,
                )
            )
        else:
            decisions.append(
                GcDecision(
                    digest=art.digest,
                    action="delete",
                    reasons=("unreferenced_expired",),
                    tags=art.tags,
                )
            )

    deleted: list[str] = []
    if not dry_run:
        if delete_fn is None:
            raise ValueError("--execute requires a live registry deletion backend (--registry-url and --repository)")
        for decision in decisions:
            if decision.action == "delete":
                delete_fn(decision.digest)
                deleted.append(decision.digest)

    return GcPlan(
        dry_run=dry_run,
        decisions=tuple(decisions),
        deleted_digests=tuple(deleted),
    )


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Fail-closed redirect handler preventing cross-origin Authorization leakage or downgrades."""

    def redirect_request(  # type: ignore[override]
        self,
        req: urllib.request.Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        raise urllib.error.HTTPError(
            req.full_url,
            code,
            f"refusing automatic redirect to {newurl!r}",
            headers,  # type: ignore[arg-type]
            None,
        )


def _is_loopback_host(hostname: str | None) -> bool:
    if not hostname:
        return False
    lower = hostname.lower()
    if lower in ("localhost", "127.0.0.1", "::1"):
        return True
    return lower.startswith("127.")


class OciRegistryClient:
    """Stdlib OCI Distribution v2 client supporting same-origin `Link` pagination and DELETE."""

    def __init__(
        self,
        registry_url: str,
        repository: str,
        bearer_token: str | None = None,
        timeout_secs: float = 30.0,
        max_pages: int = DEFAULT_MAX_PAGINATION_PAGES,
    ) -> None:
        parsed = urllib.parse.urlparse(registry_url.strip())
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError(f"invalid --registry-url: {registry_url!r}")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("--registry-url must not embed userinfo credentials")
        if parsed.query or parsed.fragment:
            raise ValueError("--registry-url must not contain query or fragment components")
        if parsed.path not in ("", "/"):
            raise ValueError("--registry-url must not contain a base path")

        tok = bearer_token.strip() if bearer_token else None
        if parsed.scheme == "http" and tok and not _is_loopback_host(parsed.hostname):
            raise ValueError(
                f"refusing to send bearer token over plaintext http:// to non-loopback host {parsed.hostname!r}"
            )

        repo = repository.strip()
        if (
            not repo
            or ".." in repo
            or "//" in repo
            or any(ch in repo for ch in ("\r", "\n", "?", "#"))
            or not OCI_REPO_RE.match(repo)
        ):
            raise ValueError(
                f"invalid OCI --repository {repository!r} (expected lowercase OCI path like 'org/repo')"
            )
        if max_pages < 1:
            raise ValueError("max_pages must be >= 1")

        self.scheme = parsed.scheme
        self.netloc = parsed.netloc
        self.base_url = f"{parsed.scheme}://{parsed.netloc}"
        self.repository = repo
        self.bearer_token = tok
        self.timeout_secs = timeout_secs
        self.max_pages = max_pages
        self._opener = urllib.request.build_opener(_NoRedirectHandler())

    def _headers(self, accept: str | None = None) -> dict[str, str]:
        hdrs: dict[str, str] = {"User-Agent": "membus-registry-gc/2.0"}
        if accept:
            hdrs["Accept"] = accept
        if self.bearer_token:
            hdrs["Authorization"] = f"Bearer {self.bearer_token}"
        return hdrs

    def _validate_same_origin_v2_url(self, candidate_url: str) -> str:
        resolved = urllib.parse.urljoin(self.base_url, candidate_url)
        parsed = urllib.parse.urlparse(resolved)
        if parsed.scheme != self.scheme or parsed.netloc != self.netloc:
            raise RuntimeError(
                f"refusing cross-origin or scheme-changing Link URL {candidate_url!r} "
                f"(expected {self.scheme}://{self.netloc})"
            )
        if not parsed.path.startswith("/v2/"):
            raise RuntimeError(
                f"refusing Link URL outside /v2/ API prefix: {candidate_url!r}"
            )
        return resolved

    def _request(
        self,
        method: str,
        url: str,
        accept: str | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        req = urllib.request.Request(url, method=method, headers=self._headers(accept))
        try:
            with self._opener.open(req, timeout=self.timeout_secs) as resp:
                headers = {k.lower(): v for k, v in resp.headers.items()}
                return resp.status, headers, resp.read()
        except urllib.error.HTTPError as err:
            body = err.read().decode("utf-8", errors="replace").strip() if err.fp else err.reason
            raise RuntimeError(
                f"OCI registry {method} {url} returned HTTP {err.code}: {body}"
            ) from err

    def list_tags(self) -> list[str]:
        tags: list[str] = []
        next_url: str | None = f"{self.base_url}/v2/{self.repository}/tags/list?n=100"
        seen_urls: set[str] = set()
        pages = 0
        while next_url and next_url not in seen_urls:
            pages += 1
            if pages > self.max_pages:
                raise RuntimeError(
                    f"OCI tag pagination exceeded safety cap of {self.max_pages} pages"
                )
            seen_urls.add(next_url)
            _, headers, body = self._request("GET", next_url)
            payload = json.loads(body.decode("utf-8"))
            page_tags = payload.get("tags") or []
            tags.extend(str(t) for t in page_tags if t)
            link_hdr = headers.get("link", "")
            m = LINK_NEXT_RE.search(link_hdr)
            if m:
                next_url = self._validate_same_origin_v2_url(m.group(1))
            else:
                next_url = None
        return tags

    def list_artifacts(self) -> list[RegistryArtifact]:
        by_digest: dict[str, tuple[list[str], dt.datetime]] = {}
        for tag in self.list_tags():
            encoded_tag = urllib.parse.quote(tag, safe="")
            url = f"{self.base_url}/v2/{self.repository}/manifests/{encoded_tag}"
            _, headers, _ = self._request("HEAD", url, accept=OCI_ACCEPT_MANIFESTS)
            digest = headers.get("docker-content-digest", "").strip()
            if not DIGEST_RE.match(digest):
                raise RuntimeError(
                    f"registry did not return valid Docker-Content-Digest for tag {tag!r}: {digest!r}"
                )
            pushed_at = _parse_http_timestamp(headers.get("last-modified"))
            if digest not in by_digest:
                by_digest[digest] = ([tag], pushed_at)
            else:
                existing_tags, existing_ts = by_digest[digest]
                existing_tags.append(tag)
                by_digest[digest] = (existing_tags, max(existing_ts, pushed_at))

        return [
            RegistryArtifact(digest=dig, tags=tuple(tags), pushed_at=ts)
            for dig, (tags, ts) in by_digest.items()
        ]

    def delete_manifest(self, digest: str) -> None:
        if not DIGEST_RE.match(digest):
            raise ValueError(f"refusing to delete invalid digest: {digest!r}")
        url = f"{self.base_url}/v2/{self.repository}/manifests/{digest}"
        status, _, _ = self._request("DELETE", url, accept=OCI_ACCEPT_MANIFESTS)
        if status not in (200, 202, 204):
            raise RuntimeError(f"unexpected HTTP {status} deleting {digest}")


def _parse_http_timestamp(raw: str | None) -> dt.datetime:
    if raw:
        from email.utils import parsedate_to_datetime

        try:
            parsed = parsedate_to_datetime(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed
        except (TypeError, ValueError):
            pass
    # Conservative fallback: treat unknown timestamp as now so we fail safe via min_age_hours.
    return dt.datetime.now(tz=dt.timezone.utc)


def load_inventory_json(path: pathlib.Path) -> list[RegistryArtifact]:
    _ensure_regular_non_symlink(path, "inventory JSON file")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("inventory JSON must be a list of artifact objects")
    return [RegistryArtifact.from_dict(item) for item in raw]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Dual-slot anti-spam OCI registry GC retaining pinned Slot A, Slot B, "
            "ab-router, rollback, and recent release digests (dry-run by default)."
        )
    )
    parser.add_argument(
        "--kustomization",
        action="append",
        default=[],
        type=pathlib.Path,
        help="Path to a Flux Kustomize file containing immutable @sha256: image pins.",
    )
    parser.add_argument(
        "--rollback-digest-file",
        type=pathlib.Path,
        default=None,
        help="Optional newline-delimited file of last-known-good rollback sha256 digests.",
    )
    parser.add_argument(
        "--pin-digest",
        action="append",
        default=[],
        help="Additional explicit sha256:<64-hex> digest to retain.",
    )
    parser.add_argument(
        "--inventory-json",
        type=pathlib.Path,
        default=None,
        help="JSON file listing registry artifacts [{digest, tags, pushed_at}] (dry-run planning).",
    )
    parser.add_argument(
        "--registry-url",
        default=None,
        help="Live OCI Distribution v2 registry base URL (e.g. https://ghcr.io).",
    )
    parser.add_argument(
        "--repository",
        default=None,
        help="OCI repository name (e.g. oulipoly/membus), required with --registry-url.",
    )
    parser.add_argument(
        "--token-env",
        default="OCI_REGISTRY_TOKEN",
        help="Environment variable holding the OCI registry bearer token (default: OCI_REGISTRY_TOKEN).",
    )
    parser.add_argument(
        "--retain-releases",
        type=int,
        default=5,
        help="Number of most recent semver-tagged releases to retain (default: 5).",
    )
    parser.add_argument(
        "--min-age-hours",
        type=float,
        default=24.0,
        help="Minimum age in hours before an unreferenced digest becomes eligible for GC.",
    )
    parser.add_argument(
        "--require-dual-slot-pins",
        action="store_true",
        help="Fail closed if Slot A, Slot B, or ab-router immutable digest pins are missing.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete eligible unreferenced digests via live OCI DELETE (default is dry-run).",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    try:
        if args.inventory_json is None and args.registry_url is None:
            raise ValueError(
                "must provide either --inventory-json <FILE> or --registry-url <URL> --repository <REPO>"
            )
        if args.registry_url is not None and not args.repository:
            raise ValueError("--registry-url requires --repository <REPO>")

        dry_run = not args.execute
        if not dry_run and args.registry_url is None:
            raise ValueError(
                "--execute requires a live OCI registry target (--registry-url and --repository); "
                "refusing to simulate deletion against --inventory-json"
            )

        pins = load_pins(
            kustomization_paths=args.kustomization,
            rollback_digest_file=args.rollback_digest_file,
            explicit_pins=args.pin_digest,
            require_dual_slot_pins=args.require_dual_slot_pins,
        )

        client: OciRegistryClient | None = None
        if args.registry_url is not None:
            token = os.environ.get(args.token_env)
            client = OciRegistryClient(args.registry_url, args.repository, bearer_token=token)

        if args.inventory_json is not None:
            artifacts = load_inventory_json(args.inventory_json)
        else:
            assert client is not None
            artifacts = client.list_artifacts()

        plan = plan_registry_gc(
            artifacts=artifacts,
            pins=pins,
            retain_releases=args.retain_releases,
            min_age_hours=args.min_age_hours,
            dry_run=dry_run,
            delete_fn=client.delete_manifest if (not dry_run and client is not None) else None,
        )
    except Exception as exc:
        print(f"registry_gc error: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(plan.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
