#!/usr/bin/env python3
"""Update a single slot's immutable `@sha256:` digest pin in a Flux Kustomize overlay.

Validates the digest format (`^sha256:[0-9a-f]{64}$`), target slot (`slot-a|slot-b|ab-router`),
and regular non-symlink target path, parses `images:` list items structurally by item
boundaries regardless of key order (`name`, `newName`, `digest`), preserves file permissions,
and atomically rewrites only the matching slot's digest pin.
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import stat
import sys
import tempfile
from typing import Iterable

DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
ITEM_START_RE = re.compile(r"^(\s*)-\s+(.*)$")
SLOT_IMAGE_NAMES = {
    "slot-a": "ghcr.io/oulipoly/membus-slot-a",
    "slot-b": "ghcr.io/oulipoly/membus-slot-b",
    "ab-router": "ghcr.io/oulipoly/membus-ab-router",
}


def _strip_scalar(raw: str) -> str:
    s = raw.strip()
    if not s:
        return ""
    if s[0] in ("'", '"'):
        q = s[0]
        end = s.find(q, 1)
        if end == -1:
            raise ValueError(f"unterminated quoted YAML scalar: {raw!r}")
        return s[1:end].strip()
    comment = s.find("#")
    if comment != -1:
        s = s[:comment].strip()
    return s


def _extract_kv(text: str) -> tuple[str, str] | None:
    if ":" not in text:
        return None
    k, v = text.split(":", 1)
    k = k.strip()
    if not k or " " in k:
        return None
    return k, _strip_scalar(v)


def _check_no_symlink_components(path: pathlib.Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"kustomization path must be an existing regular non-symlink file: {path}")
    # Check path components relative to parent directory if given as relative path, and immediate parent.
    if path.parent.is_symlink():
        raise ValueError(f"kustomization parent directory must not be a symlink: {path.parent}")
    if not path.is_absolute():
        cur = pathlib.Path(".")
        for part in path.parts:
            cur = cur / part
            if cur.is_symlink():
                raise ValueError(f"symlink component not permitted in kustomization path: {cur}")


def _rewrite_images_block(
    block_lines: list[str],
    target_image: str,
    digest: str,
) -> tuple[list[str], bool]:
    """Parse `images:` items strictly by `^(\\s*)-\\s+` item boundaries and update target item."""
    prefix_lines: list[str] = []
    items: list[list[str]] = []
    item_indent: int | None = None

    for line in block_lines:
        m = ITEM_START_RE.match(line)
        if m and (item_indent is None or len(m.group(1)) == item_indent):
            item_indent = len(m.group(1))
            items.append([line])
        elif items:
            items[-1].append(line)
        else:
            prefix_lines.append(line)

    updated = False
    rebuilt: list[str] = list(prefix_lines)

    for item_lines in items:
        item_name: str | None = None
        first_match = ITEM_START_RE.match(item_lines[0])
        assert first_match is not None
        dash_indent = first_match.group(1)
        field_indent = dash_indent + "  "

        digest_line_idx: int | None = None
        digest_on_dash_line = False

        for i, ln in enumerate(item_lines):
            stripped = ln.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if i == 0:
                kv = _extract_kv(first_match.group(2))
                if kv is not None:
                    if kv[0] == "name":
                        item_name = kv[1]
                    elif kv[0] == "digest":
                        digest_line_idx = 0
                        digest_on_dash_line = True
            else:
                kv = _extract_kv(stripped)
                if kv is not None:
                    if kv[0] == "name":
                        item_name = kv[1]
                    elif kv[0] == "digest":
                        digest_line_idx = i
                leading = ln[: len(ln) - len(ln.lstrip(" "))]
                if len(leading) > len(dash_indent):
                    field_indent = leading

        if item_name != target_image:
            rebuilt.extend(item_lines)
            continue

        new_item = list(item_lines)
        if digest_line_idx is not None:
            if digest_on_dash_line:
                new_item[0] = f"{dash_indent}- digest: {digest}\n"
            else:
                existing_indent = new_item[digest_line_idx][
                    : len(new_item[digest_line_idx]) - len(new_item[digest_line_idx].lstrip(" "))
                ]
                new_item[digest_line_idx] = f"{existing_indent}digest: {digest}\n"
        else:
            if new_item and not new_item[-1].endswith("\n"):
                new_item[-1] = new_item[-1] + "\n"
            new_item.append(f"{field_indent}digest: {digest}\n")

        rebuilt.extend(new_item)
        updated = True

    return rebuilt, updated


def update_overlay_digest(kustomization_path: pathlib.Path, slot: str, digest: str) -> None:
    if slot not in SLOT_IMAGE_NAMES:
        raise ValueError(
            f"invalid slot {slot!r} (expected one of {', '.join(sorted(SLOT_IMAGE_NAMES))})"
        )
    if not DIGEST_RE.match(digest):
        raise ValueError(
            f"invalid immutable digest {digest!r} (expected sha256:<64 lowercase hex>)"
        )
    _check_no_symlink_components(kustomization_path)

    orig_stat = kustomization_path.stat()
    target_image = SLOT_IMAGE_NAMES[slot]
    lines = kustomization_path.read_text(encoding="utf-8").splitlines(keepends=True)

    # Locate the top-level `images:` section boundaries.
    images_start: int | None = None
    images_end = len(lines)
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not line.startswith((" ", "\t")):
            if images_start is None and stripped.startswith("images:"):
                images_start = idx
            elif images_start is not None:
                images_end = idx
                break

    if images_start is None:
        raise ValueError(f"no 'images:' section found in {kustomization_path}")

    header_line = lines[images_start]
    block_lines = lines[images_start + 1 : images_end]
    rewritten_block, updated = _rewrite_images_block(block_lines, target_image, digest)
    if not updated:
        raise ValueError(
            f"target image {target_image!r} for slot {slot!r} not found in {kustomization_path}"
        )

    out = lines[:images_start] + [header_line] + rewritten_block + lines[images_end:]

    fd, tmp_name = tempfile.mkstemp(
        prefix=".kustomize_pin_",
        dir=str(kustomization_path.parent),
    )
    tmp_path = pathlib.Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.writelines(out)
        os.chmod(tmp_path, stat.S_IMODE(orig_stat.st_mode))
        os.replace(tmp_path, kustomization_path)
    except Exception:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Atomically update an immutable @sha256: digest pin in a Flux Kustomize overlay."
    )
    parser.add_argument("--kustomization", required=True, type=pathlib.Path)
    parser.add_argument("--slot", required=True, choices=sorted(SLOT_IMAGE_NAMES))
    parser.add_argument("--digest", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        update_overlay_digest(args.kustomization, args.slot, args.digest.strip())
    except Exception as exc:
        print(f"update_flux_digest error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
