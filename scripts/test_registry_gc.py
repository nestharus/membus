"""Unit and mock-OCI integration tests for `scripts/registry_gc.py` and `scripts/update_flux_digest.py`."""

from __future__ import annotations

import datetime as dt
import http.server
import json
import math
import pathlib
import sys
import tempfile
import threading
import unittest

_SCRIPTS_DIR = str(pathlib.Path(__file__).resolve().parent)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)

import registry_gc  # noqa: E402
import update_flux_digest  # noqa: E402


def _digest(char: str) -> str:
    return f"sha256:{char * 64}"


class RegistryGcTests(unittest.TestCase):
    def test_structural_kustomize_parser_quotes_comments_key_order_and_malformed_fail_closed(self) -> None:
        # Digest BEFORE name, quoted strings, and trailing comments.
        kustomize = f"""
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
images:
  - digest: "{_digest('a')}"  # slot A pin
    name: 'ghcr.io/oulipoly/membus-slot-a'
  - name: "ghcr.io/oulipoly/membus-slot-b" # slot B
    digest: '{_digest('b')}'
  - digest: {_digest('c')}
    name: ghcr.io/oulipoly/membus-ab-router # router
"""
        pins = registry_gc.extract_kustomize_pins(kustomize)
        self.assertEqual(pins["slot_a"], {_digest("a")})
        self.assertEqual(pins["slot_b"], {_digest("b")})
        self.assertEqual(pins["ab_router"], {_digest("c")})

        # Malformed digest MUST fail closed!
        bad_kustomize = """
images:
  - name: ghcr.io/oulipoly/membus-slot-a
    digest: sha256:not-a-valid-64-hex-digest
"""
        with self.assertRaises(ValueError):
            registry_gc.extract_kustomize_pins(bad_kustomize)

    def test_retains_all_pinned_slots_router_rollback_recent_releases_and_deduplicates(self) -> None:
        now = dt.datetime(2026, 11, 27, 12, 0, tzinfo=dt.timezone.utc)
        old = now - dt.timedelta(days=10)
        fresh = now - dt.timedelta(hours=2)

        pins = registry_gc.PinnedDigests(
            slot_a=frozenset({_digest("1")}),
            slot_b=frozenset({_digest("2")}),
            ab_router=frozenset({_digest("3")}),
            rollback=frozenset({_digest("4")}),
            extra=frozenset(),
        )
        artifacts = [
            registry_gc.RegistryArtifact(_digest("1"), ("v2.0.1",), old),
            # Duplicate digest entry with extra tag should deduplicate cleanly!
            registry_gc.RegistryArtifact(_digest("1"), ("content-1111",), old + dt.timedelta(hours=1)),
            registry_gc.RegistryArtifact(_digest("2"), ("v2.0.2",), old),
            registry_gc.RegistryArtifact(_digest("3"), ("router-v2",), old),
            registry_gc.RegistryArtifact(_digest("4"), ("v2.0.0",), old - dt.timedelta(days=20)),
            registry_gc.RegistryArtifact(_digest("5"), ("v2.1.0",), old + dt.timedelta(days=5)),
            registry_gc.RegistryArtifact(_digest("6"), ("v2.0.9",), old + dt.timedelta(days=4)),
            registry_gc.RegistryArtifact(_digest("7"), ("v1.9.0",), old - dt.timedelta(days=30)),
            registry_gc.RegistryArtifact(_digest("8"), ("pr-442-ephemeral",), old),
            registry_gc.RegistryArtifact(_digest("9"), ("pr-443-inflight",), fresh),
        ]

        dry_plan = registry_gc.plan_registry_gc(
            artifacts=artifacts,
            pins=pins,
            retain_releases=2,
            min_age_hours=24.0,
            now=now,
            dry_run=True,
        )
        self.assertTrue(dry_plan.dry_run)
        self.assertEqual(dry_plan.deleted_digests, ())
        # Deduplicated: 9 unique digests instead of 10.
        self.assertEqual(len(dry_plan.decisions), 9)
        self.assertEqual({d.digest for d in dry_plan.candidates}, {_digest("7"), _digest("8")})

        # Non-finite min_age_hours fails closed.
        with self.assertRaises(ValueError):
            registry_gc.plan_registry_gc(artifacts, pins, min_age_hours=math.nan)
        with self.assertRaises(ValueError):
            registry_gc.plan_registry_gc(artifacts, pins, min_age_hours=math.inf)

    def test_fail_closed_on_missing_router_pin_empty_rollback_or_execute_without_registry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            # Missing ab-router pin fails under --require-dual-slot-pins.
            kust = root / "kustomization.yaml"
            kust.write_text(
                f"""
images:
  - name: ghcr.io/oulipoly/membus-slot-a
    digest: {_digest("a")}
  - name: ghcr.io/oulipoly/membus-slot-b
    digest: {_digest("b")}
""",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError) as ctx:
                registry_gc.load_pins([kust], None, [], require_dual_slot_pins=True)
            self.assertIn("ab-router", str(ctx.exception))

            # Empty/comment-only rollback file fails closed.
            empty_rb = root / "rollback.txt"
            empty_rb.write_text("# only comments\n   \n", encoding="utf-8")
            with self.assertRaises(ValueError) as ctx_rb:
                registry_gc.load_pins([], empty_rb, [], require_dual_slot_pins=False)
            self.assertIn("no digests", str(ctx_rb.exception))

            # CLI fails closed when neither --inventory-json nor --registry-url is given.
            self.assertEqual(registry_gc.main([]), 1)

            # CLI fails closed when --execute is used against --inventory-json (no live registry!).
            inv = root / "inv.json"
            inv.write_text("[]", encoding="utf-8")
            self.assertEqual(
                registry_gc.main(["--inventory-json", str(inv), "--execute"]),
                1,
            )

    def test_live_oci_client_link_pagination_and_real_delete(self) -> None:
        dig_a = _digest("a")
        dig_b = _digest("b")
        dig_r = _digest("c")
        dig_stale = _digest("e")

        tag_map = {
            "v2.0.0": (dig_a, "Mon, 01 Sep 2026 00:00:00 GMT"),
            "v2.0.1": (dig_b, "Tue, 02 Sep 2026 00:00:00 GMT"),
            "router-v2": (dig_r, "Wed, 03 Sep 2026 00:00:00 GMT"),
            "pr-stale": (dig_stale, "Thu, 04 Sep 2026 00:00:00 GMT"),
        }
        deleted: list[str] = []

        class MockOciHandler(http.server.BaseHTTPRequestHandler):
            def log_message(self, _fmt: str, *_args: object) -> None:
                pass

            def do_GET(self) -> None:  # noqa: N802
                if self.path.startswith("/v2/oulipoly/membus/tags/list"):
                    if "page=2" in self.path:
                        body = json.dumps({"tags": ["router-v2", "pr-stale"]}).encode()
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(body)
                    else:
                        body = json.dumps({"tags": ["v2.0.0", "v2.0.1"]}).encode()
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header(
                            "Link",
                            '</v2/oulipoly/membus/tags/list?page=2>; rel="next"',
                        )
                        self.end_headers()
                        self.wfile.write(body)
                    return
                self.send_response(404)
                self.end_headers()

            def do_HEAD(self) -> None:  # noqa: N802
                prefix = "/v2/oulipoly/membus/manifests/"
                if self.path.startswith(prefix):
                    tag = self.path[len(prefix) :]
                    if tag in tag_map:
                        dig, last_mod = tag_map[tag]
                        self.send_response(200)
                        self.send_header("Docker-Content-Digest", dig)
                        self.send_header("Last-Modified", last_mod)
                        self.end_headers()
                        return
                self.send_response(404)
                self.end_headers()

            def do_DELETE(self) -> None:  # noqa: N802
                prefix = "/v2/oulipoly/membus/manifests/"
                if self.path.startswith(prefix):
                    dig = self.path[len(prefix) :]
                    deleted.append(dig)
                    self.send_response(202)
                    self.end_headers()
                    return
                self.send_response(404)
                self.end_headers()

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), MockOciHandler)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                kust = pathlib.Path(tmp) / "kustomization.yaml"
                kust.write_text(
                    f"""
images:
  - name: ghcr.io/oulipoly/membus-slot-a
    digest: {dig_a}
  - name: ghcr.io/oulipoly/membus-slot-b
    digest: {dig_b}
  - name: ghcr.io/oulipoly/membus-ab-router
    digest: {dig_r}
""",
                    encoding="utf-8",
                )
                rc = registry_gc.main(
                    [
                        "--kustomization",
                        str(kust),
                        "--registry-url",
                        f"http://127.0.0.1:{port}",
                        "--repository",
                        "oulipoly/membus",
                        "--retain-releases",
                        "1",
                        "--require-dual-slot-pins",
                        "--execute",
                    ]
                )
                self.assertEqual(rc, 0)
                self.assertEqual(deleted, [dig_stale])
        finally:
            server.shutdown()
            server.server_close()

    def test_update_flux_digest_updates_only_target_slot_atomically(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            kust = pathlib.Path(tmp) / "kustomization.yaml"
            kust.write_text(
                f"""apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
images:
  - name: ghcr.io/oulipoly/membus-slot-a
    newName: ghcr.io/oulipoly/membus
    digest: {_digest("a")}
  - name: ghcr.io/oulipoly/membus-slot-b
    newName: ghcr.io/oulipoly/membus
    digest: {_digest("b")}
  - name: ghcr.io/oulipoly/membus-ab-router
    newName: ghcr.io/oulipoly/membus
    digest: {_digest("c")}
""",
                encoding="utf-8",
            )
            new_b = _digest("f")
            rc = update_flux_digest.main(
                ["--kustomization", str(kust), "--slot", "slot-b", "--digest", new_b]
            )
            self.assertEqual(rc, 0)
            pins = registry_gc.extract_kustomize_pins(kust.read_text(encoding="utf-8"))
            self.assertEqual(pins["slot_a"], {_digest("a")})
            self.assertEqual(pins["slot_b"], {new_b})
            self.assertEqual(pins["ab_router"], {_digest("c")})

            # Rejects malformed digest.
            self.assertEqual(
                update_flux_digest.main(
                    ["--kustomization", str(kust), "--slot", "slot-b", "--digest", "sha256:bad"]
                ),
                1,
            )

    def test_update_flux_digest_reordered_keys_adjacent_preservation_and_no_trailing_newline(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            kust = pathlib.Path(tmp) / "kustomization.yaml"
            # `- newName:` appears BEFORE `name:`, slot-b has NO digest line yet, and file has NO trailing newline!
            kust.write_text(
                (
                    "apiVersion: kustomize.config.k8s.io/v1beta1\n"
                    "kind: Kustomization\n"
                    "images:\n"
                    "  - newName: ghcr.io/oulipoly/membus\n"
                    "    name: ghcr.io/oulipoly/membus-slot-a\n"
                    f"    digest: {_digest('a')}\n"
                    "  - newName: ghcr.io/oulipoly/membus\n"
                    "    name: ghcr.io/oulipoly/membus-slot-b"
                ),
                encoding="utf-8",
            )
            new_b = _digest("9")
            rc = update_flux_digest.main(
                ["--kustomization", str(kust), "--slot", "slot-b", "--digest", new_b]
            )
            self.assertEqual(rc, 0)
            updated_text = kust.read_text(encoding="utf-8")
            pins = registry_gc.extract_kustomize_pins(updated_text)
            self.assertEqual(pins["slot_a"], {_digest("a")})
            self.assertEqual(pins["slot_b"], {new_b})
            self.assertIn(f"    digest: {new_b}\n", updated_text)

    def test_oci_client_rejects_cross_origin_link_plaintext_token_and_bad_repo(self) -> None:
        # Reject plaintext HTTP with bearer token on non-loopback host.
        with self.assertRaises(ValueError):
            registry_gc.OciRegistryClient(
                "http://registry.example.com",
                "oulipoly/membus",
                bearer_token="secret-tok",
            )

        # Reject invalid OCI repository grammar (traversal, query, uppercase).
        for bad_repo in ["../escape", "org/repo?x=1", "ORG/Repo", "org//repo"]:
            with self.assertRaises(ValueError):
                registry_gc.OciRegistryClient("https://ghcr.io", bad_repo)

        # Reject userinfo or query in registry_url.
        with self.assertRaises(ValueError):
            registry_gc.OciRegistryClient("https://user:secret" + "@registry.example", "oulipoly/membus")

        # Reject cross-origin Link pagination header!
        client = registry_gc.OciRegistryClient("https://ghcr.io", "oulipoly/membus")
        with self.assertRaises(RuntimeError):
            client._validate_same_origin_v2_url("https://evil.example.com/v2/oulipoly/membus/tags/list")
        with self.assertRaises(RuntimeError):
            client._validate_same_origin_v2_url("file:///etc/passwd")
        with self.assertRaises(RuntimeError):
            client._validate_same_origin_v2_url("/not-v2/tags/list")
        self.assertEqual(
            client._validate_same_origin_v2_url("/v2/oulipoly/membus/tags/list?page=2"),
            "https://ghcr.io/v2/oulipoly/membus/tags/list?page=2",
        )


if __name__ == "__main__":
    unittest.main()
