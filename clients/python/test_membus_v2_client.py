"""End-to-end integration tests for the pure-stdlib Membus v2 Python client
against a live spawned `membus serve --http-listen 127.0.0.1:0` resident."""

from __future__ import annotations

import io
import os
import pathlib
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest

_CLIENT_DIR = str(pathlib.Path(__file__).resolve().parent)
_ADDED_SYS_PATH = _CLIENT_DIR not in sys.path
if _ADDED_SYS_PATH:
    sys.path.insert(0, _CLIENT_DIR)
try:
    from membus_v2_client import LeaseGrant, LeaseInfo, MembusClient, MembusError
finally:
    if _ADDED_SYS_PATH and _CLIENT_DIR in sys.path:
        sys.path.remove(_CLIENT_DIR)


def _find_membus_binary() -> pathlib.Path:
    env_bin = os.environ.get("MEMBUS_BIN")
    if env_bin:
        p = pathlib.Path(env_bin)
        if p.is_file():
            return p
    repo_root = pathlib.Path(__file__).resolve().parents[2]
    exe = "membus.exe" if os.name == "nt" else "membus"
    for profile in ("debug", "release"):
        candidate = repo_root / "target" / profile / exe
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "membus binary not found; build it first with `cargo build --bin membus` "
        "or set MEMBUS_BIN"
    )


class MembusPythonClientEndToEndTest(unittest.TestCase):
    proc: subprocess.Popen[str]
    temp_dir: pathlib.Path
    client: MembusClient
    _drain_threads: list[threading.Thread]

    @classmethod
    def setUpClass(cls) -> None:
        bin_path = _find_membus_binary()
        cls.temp_dir = pathlib.Path(tempfile.mkdtemp(prefix="membus_py_e2e_"))
        data_dir = cls.temp_dir / "data"
        ckpt_root = cls.temp_dir / "checkpoints"
        ckpt_root.mkdir(parents=True, exist_ok=True)
        cls._drain_threads = []

        cls.proc = subprocess.Popen(
            [
                str(bin_path),
                "serve",
                "--data-dir",
                str(data_dir),
                "--http-listen",
                "127.0.0.1:0",
                "--checkpoint-root",
                str(ckpt_root),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        ready_q: queue.Queue[str | None] = queue.Queue()

        def drain_stdout() -> None:
            assert cls.proc.stdout is not None
            found = False
            for line in cls.proc.stdout:
                stripped = line.strip()
                if not found and stripped.startswith("listening http="):
                    found = True
                    ready_q.put(stripped.split("=", 1)[1].strip())
            if not found:
                ready_q.put(None)

        def drain_stderr() -> None:
            assert cls.proc.stderr is not None
            for _ in cls.proc.stderr:
                pass

        t_out = threading.Thread(target=drain_stdout, daemon=True)
        t_err = threading.Thread(target=drain_stderr, daemon=True)
        t_out.start()
        t_err.start()
        cls._drain_threads = [t_out, t_err]

        try:
            http_addr = ready_q.get(timeout=15.0)
        except queue.Empty:
            cls.tearDownClass()
            raise RuntimeError("timed out waiting for membus serve HTTP startup") from None

        if http_addr is None:
            cls.tearDownClass()
            raise RuntimeError("membus serve exited before reporting listening http address")

        cls.client = MembusClient(f"http://{http_addr}", timeout=10.0)

    @classmethod
    def tearDownClass(cls) -> None:
        proc = getattr(cls, "proc", None)
        if proc is not None:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
            for t in getattr(cls, "_drain_threads", []):
                t.join(timeout=2.0)
            if proc.stdout is not None:
                proc.stdout.close()
            if proc.stderr is not None:
                proc.stderr.close()
        temp_dir = getattr(cls, "temp_dir", None)
        if temp_dir is not None and temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)

    def test_01_health_and_kv_tx_lifecycle(self) -> None:
        health = self.client.health()
        self.assertGreater(health["epoch"], 0)

        c1 = self.client.put("py_kv", "alpha", "hello-v2", ack="Flush", expected="Absent")
        self.assertGreater(c1.seq, 0)

        rec = self.client.get("py_kv", "alpha")
        self.assertIsNotNone(rec)
        assert rec is not None
        self.assertEqual(rec.value_text, "hello-v2")
        self.assertEqual(rec.meta.version, c1.seq)

        # CAS conflict check
        with self.assertRaises(MembusError) as ctx:
            self.client.put("py_kv", "alpha", "stale", expected=c1.seq + 99)
        self.assertEqual(ctx.exception.status, 409)

        # Scoped multi-key transaction
        tx_res = self.client.commit_tx(
            guards=[{"namespace": "py_kv", "key": "alpha", "expected": c1.seq}],
            mutations=[
                {"namespace": "py_kv", "key": "alpha", "value": "updated-via-tx"},
                {"namespace": "py_kv", "key": "beta", "value": b"\x01\x02\x03\x04"},
            ],
            ack="Flush",
        )
        self.assertGreater(tx_res.seq, c1.seq)
        beta = self.client.get("py_kv", "beta")
        self.assertIsNotNone(beta)
        assert beta is not None
        self.assertEqual(beta.value, b"\x01\x02\x03\x04")

        self.client.delete("py_kv", "beta")
        self.assertIsNone(self.client.get("py_kv", "beta"))

    def test_02_ordered_queue_and_graph_bfs(self) -> None:
        self.client.put(
            "py_work",
            "task/1",
            "payload-1",
            ordered_indexes=[
                {"index_id": "priority", "sort_key": "0010", "summary": "high"},
            ],
            graph_edges=[
                {
                    "index_id": "deps",
                    "src_id": "nA",
                    "rel_label": "calls",
                    "dst_id": "nB",
                    "meta": "w=1",
                }
            ],
        )
        self.client.put(
            "py_work",
            "task/2",
            "payload-2",
            ordered_indexes=[
                {"index_id": "priority", "sort_key": "0005", "summary": "urgent"},
            ],
            graph_edges=[
                {
                    "index_id": "deps",
                    "src_id": "nB",
                    "rel_label": "calls",
                    "dst_id": "nC",
                    "meta": "w=2",
                }
            ],
        )

        page = self.client.query_ordered_index("py_work", "priority", limit=10)
        self.assertEqual(
            [e["primary_key"] for e in page["entries"]],
            [b"task/2", b"task/1"],
        )

        bfs = self.client.bfs_traverse(
            "py_work",
            "deps",
            "nA",
            direction="Out",
            rel_filter="calls",
            max_hops=3,
            max_nodes=10,
        )
        self.assertEqual(bfs["visited_nodes"], [b"nA", b"nB", b"nC"])
        self.assertEqual(len(bfs["traversed_edges"]), 2)

    def test_03_leases_split_grant_and_info(self) -> None:
        grant = self.client.acquire_lease("py_locks", "leader", "worker-a", 30_000)
        self.assertIsInstance(grant, LeaseGrant)
        self.assertTrue(len(grant.token) >= 32)
        self.assertTrue(self.client.validate_lease(grant))

        # Inspect returns LeaseInfo WITHOUT secret token.
        info = self.client.inspect_lease("py_locks", "leader")
        self.assertIsInstance(info, LeaseInfo)
        assert info is not None
        self.assertEqual(info.holder, "worker-a")
        self.assertEqual(info.generation, grant.generation)
        self.assertFalse(hasattr(info, "token"))

        renewed = self.client.renew_lease(grant, 60_000)
        self.assertEqual(renewed.generation, grant.generation)
        self.assertGreaterEqual(renewed.expires_at_ms, grant.expires_at_ms)

        self.assertTrue(self.client.release_lease(renewed))
        self.assertIsNone(self.client.inspect_lease("py_locks", "leader"))

    def test_04_search_bm25_and_literal_substring(self) -> None:
        self.client.index_search_document(
            namespace="py_docs",
            doc_id="d1",
            kind="note",
            title="Zenoh Replication Mesh",
            tags=["mesh", "rocksdb"],
            body="Exact token AlphaBeta_9942 appears inside body.",
            visibility="Public",
        )
        bm25 = self.client.search("Zenoh Replication", mode="Bm25", namespace="py_docs")
        self.assertGreaterEqual(len(bm25["hits"]), 1)
        self.assertEqual(bm25["hits"][0]["doc_id"], "d1")

        lit = self.client.search(
            "AlphaBeta_9942",
            mode="LiteralSubstring",
            namespace="py_docs",
        )
        self.assertEqual([h["doc_id"] for h in lit["hits"]], ["d1"])

    def test_05_live_sse_signal_stream(self) -> None:
        received: queue.Queue[object] = queue.Queue()
        subscribed_evt = threading.Event()

        def reader() -> None:
            try:
                for msg in self.client.stream_signals(
                    "py_sig",
                    mode="each",
                    since_seq=self.client.health()["latest_seq"],
                    timeout=8.0,
                ):
                    if msg.event == "subscribed":
                        subscribed_evt.set()
                    elif msg.event == "signal":
                        received.put(MembusClient.parse_signal_event(msg.data))
                        break
            except Exception as exc:
                received.put(exc)

        thread = threading.Thread(target=reader, daemon=True)
        thread.start()
        self.assertTrue(subscribed_evt.wait(timeout=5.0), "SSE subscribed event timed out")

        commit = self.client.put("py_sig", "evt/1", "wake-now")
        item = received.get(timeout=5.0)
        if isinstance(item, Exception):
            raise item
        self.assertEqual(item.namespace, "py_sig")
        self.assertEqual(item.key, b"evt/1")
        self.assertEqual(item.seq, commit.seq)
        self.assertEqual(item.value_hint, b"wake-now")
        thread.join(timeout=3.0)

    def test_06_url_special_keys_cas_blobs_and_sse_spec(self) -> None:
        # Strict path-segment encoding: /, .., ?, #, %, spaces, and UTF-8.
        special_keys = [
            "dir/sub/key with spaces",
            "weird?query=1#frag%25pct",
            "a/../b/caf\u00e9-\u65e5\u672c\u8a9e",
        ]
        for idx, key in enumerate(special_keys):
            val = f"val-{idx}-{key}"
            self.client.put("py_special", key, val, ack="Flush")
            rec = self.client.get("py_special", key)
            self.assertIsNotNone(rec, f"missing special key {key!r}")
            assert rec is not None
            self.assertEqual(rec.key_text, key)
            self.assertEqual(rec.value_text, val)
            self.client.delete("py_special", key)
            self.assertIsNone(self.client.get("py_special", key))

        # CAS blobs roundtrip + percent-encoded invalid digest rejection (no path traversal).
        blob_bytes = b"\x00\xffmembus-cas-blob-payload"
        sha_hex = self.client.put_cas_blob("py_blobs", blob_bytes, ack="flush")
        self.assertEqual(len(sha_hex), 64)
        self.assertEqual(self.client.get_cas_blob("py_blobs", sha_hex), blob_bytes)
        with self.assertRaises(MembusError) as bad_blob:
            self.client.get_cas_blob("py_blobs", "../health")
        self.assertEqual(bad_blob.exception.status, 400)

        # Non-200 SSE response parses structured JSON {"error", "message"} cleanly.
        with self.assertRaises(MembusError) as sse_err:
            next(self.client.stream_signals("py_sig", mode="invalid_mode"))
        self.assertEqual(sse_err.exception.status, 400)
        self.assertEqual(sse_err.exception.code, "invalid_argument")

        # WHATWG SSE data: strips at most ONE leading U+0020 space (preserving indentation).
        raw_sse = io.BytesIO(
            b"event: custom\n"
            b"id: 42\n"
            b"data:   indented line\n"
            b"data:no-space\n"
            b"\n"
        )
        msgs = list(MembusClient._parse_sse_lines(raw_sse))
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0].event, "custom")
        self.assertEqual(msgs[0].event_id, "42")
        self.assertEqual(msgs[0].data["raw"], "  indented line\nno-space")

    def test_07_catalog_blob_head_search_revoke_and_ab_primitives(self) -> None:
        import base64
        import hashlib

        # 1. HEAD /v2/blobs/{ns}/{sha256}
        blob = b"head-probe-blob-bytes"
        sha_hex = self.client.put_cas_blob("py_blobs_head", blob, ack="flush")
        self.assertTrue(self.client.has_cas_blob("py_blobs_head", sha_hex))
        self.assertFalse(self.client.has_cas_blob("py_blobs_head", "0" * 64))
        with self.assertRaises(MembusError) as bad_head:
            self.client.has_cas_blob("py_blobs_head", "not-hex")
        self.assertEqual(bad_head.exception.status, 400)

        # 2. Collection catalog & expand-contract lifecycle
        s1 = b'{"version":1}'
        s2 = b'{"version":2}'
        s1_sha = list(hashlib.sha256(s1).digest())
        s2_sha = list(hashlib.sha256(s2).digest())
        reg = self.client.register_collection(
            {
                "namespace": "py_cat",
                "store_profile": "Kv",
                "schema_version": 1,
                "schema_sha256": s1_sha,
                "schema_bytes": base64.b64encode(s1).decode("ascii"),
                "declared_indexes": [],
                "retention_policy": {
                    "max_age_secs": None,
                    "max_entries": None,
                },
                "migration_state": "Active",
            }
        )
        self.assertEqual(reg["namespace"], "py_cat")
        self.assertEqual(reg["schema_version"], 1)
        self.assertIsNotNone(self.client.get_collection("py_cat"))
        self.assertIsNone(self.client.get_collection("py_cat_missing"))
        self.assertTrue(any(c["namespace"] == "py_cat" for c in self.client.list_collections()))
        v1 = self.client.get_collection_version("py_cat", 1)
        self.assertIsNotNone(v1)
        self.assertIsNone(self.client.get_collection_version("py_cat", 99))

        begun = self.client.begin_migration(
            "py_cat",
            expected_version=1,
            target_version=2,
            target_sha256=s2_sha,
            target_schema_bytes=s2,
        )
        self.assertEqual(begun["compatibility_mode"], "ExpandDual")
        adv = self.client.advance_migration("py_cat", target_version=2, verified_through=10)
        self.assertEqual(adv["migration_state"]["Migrating"]["verified_through"], 10)
        rev = self.client.enter_reverse_shadow("py_cat", target_version=2, verified_through=10)
        self.assertEqual(rev["compatibility_mode"], "ReverseShadow")
        rolled = self.client.rollback_migration("py_cat", rollback_version=1)
        self.assertEqual(rolled["schema_version"], 1)
        self.client.begin_migration(
            "py_cat",
            expected_version=1,
            target_version=2,
            target_sha256=s2_sha,
            target_schema_bytes=s2,
        )
        self.client.enter_reverse_shadow("py_cat", target_version=2, verified_through=12)
        contracted = self.client.contract_migration("py_cat", expected_version=2)
        self.assertEqual(contracted["compatibility_mode"], "Strict")
        self.assertEqual(contracted["schema_version"], 2)

        # 3. POST /v2/search/revoke-prefix
        self.client.index_search_document(
            namespace="py_rev",
            doc_id="drop/1",
            kind="note",
            title="Revocable Entry",
            body="RevokeToken_7711",
        )
        self.client.index_search_document(
            namespace="py_rev",
            doc_id="keep/1",
            kind="note",
            title="Kept Entry",
            body="RevokeToken_7711",
        )
        self.client.revoke_search_prefix("py_rev", "drop/", ack="Flush")
        hits = self.client.search(
            "RevokeToken_7711",
            mode="LiteralSubstring",
            namespace="py_rev",
        )["hits"]
        self.assertEqual([h["doc_id"] for h in hits], ["keep/1"])

        # 4. Idempotent PUT + Parity Digest + Slot Role Transition + Replicated Apply
        h = self.client.health()
        cur_epoch = int(h["role_epoch"])
        c_a = self.client.put(
            "py_ab",
            "item/1",
            "v1",
            ack="Flush",
            idempotency_key="py-idem-1",
            role_epoch=cur_epoch,
        )
        c_b = self.client.put(
            "py_ab",
            "item/1",
            "v1",
            ack="Flush",
            idempotency_key="py-idem-1",
            role_epoch=cur_epoch,
        )
        self.assertEqual(c_a.seq, c_b.seq)

        dig_full = self.client.namespace_digest("py_ab", mode="full", prefix="item/")
        dig_keys = self.client.namespace_digest("py_ab", mode="keys_only", prefix="item/")
        self.assertEqual(dig_full["entry_count"], 1)
        self.assertEqual(dig_keys["entry_count"], 1)
        self.assertNotEqual(dig_full["xor_digest_hex"], dig_keys["xor_digest_hex"])

        shadow_state = self.client.transition_slot_role(
            expected_role="active",
            expected_role_epoch=cur_epoch,
            new_role="shadow",
        )
        self.assertEqual(shadow_state["role"], "shadow")
        shadow_epoch = int(shadow_state["role_epoch"])
        try:
            repl = self.client.apply_replicated_delta(
                {
                    "namespace": "py_ab",
                    "key": list(b"item/2"),
                    "op": {"Put": {"value": list(b"from-peer")}},
                    "stamp": {
                        "timestamp_ms": 1_700_000_000_001,
                        "origin_node_id": "remote-peer-slot",
                        "origin_seq": 1,
                        "value_sha256": list(hashlib.sha256(b"from-peer").digest()),
                    },
                    "indexes": None,
                    "search_op": None,
                },
                ack="Flush",
                route_origin="remote-peer-slot",
                role_epoch=shadow_epoch,
            )
            self.assertTrue(repl["applied"])
            self.assertFalse(repl["echo_rejected"])
            rec = self.client.get("py_ab", "item/2")
            self.assertIsNotNone(rec)
            assert rec is not None
            self.assertEqual(rec.value_text, "from-peer")
        finally:
            self.client.transition_slot_role(
                expected_role="shadow",
                expected_role_epoch=shadow_epoch,
                new_role="active",
            )

    def test_08_direct_spec_from_file_location_without_sys_path(self) -> None:
        """Loading membus_v2_client.py directly via spec_from_file_location works with clients/python off sys.path."""
        import importlib.util

        client_file = pathlib.Path(__file__).resolve().parent / "membus_v2_client.py"
        client_dir = str(client_file.parent)
        saved_path = list(sys.path)
        saved_mods = {
            k: sys.modules.pop(k)
            for k in list(sys.modules)
            if k in ("membus_v2_client", "membus_control", "membus_protocol")
            or k.startswith("_membus_v2_sibling_")
        }
        try:
            sys.path[:] = [p for p in sys.path if os.path.abspath(p or ".") != os.path.abspath(client_dir)]
            spec = importlib.util.spec_from_file_location("standalone_membus_client", client_file)
            assert spec is not None and spec.loader is not None
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            # Confirm sys.path was NOT mutated.
            self.assertNotIn(client_dir, sys.path)
            standalone_client = mod.MembusClient(self.client.base_url)
            health = standalone_client.health()
            self.assertGreaterEqual(int(health["epoch"]), 1)
            # Verify shared MembusError class identity across client and control mixin.
            with self.assertRaises(mod.MembusError) as ctx:
                standalone_client.release_lease(
                    mod.LeaseGrant("ns_missing", "res_missing", "h", 1, "tok", 0)
                )
            self.assertEqual(ctx.exception.status, 404)
            self.assertEqual(ctx.exception.code, "not_found")
        finally:
            sys.path[:] = saved_path
            for k in list(sys.modules):
                if k.startswith("_membus_v2_sibling_") or k == "standalone_membus_client":
                    sys.modules.pop(k, None)
            sys.modules.update(saved_mods)


if __name__ == "__main__":
    unittest.main()
