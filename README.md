# Membus v2

Membus v2 is a low-latency embedded and resident key/value, secondary-index,
graph-adjacency, full-text search, multi-node replication, and signal-streaming
engine built on multi-threaded RocksDB (`OptimisticTransactionDB`), embedded
`Tantivy`, embedded `Zenoh`, a sharded in-memory signal ring with durable
commit-log catch-up, and an Axum HTTP/2 + REST + WebSocket + SSE service plane
(with optional TLS/mTLS).

See [docs/ARCHITECTURE_AND_TRUST_BOUNDARIES.md](docs/ARCHITECTURE_AND_TRUST_BOUNDARIES.md)
for consistency guarantees (single-node ACID/leases vs. multi-node HLC LWW mesh),
fail-closed transport security, global admission limits, and `CF_LOG` search recovery.

## Prerequisites

- **Rust `1.85+`** (`cargo`, `rustc`)
- **C/C++ & Clang toolchain** (`clang`, `libclang`, `cmake`, `g++` or MSVC Build Tools)
  for RocksDB `bindgen-runtime` compilation
- **Python `3.11+`** (pure stdlib; zero third-party packages) if using the Python client

## Build / Install

```bash
cargo build --release --locked --bin membus
```

The compiled binary is written to `target/release/membus` (or `target/release/membus.exe` on Windows).
Verify CLI usage at any time with:

```bash
./target/release/membus --help
```

## 2-Minute Verified Quickstart

1. **Start a local resident** (loopback binds require zero TLS/auth ceremony for local dev):

```bash
./target/release/membus serve \
  --data-dir ./membus-quickstart-data \
  --http-listen 127.0.0.1:7480 \
  --tcp 127.0.0.1:7420
```

2. **Verify health** via HTTP or the binary CLI:

```bash
curl -s http://127.0.0.1:7480/v2/health
./target/release/membus health --tcp 127.0.0.1:7420
```

3. **Write and read via the pure-stdlib Python client** (from the repository root):

```bash
PYTHONPATH=clients/python python3 -c '
from membus_v2_client import MembusClient
c = MembusClient("http://127.0.0.1:7480")
commit = c.put("demo", "hello", "world", ack="Flush")
rec = c.get("demo", "hello")
print("seq:", commit.seq, "value:", rec.value_text)
'
```

Press `Ctrl+C` (or send `SIGTERM` on Unix) to drain listeners, flush Tantivy search + RocksDB WAL,
and shut down cleanly. Non-loopback binds (`0.0.0.0`, LAN IPs) fail closed unless both
`--auth-config` and TLS (`--http-tls-cert`/`--http-tls-key` or `--tls-cert`/`--tls-key`) are
configured, or `--allow-insecure-non-loopback` is explicitly supplied.

## Unified Transport Boundary

- **Primary Client / Service Plane:** Axum HTTP/2 + HTTP/1.1 REST, Server-Sent
  Events (`/v2/signals/{ns}/stream`), and bidirectional WebSocket (`/v2/ws`)
  for local, remote, and polyglot (including pure-stdlib Python) clients.
- **Node-to-Node Mesh Plane:** Embedded `Zenoh` (`src/mesh/`) over TCP and QUIC
  (with optional `mesh-shm` Cargo feature for same-host shared memory) carrying
  off-path live delta Pub/Sub, bounded gap catch-up queryables, and XOR
  support-digest anti-entropy reconciliation.
- **Transitional / Optional Binary Framing Plane:** The multiplexed `membus/2`
  binary protocol over TCP (plaintext or TLS/mTLS) and Windows IOCP Named Pipes
  remains available for direct Rust IPC callers and legacy tooling.

## Modules

- `src/types.rs` — Core domain types (`Ack`, `ExpectedVersion`, `StoredRecord`,
  `ReadMode`, `ColdRef`, `WriteMutation`, `VersionGuard`, `IndexMutations`,
  `SearchDocument`, `SearchQuery`, `LeaseGrant`, `LeaseInfo`,
  `CollectionDescriptor`, `SignalEvent`, `ReplicatedDelta`, `ReplicationStamp`,
  `MembusError`).
- `src/storage/` — `StorageEngine` trait and `RocksDbStorage` over
  `OptimisticTransactionDB` with 8 dedicated column families (`kv`, `log`,
  `blob`, `idx_ord`, `idx_edge`, `idx_rev`, `lease`, `meta`), `BlobDB`
  (>= 16 KiB with Zstd compression), atomic reverse-posting index maintenance,
  namespace-scoped SHA-256 CAS blobs, O(1) MVCC snapshots, stateless keyset
  pagination, deterministic LWW replicated apply, watermark-coordinated safe
  log trimming, zero-downtime `Checkpoint` backups, WAL tailing, and bounded
  atomic admission backpressure.
- `src/index/` — Schema-agnostic order-preserving composite key codecs and
  validation for declarative ordered secondary indexes (`idx_ord`),
  bidirectional labeled graph edges (`idx_edge`, forward + reverse + bounded
  multi-hop BFS up to 8 hops), atomic reverse-posting retraction (`idx_rev`),
  and namespace-scoped CAS blobs (`blob`).
- `src/auth/` — `AuthConfig` (`Disabled` | `Enforced`) with constant-time
  bearer token verification, per-namespace and key-prefix ACL grants
  (`Read`/`Write`/`Admin`) enforced across transports, plus atomic per-resource
  fenced leases (`LeaseManager` over `lease` CF with CSPRNG tokens, monotonic
  clock anchoring, and split `LeaseGrant` vs token-free `LeaseInfo` views).
- `src/catalog/` — CAS-fenced versioned collection, store-profile, schema &
  migration catalog (`CollectionCatalog` over `meta` CF) with SHA-256 schema
  integrity verification, immutable version history, and explicit
  `begin_migration` / `advance_migration_verification` / `commit_migration` /
  `abort_migration` transitions.
- `src/search/` — Embedded disk-persisted `Tantivy` (`MmapDirectory`) with
  persisted `(epoch, last_indexed_seq)` checkpoint and deterministic `log` CF
  replay on restart, strict `SearchVisibility` isolation (`Public`,
  `MetadataOnly`, `Sealed`), field-weighted BM25 (`title` > `tags` > `body`),
  case-sensitive Unicode trigram literal substring search with exact
  verification, 0 ms Tier-0 RAM live delta overlay merged over committed
  segments, and epoch-fenced prefix revocation.
- `src/signals/` — Sharded per-namespace in-memory signal ring (`SignalRing`)
  and `SignalBroker` supporting lossless `EachChange` (RAM-ring fast path with
  seamless `log` CF catch-up and explicit `Gap` signaling on trimmed horizons)
  and anti-starvation `LiveCoalesced` (per-key coalescing preserving the oldest
  FIFO arrival ticket).
- `src/mesh/` — Embedded `Zenoh` multi-node mesh (`MeshNode`) providing
  off-path live delta Pub/Sub, bounded gap catch-up `Queryable`, deterministic
  XOR support-digest anti-entropy `Queryable`, deterministic LWW tie-breaking
  `(timestamp_ms, origin_node_id, origin_seq, value_sha256)`, strict loop
  prevention, persisted peer watermarks in `meta` CF, and `SearchVisibility`
  isolation (never transmitting `Sealed` plaintext).
- `src/backup/` — Off-path zero-downtime RocksDB `Checkpoint` full backups with
  canonical `manifest.json` per-file SHA-256 checksums, SHA-256-sealed
  incremental delta archives exported from `log` CF, and clean-room restore
  with full checksum verification, ordered delta replay, and deterministic
  `Tantivy` search recovery.
- `src/tiering/` — Hot/warm/cold archival (`TieringManager`, `ColdStore`,
  `FileColdStore`) replacing eligible cold `kv` payloads with compact
  `ColdRef` lineage tombstones (`RECORD_FLAG_COLD_REF`), transparent SHA-256-
  verified hydration (`ReadMode::HotOnly` vs `ReadMode::HydrateCold`), explicit
  `rehydrate_cold`, and watermark-safe log trimming (`safe_trim_log_before`).
- `src/transport/` — Axum HTTP/2 + HTTP/1.1 REST, SSE, and WebSocket server
  plus optional multiplexed binary TCP (plaintext & TLS/mTLS via `tokio-rustls`)
  and Windows IOCP Named Pipe listeners sharing a single `ServerContext`.
- `src/protocol/` & `src/client/` — Optional multiplexed `membus/2` binary
  codec and pipelined async Rust client.
- `src/cli/` — Resident and break-glass diagnostic CLI (`membus serve`,
  `membus health`, `membus checkpoint`, `membus diag inspect`,
  `membus diag verify`, `membus diag restore`).
- `clients/python/` — Zero-dependency pure-stdlib Python 3.11+ client
  (`membus_v2_client.py`) and end-to-end test suite targeting the HTTP/REST +
  SSE surface.

## Durability Semantics

- `Ack::Fast` commits with `WriteOptions::set_sync(false)`: updates land
  immediately in the RocksDB MemTable and OS WAL buffer, are immediately visible
  to readers and Tier-0 search, and immediately wake signal subscribers. An
  optional periodic background WAL sync reduces exposure, but `Ack::Fast` does
  not guarantee survival across sudden power loss.
- `Ack::Flush` commits with `WriteOptions::set_sync(true)`, participating in
  RocksDB's native lock-free `WriteThread` leader-follower group commit across
  concurrent writers.

## CLI Usage

Start a resident with the unified HTTP/2 + REST + WebSocket + SSE listener and
optional embedded Zenoh mesh peering:

```text
membus serve --data-dir <STATE_DIR> --http-listen 127.0.0.1:7480 --checkpoint-root <CHECKPOINT_ROOT>
membus serve --data-dir <STATE_DIR> --http-listen 127.0.0.1:7480 --mesh-node-id node-a --mesh-listen tcp/127.0.0.1:7447 --mesh-connect tcp/127.0.0.1:7448
```

Optional transitional binary TCP (plaintext or TLS/mTLS) and Named Pipe listeners:

```text
membus serve --data-dir <STATE_DIR> --tcp 127.0.0.1:7420 --tls-cert <CERT_PEM> --tls-key <KEY_PEM> --tls-client-ca <CA_PEM> --auth-config <AUTH_JSON>
membus serve --data-dir <STATE_DIR> --pipe \\.\pipe\membus-resident
```

Break-glass read-only / secondary diagnostics (safe against BOTH closed and live-open primary databases; never takes the primary write lock and never exposes secret lease tokens):

```text
membus diag inspect --data-dir <STATE_DIR>
membus diag verify --data-dir <STATE_DIR>
membus diag restore --backup-dir <BACKUP_DIR> --target-dir <TARGET_DIR> [--delta <DELTA_BUNDLE_JSON>...]
```

## Pure-Stdlib Python Client

```python
from clients.python.membus_v2_client import MembusClient

client = MembusClient("http://127.0.0.1:7480")
client.put("app", "greeting", "hello")
rec = client.get("app", "greeting")
for msg in client.stream_signals("app", mode="each"):
    print(msg.event, msg.data)
```
