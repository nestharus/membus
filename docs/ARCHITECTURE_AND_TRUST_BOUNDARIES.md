# Architecture and Trust Boundaries

This document defines the consistency guarantees, authority boundaries, transport security
posture, resource admission controls, and secondary search recovery model for Membus v2.

## 1. Local Transactional Authority vs. Multi-Node Mesh Consistency

Membus v2 deliberately separates **single-node transactional authority** from
**multi-node eventual convergence**:

| Capability | Scope | Mechanism | Guarantee |
| --- | --- | --- | --- |
| Primary KV + secondary index writes (`CF_KV`, `CF_IDX_ORD`, `CF_IDX_EDGE`, `CF_IDX_REV`, `CF_LOG`) | Single node | RocksDB `OptimisticTransactionDB` + `VersionGuard` CAS | Atomic, serializable-per-key optimistic transactions with strict monotonic local `seq` |
| Fenced resource leases (`CF_LEASE`) | Single node | Atomic CAS in `CF_LEASE` + CSPRNG token + monotonic generation | Exclusive lease fencing **within one Membus node** |
| Collection schema catalog (`CF_META`) | Single node | `meta_cas_batch` optimistic CAS | Atomic schema registration and migration state transitions |
| Embedded Zenoh replication (`src/mesh/`) | Multi-node cluster | Off-path `CF_LOG` delta Pub/Sub, bounded gap catch-up, XOR digest anti-entropy | **Eventual consistency** via deterministic Hybrid Logical Clock (HLC) Last-Writer-Wins (`(timestamp_ms, origin_node_id, origin_seq, value_sha256)`) |

### Critical Authority Boundary

- **Zenoh mesh replication is NOT Raft/Paxos distributed consensus.** Nodes accept local writes
  independently without a quorum round-trip and converge asynchronously under deterministic
  HLC LWW tie-breaking.
- **Local `CF_LEASE` does NOT provide cluster-wide exclusive locks.** A lease acquired on Node A
  is enforced atomically on Node A only; it does not prevent Node B from issuing or accepting
  writes during a partition or before asynchronous replication converges.
- **Callers requiring cluster-wide exclusive authority** (such as distributed leader election,
  cross-node exclusive job ownership, or globally linearizable state machines) MUST layer an
  explicit consensus/quorum authority protocol on top of Membus or route all authoritative
  mutations for a resource to a single designated owner node.

## 2. Security and Transport Trust Boundaries

### Fail-Closed Non-Loopback Listener Policy

Every networked listener enforces a fail-closed bind policy before accepting connections:

- **Loopback (`127.0.0.1`, `::1`) and local Windows Named Pipes** operate with zero friction
  for local development, embedded integration, and automated testing.
- **Non-loopback HTTP (`/v2/...`) and binary TCP listeners** (`0.0.0.0`, `::`, LAN/WAN IPs)
  are **refused** unless **BOTH**:
  1. Authentication is enforced (`AuthMode::Enforced` via `--auth-config`), AND
  2. Transport encryption is configured (`--http-tls-cert`/`--http-tls-key` for HTTP/2+REST/WS/SSE,
     `--tls-cert`/`--tls-key` for binary TCP; optional `--*-tls-client-ca` enables mutual TLS).
- **Non-loopback Zenoh mesh listen endpoints** are **refused** by default.
- An explicit break-glass override (`--allow-insecure-non-loopback` CLI flag or
  `allow_insecure_non_loopback = true` in `ServerConfig`/`MeshConfig`) permits non-loopback
  binds only when explicitly opted into (for example inside an isolated local container bridge
  published exclusively to `127.0.0.1`) and logs an explicit warning to `stderr`.

### Cross-Transport Auth & Opt-In Fencing Symmetry

- `AuthConfig` (constant-time SHA-256 token comparison and namespace/key-prefix ACL grants for
  `Read`, `Write`, `Admin`) is enforced identically across HTTP REST, SSE, WebSocket, and
  binary TCP/TLS/Named Pipe handlers.
- `/v2/health` enforces authentication by default when `AuthMode::Enforced` is active.
  Kubernetes deployments running TLS + Auth may opt into `--public-health-probe`
  (`ServerConfig::public_health_probe`) so kubelet HTTPS liveness/readiness probes can check
  `/v2/health` without embedding bearer tokens in Pod specs while all data and admin endpoints
  remain strictly authenticated.
- Per-resource leases (`LeaseManager`) provide opt-in fencing tokens (`LeaseGrant` with secret
  token returned to the holder; `LeaseInfo` omitting the secret token for inspection and
  diagnostics). Callers that require fenced writes validate `(holder, generation, token)`
  explicitly at their application or coordinator boundary.

### Checkpoint Path Containment

Remote checkpoint requests (`POST /v2/checkpoint` and `Request::Checkpoint`) are disabled
unless `checkpoint_root` is explicitly configured, and destination paths are validated to
reject absolute paths and parent-directory traversal (`..`).

## 3. Global Resource Admission Controls

To prevent single clients or read-heavy workloads from exhausting memory, file descriptors,
or RocksDB compaction headroom, Membus enforces explicit bounded admission across all
consumer surfaces:

- **Write admission (`AdmissionGate`):** Bounds concurrent in-flight write operations
  (`max_in_flight_ops`) and aggregate payload bytes (`max_in_flight_bytes`), returning
  `MembusError::Busy` (`503 Service Unavailable`).
- **HTTP/REST data-plane concurrency (`max_http_in_flight`):** Bounds concurrent in-flight
  data-plane HTTP requests via an owned semaphore permit, returning `503 Service Unavailable`
  when saturated while keeping `/v2/health` unthrottled for probes and observability.
- **Active push streams (`max_active_streams`):** Globally bounds concurrent SSE, WebSocket,
  and binary subscription streams via RAII permits across all connections in addition to
  per-connection caps.
- **Active MVCC snapshots (`max_active_snapshots`):** Globally bounds live `OwnedSnapshot`
  instances inside `src/storage/snapshot.rs` so readers cannot pin unbounded SST files and
  stall RocksDB compaction; saturation returns `MembusError::Busy` (`503`).
- **Query timeouts & scan bounds (`query_timeout`, `max_scan_limit`):** Bounds page sizes
  and enforces cooperative execution deadlines across prefix scans, range scans, ordered
  index queries, graph neighbor/BFS traversals, and search queries.

Live gauges (`active_connections`, `active_subscriptions`, `active_snapshots`,
`active_streams`, `search_ingest_deferred_total`, `search_dirty`) are exposed on both
`GET /v2/health` and binary `Request::Health`.

## 4. Post-Commit Search Sync and `CF_LOG` Recovery Model

RocksDB (`CF_KV` + `CF_LOG`) is the **sole authoritative durability boundary**. Embedded
Tantivy (`SearchEngine`) is a **derived secondary projection**:

1. Every indexed KV write (`put_indexed`, `remove_indexed`, `commit_indexed_tx`) and
   standalone search mutation (`index_document`, `delete_document`, `revoke_search_prefix`)
   first commits the visibility-sanitized `SearchLogOp` atomically inside `CF_LOG` in RocksDB.
2. Only after the RocksDB transaction commits does the shared write coordinator
   (`src/transport/write_coord.rs`) invoke `SearchEngine::ingest_committed_op(...)` to update
   the 0 ms Tier-0 RAM overlay and wake the background Tantivy committer.
3. **Commit-boundary invariant:** If post-commit search ingestion fails for any reason, the
   write coordinator **NEVER returns a write-rejection error** to the client. Instead:
   - The authoritative `CommitResult` (`seq`, `epoch`, `versions`) is returned as success
     (`200 OK` / `Response::Commit`).
   - HTTP responses explicitly signal secondary state via both the `X-Membus-Search-Sync`
     header (`synced` or `deferred`) and the `search_sync` JSON field; WebSocket commits
     include `search_sync` in the payload.
   - `SearchEngine` logs the exact error, increments `search_ingest_deferred_total`, sets
     `search_dirty = true` (observable via `/v2/health`), and schedules `CF_LOG` catch-up.
4. The background Tantivy commit worker (or an explicit `SearchEngine::catch_up_from_log()` /
   `flush_sync()` call, or restart replay) deterministically replays all committed `CF_LOG`
   entries after `search:checkpoint` in strict `(seq, op_idx)` order, flushes RocksDB WAL
   before committing Tantivy segments, reloads the reader, advances `search:checkpoint`, and
   clears `search_dirty` once caught up.

## 5. Resident Lifecycle and Graceful Shutdown

- `membus serve` listens for both interactive `SIGINT` (`Ctrl+C`) and container/service-manager
  `SIGTERM` (on Unix) through one shared `shutdown_resident(...)` orchestration path.
- Shutdown executes with a bounded drain timeout (25 seconds by default, under standard 30s
  orchestrator grace periods) in strict order:
  1. Drain and close the Zenoh mesh session (if enabled).
  2. Gracefully stop all HTTP/HTTPS/TCP/TLS/Named Pipe listeners.
  3. Synchronously flush pending `CF_LOG` search operations into Tantivy (`SearchEngine::flush_sync`).
  4. Synchronously flush the RocksDB WAL (`StorageEngine::flush_wal(true)`).
- Every shutdown step runs even if an earlier step fails. Any drain, search-flush, or WAL-flush
  error is logged explicitly to `stderr` and causes `membus serve` to exit with a **non-zero**
  exit code rather than silently reporting success.

## 6. Storage Module Decomposition and `OwnedSnapshot` Safety

`src/storage/` isolates each storage concern into a dedicated module:

- `hlc.rs` — Monotonic Hybrid Logical Clock.
- `wal_sync.rs` — Periodic background WAL flusher with observable error counting.
- `sequencer.rs` — Strict contiguous commit-sequence publication gate.
- `snapshot.rs` — Global `SnapshotAdmissionGate` and `OwnedSnapshot` (isolating the sole
  `unsafe` lifetime-extension block with explicit `ManuallyDrop` drop-order guarantees so
  the RocksDB snapshot handle always drops strictly before `Arc<OptimisticTransactionDB>`).
- `mesh_replication.rs` — Deterministic LWW replicated delta application, deduplicated
  replicated search ops, and local tier-swap lineage transitions.
- `rocksdb_kv.rs` — `RocksDbStorage` lifecycle, transactional KV/index/graph/blob/log/meta/lease
  operations, safe watermark-coordinated log trimming, and checkpoints.
