# Membus v2

Membus v2 is a low-latency embedded and resident key/value, secondary-index,
graph-adjacency, full-text search, and signal-streaming engine built on
multi-threaded RocksDB (`OptimisticTransactionDB`), embedded `Tantivy`, a
sharded in-memory signal ring with durable commit-log catch-up, and multiplexed
binary + HTTP/2/WebSocket/SSE transports.

## Modules

- `src/types.rs` — Core domain types (`Ack`, `ExpectedVersion`, `StoredRecord`,
  `WriteMutation`, `VersionGuard`, `IndexMutations`, `SearchDocument`,
  `SearchQuery`, `LeaseGrant`, `CollectionDescriptor`, `SignalEvent`,
  `MembusError`).
- `src/storage/` — `StorageEngine` trait and `RocksDbStorage` over
  `OptimisticTransactionDB` with 8 dedicated column families (`kv`, `log`,
  `blob`, `idx_ord`, `idx_edge`, `idx_rev`, `lease`, `meta`), `BlobDB`
  (>= 16 KiB with Zstd compression), atomic reverse-posting index maintenance,
  namespace-scoped SHA-256 CAS blobs, O(1) MVCC snapshots, stateless keyset
  pagination, zero-downtime `Checkpoint` backups, WAL tailing, and bounded
  atomic admission backpressure.
- `src/index/` — Schema-agnostic order-preserving composite key codecs and
  validation for declarative ordered secondary indexes (`idx_ord`),
  bidirectional labeled graph edges (`idx_edge`, forward + reverse + bounded
  multi-hop BFS up to 8 hops), atomic reverse-posting retraction (`idx_rev`),
  and namespace-scoped CAS blobs (`blob`).
- `src/auth/` — `AuthConfig` (`Disabled` | `Enforced`) with constant-time
  bearer token verification, per-namespace and key-prefix ACL grants
  (`Read`/`Write`/`Admin`) enforced across binary sockets and HTTP/WS/SSE, plus
  atomic per-resource fenced leases (`LeaseManager` over `lease` CF with
  strictly monotonic `generation`).
- `src/catalog/` — CAS-fenced versioned collection, store-profile, schema &
  migration catalog (`CollectionCatalog` over `meta` CF) with SHA-256 schema
  integrity verification, immutable version history, and explicit
  `begin_migration` / `commit_migration` / `abort_migration` transitions.
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
- `src/protocol/` — Multiplexed `membus/2` binary framing
  (`[u32 body_len][u16 version=2][u8 frame_kind][u8 flags][u64 stream_id][payload]`)
  and strongly-typed message codec covering KV, transactions, indexes, graph
  BFS, CAS blobs, auth, leases, catalog/migrations, search, and live streams.
- `src/transport/` — Async Tokio server supporting Windows IOCP Named Pipes
  (unlimited instances, current-user-SID + LocalSystem DACL, remote clients
  rejected), plaintext and TLS/mTLS TCP (`tokio-rustls`), and Axum HTTP/2 +
  HTTP/1.1 REST, SSE (`/v2/signals/{ns}/stream`), and bidirectional WebSocket
  (`/v2/ws`) sharing a single `ServerContext`.
- `src/client/` — Cloneable pipelined async `Client` supporting concurrent
  in-flight RPCs, pinned MVCC snapshots, TLS/mTLS TCP, bearer authentication,
  and live push subscription streams on a single connection.
- `src/cli/` — Resident command-line interface (`membus serve`, `membus health`,
  `membus checkpoint`).

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

Start a resident listening on binary TCP (plaintext or TLS/mTLS), Windows Named
Pipe, and/or HTTP/2 + WebSocket + SSE:

```text
membus serve --data-dir <STATE_DIR> --tcp 127.0.0.1:7420 --http-listen 127.0.0.1:7480 --checkpoint-root <CHECKPOINT_ROOT>
membus serve --data-dir <STATE_DIR> --tcp 127.0.0.1:7421 --tls-cert <CERT_PEM> --tls-key <KEY_PEM> --tls-client-ca <CA_PEM> --auth-config <AUTH_JSON>
membus serve --data-dir <STATE_DIR> --pipe \\.\pipe\membus-resident
```

Check resident health:

```text
membus health --tcp 127.0.0.1:7420
```

Create a zero-downtime RocksDB checkpoint (relative destination inside `--checkpoint-root`):

```text
membus checkpoint --tcp 127.0.0.1:7420 --dest snap_01
```
