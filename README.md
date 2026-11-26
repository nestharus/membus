# Membus v2

Membus v2 is a low-latency embedded and resident key/value engine built on
multi-threaded RocksDB (`OptimisticTransactionDB`), a sharded in-memory signal
ring with durable commit-log catch-up, and a multiplexed binary `membus/2`
async RPC protocol over Windows IOCP Named Pipes and TCP.

## Modules

- `src/types.rs` — Core domain types (`Ack`, `ExpectedVersion`, `StoredRecord`,
  `WriteMutation`, `VersionGuard`, `ScanPage`, `SignalEvent`, `MembusError`).
- `src/storage/` — `StorageEngine` trait, `RocksDbStorage` over
  `OptimisticTransactionDB` with dedicated column families (`kv`, `log`, `blob`,
  `idx_ord`, `idx_edge`, `idx_rev`, `lease`, `meta`), `BlobDB` (>= 16 KiB with
  Zstd compression), O(1) MVCC snapshots, stateless keyset pagination,
  zero-downtime `Checkpoint` backups, WAL tailing (`read_wal_since`), and
  bounded atomic admission backpressure.
- `src/signals/` — Sharded per-namespace in-memory signal ring (`SignalRing`)
  and `SignalBroker` supporting lossless `EachChange` (RAM-ring fast path with
  seamless `log` CF catch-up and explicit `Gap` signaling on trimmed horizons)
  and anti-starvation `LiveCoalesced` (per-key coalescing preserving the oldest
  FIFO arrival ticket).
- `src/protocol/` — Multiplexed `membus/2` binary framing
  (`[u32 body_len][u16 version=2][u8 frame_kind][u8 flags][u64 stream_id][payload]`)
  and strongly-typed message codec.
- `src/transport/` — Async Tokio server over Windows IOCP Named Pipes
  (unlimited instances, current-user-SID + LocalSystem DACL, remote clients
  rejected) and TCP (`TCP_NODELAY`) with per-connection credit-window
  backpressure returning retryable `Busy` without dropping connections, idle
  snapshot TTL reaping, and root-confined checkpoint paths.
- `src/client/` — Cloneable pipelined async `Client` supporting concurrent
  in-flight RPCs, pinned MVCC snapshots, and live push subscription streams on a
  single connection.
- `src/cli/` — Resident command-line interface (`membus serve`, `membus health`,
  `membus checkpoint`).

## Durability Semantics

- `Ack::Fast` commits with `WriteOptions::set_sync(false)`: updates land
  immediately in the RocksDB MemTable and OS WAL buffer, are immediately visible
  to readers, and immediately wake signal subscribers. An optional periodic
  background WAL sync reduces exposure, but `Ack::Fast` does not guarantee
  survival across sudden power loss.
- `Ack::Flush` commits with `WriteOptions::set_sync(true)`, participating in
  RocksDB's native lock-free `WriteThread` leader-follower group commit across
  concurrent writers.

## CLI Usage

Start a resident listening on TCP and/or a Windows Named Pipe (choose a state
directory outside the source tree):

```text
membus serve --data-dir <STATE_DIR> --tcp 127.0.0.1:7420 --checkpoint-root <CHECKPOINT_ROOT>
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
