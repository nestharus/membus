# membus.kv/1: protocol/replay slice, F-E6-11

This document replaces PLAN.md §3's slice-1 wire description and §13.5's outage path. It is the implemented protocol/replay subset, not adoption of all the later PLAN operations. The code is std-only Rust; the Python client uses only its standard library. Run a foreground/test server; no service packaging.

Basis: lane 83's `research/83_loop_alignment/acts/membus-interface-review/review.md` on runtime main 9b51098, SHA256 `af6d9678fbeebedd` (prefix; full snapshot hash is in the F-E6-11 evidence). Design `D:/dev/handoff/memory-ecosystem.md` SHA256 `074d22267522e57ad4ed32e1d95aef64833358a41fc9908e1bf958d99d2ffcb6`; survey `D:/dev/handoff/fast-local-store-survey.md` SHA256 `b128d2588930d54819fceae4047299e4591535763f7048d89c4cf31a9fcfd10f`. Founder corrections: no SQLite, no ordered streams, cache eviction writes no data, expiry eventually retires entire chunks. Rotation/cache/tiering remain later slices; this slice retains all values in memory and all append files on disk.

## 1. First-client evidence and allocation (lane 83 item 8)

Lane 83 review §1 (lines 30–40) replaces seq90's 10–90-row bursts and 44 KB maximum: pydantic seeded **3,065 rows in one second**, median 1,039 B, p95 1,311 B, maximum **59,499 B**; thirdkey seeded 2,387, median 1,073 B, p95 1,485 B, max 43,194 B. The runtime ledger median/p95 is 1,509/4,162 B; this ecosystem's is 1,687/7,639 B. These are located historical workloads, not a future throughput promise. Tests synthesize 3,065 rows alternating median/p95 size, one 59,499-byte row, in sequences of batches ≤128.

The client allocates seq with `conditional_put(row/<seq>, expected=absent)` and retries seq+1 on conflict. It keeps the contiguous-seq watermark and abandoned-row filling. Keys are UTF-8 identifiers, so encode numeric seqs as fixed-width hexadecimal or decimal strings for lexical order; raw big-endian binary keys from the preliminary review mapping are not accepted by this wire version. The test uses 16-digit decimal. No server counter, ordered stream, or key-order publication exists.

## 2. Framing and operations

All integers little endian, except client-defined opaque keys. A frame is `u32 body_length; u16 major=1; u16 minor=0; u8 opcode; u64 request_id; payload`. A byte string is `u32 byte_length; bytes`. Identifier fields are nonempty valid UTF-8, ≤1,024 bytes, validated before String allocation; values opaque, ≤4 MiB. Body ≤16 MiB, batch/get_many ≤128 items. The transport checks body length before body allocation, and the decoder checks field lengths/remaining bytes and UTF-8 before field allocations. Truncation closes the connection. Oversize transport input yields a `too_large` response with request_id=0 then closes; an unparseable header similarly has no usable association. The Python client treats association loss as an uncertain transport outcome.

First request on every connection is hello (opcode 1): `u32 max_frame; u8 allow_mem; string client_identity`. Offer at least 1,024 bytes so the closed refusal list fits hello_ok; the negotiated body limit is enforced for later request allocation and reply size. Version offer is the header's major/minor; unsupported versions return incompatible_version and close. Reply is hello_ok (0x81): common reply header then `u64 epoch; u32 max_frame; u32 max_value; u16 max_batch; u8 server_mem_enabled; u64 retry_window_ms; u16 refusal_count; (u16 code; u8 transient; string name)*`. No multiplexing or optional features are negotiated in this slice; one caller owns each persistent connection. This initial minor version has only these exact fields.

Every non-hello request carries `u64 deadline_unix_ms; string op_id; string namespace` before its opcode payload. Reads carry op_id for tracing, with no dedup state. Mutations scope op_id to namespace, bind it to the full mutation (including key, expected version and ack, excluding deadline), and refuse changed content under the same op_id. Modes: mem=0, os=1 (default), flush=2. Explicit client opt-in and server `--allow-mem` are required for mem. Mem has no committed visibility or restart guarantee in slice 1; it is a disposable acknowledged mutation, not a durable promotion promise.

| Opcode | Payload after common fields | Successful reply after common header |
|---|---|---|
| 2 get | string key | value |
| 3 get_many | u16 count; string keys | u16 count; values |
| 4 put | string key; u8 ack; bytes value | u64 version; u8 achieved_ack |
| 5 conditional_put | string key; u8 ack; u64 expected; bytes value | u64 version; u8 achieved_ack |
| 6 remove | string key; u8 ack; u64 expected | u64 version; u8 achieved_ack |
| 7 batch | u16 count; (u8 mutation_opcode; string item_op_id; that opcode's payload)* | u16 count; (u16 item_code; u64 version; u8 ack)* |
| 8 health | empty | u64 bytes_written; u64 flush_calls; u64 keys; u64 namespace_keys; u64 namespace_quota; u64 index_cap; u8 high_water; u8 namespace_high_water |

`expected=0` means absent, including a committed tombstone. Nonzero expected is an opaque revision token. A remove is a persisted tombstone. A value is `u8 present`, then if present `u64 version; u8 ack; bytes value`. Replies use opcode|0x80, the request_id, and `u16 code` followed by payload on success. Batch is non-atomic: validate the entire frame first, then independent item outcomes in input order; duplicate batch retry uses the unchanged item op_ids, not a fresh batch identity. A get_many too large for a reply is refused; callers page it.

## 3. Pending writes and duplicate identity (item 1)

Admission reserves the key and its future version before log I/O. A competing conditional_put sees the admitted reservation and returns **pending_conflict (19)**, even before publication; **conflict (12)** instead names a mismatching committed expected state. A pending remove does not make absent true before that remove commits. Unconditional mutations of a pending key return busy. A duplicate op_id with the same payload joins the original's eventual outcome (waiting only until the duplicate's deadline); it does not create a new conflict. A later deadline lets the caller query the same outcome. Different payload under an existing id returns op_id_mismatch.

Committed reads use a distinct snapshot lock and never take an append, admission, or flush lock. They can see the previous committed value while a mutation is pending. The conditional operation checks pending reservations and a separate metadata map under admission coordination. Key/namespace/value/dedup counters and an expiry queue avoid full key-map scans during admission; no admission holds the committed read lock. Publication is a short update after OS append or flush completes. A pending conflict is transient: re-read/reselect, and fill a client seq hole if that pending winner later fails.

## 4. Deadlines and refusals (items 2 and 3)

The server checks the request's absolute Unix-millisecond deadline immediately before admission. No new write is admitted at or after it. Clocks must be synchronized; local timeout and server admission refusal are different evidence. After admission, durable I/O may finish after the deadline; a timed-out caller has an uncertain outcome and retains op_id. A duplicate whose wait exceeds its deadline receives **pending_timeout_uncertain (17)**, without cancelling the original. **deadline (5)** is only the before-admission deadline refusal. Read requests are refused when already past deadline.

Closed list, published verbatim by hello:

| Code | Name | Class |
|---|---|---|
| 1 | incompatible_version | persistent for this request |
| 2 | unsupported | persistent |
| 3 | malformed | persistent |
| 4 | too_large | persistent |
| 5 | deadline | transient with a renewed deadline; same op_id |
| 6 | busy | transient; pending-key/queue pressure |
| 7 | capacity | persistent until byte capacity/configuration changes |
| 8 | quota | persistent until namespace quota changes |
| 9 | index_cap | persistent until index capacity grows |
| 10 | read_only | transient after storage repair; refused before admission |
| 11 | warming | transient during replay/readiness |
| 12 | conflict | persistent for the committed expected state; re-read/reselect |
| 13 | retry_window_expired | persistent; reconcile by deterministic key |
| 14 | op_id_mismatch | persistent |
| 15 | mem_not_enabled | persistent until explicit opt-in configuration |
| 16 | io_failed_uncertain | transient outcome after admission; reconcile deterministic key |
| 17 | pending_timeout_uncertain | transient outcome of a pending duplicate; same op_id |
| 18 | dedup_full | transient until retained outcomes expire or configured slots grow |
| 19 | pending_conflict | transient: competing conditional reservation is unpublished |

All 19 codes are tested through the dispatcher, including deterministic warming/read-only state and post-append failure fixtures. Before-admission refusals are safe to spool unsequenced. Codes 16/17 are uncertain outcomes, not evidence of non-admission; the client preserves op_id and reconciles its deterministic key. Busy/capacity/quota/index_cap/dedup_full remain distinct. A complete log record left by failed I/O may replay even when the same-lifetime outcome was uncertain; never mint a fresh id for it. Partial append failure fences its stripe under the file's append-owner lock before already-admitted followers can append behind the torn tail.

## 5. Visibility and snapshots (items 4 and 5)

An os/flush ack follows publication: every later committed get/get_many on any connection sees that revision or a later committed revision. Within an epoch reads are monotonic across reconnects. A mem ack makes no committed guarantee. An epoch is a persisted counter incremented once at successful startup, independent of wall-clock steps. Python reconnect raises EpochChanged(previous,current) before replaying an uncertain request across a new epoch, so the caller can reconcile a deterministic key. Acknowledged durable values are replayed before ready/hello. Startup writes/syncs 8 epoch bytes; append metrics exclude this and tests name it separately.

Later scan_prefix/scan_range cursors will cover one namespace-wide immutable index snapshot, restricted by each query prefix/range. A cursor's snapshot does not cross namespaces; scans with separate cursors do not share a transaction. Scans, shared snapshot tokens and cursor lifetime bounds are **not implemented in slice 1**. There is no scan adoption claim here. Whole-namespace snapshot scope is the requirement for the scan slice.

## 6. Retry horizon and replay (item 6)

Default retry horizon is 24 h from admission, configurable with `--retry-ms` for deterministic tests. Exact duplicate outcomes survive restart for successful os/flush writes whose admission is within the horizon. Conflicts/pre-admission refusals are not admitted and are not persisted as dedup outcomes. Mem outcomes and failed-I/O outcomes are memory-only. Retained outcomes and the retired-id ring each default to **262,144** ids, configurable with `--dedup-cap`; full active slots refuse transient dedup_full. After the horizon, an id still known in the in-memory retired-id ring returns retry_window_expired. An evicted or expired id after restart can be unknown and can execute again. Tests cover known expiry, ring membership and unknown-after-restart execution. This is a bounded retry guarantee, never permanent exactly once. Read a fixed key and its embedded op_id before retrying beyond the horizon.

Four key-hash append files (`stripe-N.log`) contain `MBK1; u32 length; u32 bitwise_length_complement; u32 CRC32; payload`, covering namespace, key, op_id, admission time, expected token, opaque version, ack, value/tombstone. The length complement distinguishes length corruption from a valid incomplete final body. One key always maps to one file, with one append owner at a time. Replay reconstructs published values, tombstones, versions and recent outcomes before readiness. A short final header or final incomplete body is truncated to the last complete frame. Complete CRC failure, invalid magic/length/complement or malformed payload is reported as interior-corrupt and prevents startup; the file remains for diagnosis. A checksum-failed complete final frame is not silently discarded.

State defaults to the caller's chosen `D:/projects/membus/state/`, outside source. Windows share-denied owner.lock is held for the server lifetime; process exit releases exclusive ownership automatically. On non-Windows the create-new lock needs operator cleanup after confirming the previous owner is dead. Named pipes reject remote clients, guard the first instance against an existing endpoint, and use an explicit ACL for the current user and LocalSystem. Both transports cap active connections at 64, with a separate aggregate 64 MiB ingress-frame budget reserved before body allocation and released after dispatch. TCP binds loopback only and has a 60 s per-I/O idle bound; Python reconnects after an idle close. Pipe I/O can hold a bounded slot until disconnect; the client enforces cancellable deadlines. A hard server-side pipe idle cutoff is left with the service/overlapped transport slice. The TCP fallback is a foreground test endpoint for the trusted local machine, with no cross-user authentication or service deployment claim. os means append completed to the OS; it survives server process kill, without an OS-crash/power-loss claim. flush additionally syncs the covering file; hardware/cache policy still limits power-loss evidence.

## 7. Capacity and growth (item 7)

Each namespace defaults to **500,000** keys, including tombstones, configurable with `--quota`, covering the located 270–350k ledger keys. Global key cap defaults to 917,000 (`--index-cap`), below the plan's approximate 256 MiB metadata ceiling; F-E6-20 measures combined processRSS including values, retry fingerprints, metadata, allocator and workers at350000keys (see implemented policy below); metadata-only overhead remains unmeasured. Separate value/dedup-byte cap256MiB and queue representation budget64MiB (fingerprint+value+128bytes, at mostonequarterperstripe) are configurable. Reservations count toward admission, so competing new-key writes cannot exceed quotas. Read-only access to old keys remains available at a cap; new keys are refused with the precise code. Capacity reserves the replacement value while the old value remains visible; this intentionally can refuse an overwrite early rather than exceed peak memory. Missing-key remove persists a tombstone and consumes a key slot.

Health reports global and requested-namespace keys, configured per-namespace quota/global cap, bytes written and flushes for this server lifetime, and 70% high-water flags for both namespace quota and global cap. Clients poll before the cap (signals later). Exact dedup payloads are charged to the value-byte capacity as well; CRC is only corruption detection, never the payload identity guard. The current workload has about 270–350k derived keys (44,326 rows/59 ledgers), 30–40% of the global cap and **54–70% of one 500k namespace**. Lane83's late answer (eco seq402) correctly notes that the namespace warning can fire at adoption; actual allocator memory per key remains unmeasured. Before growth reaches 70%, lane 83 should avoid derived link keys, raise provisioned quota/cap only after RAM accounting, or migrate namespaces to an additional membus instance. Forever namespaces cannot recover capacity by cache eviction. Cold indexing and capacity measured at allocator level are later commissions. At 100%, quarantine persistent refusals and raise an observation; do not let a forever spool loop silently.

## 8. Client-owned refusal/outage path

This replaces PLAN.md:13 and §13.5's seq90 journal/CAS-max proposal with lane 83 review §3. Runtime owns these changes; the membus slice does not implement its spool.

Before first send mint stable row id and op_id; refused rows are fsynced into the existing spool **unsequenced**, and phase dispatch pauses. Do not run a seq-bearing file journal as a second writer or CAS max(seq) at drain. For timeout/reset after send persist `state: uncertain`. Reconcile `get(src/<source hash>)`: same op_id means done; absent means conditional_put(absent) with the same op_id; a different op_id means same-content dedup or a revision. Check an in-flight row/<seq>'s embedded op_id before choosing another seq. Deterministic keys retain idempotence beyond the dedup horizon. Source claim is the commit point; intent records let the client recover its five multi-key updates; contiguous watermark/hole filling remain client-owned.

Lane83's **shadow proposal** (eco seq402) is separate from that primary-store outage path: the fsynced file ledger remains authoritative and is already the journal, so no extra shadow journal. Mirror with conditional_put(absent) on the file's own fixed-width seq key and explicit os ack; a client-owned mirror cursor catches up after an outage. Past-window safety requires all three invariants: authoritative mirror keys use only conditional puts; they are never removed (abandoned seqs get a value, not a remove); and a retry first reads the deterministic key and embedded op_id. Shadow choice belongs to lane83's Decide, with no primary migration. Grouped flushes and scans remain required before primary adoption.

## 9. Flush spacing and next slices

`--flush-ms` defaults to 50 ms as an **initial configurable value**, not an owner-approved optimum. The current comparison basis is lane 83 F-LP-33's isolated synthetic-copy report, `research/83_loop_alignment/acts/F-LP-33-load/C3-REPORT.md`, Append costs on isolated copies (late eco seq336): single-file API median **27.7 ms**, 2,094,815 logical bytes read/1,964,148 written; journal median **2.3 ms**, 138,101 read/557 written. CLI medians are **157.6/203.2 ms**, including process startup and differing from an in-process client's API cost. Hot cache, one writer, no contention; physical-drive bytes unmeasured. The earlier whole-file size observations (0.75 MB here, 6.5–8.7 MB on correction ledgers) are historical context rather than a benchmark of this slice. This slice appends only records. A device-wide ticket spaces actual flush calls by the configured value; readers take no ticket. The file append mutex is released before ticket sleep and sync, so os appends take no deliberate flush-spacing wait. Slice 1 does not group multiple flush waiters into one sync: a fully flush-acked 3,065-item serial batch can take about 153 s at 50 ms. The measured run records four flush latencies/elapsed time, burst time, bytes and flush calls. The os burst tests framing/allocation, not the final ledger flush-burst SLA. Grouped flush batching and queue workers are next, followed by scan/cache/index tiers, signals/timers/namespace policy, lifecycle rotation/tiering, and service packaging. No throughput adoption claim precedes those measurements.

## 10. Nice-to-have dispositions

| Request | Decision and reason |
|---|---|
| outcome(op_id) | Deferred; deterministic-key get plus identical retry covers this slice and the permanent guard. |
| tag co-location + atomic conditional batch | Deferred; slice batch remains non-atomic, so client intent/source recovery remains necessary. Atomicity would change the commit/recovery surface. |
| shared scan snapshot token | Deferred with scans; namespace snapshot scope is stated in §5. |
| bulk value streaming | Deferred; bounded get_many paging is available; startup reconstruction measurements precede streaming. |
| key/version write signals | Taken as later signal-slice requirement, not implemented; client currently polls. |
| shadow adoption | Recommended to lane 83 as its adoption choice: file ledger primary, membus put-if-absent mirror, compare/reconcile under kills before primary migration. This does not make membus a provider or runtime adapter. |

## 11. Run and verify

`cargo test --offline --locked -- --nocapture`, then `python tests/integration.py --binary target/debug/membus.exe --output <ignored evidence dir>`. Queue both through lineup with cpu/mem and disk:d. Build outputs stay in ignored target; replay/burst states and captures in the commissioned act-work directory. No third-party crate or dependency execution. The Python test owns and stops every server it launches, compares acknowledged ids to replay, and reports record bytes/flush calls.

### F-E6-20 implemented grouped write policy

[GROUPED-FLUSH.md](GROUPED-FLUSH.md) defines the current group closing rule, actual cohort/admission bounds, --flush-ms meaning, batch concurrency and failure behavior. It supersedes section9's historical slice1 ungrouped behavior above; kv/1 remains minor0.

Four append owners drain bounded stripe queues. Each stripe admits at most1024outstanding items and queue_bytes/4chargedbytes; all stripes share queue_bytes. Charge is fingerprint+value+128bytes, replacing the earlier value-only description in §7. Busy6 precedes pending/version/retry identity insertion. Durable stripe records also reserve their exact encoded byte length under the8GiB default state_limit_bytes; capacity7 refuses a record before admission when it would exceed the cap. Resident CLI wiring belongs to F-E6-21; epoch/heartbeat sidecars are excluded from this stripe-log cap.

When a flush waiter first arrives, one device worker gathers for flush_ms, selects a handle group (nominal64rows/144192bytes, one larger record allowed), then syncs the handle. Every complete append before the captured extent is covered, so all known covered waiters complete together, and late notifications reuse the successful extent. Actual cohort bound is the1024outstanding admitted stripe items and charged byte cap;64/144192 is not a hard covered-cohort limit. flush_ms also spaces all device sync attempts by that monotonic interval after the previous attempt completes. Default50ms bounds steady sync attempts to20/s plus the first; it adds approximatelyonewindow to an isolated row, with queue and OSsync time in addition. A serial individual caller still pays that window. Distinct batch keys overlap through up to32helpers (one executing batch; concurrent batchbusy6), while same-key and reply order remain the client's. No optimal-default claim is made: smaller0/1ms settings trade sync rate and latency, and grouping is evidenced by reduced syncs/row at the retained50ms setting.

Optional diagnostic opcode9 uses the normal deadline/op_id/namespace prefix, no suffix, and returns threeLEu64: logical stripe append bytes, actual File::write calls, sync attempts. Existing HELLO/HEALTH layouts and refusal meanings are unchanged under minor0; old servers return unsupported2 for opcode9.

F-E6-20 measured350000recorded-shape keys with explicit2GiBvalue/dedupcapacity and400000dedupcap: processRSS1140346880bytes, privatebytes1168203776, incrementalRSS3244.34bytes/key. This combines values, copied retry fingerprints, metadata, allocator and worker overhead; isolated metadata/key cost remains unknown. Namespacewarning istrueat350000/500000 and globalwarningfalseat350000/917000. Defaults256MiBcapacity and262144dedupcap are not shown to fit this workload. The earlier §7 allocator-unknown statement is historical; this located combined-process measurement replaces it without claiming a metadata-only bound.

