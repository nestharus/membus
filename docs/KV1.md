# membus.kv/1 protocol and storage behavior

The resident implements version 1.0 framing over Windows local named pipes and
loopback TCP. The supported native client is documented in
[RUST_CLIENT.md](RUST_CLIENT.md). Wire operations exceed that client's typed
surface; an opcode listed here does not imply a corresponding Rust Client method.
Values are opaque bytes. Namespace/key interpretation, operation identity,
application recovery and effect authorization belong to callers.

## Framing and discovery

Integers are little endian. A frame is:

```text
u32 body_length; u16 major=1; u16 minor=0; u8 opcode; u64 request_id; payload
```

`bytes` means `u32 length; bytes`; `text` is a nonempty UTF-8 byte string of at
most 1024 bytes. `bound` has the same encoding but permits empty text. Values
are at most 4 MiB; frame bodies at most 16 MiB; batch and GET_MANY at most 128
items. The transport checks length before allocating the body, and field
lengths/UTF-8/remaining input are validated by the decoder. No Unicode
normalization is applied. Numeric lexical order requires a caller encoding
such as fixed-width decimal keys.

The first request must be HELLO (1): `u32 max_frame; u8 allow_mem; text identity`.
Offer 1024..16 MiB. Success uses opcode `0x81`, the same request ID, then `u16
code=0` and:

```text
u64 epoch; u32 max_frame; u32 max_value; u16 max_batch;
u8 server_mem_enabled; u64 retry_window_ms;
u16 refusal_count; (u16 code; u8 transient; text name)*; feature extensions
```

The negotiated body limit is min(offered, 16 MiB) and bounds later requests and
replies. A connection processes requests serially; no multiplexing is negotiated.
Repeated HELLO is malformed. Unsupported versions refuse and close. Short or
oversize transport frames return an unassociated refusal with request ID zero
and close; truncation closes the connection. Aggregate ingress pressure can also
return unassociated busy and close. Association loss after a mutation send is
an uncertain outcome.

Identity suffixes select additive discovery vocabulary:

| Features | Refusal entries | Extension after entries |
|---|---|---|
| baseline | 1..19 | none |
| final `;scans=1` | 1..22 | none |
| `;subscribe=1` | 1..24 | 46 signal bytes |
| `;timers=1` without subscriptions | 1..22 plus 25 | 34 timer bytes |
| timers and subscriptions | 1..25 | signal bytes, then timer bytes |

Keep `;scans=1` last when combining features. The resident gates subscription
control (16..19) and timers (21..23) on their negotiated features. Scans are
advertised through refusal vocabulary; retirement operations (24..27) are
additive calls, with unsupported (2) on older implementations. See
[subscriptions](SUBSCRIPTIONS.md) and [timers](TIMERS.md) for extensions.
Identity and tokens do not authenticate application actors.

Every non-HELLO request begins with `u64 deadline_unix_ms; text op_id; text
namespace`. Read IDs have no retained deduplication or snapshot-creation lookup.
`__membus_timers_v1` and `__membus_policy_v1` are reserved and refused through
public requests. Replies use `opcode | 0x80`, the request ID, then `u16 code`
and successful payload; nonzero codes carry no successful payload.

## Key/value operations

A returned `value` is `u8 present`; when present, it continues with `u64 version;
u8 achieved_ack; bytes value`. Committed tombstones read absent.

| Opcode | Suffix after common fields | Successful payload |
|---|---|---|
| 2 GET | text key | value |
| 3 GET_MANY | u16 count; text keys | u16 count; values in input order |
| 4 PUT | text key; u8 ack; bytes value | u64 version; u8 achieved_ack |
| 5 CONDITIONAL_PUT | text key; u8 ack; u64 expected; bytes value | u64 version; u8 achieved_ack |
| 6 REMOVE | text key; u8 ack; u64 expected | u64 version; u8 achieved_ack |
| 7 BATCH | u16 count; (u8 mutation_opcode; text item_op_id; mutation suffix)* | u16 count; (u16 item_code; u64 version; u8 ack)* |
| 8 HEALTH | empty | six u64: bytes_written, flush_calls, keys, namespace_keys, namespace_quota, index_cap; u8 high_water; u8 namespace_high_water |
| 9 IO_METRICS | empty | three u64: data append bytes, write calls, accounted syncs |

BATCH accepts PUT, CONDITIONAL_PUT and REMOVE. It validates the whole request
before independently admitting items. It is non-atomic; different keys may
execute concurrently, same-key items and replies retain input order. Retry uses
the original item IDs. GET_MANY reads each committed value separately and is not
a multi-key snapshot. Oversize replies refuse rather than truncate; bound read
sets and use snapshots for a common view.

`expected=0` requires absent state (including tombstones and expired keys).
A nonzero expected value is an opaque live revision token, not a timestamp or
publication-order sequence. REMOVE writes a tombstone. A competing conditional
write against an admitted pending key returns pending_conflict (19); a mismatch
against committed state returns conflict (12). A pending remove does not yet
make a key absent. Unconditional writes against a pending key return busy (6).

## Acknowledgments, publication and uncertainty

| Ack | Wire value | Boundary |
|---|---:|---|
| mem | 0 | Disposable success; no committed publication or restart guarantee |
| os | 1 | Complete append, then committed publication |
| flush | 2 | Complete append covered by file sync, then committed publication |

Memory mode requires both HELLO opt-in and server `--allow-mem`. It does not
supply a speculative read API or eventual promotion. The public Rust Client
offers only OS and Flush and requires an explicit Ack.

A successful durable acknowledgment follows publication: a later current GET
sees that revision, a later published value, or subsequent removal/expiry.
An already open snapshot retains its older view. Committed GET reads take the
memory index's read lock, without append/flush/admission locks or backing-file
I/O. Publication and snapshot capture can contend on that memory lock; this is
not a lock-free or latency-bound claim. [Grouped flushing](GROUPED-FLUSH.md)
explains the distinct append and covering-sync paths.

OS acknowledgment covers process-kill replay, without an OS-crash/power-loss
promise. Flush adds synchronization of the covering file; filesystem and
hardware/cache behavior still limit persistence evidence. An unacknowledged
write may disappear or replay. A complete record left after I/O failure can
replay even though the original result was uncertain.

The acknowledgment byte in a recovered record states the requested mode, not
proof that a covering live sync succeeded. Before exposing recovered values or
retry outcomes, startup synchronizes present, nonempty active chunks, including
closed chunks, and active legacy stripe logs. Truncation precedes this barrier;
recovered Close publication follows it. Required sync failure refuses startup
and attempts a non-ready report; report publication itself can fail. Already
imported read-only backups do not receive a new data-file sync witness. See
[RECOVERY_FLUSH.md](RECOVERY_FLUSH.md) for ordering and historical backup limits.

Fresh mutation admission checks the absolute Unix-millisecond deadline
immediately before reserving the key/version. Deadline (5) means this send was
refused before admission. Clock conversion and a separate local monotonic budget
are caller responsibilities. After admission, I/O may finish beyond the request
deadline. A duplicate waiting past its own deadline returns
pending_timeout_uncertain (17) without cancelling the original. Local timeout,
reset, malformed/foreign reply or lost reply after send must preserve the original
uncertainty. A later refusal never proves an earlier uncertain send had no effect.

## Retry identity and refusals

Ordinary mutation IDs are scoped to namespace. The store retains a 32-byte
SHA-256 comparator over the complete canonical mutation representation with
generated version and admission time zeroed. Key, bytes, expected token, remove
flag, ack, namespace and operation ID participate; deadline does not. Matching
comparators join/return the retained outcome; a mismatch refuses op_id_mismatch
(14). Equality relies on SHA-256 collision resistance, not retained full-payload
byte equality. The existing encoding aliases expected `None` and
`Some(u64::MAX)` through the same sentinel. Full durable records are unchanged;
replay/import derive the same comparator. See
[RETRY_RETENTION.md](RETRY_RETENTION.md) for encoding, collision and resource
limits. Policy setter IDs retain the caller namespace scope; timer control IDs
share one internal scope across all caller namespaces.

The default horizon is 24 hours from admission (`--retry-ms`). Successful durable
outcomes within the horizon are rebuilt from records on restart. Conflicts and
pre-admission refusals are not persisted outcomes; mem and failed-I/O outcomes
are memory-only. Active dedup entries and the retired-ID ring each default to
262144 slots (`--dedup-cap`). A known expired ID refuses (13); after ring eviction
or restart an expired ID can be unknown and execute again. This is bounded
retry deduplication. Beyond the original epoch/horizon, or after loss of the
client Attempt, reconcile a deterministic key and embedded operation identity
before deciding whether another send is safe.

The HELLO transient flags are:

| Code | Name | Transient |
|---|---|---|
| 1 | incompatible_version | no |
| 2 | unsupported | no |
| 3 | malformed | no |
| 4 | too_large | no |
| 5 | deadline | yes |
| 6 | busy | yes |
| 7 | capacity | no |
| 8 | quota | no |
| 9 | index_cap | no |
| 10 | read_only | yes |
| 11 | warming | yes |
| 12 | conflict | no |
| 13 | retry_window_expired | no |
| 14 | op_id_mismatch | no |
| 15 | mem_not_enabled | no |
| 16 | io_failed_uncertain | yes |
| 17 | pending_timeout_uncertain | yes |
| 18 | dedup_full | yes |
| 19 | pending_conflict | yes |
| 20 | cursor_expired | no |
| 21 | invalid_cursor | no |
| 22 | snapshot_full | yes |
| 23 | resume_gap | no |
| 24 | subscription_missing | no |
| 25 | timer_missing | no |

Transient is classification, not automatic retry authorization. Busy, byte
capacity, namespace quota, index slots and dedup pressure are distinct. Read-only
requires storage/restart reconciliation. Codes 16/17 explicitly preserve uncertain
outcomes; never turn them into proof of absence or a new operation identity.

## Namespace snapshots and scans

Capture retains one immutable whole-namespace committed view, excluding
tombstones and mem mutations. It copies keys and Arc handles rather than payload
bytes; later changes, expiry or file retirement do not alter that view. Several
queries/connections can share a token in the same namespace and epoch. Separate
captures may see different revisions. There are no multi-key write transactions.

| Opcode | Suffix | Successful payload |
|---|---|---|
| 10 SNAPSHOT_OPEN | empty | bytes snapshot |
| 11 SCAN_PREFIX | bytes snapshot; bytes cursor; u16 limit; u32 max_bytes; bound prefix | bytes snapshot; bytes next_cursor; u8 done; u16 count; (text key; value)* |
| 12 SCAN_RANGE | bytes snapshot; bytes cursor; u16 limit; u32 max_bytes; bound lower; bound upper | same page layout |
| 13 SNAPSHOT_RELEASE | bytes snapshot | empty |
| 14 SCAN_KEYS_PREFIX | same as 11 | same page header; text keys without values |
| 15 SCAN_KEYS_RANGE | same as 12 | same page header; text keys without values |

Wire scans with empty snapshot and cursor capture implicitly; the Rust Client
uses an explicit `snapshot_open` token. Empty cursor starts a query. Prefix empty
selects all live keys. Ranges are half-open [lower, upper) in UTF-8 byte order;
empty upper is unbounded, empty lower starts at the beginning. Equal nonempty bounds
succeed empty; lower above a nonempty upper is malformed. Limit is 1..128;
max_bytes is at least 42 and bounds the whole reply body including its 15-byte
header, excluding the four-byte length. The negotiated frame limit also applies.

Snapshot tokens are opaque 16-byte epoch/ID pairs. Cursors bind token, absolute
row position, opcode and bounds with a snapshot-local keyed hash; they are
bounded by `2*MAX_ID+48` bytes. Preserve them as opaque bytes. This binding is not
cryptographic authority. Query/position/tag/namespace mismatch refuses
invalid_cursor (21); missing, expired, released or old-epoch snapshots refuse
cursor_expired (20). Structural cursor validation can precede token lookup.

The same token/cursor/query can be paged again until expiry/release; page limits
may change. If its first matching item cannot fit, a page refuses too_large (4)
without advancing. An existing token survives this refusal; an implicitly
created token is removed on page failure. Done means no continuation and does
not release retention. Release explicitly, including error paths. A lost
snapshot-creation reply cannot be recovered by read operation ID; recreating
allocates a different view until old retention expires. After expiry, restart
all related queries on a single fresh view rather than mixing pages.

Defaults are 8 snapshots, 256 MiB aggregate conservative charge and a fixed
30-second monotonic lifetime from capture entry (`--snapshot-max`,
`--snapshot-bytes`, `--snapshot-ttl-ms`). Pages do not renew it. Charge per view
is `256 + namespace bytes + sum(key bytes + 128 + value Vec capacity)` across
the whole namespace, even for narrow or keys-only queries. Active page handles
retain charge after release/expiry until their last reference drops. The
maintenance reaper removes expired registry entries; this is not a renewal or
unbounded retention facility.

Capture makes two traversals of the global committed hash index under a read
lock, charging before allocating owned rows, then copies namespace keys/handles.
It releases the committed and registry locks before sorting. Capture can delay
publication and other snapshot registry operations; paging reads retained data
without append/flush locks. Payload encoding and concurrent response buffers
add transient memory beyond the snapshot budget. With 64 connections and 16 MiB
page budgets, returned bodies alone can approach 1 GiB, with additional temporary
copies. Provision page size/concurrency separately from retained charges.

Opcode 20 CAPTURE_METRICS returns seven cumulative u64 values: captures,
read_lock_wait_ns, charge_walk_ns, selection_walk_ns, namespace_copy_ns,
read_lock_held_ns, sort_and_release_ns. These instrument completed captures;
concurrent atomic reads are not a transactional metrics snapshot.

## Storage, capacity and related operations

Records retain `MBK1; u32 payload_length; u32 bitwise_length_complement; u32 CRC32;
payload`. Payload records admission time, revision, ack/remove, namespace, key,
operation ID, expected token and bytes. CRC detects corruption, not identity.
Four stable key-hash stripes route records to class-specific chunks. Replay
resolves latest versions and retained outcomes before service. Open incomplete
tails may be truncated after recording evidence; complete corruption refuses.
There is no replay index checkpoint or warm/cold tier. See
[chunk retirement, import and replay](CHUNK-RETIREMENT.md) and
[catalogue append recovery](CATALOGUE-APPEND-RECOVERY.md).

Default namespace quota is 500000 live keys. Default global index cap is 917000
slots including tombstones and expiry summaries. Reservations count at admission.
A remove frees a live namespace slot at commit but its tombstone retains an index
slot until no other retained chunk can contradict it. Expiry summaries occupy
index slots for 30 days. Values, retained 32-byte retry comparators and reservations
share a 256 MiB logical capacity budget; an overwrite reserves replacement bytes
while the old value remains visible. Queue admission charges the complete canonical
mutation representation plus value length plus 128 bytes, independently of the
comparator size; durable disk admission reserves the complete MBK1 record.
Identifiers, outcomes and bookkeeping still consume per-operation memory.
Queue/snapshot/signal/timer budgets are additional,
not a measured RSS ceiling. Retained data defaults to 8 GiB, with the executable
reserving part for maintenance; [RESIDENT.md](RESIDENT.md) lists options.

Opcodes 16..19 manage [subscriptions](SUBSCRIPTIONS.md), and 21..23 manage
[timers](TIMERS.md). Opcodes 24..27 set/get retention policy, inspect key expiry
status and return HEALTH2, as described in [CHUNK-RETIREMENT.md](CHUNK-RETIREMENT.md).
Neither change transport nor timer status establishes receipt, lease authority
or useful application completion. Indexed publication/query semantics are in
[INDEXED_COLLECTIONS.md](INDEXED_COLLECTIONS.md).
