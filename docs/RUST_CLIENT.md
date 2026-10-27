# Resident Rust client

`membus::client::Client<T: Transport>` owns one connection to an explicit existing
resident endpoint. It uses the library's KV1 encoder/decoder and Windows
overlapped pipe transport. It does not open a Store or backing state. The caller
chooses endpoint, identity, namespace, operation IDs, acknowledgment, deadlines
and recovery policy. See the [README example](../README.md) for a Windows
connection and mutation; loopback TCP uses `Client::connect_tcp` with an explicit
`SocketAddr` for foreground tests.

The Client carries the admitted local principal's powers. A hosted service must
derive and authorize tenant scope before sending; see the proposed
[tenant admission boundary](TENANCY_BOUNDARY.md). Connection reuse and negotiated
features do not authenticate an application actor.

## Operations and discovery

`Options::new(identity, max_frame)` requires a nonempty identity without `;` and
a 1024..16 MiB ingress bound; appended feature suffixes must also fit the 1024-byte
identifier limit. HELLO validates version/opcode/request ID, refusal vocabulary,
limits and epoch. `negotiated()` returns actual values, not policy defaults.

The typed surface includes:

- `get` and `get_many` return committed `Option<CommittedValue>` with revision,
  achieved Ack and bytes. GET_MANY preserves input order and validates exact
  reply count; it does not capture an atomic common view.
- `prepare(MutationRequest)` and `mutate(&mut Attempt, Deadline)` perform PUT or
  conditional PUT. `expected: None` is unconditional, `Some(0)` absent, and
  `Some(version)` a live revision check. Select `Ack::Os` or `Ack::Flush` explicitly.
  Memory acknowledgment and BATCH have no typed Client methods.
- `prepare_remove(RemoveRequest)` and `remove(&mut RemoveAttempt, Deadline)`
  perform the existing conditional REMOVE. `expected: 0` requires absence;
  other values check an exact live revision. Success writes a tombstone, with
  the explicitly requested OS or Flush acknowledgment. `validate_remove`
  preflights the exact body size without sending. There is no unconditional
  typed removal: the wire always carries its expected revision. The operation
  has no value payload, including no empty value-length field.
- `validate_mutation` uses the same encoder to check identifiers, value and frame
  size without sending. It returns encoded body bytes, without establishing
  connection readiness or changing prepare/Attempt state.
- `snapshot_open`, `scan_prefix`, `scan_range` and `snapshot_release` use explicit
  caller-owned namespace snapshots. Prefix/range methods return value pages;
  the wire keys-only opcodes have no typed Client methods.
- `subscribe`, `subscribe_with`, `unsubscribe` and `subscription_status` expose
  change control; `Change::decode` decodes MBC1 bodies. Windows `ChangeListener`
  owns a receiving endpoint.
- `prepare_timer_set`, `prepare_timer_cancel`, `timer_set`, `timer_cancel` and
  `timer_status` expose persisted timers; `TimerEnvelope::decode` decodes MBT1.

Set `Options.scans` for the final `;scans=1` suffix and inspect `Negotiated.scans`.
A legacy 19-code server can connect while reporting scans unavailable.
`Options.subscriptions` requests the subscription vocabulary and typed signal
limits; zero history still permits live registration.
`AdditionalFeatures { timers: true }` opts into timers through
`handshake_with_features`, `connect_pipe_with_features` or
`connect_tcp_with_features`, preserving ordinary constructors and Options fields.
`feature_limits()` contains optional signal/timer limits; missing capabilities
refuse before sending. Timer-only discovery has codes 1..22 plus 25; combined
features have 1..25 and signal limits before timer limits. See [KV1.md](KV1.md).

A caller-supplied Transport must enforce each supplied monotonic deadline on
reads and writes. Built-in transports reset remaining budgets before each
transfer. The client serializes operations on its connection; it does not
multiplex, reconnect or retry automatically. Transport, malformed payload or
association failure makes the connection unusable (`is_usable()` becomes false).
Reconnect constructs a new Client and negotiates its actual epoch.

The client retains each bounded, header-validated reply body while its typed
decoder borrows the payload. It does not allocate and copy the complete payload
into a second temporary buffer. The received allocation includes its 15-byte
version/opcode/request-ID/status header until decoding finishes. Returned values,
keys and cursors still own their data independently of that buffer and transport;
no reply buffer is cached or retained by a returned value. Payload validation and
connection invalidation on malformed input follow the same typed decode path.

Native epoch equality does not identify a logical store across independent
endpoints. A future placement adapter must preserve source/partition identity,
uncertainty and opaque handles; see [routing contracts](SHARD_ROUTING.md).

## Stable attempts and uncertainty

A `Deadline` contains two caller-supplied clocks: absolute Unix milliseconds for
server admission and local `Instant` expiry for transport. Convert application
budgets explicitly; neither value substitutes for the other.

Across a gateway or worker, preserve the original budget through queueing and
reconnect, with an explicit remote clock policy. See [deadline boundaries](DEADLINES.md)
for propagation, timeout uncertainty and cancellation/settlement distinctions.

A MutationRequest retains the complete logical mutation, including caller
operation ID, namespace, deterministic key, bytes, Ack and expectation. `prepare`
is for a new intended operation. It validates identifiers/value and captures the
negotiated epoch/horizon; `validate_mutation` provides full frame preflight.
`Attempt` keeps that immutable request, original epoch and conservative retry
horizon starting before its first send.

A second explicit `mutate` on the same Attempt refuses epoch change or expired
horizon before sending. With a zero negotiated retry horizon, an initial send
is possible and a second send refuses. Success requires a nonzero revision and
exactly the requested achieved acknowledgment. The API never upgrades/downgrades
Ack silently.

`Error::Uncertain { epoch, operation_id, cause }` retains transport, association,
payload or achieved-mode failures after send and server refusals 16/17. Committed
conflict (12) and pending conflict (19) remain distinct typed refusals, as do
other definitive refusals. A transient flag does not authorize a retry. A later
status/refusal cannot erase uncertainty about the original send.

Retain the Attempt and original uncertain result. If the caller decides on an
explicit reconnect/retry, reuse the original Attempt only within its original
epoch, horizon and application budget. Do not prepare again to reset those
boundaries. After losing the token or crossing epoch/horizon, reconcile the
deterministic key and embedded operation identity and make an explicit application
decision. Fresh connection, identity or operation ID is not evidence of absence.
These in-memory tokens are not a second journal or permanent exactly-once guarantee.

`RemoveAttempt` retains its complete immutable `RemoveRequest` and reuses the
same mutation completion path, epoch check and original first-send retry bound.
Retain it and the original uncertainty across an explicit reconnect. A duplicate
returns the original retained removal outcome; it does not remove a newer value
that replaced that tombstone. Conflict, pending conflict, deadline, capacity and
unsupported remain distinct refusals. Lost, partial, foreign, malformed or
wrong-Ack replies and refusals 16/17 retain uncertainty. A later refusal does not
erase a prior uncertain removal. Neither request accessors nor a new prepare
call restore lost retry context after caller restart.

Removal is one key's conditional mutation. It does not atomically check another
key, a snapshot or phantom additions; transact a collection of records; implement
archive eligibility; or authorize an OS effect. Its tombstone/retry retention
remains governed by the existing Store policy and resource contracts.

Physical restore/clone can reuse an epoch from an abandoned history. Equal
negotiated epochs alone cannot authorize reuse of an old Attempt across that
boundary. Preserve its uncertainty for application reconciliation; see
[backup lineage and cutover](BACKUP_RECOVERY.md).

The caller's Attempt still retains its full immutable request. The resident's
retained retry comparison instead uses a 32-byte SHA-256 comparator, subject to
collision resistance and the existing expected-revision sentinel alias; see
[RETRY_RETENTION.md](RETRY_RETENTION.md). Successful startup requires the
[recovered data-file sync barrier](RECOVERY_FLUSH.md) before exposing recovered
values/retry outcomes, with the historical imported-backup limit stated there.
Neither representation nor that barrier removes caller-owned uncertainty.

## Snapshots and subscription ownership

Enable scans, open a snapshot, retain its token across every related query and
page, and explicitly release it on success/error. `ScanPage` contains snapshot,
cursor, done and key/value items. The client checks token association, ordering,
bounds and progress; preserve cursors as opaque bytes. Done does not release.
Fixed expiry requires restarting related queries on one fresh view. Bound total
pages/bytes/work as well as individual requests. [KV1.md](KV1.md) gives charge,
TTL and frame limits. Snapshot creation is not deduplicated by read ID; an
unknown open outcome may retain a view until expiry.

The current typed `scan_range` requires `lower < upper` in UTF-8 lexical order.
It rejects equal bounds and an empty unbounded upper bound, although the wire
operation accepts those cases. This is a client-surface restriction; the wire
range contract in [KV1.md](KV1.md) remains distinct.

Bind a Windows ChangeListener before registration. `subscribe` requests no values
or resume cursor; `subscribe_with` takes
`SubscribeOptions { include_values, resume, max_frame }`. This receive-frame
bound is separate from the request ingress bound and cannot exceed advertised
signal max_frame. A resume cursor is epoch-scoped. Unknown registration outcomes
must retain a cleanup obligation; creation is never automatically retried.
Known tokens need an explicit cleanup owner, including across caller failures.
Tokens alone do not provide durable cleanup after owner loss.

`ChangeListener::bind` owns a caller-selected local first-instance pipe and
rejects remote clients. `receive` validates token, namespace, prefix and cursor
epoch. Quiet timeout preserves a clean stream; complete buffered frames can be
decoded after writer closure. Success always requires decoding and association,
not inference from a native error. Partial timeout, truncation, malformed or
foreign input retires the connection. Successful reset accepts another writer;
failed native reset closes the handle, subsequent receive returns Closed, and
the caller must bind again explicitly. `cancel`/`close` release the endpoint
after I/O drainage and require exclusive mutable access. They are not a
cross-thread cancellation channel and do not unsubscribe the server token.

The caller owns buffering hints during capture, periodic refresh, receiver
cursor persistence, unsubscribe and application reconciliation. Included KV bytes
belong to that publication; later GET can see newer state. Server written/status
is not receiver receipt. See [SUBSCRIPTIONS.md](SUBSCRIPTIONS.md).

Callers caching GET results also need an explicit read/freshness contract and
protection against delayed fills crossing invalidation, absence or epoch change.
See [external client caching](CLIENT_CACHE.md) for the proposed adapter contract;
the Client does not implement a read-through or coherence layer.

## Timers and authority

Timer attempts retain full immutable typed requests with original epoch/horizon.
`TimerExpectation` distinguishes unconditional, absent and exact generation.
Set returns only generation; cancel returns no payload and uses fixed OS mode.
Control IDs must be globally unique across timer namespaces. Preserve original
uncertain outcomes even when status or a subsequent explicit retry settles
current state. Timer status returns version-bound schedule/latest envelope.

Timer change revisions are configuration generations. Signal keys do not name
public KV values; decode MBT1 and reconcile timer/application status instead of
GET at that key. Wakes precede progress persistence, can repeat after restart,
can be lost after checkpoint, and stop on progress refusal. Cancellation retains
identity charge in the running timer state, but startup can release it when Store
filters out the tombstoned schedule before restoration. Retained set/cancel bytes
alone do not guarantee retained timer identity charge across restart.
[TIMERS.md](TIMERS.md) states the limits, startup behavior and crash windows.

Publication, GET, status, a decoded hint or attempted command establishes no
useful duty completion or lease authority. Connection admission carries shared
local-principal powers. HELLO identity, tokens, generations and frame association
do not authenticate application actors or peers. Endpoint authenticity, effective
OS security and separation of admitted actors remain deployment concerns.

## Available checks

HELLO, GET, batched GET, mutation, scan and snapshot-control requests encode into
one final frame after validating their inputs. Mutation preflight computes that
same validated size without encoding the value. HELLO writes borrowed feature
suffixes in timers/subscriptions/scans order and checks the expected refusal set
without a temporary integer list. A refused bootstrap HELLO materializes only
the returned legacy refusal; later operations use the negotiated vocabulary.
Connection, deadline, frame admission and request-ID behavior are preserved.

GET, batched GET and value scans validate their entire reply before transferring
the receive buffer to a present final value. Transfer requires capacity at most
twice that value's length; small tails copy only their bytes, and earlier values
and cursors retain independent ownership. Scan slot reservations are bounded by
both the validated count and received bytes, while decoding retains malformed
reply diagnostics and connection poisoning. These are call-owned buffers, not
cached results or changes to freshness or snapshot lifetime.

`cargo test --offline --locked` runs Rust controls; some are Windows-only and
launch isolated residents. `tests/client.rs` supplies independent expected frames
for negotiation, reads, mutation encoding, preflight, refusals and uncertainty.
`tests/client_native.rs` exercises this package's resident, including restart,
pending conflicts, scans and zero-history subscriptions. `tests/client_forwarder.rs`
adds typed timer/listener frame controls. `tests/forwarder_native.rs` and the
library listener regression module use an explicitly selected external resident.

On Windows those external-resident controls require both:

- `MEMBUS_FORWARDER_TEST_ROOT`: a fresh private fixture storage directory.
- `MEMBUS_FORWARDER_TEST_SERVER`: absolute path to the selected resident executable.

Select and preserve the intended executable's source/build/hash identity before
running these controls; they do not discover an installed service automatically.
`MEMBUS_CLIENT_TEST_ROOT` is optional for package-resident tests, otherwise
fixtures use ignored `target/client-fixtures`. Child support starts residents
suspended with NoWindow, places them in an owned Job before resume, directly
waits them and checks Job settlement. Use finite resource/runner bounds when
running process tests. Test presence or passing against one selected image does
not establish installed compatibility, a kernel cancellation race or universal
endpoint security. Listener controls distinguish complete, idle, partial,
malformed, closed/reset and endpoint-reuse paths.

`tests/client_remove.rs` independently fixes REMOVE wire bytes, exact revisions,
acknowledgments, preflight, refusal classifications and original-token retry
bounds with in-memory transports. `tests/store_remove.rs` connects that typed
client to real Store dispatch on a required explicit `MEMBUS_REMOVE_TEST_ROOT`.
It covers conflict/absence, duplicate removal after a newer replacement,
replay, reply loss after commit and failed live/recovery sync ordering. These
authored library controls retain their files and start no resident endpoint or
timer owner. They do not qualify process-kill transport, physical durability,
complete backup/restore or durable client-operation custody.

The same Store controls demonstrate why independent key CAS and BATCH cannot
replace conditional atomic publication: two key reservations both succeed from
one captured population, and quota pressure leaves one batch item committed.
`tests/recovery_boundary.rs` preserves an authored stopped image and demonstrates
that its clone can reuse the original epoch and accept an original uncertain
removal. These are counterexamples to stronger guarantees, not implementations
of atomic publication, complete capture/restore or independent authority lineage.
