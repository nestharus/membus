# Epoch-scoped change subscriptions

Subscriptions send bounded change hints to explicit local Windows named-pipe
endpoints. A complete pipe write is transport progress, not receiver receipt,
durable work acknowledgment or permission to execute an effect. Subscriptions,
status and history are memory-only and disappear on resident restart.

## Discovery and controls

HELLO identity feature `;subscribe=1` selects codes 1..24 and a 46-byte extension
after the refusal list. Combine with final `;scans=1` for scan discovery against
older servers. The extension is little endian:

```text
u16 change_version=1; u32 max_frame=16777216;
u64 history_items; u64 history_bytes; u16 status_slots=64;
u16 queued_items=256; u64 subscription_bytes=8388608;
u64 global_queue_bytes=16777216; u32 write_ms=100
```

The resident gates opcodes 16..19 on subscription negotiation. Rust uses
`Options.subscriptions`, reports `Negotiated.subscriptions`, and exposes typed
`feature_limits().signals`. Zero retained history still permits live subscriptions.

All requests use the [KV1](KV1.md) deadline/operation ID/namespace prefix.
Subscription controls do not deduplicate operation IDs.

| Opcode | Request suffix | Successful payload |
|---|---|---|
| 16 SUBSCRIBE | bound prefix; text endpoint; u8 include_value; bytes cursor; text protocol; u32 max_frame | bytes subscription; bytes starting_cursor |
| 17 UNSUBSCRIBE | bytes subscription | empty |
| 18 SUBSCRIPTION_STATUS | bytes subscription | u8 active; u8 status; u32 queued; u64 charged_bytes; u64 written; u64 failed; u64 dropped; bytes last_written_cursor |
| 19 SIGNAL_USAGE | empty | nine u64: head, floor, history_count, history_bytes, queue_bytes, status_slots, history_max, history_budget, queue_budget |

The namespace is exact and prefix is a UTF-8 byte prefix, permitting empty and
without normalization. Protocol must be `membus.change/1`. include_value is 0/1.
max_frame is 1024..16 MiB for the change body, separately selected from the
request connection's ingress limit. An oversize replay refuses too_large before
registration; an oversize live event deactivates/drops the subscription.

Endpoints must be local `\\.\pipe\...` paths. Empty, dot, dot-dot, trailing-dot
or trailing-space segments, slash, colon and NUL are rejected. The writer opens
with identification-level security QoS. Endpoint security and peer/application
authenticity remain deployment concerns; protocol tokens do not authenticate actors.

Tokens/cursors are opaque 16-byte little-endian epoch/ID pairs. Tokens are
namespace-bound; missing, evicted or old-epoch tokens refuse subscription_missing
(24). Cursors use a global publication sequence, can be reused with another
filter, and do not certify complete processing by any receiver. Empty cursor
starts at the current head without replay.

## Change frames and publication order

A `membus.change/1` frame is:

```text
u32 body_length; magic MBC1; u16 version=1;
16-byte subscription token; 16-byte cursor;
text namespace; text key; u64 revision; u8 achieved_ack;
u8 removed; u8 include_value; [bytes value when included]
```

Included KV bytes belong to that publication; a later GET can see another value.
A tombstone has removed=1 and included empty bytes when values were requested.
The cursor orders committed publication, which can differ from revision admission
order across stripes. The same publication has the same cursor across matching
registrations. Signals can precede the origin's reply. Refused writes, retained
idempotent retries, disposable mem writes and restart replay emit no KV changes.

Publication assigns a cursor and enqueues under the committed write lock, giving
per-subscription FIFO publication order without endpoint I/O there. One delivery
worker picks registrations round-robin, encodes outside publication locks and
writes through a persistent pipe connection. Written advances only after a
complete frame; no FlushFileBuffers/flush-to-reader call is made on that pipe.
Cached connections are bounded by status slots and closed on deactivation,
unsubscribe or Store teardown.

The entire write has a 100 ms deadline, including busy-pipe open retries. Missing
pipes fail immediately. A slow receiver or full kernel buffer can fail delivery.
Cancellation drains overlapped completion before releasing buffers/handles.
Receivers must discard partial frames and recover using their last fully parsed
cursor. Complete buffered frames can remain readable after writer closure; an
OS error alone cannot count as a decoded/associated event.

## Retention, queues and cleanup

History defaults to 4096 changes and 16 MiB of conservative charge globally,
configurable with `--signal-history` and `--signal-bytes`; zero disables retention.
Charge is `256 + namespace bytes + key bytes + value Vec capacity`, even for
key-only consumers. Oldest eviction advances floor. There is no minimum time
horizon or disk change log. Resume before floor, beyond head, malformed, or in
another epoch refuses resume_gap (23) without registration; a cursor at floor
is valid.

Resume queues retained matching events and registers live changes under the
same hub mutex. Catch-up exceeding 256 queued items, 8 MiB per registration or
16 MiB globally refuses busy before registration rather than truncating. Full
history capacity does not guarantee one subscriber can queue it all. During live
overflow the subscription becomes dropped/inactive and queued work is cleared;
the KV write still commits. Endpoint failure marks failed/inactive and clears
queued work. Slow subscribers can delay others through the single worker and
global pressure; there is no arbitrary-fanout latency guarantee.

Status is 0 queued, 1 written, 2 failed, 3 dropped, 4 cancelled. A fresh empty
registration reports written with zero writes and its starting cursor; this is
not a receipt. Queue count excludes the one delivery in flight, while charged
bytes include it. Encoding/in-flight temporary buffers add memory beyond logical
ring/queue charges. These are bounded representation accounts, not exact RSS.

At most 64 status slots retain active registrations and terminal records. Inactive
slots with no in-flight charge can be evicted for another registration. IDs do not
reuse within an epoch. Quiet registrations have no lease/idle expiry, and a quiet
dead endpoint is not detected until delivery. Unsubscribe explicitly. It clears
queued work but an already started delivery may finish after the call; terminal
state remains cancelled/dropped while counters can still advance.

A lost subscribe reply can leave a registration whose token is unknown. Do not
automatically retry creation or accumulate registrations; keep that uncertainty
and a cleanup owner. Known tokens require explicit cleanup, and a token alone
is not durable cleanup across owner loss. Closing a listener does not unsubscribe
its server token. Restart clears every registration, status row and history.

## Native client and reconciliation

On Windows, bind `ChangeListener` before subscribing. `subscribe` requests no
values/resume and uses the negotiated request frame bound. `subscribe_with` takes
`SubscribeOptions { include_values, resume, max_frame }`. Receive checks token,
namespace, prefix and epoch association. A quiet timeout preserves a clean stream;
partial timeout, truncation, malformed or foreign input retires the connection.
Failed reset closes the handle and requires an explicit new bind; successful
reset accepts another writer. `cancel`/`close` release the endpoint after I/O
drain. Exclusive mutable access is required; they are not cross-thread async
cancellation calls. See [RUST_CLIENT.md](RUST_CLIENT.md).

Track the receiver's last fully parsed/handled cursor separately from server
last_written_cursor. Persisting after handling can repeat a change after receiver
failure, so application deduplication remains necessary. On a gap/restart,
register live first, then capture current state and reconcile buffered changes
by their KV revisions. Registering after capture leaves a race. Immutable event
consumers also need state/retention policy to retain their reconstruction data.
Snapshot TTL, page bounds, periodic refresh and release remain caller obligations.

[Timers](TIMERS.md) share the same transport with MBT1 values. They create no
public KV signal-key value: MBC1 revision is timer configuration generation,
not a KV revision at that key. Use a dedicated prefix, envelope due identity and
timer-status/application reconciliation. Catch-up may precede watcher reconnect.
Neither timer status nor change transport establishes an effect receipt or lease.

`src/signals.rs` unit tests, `tests/client_native.rs`,
`tests/client_forwarder.rs`, `tests/forwarder_native.rs` and listener regression
controls exercise bounded delivery and failure handling. The existing
`tests/subscribe_native.py` is an additional native harness. Test code and
historical measurements do not certify arbitrary endpoint security, complete
receipt or a universal delivery deadline.
