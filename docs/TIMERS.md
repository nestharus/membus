# Persisted timer wakes

Timers persist schedules and compact progress through the existing MBK1 writers
and replay. They publish ephemeral MBC1 wakes through the bounded subscription
hub, without creating a public KV event key or launching payloads. A wake grants
no lease, durable delivery guarantee or receiver receipt. Application due-ID,
lease and result handling govern useful effects.

## Discovery and control

HELLO identity feature `;timers=1` advertises timer_missing (25) and a 34-byte
little-endian extension: u16 envelope version=1, u32 retained identity limit=4096,
u64 charge budget=8388608, u32 payload maximum=65536, u64 minimum repeat=100 ms,
u64 maximum jitter magnitude=86400000 ms. Timer-only HELLO returns codes 1..22
plus 25 and only timer extension bytes. With subscriptions it returns 1..25,
then 46 signal and 34 timer bytes. Legacy vocabularies remain unchanged.
The resident gates timer calls on negotiation.

The Rust client opts in with `AdditionalFeatures { timers: true }` through
`handshake_with_features`, `connect_pipe_with_features` or
`connect_tcp_with_features`. `feature_limits().timers` reports actual availability;
unavailable operations refuse locally before sending.

After the [KV1 common fields](KV1.md):

| Opcode | Suffix | Successful payload |
|---|---|---|
| 21 TIMER_SET | text id; u64 expected; text signal_key; u64 UTC anchor_ms; u64 period_ms; i64 jitter_low_ms; i64 jitter_high_ms; bytes payload; u8 ack (1 OS, 2 flush) | u64 generation |
| 22 TIMER_CANCEL | text id; u64 expected_generation | empty |
| 23 TIMER_STATUS | text id | u8 present; if present: u64 generation; u8 active; u16 error; u64 next_index; u64 next_due_ms; bytes schedule; bytes latest_envelope |

Set expectation is u64::MAX for unconditional, zero for absent, or an exact
generation. Cancel requires the current generation and always persists with OS
mode. Missing timer cancels refuse 25; stale generations refuse conflict (12).
Set returns only generation, without an achieved-ack field. Generations are
schedule mutation revisions retained across restart.

Namespace/id are nonempty, contain no NUL, and their combined UTF-8 byte lengths
plus three must fit 1024. Signal keys are nonempty and at most 1024 bytes.
Payload is at most 65536 bytes. Repeats have period at least 100 ms; zero is
one-off. Low/high jitter are inclusive, ordered, within +/-86400000 ms, and their
difference (high - low) must be strictly less than a repeat period. Both extreme first due times
must fit u64. Future time overflow completes the timer.

Set/cancel use bounded mutation dedup. An identical completed set does not rewind
a current generation; an old set retry after replacement can conflict (12).
That no-rewind boundary depends on retained history. After history is gone,
unconditional reuse can create a new generation. Timer control operation IDs
share the reserved internal namespace across *all* caller namespaces: make them
globally unique within the store and bind them to the intended timer. Changed
namespace/id/content under the same ID is compared through the scalar store's
[retained retry comparator](RETRY_RETENTION.md); a mismatch refuses
op_id_mismatch (14), subject to its encoding and collision-resistance limits.

`prepare_timer_set` and `prepare_timer_cancel` retain immutable typed requests,
original epoch and conservative retry horizon. Explicit `timer_set`/`timer_cancel`
never retry or reconnect. Retain uncertain results and reconcile status before
choosing another send; later status or refusal does not disprove an earlier
uncertain effect. A successfully applied cancel can reconcile absent. Cancellation
persists a tombstone and retains identity charge in the running timer state;
startup can reclaim that charge as described under capacity below. Completed
one-off status is present but inactive. Already queued/in-flight wakes can finish
after cancel or replace; neither retracts receiver bytes or revokes application
leases.

## Anchored time and catch-up

For index i, nominal time is `anchor + period*i` and due is `nominal + draw(i)`.
Draw uses deterministic rejection sampling over seeded SplitMix64, inclusive of
low/high. Seed derives from namespace/id and operation ID. This is scheduling
jitter, not cryptographic randomness. The versioned schedule and progress retain
the rule/draw across replay. Anchored offsets avoid cumulative drift but do not
bound adjacent intervals to the same percentage: period 60000 with offsets
[-9000,9000] permits 42000..78000 ms between dues.

One condition-variable owner keeps an ordered deadline set. Distant waits recheck
wall-clock corrections at most once per second. UTC scheduling is not real time:
forward jumps and overload coalesce overdue indices; backward jumps delay them.
Set/cancel/fire serialize on the timer mutex, including progress append, so
storage delays can delay other controls/dues. Hot committed KV reads remain
independent of this mutex.

At restart or normal overload, publish at most one catch-up for the latest due
index and set `missed_count = latest_index - next_index + 1`. An on-time wake has
count 1; covered earlier indices are count minus one. Completed one-offs do not
recur. This count is exact relative to successful progress checkpoints, not a
count of receiver misses: crash-boundary repeats can count already seen dues
again. There is no noncoalescing durable request-stream mode.

With values included, decode this envelope:

```text
magic MBT1; u16 version=1; text timer_id;
u64 generation; u64 index; u64 nominal_ms; u64 due_ms;
i64 draw_ms; u64 missed_count; bytes payload
```

`TimerEnvelope::decode` owns decoding. Namespace comes from the surrounding
change/status operation, not MBT1. Due identity is `(namespace, generation, index)`
and remains stable across resident epochs. MBC1 cursor remains epoch-scoped and
its revision is schedule generation, not a KV revision at signal_key. Timer MBC1
ack=1 describes OS-accepted configuration semantics; it is not progress durability
or receipt, even when set requested flush.

## Crash windows and lifecycle

Wake enqueue precedes OS progress append. Killing between them can repeat the
same due identity. Queue overflow, pipe failure or crash after checkpoint but
before delivery can lose the wake. Register live before setting a one-off and,
after gap/restart, register live before timer-status/application reconciliation.
Restart catch-up can precede watcher reconnect; bounded ring replay can also
expire. Latest status is not a complete fire history.

Any progress refusal, including transient pressure, stops that timer with
active=false and an error until replacement or restart. There is no automatic
checkpoint retry, failure wake or aggregate timer-error health field. In-memory
latest envelope records the published attempt even if its checkpoint failed;
after restart it returns to the last successful checkpoint (or none), allowing
the attempted identity to repeat. Preserve application effect results separately.

The executable starts timers after replay. An embedding using `Store::open` must
explicitly keep the `Store::start_timers` guard alive and drop it before the Store.
Only one owner per Store can run; this guard does not replace the cross-process
state lock. Drop joins the owner and can wait on the current progress operation.
Its ten-second deadline bounds admission, not admitted completion or shutdown;
the timer mutex and owner join provide no fixed shutdown-time bound.
Unexpected lock poison is fatal rather than a
state repair mechanism; supervise the resident.

## Persisted bytes and capacity

Internal `__membus_timers_v1` records are denied through public wire operations.
`s/namespace-NUL-id` holds MTS1: magic, three texts (namespace,id,key), five
64-bit fields (anchor,period,low,high,seed) and bytes payload. `p/namespace-NUL-id`
holds exactly 48 progress value bytes (the six envelope fields). Replace ignores
older-generation progress. Inconsistent persisted schedules/progress refuse
startup. Version-one draw/time/validation rules must not be silently reinterpreted
by another reader. The trusted Store embedding API is not a wire security boundary;
arbitrary internal writes can violate these invariants.

An MBK1 record is `58 + namespace bytes + key bytes + op_id bytes + value bytes`.
Each fire appends one 48-byte progress value, using a unique
`tf-EPOCH-GENERATION-INDEX` operation ID. Decimal ID lengths grow at powers of ten.
This is logical append cost, not physical I/O. Progress bypasses the ordinary
client dedup table on admission and replay; its owner makes one attempt and
stops on failure. It still obeys pending-key, queue, disk, index, namespace and
value limits. Set/cancel and ordinary mutations retain normal dedup behavior.

Identity charge is `256 + identity length`. A present schedule adds
`768 + 3*(namespace length + id length) + signal_key length + payload capacity`.
The 4096 identity/8 MiB limits include cancelled identities in the running timer
state. Replay checks the entries that survive Store startup's tombstone filtering.
When no other retained chunk holds a cancelled schedule key, startup can remove
its tombstone before timer restoration, releasing its timer identity charge even
while that chunk still contains the set/cancel records. This does not reactivate
the cancelled timer. Identity-charge retention across restart is therefore not
guaranteed by retained record bytes alone. Reuse fixed timer IDs for intentional
replacements to avoid exhausting retained identity capacity.
This is conservative representation accounting,
not a calibrated process RSS limit; KV and signal budgets also apply.

Current timer chunks use latest retention, so superseded chunks can retire under
[CHUNK-RETIREMENT.md](CHUNK-RETIREMENT.md). A chunk still holding a current record
remains pinned. Cancel does not promise immediate disk or identity reclamation.
There is no aggregate fire-rate admission bound: high-frequency timers share the
ordinary state ceiling with other writers and can exhaust it before retirement
can help. Capacity refusal stops progress. Monitor shared bytes and timer status;
no sustainable timer count/rate or physical-device cost is established here.
Older binaries may rebuild progress into ordinary dedup or ignore current
internal semantics; qualify downgrades against actual retained logs.

Controls in `src/timers.rs`, native client/forwarder tests and the existing
`tests/timers_native.py` exercise schedules, generation fencing, catch-up,
compact progress, retained identity limits and failure/replay windows. These
controls do not establish exactly-once effects, hard scheduling latency,
power-loss survival or complete receiver delivery.
