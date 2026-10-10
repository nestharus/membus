# Persisted timer wakes — F-E6-41

Implemented std-only Rust, no per-agent process or payload launcher. Schedule/configuration and progress use existing MBK1 stripe writer/replay; delivery uses the MBC1 ring and bounded subscription queues. A fire is an ephemeral wake and creates no KV event key. MBC1 ack describes accepted timer configuration (os=1), not durable signal/receiver receipt.

## Discovery and control

Identity feature `timers=1` adds refusal25 `timer_missing` and a34-byte hello extension after the subscription extension when also negotiated: LEu16 envelope version1, u32 retained identity limit4096, u64 timer charge budget8MiB, u32 payload maximum65536, u64 minimum repeat100ms, u64 maximum jitter magnitude86400000ms. Refusal25 is the capability marker. Old hellos keep exact19/22/24-code lists and extension lengths. Connection negotiation gates timer operations; Python refuses unsupported before sending to an old server. Existing client signatures are unchanged.

Timers-only hello has23codes (base/scan1–22 plus25), followed by only34timer bytes;23/24subscription codes and46subscription bytes occur only with subscribe=1. With both features,25codes and80extension bytes. Store::start_timers returns an owned guard and refuses a second owner; dropping the guard joins it before another owner can start.

After usual deadline/op_id/namespace prefix:

| Opcode | Request suffix | Reply suffix |
|---|---|---|
|21 timer_set|text id; u64 expected generation (MAX unconditional,0 absent); text signal key; u64 UTC anchor ms; u64 period ms (0 one-off); i64 jitter low; i64 jitter high; bytes payload; u8 ack1/os or2/flush|u64 generation|
|22 timer_cancel|text id; u64 expected generation|empty|
|23 timer_status|text id|u8 present; if1: u64 generation,u8 active,u16 error,u64 next index,u64 next due,bytes schedule,bytes latest envelope|

IDs are namespace scoped; neither namespace nor id can contain NUL. Combined UTF-8 lengths plus3 fit1024. Signal key obeys existing nonempty1024-byte limit. Generation is the schedule mutation revision, retained across restart. Set/replace uses ordinary dedup op_id and expected-generation checks. Same completed op_id does not rewind; an older set retry after replacement conflicts12. Cancel requires current generation; missing25, stale12. Cancel persists a tombstone, retaining its identity slot; status becomes absent. Completed one-off status remains present/inactive. Progress refusal stops the timer and exposes error until replacement/restart.

Lost set/cancel replies are uncertain: Python does not automatically retry; reconcile status before explicit retry. A cancel already applied can reconcile absent. Deadline is rechecked after waiting on the central timer mutex. Set/cancel/fire serialize; queued/in-flight delivery can finish after cancel or replace. Cancellation does not retract receiver bytes or revoke application leases.

Schedule/control dedup uses the existing configured retry horizon (default24h), retained-id ring and refusal13 policy. The no-rewind statement applies within retained dedup history; after that history is gone, unconditional reuse can create a new generation. Reconcile status and use expected generation; reuse fixed timer ids such as reopen/pass, while using a new op_id for each intentional replacement.

Control op_ids share the reserved internal namespace's dedup scope across all client namespaces. Unlike ordinary KV's per-namespace ids, timer set/cancel op_ids must be globally unique in the store (Python defaults to UUID4). Reusing one with a different namespace/id or request returns14 op_id_mismatch, never a wrong cached success. Deterministic caller ids must include their namespace and timer identity.

## Time, draw and recovery

Index i has nominal=`anchor+period*i`, due=`nominal+draw(i)`. Integer draw is inclusive[low,high], deterministic rejection sampling over seeded SplitMix64; seed derives from namespace/id and op_id. This is scheduling jitter, not cryptographic randomness. Each index has its own draw, carried in the wake and recorded in progress; replay reproduces it. Repeat range width is strictly less than period, giving increasing dues. First due cannot overflow; future overflow completes the timer. Minute+/-15% uses[-9000,9000]ms. Exact quota reset uses[0,0]; waiter delay0–5minutes (spokesperson0–1) and exponential full-jitter retries remain caller policy.

Adjacent intervals can vary by up to twice the offset magnitude: minute+/-9000ms permits42–78seconds between due times. Lane87 explicitly accepted anchored jitter at eco ledger seq1144 (E-4c1b26f81bc040e18165b164b0f9e149): +/-15% concerns anchored timing/no lockstep, not an inter-fire interval bound. This avoids drift and keeps logarithmic recovery/stable due identities. Lane88 chooses its slots adoption.

One condition-variable owner keeps a bounded ordered deadline set (timer-wheel equivalent), wakes on set/cancel/deadline, and checks clock corrections at most once/second while waiting for a distant due. No endpoint I/O or client polling process. UTC scheduling is not real-time: forward jumps/overload coalesce overdue indices, backward jumps delay them. Timer mutex stays held during progress append; another due/control can wait behind storage. Ordinary hot KV reads remain independent.

Replay loads schedules/progress before owner starts. Emit at most one catch-up per overdue timer for its latest due index, `missed_count=latest-next+1`, then continue next index. Normal overload uses the same rule; an on-time wake has missed_count1. Completed one-offs never recur. Two watchers receive the same due identity and draw; watchers do not own timers.

Include-value envelope: magic MBT1; LEu16 version1; text timer id; six64-bit fields generation,index,nominal_ms,due_ms,signed draw_ms,missed_count; bytes payload. Due identity is `(namespace,generation,index)`, stable across epoch. Python membus_timers.decode_timer exposes it. MBC1 cursor stays epoch-scoped; its version is configuration generation, not a KV revision for the signal key. Use a dedicated timer key prefix; do not reconcile it as a KV mutation.

Subscribe before setting one-off. Subscriptions do not persist: catch-up may precede reconnection. After an epoch gap, register live first, then read timer_status and reconcile latest due identity/draw/missed count. Current-epoch ring replay is another option within existing count/byte limits. Status is not a complete fire history/receiver receipt. Applications needing durable work retain their own due-id/lease/result; duplicate wakes cannot authorize duplicate payload runs.

Wake enqueue precedes os progress append. Kill between them can repeat that identity. Pipe failure/queue overflow/crash after checkpoint before delivery can lose the transport wake; reconcile timer status/application state. No exactly-once delivery or durable endpoint retry is claimed. Scheduler/leases govern effect dedup. Process-kill persistence is existing os semantics, not power-loss survival.

missed_count means due indices covered since the last successful checkpoint, including this wake's index. It is1for a normal fire; missed prior indices are count−1. At a publication/checkpoint crash boundary, already seen indices can be counted again. It is exact relative to checkpoints and does not establish an exact count of receiver misses. Timer-set explicitly selects this coalescible wake primitive; it is not an arbitrary durable request stream. There is no noncoalescing request mode in this slice.

Progress failure stops this timer with active=false/error even for a transient refusal, as specified in the slice plan. There is no automatic retry policy or failure wake; a central consumer reconciles timer_status alongside subscription status or its existing fallback and replaces an errored schedule. Lane88 retains its quota task for a week. No per-agent polling process is required. This explicit limitation avoids retry storms during storage failure; health currently reports general store state, not aggregate timer failures.

After a refused checkpoint, in-memory status last names the published attempt although it was not persisted. On restart, last returns to the previous successful checkpoint (or none); the recovered owner can publish that same attempted due identity again. Do not treat last or an increasing index across epochs as a durable effect receipt; retain application due-id/lease results outside this status.

Embedding Store::open alone does not run timers. Library callers must keep the Store::start_timers guard alive and drop it before the Store. The guard is per Store, not a cross-process lock. Drop may wait for the current progress deadline (up to10s). Existing lock-poison handling treats an unexpected owner panic as fatal rather than repairing state; callers must supervise the service.

## Bytes, limits and growth

Reserved internal namespace __membus_timers_v1 is denied through public operations. Schedule s/<namespace>NUL<id> holds MTS1, three length-prefixed texts(namespace,id,key), five64-bit fields(anchor,period,low,high,seed), and bytes payload. Progress p/<namespace>NUL<id> is exactly48bytes (six envelope fields). Replace ignores older-generation progress. Internal writes emit no ordinary KV change. Timer wakes create no log record or event key.

MTS1 freezes version1 seed derivation, draw function and anchored-time rule. Restore validates those rules and bounds and refuses inconsistent timer records, like invalid MBK1 records. A future change to these rules/limits must introduce a migration/version reader; it must not reinterpret version1 progress. A namespace collision is checked on the old resident before first swap. Downgrading to an older build would treat internal records as ordinary KV; old replay's24h dedup accounting can fail after high-volume timer activity, so rollback needs qualification against the actual retained logs.

MBK1 append bytes=`58+len(namespace)+len(key)+len(op_id)+len(value)`, UTF-8 lengths. Fire writes one compact progress record: `106+len(internal namespace)+len(progress key)+len(tf-<epoch>-<generation>-<index>)`. Decimal lengths grow at powers of10. Minute timer writes1440records/day plus setup/replacement/cancel/restart catch-up. Tests inspect stripe records and actual length deltas; timer-day is a qualified extrapolation. These are logical appends, not physical-device utilization. Full schedule-per-fire rewrites payload; anchor-only avoids fire writes but cannot locate exact missed count without another persisted state or receipt. Compact progress selected.

Charge: retained identity256+identity length plus present schedule768+3*(namespace length+id length)+signal key length+payload capacity; conservatively covers duplicate map/deadline strings and metadata. Cancel retains identity charge. Max4096identities including cancelled,8MiBcharge; replay enforces both. Additional KV value/dedup/index budgets and existing signal ring16MiB/queue16MiB/buffers apply. One transient timer envelope is at most about66KiB. No calibrated RSS guarantee. Progress/config consume KV and state limits too.

Internal progress is never retained in the client retry dedup table, on admission or replay. Its single owner makes one append attempt with an epoch/generation/index op_id and stops on failure. It still obeys pending-key, queue, disk, index, namespace and value limits and the same writer/publication boundary. This prevents the24h client dedup table from growing per fire. Schedule set/cancel and every ordinary client mutation retain existing retry dedup behavior.

The unique progress op_id remains in MBK1 even though the current owner does not retry it: older binaries replay all records with dedup and a constant id would produce fingerprint mismatches. Keeping the shared MBK1 format and distinct ids preserves that replay property; it does not remove the older build's capacity limits. Compact progress is the selected compatible form, not a claim of a theoretical minimum encoding.

Tombstones/rewrites reclaim neither logs nor KV index slots. Reuse timer ids to avoid new identities;4096different cancelled ids fill admission. At100ms,864000fires/day create substantial growth. Rotation/retirement remains separate work; capacity refusal stops that timer with explicit error. No hidden rewrite, separate timer WAL, downloader, command execution or dependency.

The existing state_limit_bytes is shared by timers and every ordinary writer. At its default8GiB, one100ms timer at about144bytes/fire consumes about124MB/day; the theoretical4096-timer maximum can exhaust it in about24minutes if storage sustains that rate. Other records and longer ids reduce that time. Once full, all persisted writes return7 capacity until explicit maintenance or an increased budget; a timer memory limit is not a log-rate budget. There is no aggregate fire-rate admission bound in this slice. The commissioned minute/reset consumers use about207–213KB per minute-timer-day; monitor total persisted bytes and adopt rotation before approaching the shared limit.

Verification: tests/timers_native.py, timers.rs units and unchanged Rust/protocol/native regressions; artifacts at D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e9/act-work/F-E6-41, bulk in ignored .test-output. Faults kill owned private processes only. Resident swap waits for F-E6-48 endpoint marker and lane88 source/hash notice.
