# Deadlines across clients, gateways and workers

This guide separates existing native time behavior from a proposed remote-service
budget contract. Current KV1 has absolute UTC deadlines. The Rust Client also
requires a local monotonic `Instant` deadline. Neither supplies distributed clock
synchronization, end-to-end cancellation, a remote budget header or writer fencing.
See [instance time](CLOCK.md) for the owning Store's clock domains.

## Native boundaries

| Time value | What it governs | What it does not prove |
|---|---|---|
| `Deadline.unix_ms` | Absolute UTC supplied on the wire and compared with the resident's UTC for admission/checks. | A duration on a remote host, a bound independent of clock skew, or cancellation of an admitted mutation. |
| `Deadline.local` | Caller-process `Instant` used by the transport for the whole call, reapplying remaining time before partial reads/writes. Connect and HELLO also need an explicit budget. | A portable timestamp, server execution deadline, or proof that a timed-out mutation did not commit. |
| Attempt first-send time and retry window | Conservative local elapsed bound retained with the original request and epoch for explicit retry. | A new application budget on reconnect, durable retry ownership, or safe retry at an unrelated endpoint with the same epoch. |
| Resident cache/snapshot lifetime | Local elapsed residency/view expiry under the instance clock. | Remote freshness, authorization lifetime, returned-copy erasure or a caller's remaining request budget. |
| Timer UTC anchor/due time | Schedule/catch-up selection under resident UTC. | A real-time delivery deadline, lease handoff, process-stop receipt or permission for an external effect. |

The caller supplies the two Deadline fields; the SDK does not prove that they
represent the same intended budget. Advancing wall time can make native UTC
admission expire early, and moving it backward can keep that check open longer.
Local monotonic expiry remains the caller-side transport budget. Native duplicate handling
can return a retained outcome before ordinary new-mutation deadline admission;
a late idempotent result is not evidence of a new admitted write.

After possible dispatch, transport timeout, partial reply or lost association
preserves mutation uncertainty. A later pre-send deadline refusal on that Attempt
does not erase its earlier uncertain dispatch. Keep original IDs, bytes, Ack,
expectation, retry horizon and reconciliation duty. A client timing out or closing
its connection does not cancel a queued/admitted append or covering sync. Separate
cancel requested, received, applied and externally observed settlement where a
job protocol needs those facts; native KV has no cancel-admitted-mutation opcode.

## Choose the remote budget contract

| Contract | Support, extension and trade-off |
|---|---|
| Preserve an absolute UTC expiry plus local monotonic wait | Native fields already support this locally. A remote service needs declared clock-error assumptions and policy for unhealthy/uncertain clock state. Absolute expiry includes queue/transit time only to the accuracy of the participating clocks; an ahead server can refuse early and a behind server can admit later than the caller intended. |
| Propagate a remaining-duration budget as well | A future gateway/worker envelope can reduce remaining time for local queueing/processing and clamp it to the original request bound and service maximum. Native KV1 has no such duration field. Relative time alone cannot account for unknown transit or a paused intermediary, so it cannot replace an end-to-end expiry/clock policy. |
| Ask a central time/policy service before every call | Can centralize a chosen check but adds a network round trip, queueing and dependency; it still does not cancel already admitted work or establish writer authority. This is not required by native local semantics and works against the low-latency purpose. Prefer locally enforceable bounds and background clock-health observation where the service contract permits them. |

Define the budget at trusted ingress. Authenticate caller/tenant scope, validate
units/range and choose a service maximum; a producer must not grant itself an
unbounded lifetime by supplying a distant UTC date. Preserve the original
operation identity, expiry and intended completion boundary through route changes.
Use local monotonic accounting for each process's queue, connection acquisition,
connect/HELLO, retries/backoff, encoding, transport and any required downstream wait.
Pass only the remaining budget and never restart the full duration at each hop.
For example, a 500 ms budget after 400 ms of local queueing has at most 100 ms
left before further transit/work; this is arithmetic, not a latency measurement.

An `Instant`, resident elapsed counter or snapshot TTL cannot be serialized as a
universal cross-host deadline. At a gateway-to-native boundary, construct local
transport expiry within the remaining caller budget and supply native UTC expiry
under the service's declared clock policy. Do not silently convert an uncertain
remote clock into a fresh full timeout. If the required end-to-end bound cannot
be established, refuse or report the weaker supported contract explicitly.
Monitoring a skew bound is an operational qualification, not a native guarantee.

Keep application deadline, native retry horizon, broker visibility/retention,
credential lifetime and lease lifetime separate. A transient refusal or a new
connection is not permission to retry beyond the original application budget.
An explicitly chosen later application operation needs its own decision and
reconciliation of prior uncertainty, not a renamed old send. Local cache hits
still need their declared data-freshness and authorization checks.

## Restart, schedules and authority

Previous process monotonic readings have no meaning in a new instance. Replay
uses persisted UTC age to select retry outcomes and converts their remaining
window into new local monotonic cutoffs. Later UTC correction does not rewrite
that local cutoff. Restart/restore therefore has a different time boundary from
ordinary live elapsed accounting; it cannot renew a caller's retained retry or
application budget. Preserve the [authority lineage](BACKUP_RECOVERY.md) and
[routed-operation contract](SHARD_ROUTING.md) independently of clock values.

Timer scheduling uses UTC; forward jumps/overload can coalesce dues and backward
jumps can delay them. Wakes can repeat or be lost, and queued wakes can complete
after cancellation. Reconcile the original due identity against authoritative
job/lease/effect state before acting. Time passage, clock synchronization and
timer cancellation alone cannot fence an old writer or confirm that a worker
stopped. A cloud deadline/availability promise needs those separate protocols.

Keep occurrence time, observation time, elapsed duration and clock/source domain
distinct in records and diagnostics. A wall timestamp cannot supply a globally
ordered event position; use the owning stream/commit position under its contract.
Expiry and timer health are not substitutes for useful-effect settlement.

## Latency, volume and qualification

Local `Instant`/UTC reads, remaining-budget arithmetic and cached admission checks
add no network hop or data fsync. Preserve the embedded/same-machine path; do not
introduce a mandatory remote clock RPC or global timing lock per GET/write.
Gateway forwarding still adds its actual RPC, serialization/copies and bounded
queues. Deadline checks can shed expired queued work, but admitted I/O and uncertain
outcomes consume resources until settled. Cancellation must not release capacity
solely because a caller stopped waiting.

Measure queue time, accepted operations/bytes, deadline refusals, uncertainty,
live outstanding work and end-to-end latency separately. Include clock corrections,
bounded skew, partial I/O, stale routes, lost replies, slow covering sync, reconnect,
restart, late timer delivery and cancel-versus-settlement races. A very short
timeout can improve successful-call percentiles while useful completion worsens;
retain refusal and unknown populations in the account.

Existing client controls and manual-clock recovery tests establish native local
boundaries. They do not measure host clock-error bounds, qualify a remote budget
adapter or certify real-time completion/cancellation.
