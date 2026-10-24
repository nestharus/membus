# Health evidence and hosted readiness

This is a proposed service/supervisor contract around the native resident. Current
membus exposes local health, heartbeat and recovery observations; it does not
implement a hosted readiness endpoint, placement-aware health policy or automatic
failover. Choose the evidence and permitted traffic class before routing tenants.

## Existing observations have different meanings

| Observation | What it establishes | Limits |
|---|---|---|
| Process exists or a ready message/report was seen | A process/lifecycle observation at a particular time. Store startup reached a publication stage if the report actually belongs to that attempt. | Reports can be old, publication can fail, and selected missing/unaccounted storage can start read-only. Store readiness is not proof that a listener subsequently bound or that writes are eligible. |
| Native Windows `health` succeeds | HELLO/HEALTH association, pipe server PID matching the selected heartbeat, matching epoch, heartbeat age below 90 seconds and bounded I/O. | Does not inspect HEALTH2 degradation flags or individual timer errors. A disabled heartbeat cannot pass this command. PID/epoch are local association, not globally unique restore lineage, tenant authentication or writer fencing. |
| HEALTH2 fields | Resident-wide sampled state, storage/read-only/expiry flags, quotas/pressure, cache/retention and other diagnostics at observation time. | Not a lease on future service, per-tenant attribution or a media/integrity scan. Some fields require locks and collection walks. No aggregate timer-error field is supplied. |
| Successful data operation | That operation's actual result at its requested/achieved native boundary. | A warm cache hit need not inspect backing bytes. One read does not validate every key; one local Flush does not prove remote/quorum durability. |
| Timer or delivery status | The specific control/enqueue/write observation described by that interface. | A pipe write, heartbeat, inactive timer or cancel reply cannot prove receiver processing, external effect settlement or a stopped worker. |

The current convenience Store read can collapse an I/O error into absence; use
the fallible/wire error contract when interpreting failed reads. Do not turn a
storage failure into a negative cache entry or silently serve another authority's
state. See [client caching](CLIENT_CACHE.md) and [routing](SHARD_ROUTING.md).

## Define eligibility by traffic class

| Service state | Proposed admission/response policy |
|---|---|
| Starting, recovering or identity unknown | Do not route tenant work from a stale report. Establish the expected endpoint, capabilities and authority incarnation, complete the actual startup barrier and observe the live service. Bound startup/retry work. |
| Responsive and qualified for writes | Admit writes only under current authority, authorization, compatible capability/durability policy and available admission budget. Health observations inform routing but cannot make the next operation infallible. |
| Read-only or storage error | Stop ordinary write admission and expose the reason to its trusted owner. Permit diagnostic/explicit read-only service only under a declared profile that preserves errors and provenance; cached success is not evidence that storage recovered. |
| Pressure without integrity/authority failure | Apply bounded admission/backpressure, tenant fairness and capacity/retention investigation. A high-water flag is not proof that the process must be killed or that all reads are invalid. Preserve original operation uncertainty on refusals/timeouts. |
| Expiry paused, retirement blocked or catalogue rewrite error | Treat the actual maintenance/recovery condition and its storage-growth risk. Continued reads/writes depend on the operation and service policy; do not infer universal availability or erase recovery evidence to clear a flag. |
| Timers, bridge or backend degraded | Report the affected workflow separately from native KV eligibility. Reconcile timer/effect state and durable backlog. A healthy KV service does not certify every downstream consumer. |
| Draining or authority lost | Reject new work, fence stale routes/writers and settle or retain admitted uncertainty/cleanup obligations. A local health success or a caller disconnect cannot restore authority or prove drain completion. |

Native flags remain observations, not a complete hosted state machine. A service
must define their precedence, allowed operations, freshness and failure handling.
For example, a failed cold read raises storage_error without itself setting the
native read_only state; a service must enforce its chosen stricter admission policy.
Keep write eligibility distinct from a separately offered read-only profile;
do not advertise a single green boolean as all capabilities succeeding.

## Observe once, associate correctly and keep the hot path

Use bounded background observations per resident/cell and share their result
among trusted routers. Bind the sample to endpoint, expected native epoch, service
authority incarnation, time/source and the policy it supports. Expire stale
eligibility under that policy; an old success must not authorize a new owner.
Preserve [restore lineage](BACKUP_RECOVERY.md) and [clock/deadline](DEADLINES.md)
boundaries. Do not use a heartbeat timestamp as a lease or election result.

HEALTH2 reads shared state and aggregates namespaces/chunks/cache diagnostics.
Polling per tenant or before every GET multiplies that work and contention. The
native health command also performs protocol exchanges and reads a heartbeat.
Prefer coalesced bounded probes, local eligibility checks and direct operation
outcomes. This adds no required data fsync or new RPC to each local hot read/write.
A separate hosted gateway still pays its real forwarding/encoding/copy costs.
Choose probe cadence and stale-sample tolerance from the service promise and
measured contention; the native 90-second heartbeat check is not a cloud recovery
target or a guarantee of uninterrupted eligibility.

Global health fields may contain namespace names and resource information from
other tenants. Keep raw diagnostics behind trusted administration and expose only
authorized scoped service observations. Feature negotiation and a caller-supplied
namespace do not authenticate health access or make global pressure attributable.
See [tenant admission](TENANCY_BOUNDARY.md).

## Recovery action is a separate decision

Pressure, a collector outage or an expired caller deadline should not create a
restart loop. Restart does not add disk capacity or repair damaged bytes, and it
retires epochs/views/subscriptions while adding replay and cache-warmup load.
Drain/stop/restart must retain pending operations and cleanup responsibility.
Some storage/read-only flags are sticky; do not clear them by editing catalogue,
epoch or marker files or deleting required state. Preserve the evidence and use
the qualified recovery procedure.

Routing withdrawal is different from process termination or fenced replica
promotion. Automatic failover requires its own replication, durability, exclusive
writer and uncertain-operation protocol. A green replacement health check cannot
prove continuity with the old owner's history. Backups, paused complete-store
cutover and distributed promotion have different failure and availability costs.

Before automating decisions, qualify stale/mismatched heartbeats/reports, disabled
heartbeat, listener bind failure, read-only startup, required-sync refusal, warm
cached success over a failed backing read, pressure/retry storms, timer failure,
blocked retirement, lost authority, slow drain and replacement/restore lineage.
Measure detection and admission response alongside accepted bytes/operations,
p99, refusals, unknown outcomes and useful recovery time under load. Existing
health/chunk tests support native observations and recovery cases; they do not
qualify a hosted supervisor or automatic failover.
