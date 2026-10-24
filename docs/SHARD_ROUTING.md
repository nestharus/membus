# Routing callers between authoritative residents

This is a proposed service contract for placing logical namespaces on different
membus owners. Current KV1 and the Rust Client connect to one explicitly selected
resident. They do not implement a placement catalogue, redirects, replication,
cross-store fencing or migration. The private storage Route is a different seam;
see [storage capabilities](STORAGE_CAPABILITIES.md).

## Keep identities separate

| Identity | Meaning in a routed service |
|---|---|
| Tenant and logical namespace | Stable application scope derived from authenticated admission. Mapping to a native namespace must be unambiguous and fit its limits. |
| Logical partition | Stable placement unit, initially a complete namespace. A route maps it to an owner without changing application keys or operation IDs. |
| Placement generation | Version of that mapping; detects stale routing. It does not itself prevent an old owner from writing. |
| Authority incarnation and writer term | Authenticated lineage and exclusive-writer fencing. Distinguish restore/clone from an ordinary route update. The enforcement boundary must reject stale owners. |
| Native epoch and revision | Local resident lifetime and opaque value revision. Neither is a globally unique store identity, route generation or quorum commit position. |
| Application operation ID | Original intended operation, preserved with namespace, bytes, expectation, Ack and uncertainty through reconnect and movement. |

Each independent fresh state root can start at epoch 1. `Attempt` retains a
request, epoch, first-send time and retry window; it does not retain a globally
unique source or partition identifier. Its native epoch/horizon check therefore
cannot establish that a new endpoint is the same logical authority. Equal epochs
on two connections do not authorize moving an uncertain Attempt between them.
This follows from startup and Client source, not an observed routing failure.
Physical restore can also reuse an abandoned epoch; see [backup
lineage](BACKUP_RECOVERY.md).

The service must bind its connection pool, request admission, receipts, cache
fills and handles to authenticated tenant/namespace/partition and authority
incarnation. Bind routing decisions to placement generation as well. A native
endpoint string or caller-supplied identity is not that proof. Keep remote source
revision/Ack distinct from the version of a disposable [cached copy](CLIENT_CACHE.md).

## Redirects and uncertain writes

| State at the router | Required handling |
|---|---|
| No mutation dispatched; route is stale | Establish that no prior dispatch occurred. Refresh authenticated placement, validate target capabilities and retry within the original application deadline and bounded redirect budget. |
| Complete route refusal before owner admission | A future protocol may permit redirection if this refusal is authenticated, associated with the operation and proves that this dispatch was not admitted. KV1 has no such route-refusal contract today. |
| Mutation may have been sent; reply lost, timed out or uncertain | Preserve the original Attempt and evidence. A redirect, empty GET or timeout does not prove absence. Reconnect/retry only when authority continuity, native epoch, original retry horizon and application policy permit it; otherwise reconcile. |
| Owner or lineage changed | Require an explicit continuity/reconciliation protocol. Do not prepare a new Attempt to reset time, change an operation ID, rewrite an expected revision or fan the write to both owners. |
| Local Flush succeeded | Preserve that local receipt. A remote follower, upload or newly selected owner does not turn it into quorum durability or prove that it has applied the write. |

A later definitive refusal cannot erase an earlier uncertain dispatch. Preserve
the full logical request, original time budget and all unresolved dispatches.
Read-only retries also need an explicit consistency contract: a successful GET
at another owner can be stale. Missing state at a follower is not proof that an
uncertain write never committed. Current Rust Client does not retry or reconnect
automatically; retain that caller-owned policy at the routed boundary.

Deduplication is local retained state keyed by namespace and operation ID. A
different store without the outcome can admit the same ID as new; matching bytes
alone cannot recreate its original result or conditional-write history. Copying
latest values with ordinary PUT generates destination revisions and retry state,
so it does not transparently preserve CAS receipts or retry continuity. A service
must either preserve these through a qualified migration protocol or explicitly
end their continuity and reconcile outstanding operations. Never translate an old
CAS token into a new one and silently reissue the old intended write.

## Placement and cutover

Start with a complete namespace on one owner. Keep each indexed collection's
schema, rows, postings and head together: append publishes through a conditional
head after staged writes, and query reads one namespace snapshot. Hashing those
keys independently breaks the assumptions of the existing collection API.
BATCH is already nontransactional locally; splitting it does not create atomicity.
GET_MANY is a series of observations, not a common multi-key or cross-shard view.

For a first pilot, moving a dedicated stopped store using the complete-state
capture/recovery procedure is simpler to qualify than moving one namespace from
a shared root. Pause admission, settle or retain in-flight uncertainty, stop and
fence the old owner, capture and validate the destination privately, then publish
the new placement after readiness and authority checks. Restart and restore
boundaries still require client reconciliation. Prevent every route, worker and
timer path from reactivating the old writer. The directory owner lock protects
one local root; copying it does not fence another root or machine.

Moving one namespace out of shared chunks requires an explicit state-transfer
boundary for retained outcomes, tombstones, policies and control state, including
timer scheduling/progress and its globally scoped control operation IDs. Current
namespace query snapshots expose public captured values, not that complete state.
Live movement additionally needs an ordered catch-up boundary, retained source
dependencies, writer fencing and crash-safe route publication. Neither protocol
is supplied today. Do not implement migration as uncoordinated dual writes.

Keep snapshot, cursor and subscription handles bound to their original source,
namespace and lifetime. Tokens are opaque; equal token bytes at another store do
not make it the same view. Release/unsubscribe must reach the owning resource, or
retain a bounded cleanup obligation until its documented expiry/settlement.
During cutover, finish a permitted old read view or explicitly expire and restart
the whole related query. Do not combine old and new pages into one claimed view.
Hints must be re-established and state reconciled under the new authority.

## Latency and volume

A cached in-process placement lookup adds no network hop or durability sync.
Direct owner routing pays one authority RPC; a separate gateway can add another
forwarding leg, request/reply buffers and serialization. Use bounded warm pools
and colocate the worker or admission with the owner. Current Client serializes
one connection, so cap aggregate queued bytes/work while adding concurrency.
Do not add a remote catalogue lookup, global lock or fsync to every KV call.
Writer fencing must remain valid when placement is cached; availability of a
stale cache cannot override loss of authority.

A redirect adds a lookup/response and possibly another RPC; cap attempts and keep
the original deadline. Whole-namespace shards add aggregate volume only when load
is distributed. Cross-shard queries add requests, merge/copy work and retained
buffers, and completion follows the slowest required participant. Parallel fan-out
does not provide a global snapshot. Stop/copy cutover adds downtime and full
transfer/replay work; live catch-up adds replication traffic and backlog pressure.
Bound that work separately from tenant requests. These costs are architectural
predictions, not measured distributed throughput or latency.

Preserve the local authoritative read/write path and local Os/Flush semantics.
For remote reads needing low latency, retain an explicitly scoped local immutable
or age-bounded cache. Current authoritative reads and strong writes still need
their chosen authority/replication boundary. A routing layer cannot remove that
network or durable-write cost.

## Qualification before routing production traffic

Exercise two independent residents with equal epochs; stale routes and owners;
lost replies across cutover; changed payloads using an existing operation ID;
expiry of retry budgets; CAS receipts after transfer; old snapshot/release and
subscription/unsubscribe paths; and timers attempting to resume on both sides.
Inject crashes around capture, readiness and placement publication. Check that
each external operation has a reconcilable outcome rather than claiming universal
exactly-once delivery. Measure warm direct, gateway and redirect paths, bytes,
refusals, p99 and bounded backlog at the intended Ack/fan-out.

Existing `tests/client.rs` checks native epoch/horizon guards and uncertainty;
`tests/indexed.rs` checks local publication and pinned collection queries. Passing
them does not qualify a distributed router or migration protocol.
