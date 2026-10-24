# Tenant admission around a resident

This is a proposed hosted-service contract, not an implemented authentication,
tenant gateway or fairness feature. Current membus is a trusted local state
engine: an admitted client chooses its namespace. The named-pipe ACL admits the
resident's current user and LocalSystem, while TCP is a loopback test transport.
HELLO identity is correlation; feature flags negotiate operations. Neither
grants an application actor permission to read or mutate a tenant's data.

## Place the boundary before backend admission

An authenticated service derives tenant, actor, allowed namespace/workspace,
operation classes, limits and policy generation from trusted identity/policy.
Authorize every operation before borrowing a backend connection or allocating
large payloads. A producer-supplied tenant field, namespace, subscription token,
timer ID or endpoint is not proof of that scope. Keep direct resident endpoints
and state files reachable only by trusted service components. A tenant's code
sharing that OS principal could bypass an application gateway; worker isolation
is a separate deployment requirement.

Map `(tenant_id, logical_namespace_id)` to a backend namespace using an explicit
versioned, injective encoding. Validate the final UTF-8 byte length and reserved
names before backend admission. Plain concatenation or ambiguous prefix matching
must not merge scopes. Do not silently truncate or normalize identities; display
names can change without reusing logical tenant IDs. Current identifiers are
bounded at 1024 bytes, and timer namespace/id encoding has its own combined
bound. Mapping overhead consumes that budget.

Keep original application operation identity stable through routing and retries.
Ordinary mutation IDs are scoped by backend namespace; policy setter IDs retain
the caller namespace. Timer control operation IDs share one internal namespace
across all caller namespaces, so qualify them by tenant, namespace and intended
operation before the first send. Retain that exact mapping through uncertain
retry/reconciliation. A fresh connection, actor session or policy generation
must not silently generate a new ID for the same uncertain operation.

## Authorize the complete surface

Use an explicit operation allowlist. New native opcodes remain unavailable to
tenant callers until their authorization, resource and response policies exist.
Negotiating `subscribe=1`, `scans=1` or `timers=1` does not supply those policies.

| Surface | Current backend behavior | Required hosted contract |
|---|---|---|
| GET, GET_MANY, GET_STATUS | Uses the request namespace and keys. | Resolve the authorized logical namespace first; validate each requested key/range and preserve absence versus error. |
| PUT, conditional PUT, REMOVE, BATCH | Mutations use caller-selected namespace; BATCH items independently admit and commit. | Authorize every item and acknowledgment mode before send. Reject disallowed items before any backend item is admitted. Passing gateway validation does not make BATCH atomic. |
| Snapshot open, scans, keys-only scans and release | Tokens bind namespace/epoch; cursors also bind the query. Tokens can be shared across connections in that scope. | Bind external handles to authorized tenant/namespace, authority incarnation, policy and resource lifetime. Recheck permission on each page/release. Token possession alone is insufficient authorization. |
| Subscribe, status and unsubscribe | Registration is exact-namespace/prefix; tokens are namespace-bound. Endpoints are local Windows pipes. | The gateway owns the local listener/endpoint and forwards to an authorized remote channel. Clients choose logical subscriptions, not native paths. Own cleanup, including unknown subscribe outcomes; bound downstream buffers and registrations. |
| Timer set, cancel and status | Schedule identity includes namespace/id; control dedup is globally shared internally. | Authorize timer creation, payload/key, rate, replacement, cancel and status. Preserve tenant-qualified control identity and cleanup. A wake or successful cancel call is not execution settlement. |
| Policy get/set | Native policy operations use the request namespace and prefix. | Separate policy administration from ordinary data access; define who may change retention and account for the consequences. Internal namespace denial does not prevent admitted public policy calls. |
| HEALTH, HEALTH2, IO_METRICS, SIGNAL_USAGE, CAPTURE_METRICS | Several fields/counters describe the whole resident, even when a request contains a namespace. | Operator-only raw access by default. Expose a deliberately defined tenant projection from associated observations; adding a namespace to a global report does not make it tenant-scoped. |
| Storage, recovery, expiry administration and worker execution | Local deployment controls and trusted embeddings have powers outside public KV permissions. | Separate operator/service roles and OS access. Logical tenant identifiers are not storage paths, volume credentials or permission to run shared-principal code. |

External handles can wrap native tokens in service-owned records that bind scope,
backend generation and cleanup owner. Apply explicit expiry and count/byte/work
budgets; eviction must not lose the obligation to release known backend state.
Receiver progress, server-written counters and a quiet stream cannot establish
authorization, useful delivery or cache freshness. See
[subscriptions](SUBSCRIPTIONS.md), [client caching](CLIENT_CACHE.md) and
[restore lineage](BACKUP_RECOVERY.md).

## Fairness is more than namespace keys

The namespace quota counts live keys. It does not reserve fair shares of bytes,
CPU, disk bandwidth, append capacity, scan work, retry history or retained timer
identities. Cache, snapshot, signal and timer budgets also have shared limits.
Retired/tombstone/retry state can outlive a live-key slot. Declaring multiple
namespaces does not partition these resources.

Bound request and response bytes, in-flight work, queued admissions, retry
attempts, snapshots/pages/work, subscriptions/fan-out/backlog and timer creation
per tenant and per cell. Account admitted and refused work separately. A global
resident refusal does not identify which tenant caused pressure; use associated
service observations rather than inventing attribution from a global counter.
Persistent accounting and charges require their own durable receipts.

The resident has at most 64 active connections and a shared 64 MiB ingress-body
reservation. The current Client serializes operations on one connection. A
gateway cannot assume unlimited multiplexing or retain one permanent connection
for every tenant. Use bounded pools/waiters, preserve request association, retire
poisoned connections and enforce whole-request budgets as well as idle timeouts.
Pooling does not carry tenant authority: authorize/rewrite each request, and
bind every response/handle to the originating scope.

Start with dedicated stores/cells when retention, physical deletion, recovery or
noisy-neighbor requirements cannot be met by a shared resident. Dedicated stores
still require authenticated admission. Shared chunks can mix namespaces; logical
deletion is not immediate physical erasure, and restoring a shared root rolls
back all tenants in it. These are product/recovery boundaries, not properties
that an identifier prefix can change.

## Preserve latency and uncertainty

Authenticate a bounded session and authorize each operation from local trusted
scope/policy, with declared credential expiry and revocation freshness. A policy
cache needs its own invalidation/lifetime rules, separate from data-cache age.
Keep backend transports warm where useful. This adds local checks and, for a
separate gateway, an RPC leg plus parsing/encoding/copies and bounded queueing.
It adds no inherent data fsync. Avoid a remote identity-service call, synchronous
telemetry export or broker receipt on every KV operation unless an explicitly
chosen contract requires that cost. Connection authentication amortization must
not silently extend an expired/revoked actor's permissions.

Preserve the trusted embedded/same-machine path and colocate the gateway with
its shard owner. A cache hit can keep its local read cost under its declared
data-freshness and authorization contract. An authoritative remote write still
pays its actual network/storage coordination. Measure gateway versus direct
hot/miss p50/p99, accepted operations/bytes, refusals and per-tenant wait/backlog;
no gateway performance or isolation result is established here.

A definitive pre-send authorization denial is different from revocation or
disconnect after a mutation may have reached the backend. Preserve the original
operation and uncertain outcome through that boundary. Disabling an actor or
closing a channel does not prove an admitted write or external job stopped.
Cleanup/reconciliation may need a trusted service owner after caller access ends.

Before offering shared tenancy, qualify cross-tenant namespace/key injection,
ambiguous/oversize mappings, mixed batches, foreign tokens/pages, subscriptions
and endpoint selection, timer ID/cancel collisions, policy and diagnostic access,
pool response association, revoked/expired actors, admitted uncertain writes,
resource starvation, restore-generation changes and tenant deletion/recovery.
Existing protocol/scan tests exercise native framing, namespace-bound snapshots
and resource limits. They do not qualify this proposed gateway or authorization
for mutually untrusted tenants.

Tenant routing must also distinguish live endpoint association from write/read
eligibility and downstream workflow health. See the proposed
[service-health contract](SERVICE_HEALTH.md).

Admission revocation, logical key removal, chunk retirement and downstream-copy
completion have different evidence and costs. Choose a compatible storage and
recovery boundary before signup; see [tenant lifecycle](TENANT_LIFECYCLE.md).
