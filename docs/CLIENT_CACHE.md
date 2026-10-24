# Caching values outside the authoritative resident

This guide describes the contracts a caller needs before caching reads from
another membus instance. It is design guidance, not an implemented read-through,
remote-storage, replication or cache-coherence feature. The resident's existing
[value cache](VALUE_CACHE.md) is internal to one Store and shares its committed
index and retirement state. Opening a second Store does not connect their caches.

## Choose what a cached read means

| Read contract | What can be served locally | Required boundary |
|---|---|---|
| Immutable version/content | Bytes identified by a complete source identity and immutable version or content digest. | The application must actually guarantee immutability and separately enforce authorization, deletion and retention. An ordinary mutable KV key is insufficient. |
| Bounded age of an authoritative observation | A result whose last successful authoritative observation is within a declared age limit. | Track that age independently of LRU/access frequency; hints may invalidate it sooner. This permits changes after that observation. |
| Current authoritative value | A read at the authoritative resident. | A local hit cannot establish current state solely from an apparently healthy subscription. A future authority-issued coherence lease/read barrier would need a separate proven contract. |
| Read after a caller's write | A value satisfying that caller's receipt in the same authority generation. | A retained positive value with a sufficient same-key version can satisfy a specifically defined minimum-version contract. Current/follower consistency and removal/uncertainty require explicit reconciliation. |
| Common view of several keys | Values captured under the same unexpired namespace snapshot. | Reuse one snapshot token and its opaque pagination cursors. GET_MANY and independently cached GETs are not a snapshot. |

The current Rust client returns `Option<CommittedValue>`: present values carry
version, achieved acknowledgment and bytes. An absent GET carries no tombstone
version. Neither reply supplies a cache-valid-until time, distributed commit
position or authority lease. `Store::try_get` preserves storage errors separately
from absence; the convenience `Store::get` collapses a disk read error to None.
The wire GET reports that error as refusal 16. Never negative-cache an error.
GET_STATUS is a wire operation, not a typed Rust Client method, and a separate
status/GET pair does not form an atomic read.

## Identity and lifetime

Scope every entry and in-flight fill by application tenant/authorization scope,
logical authority, namespace, key and the observed resident epoch. Preserve the
remote version and achieved Ack as remote metadata; a local Store's revision or
Ack describes the local copy, not the source. A future routed service also needs
an explicit authority/placement generation. HELLO identity and namespace naming
do not authenticate a tenant today.

A restored or cloned directory can reuse an earlier resident epoch. Cache scope
therefore needs a separate [restore-lineage boundary](BACKUP_RECOVERY.md), and
old in-flight fills must be fenced even when the numeric epoch is unchanged.

Use a local monotonic clock for age. For a direct authoritative GET, anchor the
age budget no later than request start, rather than response arrival. A slow
response must not receive a new full freshness budget after spending it in
flight. Reject it for that read mode if the budget is already exhausted. A cache
hit, replayed hint, retained idempotent write outcome or local copy must not
renew the observation's age. A retained write reply can describe an earlier
outcome, so receiving it now is not a new current-state observation.

Drop process-local freshness evidence after restart. A persisted timestamp or
cache lease is not a persisted monotonic deadline. Bound negative entries too:
a new key can be created after an absent GET. The resident's initial/active
cache TTL controls memory residency; its expiry reloads disk and does not express
remote freshness, authorization lifetime or the value's retention deadline.

Finite-retention data can disappear during retirement without a KV change
publication. A forever-retained value can still be overwritten or removed.
Even immutable bytes need independent permission and deletion policy. A bounded
observation age permits a corresponding stale-visibility window; if immediate
revocation or expiry enforcement is required, validate at the authority or use
a separately proven authorization/coherence contract.

## Hints and the fill race

[Change subscriptions](SUBSCRIPTIONS.md) accelerate invalidation. They have
bounded memory-only history and no receiver acknowledgment or freshness lease.
Subscription status records transport progress; a quiet, active connection is
not evidence that no authoritative change or retirement has occurred.

The following race is valid with today's reads:

1. A GET captures committed revision A, then waits for a cold read or transport.
2. Revision B commits. The caller receives its hint and evicts A.
3. The delayed GET returns A. An unconditional cache insertion restores A.

Eviction alone therefore does not prevent stale refill. The same race exists
for an absent GET followed by creation, and for a read that crosses a source
restart or scope change. The internal resident cache has shared entry eligibility
that replacement/retirement clears before cache removal. An external cache has
no access to that flag and needs its own synchronization.

A proposed external-cache implementation should:

- Capture a scope/session generation and per-key invalidation generation before
  starting each authoritative read. Associate its response with that context.
- Serialize hint handling and fill installation for the affected cache key.
  Every associated hint advances the invalidation generation and evicts or marks
  the entry dirty. Install a response only if both captured generations still
  match. This also fences absent responses, which have no returned revision.
- Retain the highest observed same-key revision where useful, and refuse an
  older positive response within that same source epoch. Revisions admitted on
  different append stripes need not publish in numerical order; a high revision
  for one key proves nothing about another key or a complete replay prefix.
- Keep generation state while fills are in flight, or cancel those fills when
  reclaiming it. Evicting and recreating the generation counter must not make an
  old read appear current. Bound entries, fills, hint buffers, retries and work.
- On a gap, failed/dropped/missing registration, malformed stream, epoch change
  or authority/scope change, fence in-flight fills before rebuilding. Do not let
  a delayed reply repopulate the old cache after it has been cleared.

These guards prevent known invalidations being undone; they do not make a hit
linearizable. A hint may still be delayed, lost or never emitted for retirement.
Keep the independently declared age limit or read the authority. An included
publication value is historical publication data, not a new current observation.

For a materialized common view, bind/listen and register live hints before
opening a snapshot, buffer within explicit limits, and retain one snapshot for
all pages. Do not blindly apply older buffered events over newer snapshot rows.
Reconcile by source identity and same-key version, or mark dirty and reread.
Absence has no returned revision, so dirty rereads are safer than reconstructing
deletes from an assumed complete event history. Overflow or snapshot expiry
requires discarding the incomplete rebuild and starting again. Periodic refresh
still covers retirement and missed delivery. A completed scan plus drained
local buffer is not an atomic server snapshot-and-stream checkpoint.

## Writes, uncertainty and cost

Send mutations to the authority with their original operation identity,
expectation, acknowledgment and caller budget. A local cache write cannot
acknowledge an authoritative write or satisfy a remote Flush. Invalidate/fence
the affected cached key around an uncertain mutation; reconcile the deterministic
key and embedded operation identity under the [Attempt rules](RUST_CLIENT.md).
Do not turn an uncertain result into a successful write or a negative read.

An embedded hit needs local lookup/synchronization and whatever payload copy the
application requires; a local resident adds IPC framing/serialization and an
owned response. A miss adds an authority round trip, server read, decode/copy and
bounded fill work. A hint adds bounded local invalidation work; it should not
block every write on a remote subscriber. Per-key/scope bookkeeping consumes
memory and synchronization, but avoids a remote validation RTT on every permitted
hit. Strong-current reads retain their authority cost. Coalesce same-key misses
only with bounded waiters/deadlines; continuous retries must not monopolize the
fill pool or silently extend freshness.

Before offering this profile, qualify delayed positive and negative fills,
remove/recreate, retirement without hints, lost/partial streams, quiet stale
connections, epoch/route changes, generation eviction with reads in flight,
slow reads exceeding the age limit, snapshot overflow/expiry and uncertain
writes. Measure hot/miss p50/p99, accepted operations and bytes per second,
coalesced waiters, rejected fills, hint backlog and stale-read window separately.
Existing cache/client/subscription tests validate their individual contracts;
they do not qualify this proposed adapter.
