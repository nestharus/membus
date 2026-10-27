# Tenant offboarding and retained copies

This guide defines a proposed hosted-service deletion contract. Native membus
has key removal, chunk retention, snapshots and timer controls; it has no
tenant-wide deletion job or universal erasure receipt. Tenant identity and
authorization belong to the [service admission boundary](TENANCY_BOUNDARY.md).
Choose the promised scope and completion evidence before accepting tenant data.

## Different boundaries need different completion evidence

| Boundary | What it establishes | What remains |
|---|---|---|
| Access disabled | Trusted admission rejects new work for the tenant/generation; bounded authorization caches and enrolled workers follow the revocation contract. | Already admitted mutations, running jobs, queued delivery and disconnected workers need settlement. Disconnect is not proof of stop or deletion. |
| Current keys removed | Committed REMOVE writes tombstones and makes those keys absent from new current reads; the live-key quota slot is released. | Older frames, tombstones/retry state, captured snapshots and previously returned/copied bytes can remain. An absent GET is not a storage-erasure receipt. |
| Store chunks retired | The owning store completes its catalogue/deletion protocol for eligible whole chunks. | Shared chunks can retain other current keys, and metadata, import provenance, open handles, independent backups and exports have separate lifetimes. |
| Downstream copies settled | Each declared cache, worker spool, artifact/export destination and backup policy has acknowledged completion or reached its qualified retention boundary. | An attempted notification, timer cancel or volatile hint is not that evidence. Disconnected or failed destinations remain outstanding. |

Record which boundary completed and which copies remain. Do not label access
revocation, logical absence or an accepted cleanup request as universal erasure.
Unlinking a file and releasing store byte accounting do not certify physical
media sanitization. Such a promise needs the selected storage's own qualification.

## What native retention does

REMOVE appends a tombstone; it does not rewrite historical data frames. Missing-key
REMOVE also appends. A tombstone may keep an index slot until older retained
records can no longer resurrect the key. Retry outcomes, horizons and expiry
summaries have correctness roles and their own retention. Do not delete them or
edit catalogue files to make a tenant appear gone.

Retention policy selects a class for new records; changing or clearing policy
does not migrate old chunks. Forever chunks never retire automatically. Latest
chunks wait for closure, the retry horizon and absence of any key's latest
record; one retained current key can keep a chunk containing removed keys.
Expire classes retire whole closed chunks, including their latest values, after
the class boundary and other eligibility checks. Rotation, maintenance, clock
degradation, pending work and blocked deletion prevent an exact per-key expiry
deadline. See [chunk retention](CHUNK-RETIREMENT.md).

Open chunks are selected by class and stripe, not tenant/namespace. Equal-class
records from different namespaces can share a file. A distinct policy label is
not a tenant-owned physical chunk. Classes have a finite cap and represent kind
and retention duration; inventing a unique duration per tenant is not a stable
isolation contract. Force-retiring a shared file is not a tenant deletion API.

Cache TTL controls residency, not durable retention. Current replacement/removal
invalidates the resident's previous cache entry, but cannot retract bytes already
returned to a caller. Existing namespace snapshots retain captured payloads
through removal/expiry until release/expiry and the last reader. Snapshot capture
retains record locations and file handles without loading payloads; value pages
load and pin selected payloads once per view. Active disk reads also retain an
entry handle. Removing registry/current-index ownership does not instantly end
every reader's lifetime. External caches need explicit invalidation, authorization and
delayed-fill fencing under their [own contract](CLIENT_CACHE.md).

The timer namespace and policy namespace are internal control state. Timer cancel
retains a tombstone and cannot retract already queued/in-flight wakes or revoke
application authority. Do not treat a scan of public tenant keys as a complete
inventory of schedules, progress, policies or retained source material.

## Pick a storage boundary compatible with the promise

| Layout | Support and changes | Latency, volume and operating cost |
|---|---|---|
| Shared Store, explicit logical deletion and retention schedule | Reuses native namespaces, REMOVE and existing retention. Add authenticated offboarding, complete copy inventory, bounded cleanup and honest outstanding-state reporting. | Keeps shared capacity efficient and adds no hop/fsync to normal reads/writes. An N-key cleanup adds enumeration, serialization, tombstone append/retry and catalogue work; snapshots may load/pin the full namespace. Throttle it against live tenants. It cannot guarantee prompt tenant-only physical removal from shared files. |
| Dedicated tenant Store/volume plus scoped backups | Reuses the existing Store boundary. Add lifecycle/ownership fencing, complete stopped-root and downstream-copy inventory, qualified storage deletion and restore exclusion. | Normal embedded/local hot reads and Os/Flush write costs can remain unchanged. Separate residents, caches, pools, recovery and backups add fixed cost and reduce packing efficiency. Stopping an owner releases its workers/readers before whole-root cleanup, but no universal stop-time or media-erasure bound is supplied. |
| Future tenant-owned chunks or validated rewrite/compaction | Requires a new layout/transfer protocol preserving other tenants, latest state, CAS/retry outcomes, tombstones/horizons, control state and recovery publication. Not implemented. | Could improve shared-store tenant removal, at the cost of extra chunk metadata, write/read amplification, temporary space, sync/publication work and background contention. Bound bytes/work and measure its effect on live p99 and accepted volume before promising a deadline. |

## Offboarding is a durable control workflow

Keep the authoritative tenant lifecycle generation and cleanup job outside the
tenant data being removed or rolled back. Use stable job/operation identities,
minimal required reconciliation metadata and an explicit retention purpose for
that control record. A newly created tenant must not inherit a retired tenant's
authority merely by reusing a namespace string.

First reject new tenant work and fence stale routes, credentials and worker
admission. Then settle or retain the uncertainty of in-flight writes, jobs and
effects; issue explicit cancel/stop requests and observe their actual settlement.
Keep a trusted cleanup owner after caller access ends. Losing a connection or
receiving a timer wake does not establish that the old process stopped.

Inventory namespaces, internal controls, handles, local/remote caches, worker
spools, artifacts, collector/export queues, specialist backends and backups by
tenant/generation. Clean up through each owner's supported interface using bounded
work and stable operation IDs. Preserve necessary anti-resurrection/recovery
state. The native [Rust Client](RUST_CLIENT.md) supports one-key conditional
tombstones through `prepare_remove` and `remove`, with an explicitly requested
OS or Flush acknowledgment. Retain the original `RemoveAttempt` and uncertainty
within its original epoch and retry horizon; this in-memory token does not
restore lost retry context after caller restart. This primitive does not
implement archive eligibility, physical erasure or restore authority. There is
no tenant-delete method or tenant-wide offboarding workflow; a future service
must implement and qualify its supported cleanup surface rather than inventing
an unchecked private wire path.

Backup restoration must consult an authoritative exclusion/lifecycle record
outside the restored root before enabling access, timers, exporters or workers.
An old backup can contain both removed data and formerly valid authority. The
[backup lineage boundary](BACKUP_RECOVERY.md) also retires old connections,
receipts and fills; restoring a whole shared root is not tenant-selective recovery.
Track retained backups and their declared expiry separately from active-store
cleanup. Do not delete required import provenance to bypass startup verification.

Qualify interrupted/repeated offboarding, stale or disconnected workers, fills
finishing after revocation, retained snapshots/readers, full-store cleanup
pressure, mixed-tenant chunks, internal timers, blocked deletion, backend/export
failures and restore from a pre-offboarding backup. Existing retirement/cache
tests cover local retention, anti-resurrection and residency behavior; they do
not certify hosted deletion, downstream completion or storage sanitization.
