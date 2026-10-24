# Backup boundaries and isolated recovery

This guide defines what an operator or future backup adapter must preserve.
There is no online backup/export/restore protocol in the current public client.
A query snapshot captures namespace values for a bounded lifetime; it does not
capture the complete recovery account, pin every required backing file for an
external copier, or create a durable checkpoint. Replication and automatic
promotion also require contracts beyond today's local Store.

## A complete recovery account

Begin with an independently retained copy of the **whole stopped state root**,
after the writer and its timer/maintenance owners have stopped and been waited.
Prevent supervisors or another host from reopening that source during capture.
A directory's owner lock fences cooperative access to that directory; it does
not fence another copied directory or a remote authority. A stop request alone
does not prove that outstanding writes and owners have settled. A successful
copy/readback also does not retrospectively prove every original requested Flush.

Preserve the complete root layout, attributes needed by the reader, and any
interrupted-operation evidence. This is intentionally broader than a handpicked
list of currently live values:

| State | Why it matters |
|---|---|
| `chunks/catalogue.log`, class directories and chunk files | Open/Closed/Retiring/Retired state, policies, version high-water, horizons, latest records, tombstones and retained retry records belong together. |
| Catalogue append markers and retained sidecars | An unresolved append can refuse startup even when a catalogue frame looks complete. Removing evidence or guessing a new boundary is not recovery. |
| Legacy stripe inputs, `legacy-<epoch>` and import-related trees/inputs | Import records retain exact size/hash provenance. Startup continues verifying imported inputs even after their dedup contribution expires; latest-value export omits that history. Preserve interrupted import evidence too. |
| `epoch` and clock/heartbeat state | Startup validates and increments the persisted epoch; clock history influences retirement/retry reconstruction. Omitting these silently changes recovery behavior. |
| Timer schedules/progress and policy records in the data | They are internal persisted records, not a separate list of public GET results. Their replay can change subsequent activity. |
| Replay reports/history and other retained files | Preserve the evidence explaining the capture and any prior refusal. An old ready report does not prove this restore will start or serve. |

The ownership artifact is not a portable live lock. Preserve it as captured
evidence, then follow the target platform's owner-lock rules when preparing a
private working copy. Windows enforces a live share-denied handle; non-Windows
create-new ownership requires confirmed-stopped cleanup. Never bypass a live
owner. Keep deployment configuration, executable/source identity, storage
capabilities and application recovery obligations with the backup record;
credentials remain separately managed.

A stopped copy avoids concurrent append/retirement races but still needs a
qualified durable destination and complete readback/integrity checks. Hashes
detect mismatched copied bytes; they do not prove a live copy was consistent,
that the source had no corruption, or that a particular device survives power
loss. Retain the original capture unchanged. Open a disposable working copy for
validation, rather than letting recovery modify the sole retained image.

## Rollback can reuse an epoch

The resident epoch is a checked counter in this state directory. Consider a
capture containing epoch 10. The original directory subsequently starts at
epoch 11. Starting a restored copy of that capture also computes epoch 11.
This follows from the startup code; it is not an experiment or a claim of a
live incident. Separate clones can likewise have the same epoch and versions.

An equal HELLO epoch therefore does not prove continuity after restore or clone.
The current Rust Attempt guard compares that number and its retry horizon; it
has no external restore-lineage witness. Version, snapshot, subscription and
cursor identities must not acquire global uniqueness from this local counter.
Changing an endpoint or incrementing a copied epoch by hand is not an adequate
fencing or application-reconciliation protocol.

Before a restored history can serve as authority, the deployment must fence the
abandoned writer and retire old caller state from use: connections, cache fills,
cached current-state claims, snapshot tokens, subscription tokens and cursors.
Preserve original operation/Attempt and uncertain-outcome evidence for explicit
application reconciliation; do not resend just because the restored epoch is
equal, or mint a new operation ID to hide the uncertainty. A backup may predate
a successful effect, its acknowledgment, or its dedup record.

For a hosted service, introduce an authenticated authority incarnation/restore
generation outside the rollback domain, with a durable placement/fencing owner.
Scope cache identity, routing and receipts by that generation as well as the
resident epoch. This is a proposed service contract, absent from today's wire
protocol. It must coexist with stable original application operation IDs, rather
than reset deduplication by renaming an uncertain operation.

## Recovery qualification and cutover

1. **Capture and retain.** Fence admission, settle the owner, capture the complete
   stopped root, and establish independent destination durability/integrity.
   Record the actual capture interval and observed state boundary. An intended
   backup schedule is not an achieved recovery-point promise.
2. **Inspect an isolated working copy.** Verify inventory and source/binary
   compatibility before opening it. The read-only catalogue inspector can help
   with individual chunk association; it is not a complete restore verifier.
   Keep the retained original unchanged, including refusal evidence.
3. **Recover within isolation.** Store opening can truncate permitted torn Open
   tails, finish catalogue/retirement/import work, increment epoch and write
   reports. It also starts maintenance. It is not a read-only preview. The
   executable additionally starts timer ownership after replay; keep restored
   timers, dispatchers and outward effect paths isolated during qualification.
   A Store embedding controls timer-owner startup but still mutates on recovery.
4. **Reconcile state and effects.** Check current health, storage/read-only flags,
   retirement status, required records, policies and timer/application state.
   Startup can refuse, or return a storage-error/read-only Store; a ready report
   alone is insufficient. Retention and retry age use their real recovery clocks,
   rather than pausing at backup time. Restored timer progress may repeat wakes;
   broker/outbox and provider/account state may be ahead of the backup. Reconcile
   original IDs and useful effects before enabling delivery, reassignment or
   spending. An old cancel receipt may also postdate the restored history.
5. **Cut over deliberately.** Establish the new authority generation, fence the
   former route/writer, retire old caller state, and expose only qualified
   service. Record missing post-capture state and the observed recovery time.
   Retain independent backups and the abandoned history for their declared
   retention purposes. Restoring the same shared root rolls back every tenant
   in it; it is not a tenant-selective restore.

The [recovered Flush prerequisite](RECOVERY_FLUSH.md),
[catalogue append recovery](CATALOGUE-APPEND-RECOVERY.md),
[directory barriers](RECOVERY-DIRECTORIES.md),
[retirement/import rules](CHUNK-RETIREMENT.md) and
[timer crash windows](TIMERS.md) remain applicable. Recovery may expose a
previously uncertain complete append after its required barrier; reconstructing
latest values alone cannot decide whether an external effect already happened.

## Future manifest and performance boundary

A future versioned backup manifest should bind logical authority and tenant
coverage, capture/restore lineage, source format and executable identity,
configuration, clock/capture interval, full relative-file inventory with sizes
and digests, imported-input provenance, required storage durability and validated
recovery results. Publish it only after all referenced data is durable; retain
those objects independently of active-store GC. Do not label an untested file
copy with a distributed commit position it never captured. A live backup needs
an implemented capture/seal barrier and retained file/metadata dependencies.

Offline capture adds downtime and copying/readback I/O, but no per-operation
broker hop, serialization, lock or fsync to the running KV path. A future live
backup can reduce downtime at the cost of capture coordination, pinned storage
and competing upload/CPU/I/O. Remote uploads belong outside ordinary KV
completion unless the caller explicitly requests a remote durability contract.
Restore pays replay/integrity work, required file/directory syncs and cold-cache
reads; a successful hot-read benchmark says nothing about that recovery time.

Before offering recovery commitments, qualify full capture/restore on authored
state, missing/corrupt files and unresolved markers, interrupted copy/import,
rollback epoch reuse, old-client fencing, aged retention/retry state, timer and
outbox replay, independent-destination loss and shared-tenant scope. Existing
`recovery_epoch` and `recovery_flush` tests cover startup primitives, not an
end-to-end backup service, physical power loss or a recovery-time objective.
