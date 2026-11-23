# Atomic namespace publication

This additive library API publishes a bounded final set only when the complete
captured live key/revision population still matches, including absent-key
phantoms, removals and replacements. The initial compatible class is FOREVER:
any different prefix policy or stored class in the namespace refuses explicitly.
It does not establish the suitability of that class for every application's data.
Existing BATCH remains independent per-item publication.

Use [read-only result inspection](ATOMIC-RESULT-INSPECTION.md) to observe retained
same-original evidence without allowing first admission on a miss.

For complete populations beyond the legacy capture profile, use
`capture_namespace_funded` with an actual finite Store reservation. Retained
originals and clones keep their backing funded, and startup validates borrowed
originals. See [allocation ownership](ATOMIC-RESOURCES.md) for the public budget
and codec contracts, conservative charging, and compatibility limits.

```rust,no_run
use membus::{Store, atomic::{AtomicAck, AtomicRequest, FinalChange, PublicationError}};
use std::path::Path;
fn decide(store: &Store, custody: &Path, deadline: u64) -> Result<(), PublicationError> {
    let view = store.capture_namespace("authored")?;
    // Compute every final change using view.rows(), including any permitted alternative,
    // before submitting. Do not publish intermediate scheduling decisions.
    let original = view.prepare("unique-original-operation", AtomicAck::Flush,
        vec![FinalChange::put("first", b"one".to_vec()),
             FinalChange::put("second", b"two".to_vec())])?;
    original.persist_new(custody).map_err(|_| PublicationError::Unavailable)?;
    // Reconnect/restart custody uses AtomicRequest::from_bytes on THESE exact bytes.
    // A new capture or operation identity cannot reconcile an uncertain original.
    let outcome = store.publish_atomic(&original, deadline)?;
    assert_eq!(outcome.ack, 2);
    Ok(())
}
```

NamespaceView owns a charged complete value/revision capture; its lifetime is
bounded by the lesser of snapshot TTL and retry lifetime. AtomicRequest owns the
immutable namespace, operation ID, capture epoch/horizon, population, requested
Ack and unique final targets. Its versioned bytes and new-only, file+directory
synced custody writer let a caller retain the original before transmission.
If the save fails or its reply is lost, keep the original and preserve the
selected path. Reading the file can verify its contents. Sending requires
evidence that both custody barriers completed.
The provider's admitted envelope independently retains the original bytes and
whole-set outcome. This is an in-process Rust API; no new wire opcode, Python
binding or resident service protocol is claimed.

On Windows a custody owner can use `let held = original.persist_new_held(path)?;`
and bind `held.file_id()` into its own confirmation while retaining `held`.
The 24 bytes contain the volume serial in little-endian order followed by the
128-bit Windows file ID. Membus queries the original writer handle after both
barriers. It keeps that handle open with read sharing only until the holder
drops. A separate reader opened during that interval must share write access
with the existing writer. The caller supplies a new file name in a directory
whose path, including its ancestors, it keeps stable throughout the save.
Named data streams are refused because their IDs identify the containing file.

Allow `HeldOriginalFile::SAVE_BYTES` of headroom on the account that funded
the original. The save reserves its fixed encoding buffer and holder before
creation. Only the holder's charge survives success. A refusal for insufficient
headroom returns `OutOfMemory` before touching the path. After an I/O error,
keep the same prepared request and any file the writer created. Matching bytes
alone cannot establish which barriers completed. The returned ID identifies
the file object. The domain owner supplies its own signing authority and binds
the ID to its authenticated original.

Before any target effect, publication checks complete population, supported
classes/policies, final net quota, new index slots, aggregate queue/stripe slots,
retry slot, frame, memory (including chunk/holder and decoding charges), and disk
budget. One reserved version range identifies the final entries. One MBK1 frame
with a reserved multi-entry discriminator, envelope version and CRC contains the
complete request. One append and selected covering Ack precede a single committed
index publication. Each indexed entry selects its own subrecord inside that same
frame. Routing registers every key with the chunk/holder owner. Replay validates
the complete envelope before projecting entries and installs one original retry
identity/outcome. Retirement preserves the whole frame through the outcome's
retry horizon, even through a forced request. No per-key CAS emulation is used.

The provider gate is acquired before index/state coordination. Raw KV holds a
shared gate through completion; policy mutation holds it through projection;
expiry/tombstone maintenance holds it through index removal. Atomic capture and
publication hold the exclusive gate, and take committed-index before state.
Policy's internal mutation avoids recursive shared acquisition. Readers continue
against the old index during append/flush and observe all final entries after
one index publication. External signal messages are per-key notifications;
they are not an atomic external-effects transaction.

Conflict, Expired, Unsupported, Invalid, Resource(code), IdentityMismatch,
RetryExpired and Unavailable are pre-admission refusals. Uncertain means admitted
append/sync/completion failure; the instance fences writes and retains the
original outcome as uncertain. Lost caller replies are also uncertain to that
caller; retry only the original. Retained success is returned before consulting
a fresh population, deadline or epoch, so a duplicate cannot overwrite newer
values. RestartUncertain means no retained original outcome exists in this
instance; it forbids re-running the original decision after a changed epoch.
A complete failed-sync envelope may become a successful retained Flush outcome
only after startup's actual covering recovery barriers. A torn envelope produces
no prefix and no inferred success. OS Ack remains OS even if recovery syncs it.

The image recovery API exposes an isolated read-only inspection handle. Neither
a copied epoch nor this custody file grants independent serving, timers or OS
effect authority. The smallest external boundary still needed to resume serving
is a rollback-independent monotonic fencing grant, checked by every old/new
writer and effect sink, binding store identity, original operation identity and
outcome custody. Restoring that witness with the image would defeat it. This
library adds no authority manager and makes no universal host-rollback claim.
A deployment and its existing domain owner still owe that actual noncopied
witness, all-data/retention semantics and an identity-preserving live handover.
