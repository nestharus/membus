# Read-only atomic result inspection

`Store::inspect_atomic_result(OriginalRef, &OriginalBudget)` observes existing retry evidence for
one exact atomic original. It is an embedded Rust Store operation, alongside
`publish_atomic`; it does not invoke publication or use a resident wire request.
No new deadline, operation ID, capture or decision is supplied.

The public types are exported from `membus::atomic`:

```rust,no_run
use membus::{Store, atomic::{
    AtomicRequest, AtomicResultEvidence, BorrowedOriginal, InspectionError, OriginalRef,
    OriginalBudget, RESULT_INSPECTION_SCRATCH_BYTES,
}};

fn inspect_owned(store: &Store, original: &AtomicRequest, scratch: &OriginalBudget) -> Result<(), InspectionError> {
    let observation = store.inspect_atomic_result(OriginalRef::Request(original), scratch)?;
    match observation.evidence {
        AtomicResultEvidence::NotRetained => {
            // Keep prior uncertainty. Absence supplies no permission to send.
        }
        _ => { /* Reconcile the typed evidence with the caller's existing custody. */ }
    }
    Ok(())
}

fn inspect_serialized(store: &Store, bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    // The existing borrowed validator refuses incomplete/corrupt/oversized MAR1.
    let original = BorrowedOriginal::from_bytes(bytes)
        .map_err(|e| format!("invalid original: {e:?}"))?;
    let scratch = OriginalBudget::new(RESULT_INSPECTION_SCRATCH_BYTES);
    let _observation = store.inspect_atomic_result(OriginalRef::Borrowed(&original), &scratch)?;
    Ok(())
}
```

The exact signature is:

```text
Store::inspect_atomic_result(&self, original: OriginalRef<'_>, scratch: &OriginalBudget)
    -> Result<AtomicResultInspection, InspectionError>
```

`OriginalRef::Request(&AtomicRequest)` borrows a prepared/funded immutable
original; `OriginalRef::Borrowed(&BorrowedOriginal)` borrows an existing validated
MAR1 frame. Neither variant converts the population to owned storage or copies
the original body. The existing canonical SHA-256 fingerprint covers namespace,
operation ID, epoch, original monotonic horizon, requested Ack, complete ordered
population and final changes, including exact opaque value bytes. No caller
supplied digest or namespace/ID-only association is accepted. Association uses
the provider's existing fingerprint contract, not a new cryptographic authority.

## Evidence and time

The observation carries `store_epoch`, `original_epoch`,
`original_expires_mono_ms`, `observed_mono_ms`, `fingerprint`, and typed evidence:

| Evidence | Meaning |
|---|---|
| `Matching { recorded_at_utc_ms, retry_until_mono_ms, retention, result }` | Current retry entry matches the original's exact canonical fingerprint. |
| `IdentityMismatch` | This namespace/operation ID has a retry entry with another fingerprint, even if that entry is expired. |
| `RetiredIdentity` | The bounded anti-resurrection ring still names the namespace/ID; its fingerprint, result and exact cutoff are no longer available. |
| `NotRetained` | Neither retry entry nor retirement marker is currently retained. Never admitted, refused, expired, evicted and absent history cannot be distinguished. |

For `Matching`, `RetryRetention::WithinWindow` includes exact equality with the
retry cutoff, as publication does. `Expired` means `observed_mono_ms` exceeds
that cutoff. The result is separately retained, so an expired historical outcome
is not silently converted to current success or loss of prior uncertainty:

| Retained result | Meaning |
|---|---|
| `Committed { version, achieved: AtomicAck }` | Recorded success with a nonzero valid revision range and achieved Ack matching this original. The version is the first revision of the atomic set. |
| `Pending` | The admitted retry identity has no settled outcome. Effects may already be in progress. The query does not join or wait for publication. |
| `Uncertain { recorded_code }` | Retained non-success code, preserved literally. It does not prove absence of effects. |
| `InvalidOutcome` | A success-shaped stored outcome has an invalid revision/Ack association. Preserve uncertainty. |

Pre-admission refusals are not installed as atomic retry outcomes by the
published Store. This operation therefore cannot recover an unrecorded refusal
or distinguish it from an unsent original. It reports `NotRetained` in that case,
not an invented definitive no-effect result. `Ok(observation)` means inspection
completed, not that the atomic operation succeeded.

Retained evidence is inspected before making any inference from original epoch
or capture horizon, consistent with publication's existing retained-result
priority. The original horizon is unchanged; it is meaningful on the original
instance's monotonic clock. A retained receipt may outlive that capture horizon.
Across restart, the original horizon must not be compared with another epoch's
clock. Recovered retained entries use the existing startup-derived local retry
cutoff. No query extends either horizon, retains an entry longer, or resurrects
an identity after eviction. Any later query may observe a different retention
state, completion, retirement marker or missing record.

## No-effect boundary and resources

The implementation reserves 256 bytes in the caller's supplied finite
`OriginalBudget` before streaming the original fingerprint and taking one state
mutex, then borrows the existing tuple key for HashMap/HashSet lookups. It reads
only the current retry entry or retirement marker and the monotonic clock. It
never calls the effecting `retained_outcome`/`publish_atomic` admission path,
enqueues an append, obtains a population/view, takes the publication gate, waits
for pending work, loads a value, trims a cache, expires/prunes a retry, changes
a Store counter/flag, restores data or clears a fence. A miss returns directly.

The transient reservation is released on every return, including errors. The
same caller account can bound concurrent query scratch. It creates no Store
reservation or original/view grant. The result is fixed-size and retains no
references, grants or Store resources.
Input lifetime remains with the caller's existing original/frame. Original
validation and the existing frame/identifier/change-count limits bound input
work. Fingerprinting is linear in encoded original bytes; HashMap collision
work remains subject to the existing Store table/capacity and hashing behavior.
There is no worst-case constant-time lookup, hard timeout or cancellation claim.

The existing SHA helper uses small transient heap buffers, rather than a
zero-allocation implementation. Its partial block and padding storage are
bounded independently of original/population size; it repeatedly recycles small
buffers when hashing owned original fields. The caller funds the conservative
`RESULT_INSPECTION_SCRATCH_BYTES` allowance; no body/population allocation or new
Store grant is needed. This is managed logical accounting, not a physical memory
or allocator-overhead guarantee. Authored allocator controls on a 4161-guard,
maximum-value original observed 6780 allocation/reallocation events across the
owned+borrowed inspections, maximum request104 bytes, peak104 bytes, zero
remaining scratch; a retained small original observed18 events, maximum64,
peak128, zero remaining. These are requested managed scratch bytes on that test
build, not allocator overhead/RSS or a universal platform allocation count.

`InspectionError::ScratchUnavailable` refuses before hashing/state access if the
caller account lacks the full allowance; it is not evidence about the operation.
`InspectionError::StateUnavailable` reports a poisoned state mutex without
clearing it. Infallible standard-library allocation abort and process death are
not converted into recoverable errors or proofs of no effects. Existing
read-only/warming/storage-error flags are not changed or treated as a serving
grant; an already retained receipt remains observable in a fenced Store.

## Recovery and caller applicability

The query performs no replay or recovery barrier. An opened Store exposes replay
outcomes only after its existing required covering startup barriers. A complete
previously uncertain frame can then have retained committed evidence; a torn
frame supplies no inferred atomic success. Requested OS stays achieved OS even
when recovery synchronizes the file. Flush evidence depends on the existing
covering-barrier contract and hardware/filesystem limits. No new physical
durability claim follows from this query.

Callers must keep prior uncertainty and bind the observation to their original
Store/endpoint and outcome custody. Matching fingerprints and equal epochs do
not authenticate a restored clone, serving incarnation, domain owner or native
process/Job. Two copied authored histories can report the same receipt and epoch.
A stale successful receipt is not a current grant or permission to repeat an
external effect. A missing, expired, mismatched, invalid or unavailable result
cannot be promoted to successful no-effect/empty input or a fresh decision.

This supplies only the embedded provider result-inspection edge. Existing-file
barrier confirmation, immutable separate admission-deadline/prior-result custody,
authenticated domain/native process-loss restoration and external serving fences
remain separate caller/provider contracts. MAR1 parsing does not create them.
There is no new resident protocol or client binding, application append-receipt
composition, installed integration, migration or operating acceptance here.
