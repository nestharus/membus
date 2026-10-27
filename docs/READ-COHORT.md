# Explicit read cohorts

`Store::read_cohort(byte_budget)` reserves a finite managed-memory budget and
returns a `ReadCohort`. Its `try_get(namespace, key)` returns an optional
`ReadValue`, or an I/O/capacity error. `ReadValue` dereferences to `Value`;
cloning it preserves ownership. An explicit `Value` copy is caller-owned.

Each call selects its current committed Entry. A cohort does not capture a
namespace, freeze the live index or promise a whole-store generation. An exact
file-handle identity, offset and indexed extent bind reusable interpretations;
every selection still checks namespace, key, revision, Ack, tombstone and length.
The complete MBK1 frame, CRC, original and every MAR1 member/tail are validated
before installing the interpretation. Durable formats and publication bounds
are unchanged.

One frame is retained. Crossing to another frame or calling `clear()` evicts that
retention. Returned values can keep an evicted frame alive; their memory remains
inside the cohort budget and may make the next frame refuse. Dropping the cohort
does not return its reservation until the last returned value drops. Clearing and
reacquiring a frame repeats capture and validation. Failures are not retained;
integrity/I/O errors keep the existing sticky storage-error observation.

Budget reservation and frame admission happen before allocation. Small/zero
budgets return InvalidInput; exhausted managed capacity or a frame that cannot
fit returns WouldBlock. `Store::read_cohort_bytes()` reports reserved cohort
bytes, including returned values that outlive their cohort. These bytes also
contribute to managed memory and reduce the ordinary cache allowance. Charges
conservatively cover encoded bytes, interpreted payloads and member bookkeeping;
they are not operating-system RSS measurements.

Ordinary snapshots share the same envelope interpreter. Additional frame memory
is admitted under the existing snapshot byte and managed-memory limits, and is
retained through ScanCharge ownership. A page can refuse snapshot-full (22) if
workspace capacity is unavailable; no partial frame is installed. Snapshot row
values and immutable captured revisions survive registry release/expiry while
owned. Release/expiry does not pretend a live owner is gone. Singleton snapshot
loads preserve the revision-cache path and per-row pinning.

Default `Store::{get,try_get}`, GET and GET_MANY retain their cache-bypass
contracts. Disabled caching, never-cache and zero active TTL do not silently
acquire cohort retention. Callers must explicitly select a cohort to share work
across point calls. No protocol or SDK method is added by this library API.
