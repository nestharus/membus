# Live Store namespace data headroom

`Store::reserve_namespace(namespace, ReserveRequest { bytes, appends,
lifetime_ms })` issues a finite **live-Store** lease. It reserves unused
data-record byte headroom against the actual Store's admitted data limit,
excluding its maintenance reserve. All supported writers to that namespace
share the account, including writers without the handle. Other namespaces and
internal control records must leave its unused bytes available. The owner must
choose a real budget; there is no default or automatically provisioned namespace.

This is a bounded embedded Rust operation. The resident protocol has no grant
issuance/import operation. It is not authentication, a hosted tenant boundary,
filesystem free-space certification or a durable capacity transfer. Arbitrary
filesystem writers, another Store instance, restored/cloned roots, catalogue
files, image copies and sidecars are outside the promise. Existing maintenance
provision remains separate. Internal namespaces beginning `__membus_` refuse
issuance, while their admitted data records compete through the common limit.

## Admission and account meaning

The account spends before a possible append, under the same State coordination
as existing admission. Ordinary PUT, conditional PUT and REMOVE, each BATCH
item, policy/timer control/progress mutations, and atomic publication enter one
shared check. An atomic envelope consumes one append allowance and its complete
envelope/framing bytes, not one allowance per final change. Ordinary records
include identifiers, encoding and framing. A Mem acknowledgment consumes one
admission and zero persisted bytes; it does not acquire durable semantics.

For a new admission the invariant is:

```
retained charged data + pending data + new record + unused other entitlement
    <= state_limit_bytes - maintenance_reserve_bytes
```

When an admitted record spends its own lease, its bytes move from unused
entitlement to pending data and then retained charging; they are counted once.
All namespace writers share the lease's remaining bytes and append count.
Exhaustion refuses with the existing capacity code 7 before assigning a version
or enqueueing the write. A retained duplicate returns its original outcome
without spending again, even when the lease is exhausted. Definitive earlier
validation, conflict, queue, memory, quota, deadline and retry-slot refusals do
not spend. After admission, failed/partial/uncertain appends retain the full
conservative data charge and lease spend. A later refusal never erases earlier
uncertainty.

Future queue concurrency, index/live-key slots, retry slots, managed memory,
bandwidth and successful synchronization are **not reserved**. Their existing
limits may still refuse a request. This operation reserves data headroom and
bounds cumulative admissions; it does not guarantee that a future append will
succeed. Active population grows through the same issuance operation, against
actual configured DATA and managed-memory headroom. There is no compiled
population ceiling. Existing accounts are not dropped, expired or reissued to
make room. No live Config setter or Store restart is part of this operation.

The Store charges `512 + 4 * namespace.len()` managed bookkeeping bytes per
active account before allocating its names or growing the dynamic book. This
is the provider's conservative managed charge, not an exact allocator/RSS or
free-RAM measurement. Table growth and name allocations use fallible standard
library reservation; refusal rolls back the new charge without consuming an ID
or changing prior accounts. Remaining Rust infallible allocations (including
the shared token) can abort on process OOM. Managed admission does not certify
physical allocator availability. Scans still do work proportional to active
book storage; no constant-time or unlimited-population guarantee is made.

## Pressure and maintained retry

`Store::reserve_namespace_detailed(namespace, request)` performs the same
issuance and returns `Result<NamespaceReservation<'_>, ReserveRefusal>`.
The compatibility `reserve_namespace` maps only the same refusal's `error`.
`ReserveRefusal { error, pressure }` retains the existing `ReserveError`;
optional `ReservePressure { resource, limit, required, available,
active_accounts }` observes the refusal under the State lock used by admission.
Numeric fields are `Option<u64>`; None explicitly means unknown/unexposed.
This is a point observation, not a future grant, reservation or no-effect query.

For `DataBytes`, the limit excludes maintenance provision; available subtracts
retained, pending and all unused lease entitlement. Required is the new byte
request. For `ManagedMemory`, the limit is configured Store capacity and required
is this account's bookkeeping charge. Available is the existing non-cache
managed headroom; evictable cache can be trimmed, while pending/index/retry,
snapshot and account ownership are retained. These figures are not physical
free memory. For `Allocation`, the allocator exposes no truthful total free,
required or available bytes through this API: all three are None. For `Identity`,
the representable monotonic ID space is exhausted; no safe reset/reuse exists.
Namespace encoding bounds and checked expiry/ID arithmetic remain distinct
representation invariants, not active-population policy.

The owner holding an actual lease can call `release(self)` or drop its last
alias to return unused entitlement and account backing. Other active owners
retain their rights; issuance cannot revoke them. The Store owner may then
retry the same namespace/request through `reserve_namespace_detailed`. A refused
attempt created no lease ID, spend or publication to renew. A successful one
issues a new live lease; an already-owned namespace refuses AlreadyReserved.
There is no automatic retry, capacity raise, backoff engine or expiry renewal.

Legitimate whole-chunk retirement can separately replenish global DATA only
after the existing deletion, directory and catalogue barriers. `retire_chunk`
requires its actual eligible operand; `retry_retiring` finishes retained
obligations. Neither refunds spent lease allowance. Managed owners may release
their genuine accounts/views or let pending work settle. If no owner can safely
release enough backing, the refusal remains: this API cannot create RAM, storage,
an allocation guarantee or new identity representation. Store restart loses
borrowed leases and cannot stand in for live population growth.

`NamespaceReservation::id()` identifies an account within its issuing Store.
Aliases retain that identity across growth. Release revokes every alias; a stale
alias cannot observe or revoke a replacement ID. The ID is not a durable grant
credential and may have the same numeric value in a different Store instance.

## Lifetime, aliases and retention

`NamespaceReservation::status()` returns remaining/admitted bytes and appends
and its fixed monotonic expiry. Status is an observation, not an operation
receipt. Clones share one token and account; the last alias drop releases only
unused entitlement. `release(self)` explicitly revokes every alias and returns
the final account observation. Old aliases cannot revoke a replacement lease.
Expiry also releases only unused entitlement, at the next coordinated access;
wall-clock correction does not renew it. Writes admitted before expiry retain
their charges and uncertainty. After release/expiry, later namespace writes use
ordinary unreserved admission; this is not a permanent namespace write fence.

Overwrite, REMOVE, logical document sealing, retry expiry, retained completed
create intents and tombstone/index changes do not refund cumulative lease spend.
Normal retention policy remains authoritative. The existing retirement path
reduces Store data charging only after its actual deletion/barrier/Retired
publication; that frees global data capacity, not spent lease tokens. Held or
failed retirement retains its charge. This lease neither changes retention nor
claims erasure of bodies, histories, retries, images or downstream copies.

## Restart and unsupported transfer

The token borrows the actual Store. It has no serialized representation,
restoration, renewal or transfer API; the borrow prevents closing or capturing
that Store while a token is retained. Unused grants cannot be restored from
copied metadata, an epoch or an old status. Process/Store loss ends the lease;
there is no durable post-restart entitlement or automatic reissuance.

Startup import/replay completes before grants can be issued and reconstructs
actual retained data and retry state using existing barriers and admission
limits. A subsequent owner may explicitly request a **new** finite lease against
that recovered capacity. Replay does not refund an old caller's uncertain
operation or authorize its resend. A durable all-writer namespace grant across
restart requires an owner-qualified durable grant/incarnation and transfer
contract that this operation does not expose. No new persistence subsystem or
claim of that larger capability is made here.

The public controls in `tests/namespace_reserve.rs` require an explicitly
selected `MEMBUS_NAMESPACE_RESERVE_TEST_ROOT` directory and retain authored
Stores there. They exercise actual Store data admission, not installed caller
provisioning, OS-principal isolation, power loss or a crash harness.
