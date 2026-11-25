# Serving and effect participation boundary

The `serving_authority` module implements a checked participating atomic publisher.
It does not issue authority, promote quarantined roots, or activate a resident.
A production rollback-independent grant adapter and exhaustive deployment
participation are still missing. The generic provider owns this boundary; there
is no new authority service, journal store, queue or general manager.

Published leases provides journal-backed epochs, quarantine and cooperative
current validation. Its AUTHORITY contract explicitly says that the journal
cannot detect restoration of its own older bytes, and that validation followed
by an arbitrary unlocked effect is insufficient. This boundary therefore does
not turn a lease token, copied Store epoch or image label into a serving grant.

## Required independent owner

`EffectAuthority::enter(&EffectContext)` must atomically check a current external
grant, admit immutable original custody, and acquire a permit excluding grant
replacement for the ENTIRE provider effect and durable receipt settlement.
`EffectPermit::settle` must persist and read back the exact original context and
outcome before releasing that exclusion. Dropping a permit, process/channel loss,
a missing receipt or unknown effect must retain UNSETTLED. No timeout alone
makes effects absent. A pre-effect current validation without exclusion does not
implement this interface correctly. The contract applies across every admitted
writer and receiver; a caller-provided list is not discovery or completeness proof.

`Grant` binds store identity, independent authority incarnation, monotonic
generation, selected complete history digest and the exact participating writer/
effect-sink population. The selected history must include original receipts,
operations and reconciliation of ALL post-capture effects, not merely the image
manifest. Every participating receiver must reject former grants after replacement,
retain its fence with its own effects, and coordinate its own recovery with that
same external authority. Unknown legacy/raw/timer/OS/external effects cannot be
added to the supported population by naming them in a list.

The independent authority, fence and outcome custody must be excluded from the
Store/image rollback domain. Their own restore/restart must establish a qualified
non-regressing floor or new receiver incarnation; missing state must refuse,
not initialize an empty replacement. A trait implementation or a matching hash
alone proves none of these deployment properties. There is deliberately no
production adapter or constructor that fabricates a rollback-independent floor.

## Implemented source capability

`AtomicPublisher::new(authority, grant, participant)` checks bounded nonempty
identity/incarnation, positive generation, nonzero history digest and a bounded
unique participant set containing the selected writer. Those checks are input
validation, not authority issuance. `publish(&mut client, &original, deadline)`
passes the exact original MAR1 bytes and grant to the authority before any provider
I/O and holds its permit through the canonical client operation and settlement.

Provider success requires a second, exact external receipt binding the full
context, version and original achieved Ack. Missing/inexact external receipt
returns `ServingError::Unsettled` with original context and any known provider
receipt. Transport or provider uncertainty drops the permit without settlement,
retaining admitted external custody by contract. A definitive pre-admission
refusal can settle only that no NEW effect was admitted; the external authority
must never erase uncertainty from a previously admitted original. Stale, unknown,
unavailable or unsettled authority refuses before provider I/O. The wrapper
neither retries nor generates another operation/view/custody context.

This is an independent source participation capability conditional on a real
adapter. Its authored witness holds a serialized noncopied fixture state and
synced original/receipt transcript outside the selected Store root. Controls
show former-grant rejection, replacement exclusion during an admitted effect,
unknown-sink refusal, and post-provider/receipt-loss UNSETTLED retention. This
fixture is not a deployed authority, persistent recovery implementation, hardware
monotonic floor, adversarial proof or restart-safe authority service.

## Exact remaining deployment and recovery evidence

- A qualified independent grant owner/adapter with durable original custody,
  authenticated dataset/endpoint binding, non-regressing grant replacement,
  receipt readback and independent-authority recovery/floor evidence.
- Complete inventory and receiving changes for every old/new raw, atomic,
  policy, timer, maintenance and external writer/effect sink. This wrapper gates
  only its own explicit atomic publishing path; ordinary Store/resident APIs
  remain legacy and cannot be called fenced by this module.
- All admitted originals and every post-capture provider/OS/external outcome
  settled against the selected history. Missing/unsupported outcomes stay
  UNSETTLED; no fake empty history or unsupported success is supplied.
- Separate recovery qualification and explicit grant for a promoted history,
  with old writer exclusion and all receiver floors tested. Current quarantine
  is permanent and has no promotion operation.
- Actual caller adoption, complete existing-data handover, deployment transition,
  physical durability/power-state and authority-domain rollback qualification.

Until those exist, quarantined recovery remains read-only and no live serving,
OS effect, safe takeover or universal host rollback protection is claimed.
