# Store-owned subscription resource reservations

The Store issues `SubscriptionReservation` through
`reserve_subscription_resources`. This debits its existing State
`memory_reserved` account under the same mutex used by ordinary admission.
Snapshots and atomic publication still check the same cache allowance. A demand
or `SubscriptionResourceProfile` is never an allocation grant. No free-RAM or
RSS sample participates. The public source module is `subscription_resources`.

`SubscriptionResources` has five checked managed-byte parts: metadata, retained,
in_flight, encoding and publication. Demand builders describe the existing
concrete registration and Change/Value layouts, string/queue backing capacities,
two encoding Vec capacities and a prospective publication. They allocate nothing.
Supply actual backing capacities, including growth slack, not just payload lengths.
The connection-map coefficient is conservative managed bookkeeping; native kernel
buffers, allocator overhead and RSS are not measured. Existing memory margin is
separate. Reaudit layout charges when the receiving Hub changes its structures.

Only the Store creates new owned bytes. `extend_subscription_resources` verifies
the exact original account identity and reserves additional demand at the current
allowance. Refusal changes neither account nor token. `ReservationError` separates
Capacity, InvalidDemand, ForeignOwner, Returned and InsufficientPart. No new slot,
queue, byte or time hard cap is imposed by these APIs.

Move the non-Clone token into the representation's owner. `split` transfers chosen
parts without increasing the shared debit. `transfer` reclassifies already owned
bytes, for example Retained to InFlight. `return_to_owner` is idempotent and returns
only that token's remaining charge. A destructor is a fallback. Release covered
allocations first; when embedding the token, arrange field destruction after those
allocations. An old in-flight child survives registration cancellation/expiry and
returns only after that exact callback. It cannot identify or release a replacement.

Lock order is part of the receiving contract. Existing HEALTH2 takes State
before the Hub observation lock. Grant, grow and return (including token Drop)
take State. Never hold the Hub lock while invoking them or discarding a charged
token. Reserve before acquiring Hub, then install or roll back after releasing
Hub; detach returned/cancelled portions under Hub and settle them outside it.
The prospective allowance calculation itself needs no Hub lock.

The profile reports configured capacity, committed index, retained retries,
existing reservations, retained snapshots, margin, chunk index and a prospective
legacy signal envelope. `available` is the cache-yielding managed allowance at
that observation, not a promise for the next request. Grant admission recomputes
it under State and trims the cache before returning. Charge return sets a cold-read
reclaim hint; a later cold miss recomputes current allowance rather than adding a
stale released amount. Hot-hit clocks, cache hashes and snapshot lifetimes remain.

Legacy publication grows independently of State. While any new grant survives,
its future allocations therefore cannot be covered by a current history/queue
sample. The allowance now reserves a conservative entire prospective legacy
envelope: configured history, existing bounded queue charges, row/string and
queue/map backing, append-before-evict/replay links, one maximum Change and dual
encoding buffers with growth slack. This uses existing legacy representation and
policies; it does not enlarge them. Even a quiet or history-disabled legacy Hub
has prospective publication/encoding ownership. The reservation reduces cache
and other admission allowance substantially; a small configured capacity can
have no available allowance. This is a source accounting change, not operating
capacity or a measured physical-memory ceiling. HEALTH2 retains its current
logical signal sample; the resource profile exposes the prospective reservation.

Separate read-cohort source adds ReadUsage to the same cache allowance. Receiving
must retain that subtraction and the last-owner release hint. This module does
not compose or replace the read-cohort implementation. Grants in memory_reserved
continue to reduce allowance alongside that independent account.

This is a callable resource-owner source capability. It does not register or expire
a subscription, select a wire profile, renew a lifetime or acknowledge delivery.
The legacy and explicit unavailable-discovery contracts are unchanged. The next
Hub implementation must reserve before allocation, transfer to actual owners,
account new publication growth before allocating, retain in-flight children and
return on exact completion. It must also positively negotiate lifetime, binding,
buffering/replay/coalescing/overload and time/recipient acceptance requirements.
No client, Python or caller migration is supplied here.

Pure account tests execute production State arithmetic, issuance, growth,
transfer and explicit return with authored operands and finite contention. Public
tests compile actual Store signatures and exercise demand layout builders. No
Store/Hub/native endpoint/data/process/retirement/destructor fixture is used;
public Store runtime and subsequent Hub integration remain separate qualification.
