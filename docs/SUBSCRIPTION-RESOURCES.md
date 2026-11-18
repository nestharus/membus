# Actual allocation ownership and quiet Store compatibility

The existing 16 MiB quiet configuration keeps its 14 MiB allowance after the
existing 2 MiB Store margin. No future signal envelope is debited merely because
the Hub exists. Actual admitted signal ownership is still deducted from the
one Store State allowance; ordinary pending/atomic, retry index, snapshots,
chunk index and separate retained-read ownership are not overwritten or stolen.

The internal State now owns a shared subscription-usage counter. New grants and
growth still serialize on State and check its complete current allowance.
Exact token returns subtract only that subscription portion atomically after
the covered allocation is freed and set the existing cold-read recalculation
hint. They do not lock State under Hub or alter ordinary memory_reserved.
Public reservation methods and v2 wire fields are unchanged. The advisory
profile's reserved field includes ordinary plus live subscription ownership;
legacy_signals reports the actual legacy subset already included in reserved,
not a second debit or a hypothetical empty-Store envelope. Concurrent release
can change advisory readings; a profile never grants later capacity.

In production, the Store holds State and lends the Hub a budget that includes
current snapshot, chunk-index and retained-read charges. The budget can shrink
the cache before debiting a new grant. A test-only adapter uses weak references
to exercise the same budget arithmetic with authored capture inputs.

Legacy registration reserves its real row, namespace/prefix, linked-node
backing and endpoint/connection state. Its existing queue policy authorizes at
most 256 linked nodes; its negotiated frame bound reserves exact prospective
body/frame encode capacities, limited by existing representable payload fields.
The legacy registration returns a capacity refusal when that backing exceeds
available headroom. A value-mode replay's encoding demand follows the negotiated
frame bound, so an affordable small-frame registration does not establish
admission at the maximum frame. Deactivation returns empty queue authorization.
Terminal status metadata stays owned until eviction or controlled stop. A live
worker retains its endpoint and encoding child through completion.

Each legacy publication reserves its actual Change/Value/string/payload capacity
and history-node ownership before allocation. Linked queues/ring avoid retained
uncharged Vec/map capacity. Shared Change and slot wrappers consume the final Arc
before freeing retained representations and returning exact ownership. Queue
links belong to row backing; history links belong to their publication owner.
Allocation pressure explicitly drops matching legacy subscriptions and clears
history to a resume gap. An already committed ordinary operation keeps its
original result. With history disabled and no legacy subscriber, finite dirty
hints require no retained Change/value allocation.

Legacy change/1 retains its framing, statuses, limits and token/cursor layout.
Finite profiles retain negotiation, expiry, renewal, full binding and coalescing.
The publisher holds the committed map and State, samples chunk and read-cohort
ownership, then enters the Hub with the borrowed budget. Grant issuance can trim
the cache while the Hub is held. Worker releases return bytes atomically.

No physical RSS/kernel bound, recipient acceptance, authenticated process death,
hard native-drain deadline or unknown-creation settlement is established. Unknown
creation remains unresolved without actual clock/incarnation witnesses and cannot
be blindly retried or resolved from endpoint/operation names. This source change
does not migrate SDK/Python/leases/current callers or install a default.


Public methods remain reserve_subscription_resources,
extend_subscription_resources and subscription_resource_profile. Five demand
parts remain metadata, retained, in_flight, encoding and publication. Demand
builders do not allocate or grant. The registration builder now describes the
actual linked-row/queue and Arc-slot representation, including its token and
endpoint/connection lifetime; callers must re-evaluate costs after this layout
change. split moves existing ownership; transfer reclassifies without a debit;
return_to_owner is idempotent. Foreign/returned/overflow/growth refusal is atomic.

The public grant and growth methods acquire State before sampling other Store
owners. The Hub uses the caller's existing State guard. Atomic release leaves
the cold-read recalculation hint without taking either lock. An admitted legacy
registration owns its queue and encoding budget. Each publication funds its body
when needed.
