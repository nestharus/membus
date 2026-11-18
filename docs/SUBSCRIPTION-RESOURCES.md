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

The Hub binds to its actual Store State/cache/chunk/snapshot context. It holds
weak State/cache/chunk references, with no Store/Hub/worker strong cycle.
Capture controls instantiate actual State and account arithmetic with authored
snapshot/chunk inputs; they do not construct an empty Store or replace grant
issuance. The independent ReadUsage candidate remains uncomposed and needs its
own subtraction/release hint when later received.

Legacy registration reserves its real row, namespace/prefix, linked-node
backing and endpoint/connection state. Its existing queue policy authorizes at
most 256 linked nodes; its negotiated frame bound reserves exact prospective
body/frame encode capacities, limited by existing representable payload fields.
This is an existing legacy selected demand, not a new finite-profile cap. On
deactivation empty queue authorization returns; terminal status metadata stays
owned until actual eviction/controlled stop. A live worker retains its endpoint
and encoding child through completion.

Each legacy publication reserves its actual Change/Value/string/payload capacity
and history-node ownership before allocation. Linked queues/ring avoid retained
uncharged Vec/map capacity. Shared Change and slot wrappers consume the final Arc
before freeing retained representations and returning exact ownership. Queue
links belong to row backing; history links belong to their publication owner.
Allocation pressure explicitly drops matching legacy subscriptions and clears
history to a resume gap. An already committed ordinary operation keeps its
original result. With history disabled and no legacy subscriber, finite dirty
hints require no retained Change/value allocation.

Legacy change/1 framing, statuses, limits and token/cursor layout remain; finite
profile negotiation, expiry/renewal/full binding, coalescing and refusal of stronger
delivery semantics remain unchanged. The added publication-admission mutex only
serializes existing Hub ownership changes; it is not a queue/controller/service.
Actual publisher order is committed -> State -> chunks/cache -> Hub, and no
State grant is acquired under Hub. Atomic releases require no State lock.

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

Grant and growth acquire State and preserve ordinary State -> chunks/cache
ordering. Atomic release acquires neither State nor Hub and leaves the existing
cold-read recalculation hint. Prospective queue/encoding authorization is owned
only by an admitted legacy registration; per-publication bodies are granted on
real demand. The former fixed envelope is not a charge on an empty Hub.
