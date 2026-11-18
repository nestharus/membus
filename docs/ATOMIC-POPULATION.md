# Bounded complete atomic populations

The opt-in `atomic_population::PopulationLimits` and `PopulationView` extend the
existing canonical atomic publisher. A caller supplies maximum live keys,
conservative capture bytes, final changes and original bytes. Resource limits
remain refusals. This API does not select application retention, allocate domain
sequence numbers, or provide an SQL transaction surface.

```rust,ignore
use membus::{atomic::{AtomicAck, FinalChange}, atomic_population::PopulationLimits};
let limits = PopulationLimits {
    max_keys: 60_000,
    max_capture_bytes: 96 * 1024 * 1024,
    max_changes: 10_000,
    max_original_bytes: (membus::MAX_FRAME / 3) as u32,
};
let view = client.capture_atomic_namespace_bounded(namespace, read_id, limits, deadline)?;
// Derive ALL related final keys from view.rows(), including domain sequence,
// source identity, reservation conditions and any required metadata.
let original = view.prepare(operation_id, AtomicAck::Flush, changes)?;
original.persist_new(independent_new_custody_path)?;
let receipt = client.publish_atomic(&original, deadline)?;
```

The bounded method requires the scan refusal vocabulary (20, 21 and 22) in
HELLO. Set `Options.scans = true` to request that vocabulary on a scans-only
connection. Existing subscription and timer offers also advertise it when
supported. Those extensions retain their own discovery and controls. A baseline
19-code connection refuses the bounded call locally with
`Error::Invalid("scans unavailable")`. The unpaged opcode 28 and native Store
capture keep their existing admission contracts.

Those numerical limits are an authored qualification profile, not established
live workload requirements or guaranteed resource admission. The in-process
counterpart is `store.capture_namespace_bounded(namespace, limits)`; both return
the same complete `PopulationView`. `rows()` exposes every live key/value/revision
at the captured boundary. `charged_bytes()` reports its conservative provider
reservation. Only a complete view has `prepare`.

The provider captures immutable locations through the existing snapshot registry,
under its publication gate. It verifies the entire namespace's FOREVER policies
and committed classes, including prefixes and tombstones. LATEST, expiration
classes and reserved control namespaces remain unsupported. No retention policy
is installed or inferred from application row age.

The server's initial response contains an identified snapshot, epoch, original
expiry, exact count, charge, caller bounds and population fingerprint. The Rust
and Python bindings page the entire namespace with that same snapshot, enforcing
ordered unique keys, values, exact acknowledgments, row/byte bounds, continuation
progress, final count and fingerprint. They release the server capture before
returning the view. An error, expired/missing page, malformed response or budget
exhaustion returns no partial view. Pages are an internal read mechanism, never
individual publication units. No complete view is reconstructed from current GETs.

Both clients attempt same-token release after an associated page reply fails
payload validation. A transport or frame-association failure can prevent release.
The channel is discarded and the provider keeps its original fixed expiry.
Pages use the read deadline and the negotiated page and per-value limits.
Release uses a fresh one-second budget when the stream remains associated.

The capture charge on this implementation is:

```text
256 + namespace_UTF8_bytes + 4 * floor(MAX_FRAME / 3) + 4096
+ sum(640 + 2 * key_UTF8_bytes + 2 * value_length)
```

The fixed workspace bounds one serialized cold-envelope validation. A bounded
snapshot validates each encountered atomic envelope once, installs only exact
matching captured revisions in its existing per-row loaded slots, and never loads
unbounded history. It does not create a global cache or provider. The ordinary
snapshot path retains its existing allowance and load behavior. Capture reserves
before allocating its owning rows; shared snapshot count/bytes and available
process capacity still apply. In-process views retain their reservation until
their last snapshot owner drops, including expired objects. Remote owning views
consume caller memory after server release; this charge is not a measured RSS cap.

The compact guard hashes `MAP2`, each ordered length-prefixed UTF-8 key and its
little-endian u64 revision, then the little-endian u64 live count, with SHA256.
Values belong to immutable revisions. At publication, the existing exclusive
provider gate compares the complete current live count/fingerprint. Additions,
removals and changed revisions conflict; another namespace's mutation does not.
Thus domain sequence/ID/phantom or required reservation conditions can be decided
from one complete view and published together. The provider does not interpret
those application invariants or prove an adapter preserves them.

`MAR2` retains the original namespace, operation ID, epoch, expiry, requested Ack,
count/fingerprint/bounds and unique final changes. Its exact length is
`85 + namespace_bytes + operation_bytes + sum(9 + key_bytes + value_bytes)`.
The original must fit the caller's bound AND `MAX_FRAME / 3` (5,592,405 bytes at
the unchanged 16 MiB frame limit). Each value remains at most 4 MiB; the u16
member count limits representation to 65,535. Larger aggregates are unsupported,
not split across publications. The one durable envelope remains `40 + MAR2_length`
bytes including frame overhead. Existing global/stripe queue, index, net quota,
memory, retry-identity, disk and deadline admission still precede effects. Count
alone or one maximum value cannot prove admission.

After admission, one append and selected covering acknowledgment precede one
committed index publication and one retained whole outcome. Decode, routing,
subrecord reads, replay and retry custody accept the same canonical MAR2. Retained
original outcomes are consulted before changed epoch/expiry checks. An unknown
original after provider restart stays RestartUncertain; it cannot acquire a new
epoch, horizon or original ID. Later refusal cannot erase an earlier uncertain
send. Do not re-decide a possibly sent operation from a successor view.

The covering Flush is for the publication envelope. Caller custody, catalogue
updates, chunk rotation and recovery may require additional filesystem barriers.
Neither the operation nor the diagnostic flush counter promises exactly one OS
fsync over an entire application action or physical power-loss safety.

Wire opcode 30 opens this bounded capture; `MAS2` carries its header. Publication
uses existing opcode 29 with MAR2. Old servers refuse unsupported capture or the
unknown original encoding. Existing HELLO fields, negotiated refusal vocabulary,
wire defaults, MAV1/MAR1 and the legacy 128-member per-item BATCH are unchanged.
Unpaged capture and opcode 29 MAR1 retain the 4,096-row enumerable limit.
MAR1 retains its 128-change limit. Main's funded library capture and native
publisher support larger enumerable MAR1 originals within their byte and
resource bounds. The public MAR1
borrowed projection continues to require an enumerable original. Owned
decoding and the provider's envelope decoder accept the compact MAR2 guard.

Whole stopped-store capture and evidence-backed permanently quarantined recovery
remain the separate recovery-image contract. A logical population view omits
catalogues, retry history, timers, clocks, sidecars and other recovery custody.
No copied epoch or capture completion witness grants serving authority.

Actual application mapping, complete historical population, effective resource
occupancy and configuration, every writer/effect participant, rollback-independent
authority, permitted history acquisition, live handover and receiving adapters
require their own evidence. Bounded authored profiles establish none of these.
