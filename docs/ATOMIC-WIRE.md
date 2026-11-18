# Atomic publication over the supported Rust client

`Client::capture_atomic_namespace(namespace, read_operation_id, deadline)` returns
one complete `atomic::NamespaceView`. Use its `rows()` as the decision input and
`prepare` with an independent original operation ID, explicit AtomicAck and final
changes. Persist the original with `AtomicRequest::persist_new` before first send.
`Client::publish_atomic(&original, deadline)` sends that exact canonical original.
The client opens no Store and supplies no application queue, SQL schema or retry pump.

```rust,no_run
use membus::{atomic::{AtomicAck, FinalChange}, client::{Client, Deadline, Transport}};
fn publish<T: Transport>(client: &mut Client<T>, custody: &std::path::Path,
                        deadline: Deadline) -> Result<(), Box<dyn std::error::Error>> {
    let view = client.capture_atomic_namespace("authored", "capture-original", deadline)?;
    let original = view.prepare("independent-original", AtomicAck::Flush,
        vec![FinalChange::put("first", b"one".to_vec()), FinalChange::remove("old")])
        .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
    original.persist_new(custody)?;
    let receipt = client.publish_atomic(&original, deadline)?;
    assert_eq!(receipt.achieved, membus::client::Ack::Flush);
    Ok(())
}
```

The example requires an explicitly connected client bound by its owner to the
correct dataset/endpoint and an unused custody path. Store epoch is not an
authenticated dataset identity or independent recovery authority.

## Additive wire

All integers are little endian. The existing kv/1 frame/header, HELLO, refusal
vocabulary and legacy defaults are unchanged. No new HELLO feature is needed.
An older provider refuses unknown operations with unsupported. Legacy BATCH is
still independent per item; typed REMOVE is unchanged.

| Opcode | Request after existing frame header |
| --- | --- |
| 28 ATOMIC_CAPTURE | u64 deadline UTC ms, lp operation ID, lp namespace; no tail |
| 29 ATOMIC_PUBLISH | same prefix, lp original canonical MAR1 or MAR2 bytes |
| 30 ATOMIC_POPULATION | same prefix, then the bounded population limits |
| 31 SNAPSHOT_REQUIRE_COMPLETE | same prefix, then the lp 16-byte snapshot token |

The prefix namespace and operation ID must equal the MAR1 original. MAR1 is the
same provider-owned serialization used by library custody and the durable
atomic envelope. No second transaction encoding or original-attempt renewal exists.
Request MAR1 remains bounded by MAX_FRAME/3; the complete frame must fit the
negotiated bound. Native resident ingress reservations and whole provider atomic
admission still apply. Read/capture identifiers do not become publication identities.
The provider associates and validates the original before reserving shared
managed memory for the owning decode. Retained results remain available even
when the provider cannot fund a new original.

A syntactically valid atomic reply uses ordinary status zero, then magic MAW1
and a u8 atomic status. Status zero has the operation-specific success payload.
Statuses 1 conflict, 2 expired, 3 unsupported, 4 resource, 5 invalid,
6 identity mismatch, 7 uncertain, 8 restart uncertain, 9 retry expired and
10 unavailable have no tail, except resource has one u16 existing resource code
(4, 6, 7, 8, 9 or 18). Unknown status/resource, magic, trailing bytes, missing
fields, association errors and oversized frames fail closed.

Capture success is MAV1, lp namespace, u64 Store epoch, u64 fixed provider
monotonic expiry, u32 count, then exactly count rows: lp UTF8 key, u64 nonzero
version, u8 original achieved Ack (1 or 2), lp value. Keys are strictly ordered
and unique; count is at most MAX_POPULATION. The provider computes the whole
response size before encoding and refuses if it exceeds the negotiated frame.
There is no partial-population success, pagination or refresh of the original
view horizon. The decoder validates the complete population and tail before
allocating owning row values. Empty complete population is valid only for this
explicitly selected namespace; it proves no historical handover.

Publication success is u64 nonzero first version and u8 exact requested Ack.
The complete final set is still one original request, one durable envelope and
one committed index publication; all FOREVER restrictions, phantom guards,
whole-request queue/index/quota/memory/disk admission and replay/dedup semantics
are those of [atomic publication](ATOMIC-PUBLICATION.md).

Opcode 30 uses the [bounded population contract](ATOMIC-POPULATION.md),
including the negotiated SCANS vocabulary and page limit. Opcode 31 reports
eligibility for the captured token. Its empty successful reply and refusal
codes 16, 20 and 21 are independent of subsequent live health. Existing
subscription and timer HELLO extensions keep their byte layouts. Finite hints
require their own affirmative discovery with the published MSH2 suffix.

## Failure, reconnect and restart

No transport retry or reconnect is automatic. A typed atomic pre-admission
refusal is `client::Error::Atomic(PublicationError)`. Mutation transport,
malformed/unknown replies, uncertainty codes and RestartUncertain become
`Error::Uncertain`, invalidate the connection and preserve the immutable original.
Reconnect explicitly and submit only those same retained original bytes. Original
provider epoch and fixed horizon stay unchanged; a new connection cannot extend
or reconstruct them. Read/capture protocol failures return no view.

The server checks retained original outcomes before a new deadline/view/epoch.
Thus an exact duplicate after restart can reconcile a retained success, while
an original lacking an outcome in the new epoch remains RestartUncertain and
cannot apply a renewed decision. The reply epoch is the current connection's
negotiation; it does not rewrite the original epoch. Retry-expired, later
refusal or caller restart never proves that an earlier uncertain attempt had
no effect. Keep all prior uncertainty until the actual owner settles it.

The Python binding uses the same dispatcher and original encoding. Its
capture, preparation and custody API is described in [Python atomic publication](PYTHON_ATOMIC.md).
