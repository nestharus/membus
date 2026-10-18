# Resident Rust client

`membus::client::Client` connects to an explicit existing resident endpoint. It
uses the library's KV1 framing, opcode, refusal and Cursor primitives and the
existing overlapped pipe transport. It never opens a Store or backing files.
There are no provider, account, namespace or acknowledgment policy defaults.

The supported operations are legacy HELLO, committed GET and GET_MANY,
unconditional PUT and conditional PUT. OS and flush acknowledgments are explicit
`Ack` values; a successful mutation must report exactly the selected mode and a
nonzero revision. Committed values retain revision, achieved mode and bytes.
GET_MANY preserves input order and validates the exact returned count.
`validate_mutation` performs a no-send frame/value/identifier check using the same
encoder as mutation entry. It reports encoded body bytes, without establishing
transport readiness or changing the existing `prepare`/Attempt retry contract.

`Options::new(identity, max_frame)` supplies the caller identity and ingress
bound. Baseline negotiation validates all original 19 refusal names/classes,
epoch, frame/value/batch limits and retry horizon. Optional `Options.scans` uses
the existing `;scans=1` suffix and accepts the current scan22 list or an older
legacy19 server. `Negotiated.scans` reports actual availability. Snapshot open,
prefix paging and release use the current service cursor bound. Set
`Options.subscriptions` to request the existing subscribe24 vocabulary and its
change limits; zero history retention still permits live subscriptions.
Subscription creation, cancellation, status and change-frame decoding are public.
Memory acknowledgment is not offered by this client.

### Additional resident capabilities

Existing `Options`, `Negotiated`, `Subscription`, mutation `Attempt` and ordinary
constructors retain their fields and behavior. `AdditionalFeatures { timers: true }`
opts into timers with `handshake_with_features`, `connect_pipe_with_features` or
`connect_tcp_with_features`. `feature_limits()` retains typed optional signal and
timer limits. Absence remains unavailable before an operation is sent. Timer-only
HELLO has codes 1–22 plus 25; combined HELLO has 1–25 and signal then timer limits.

`subscribe_with` takes `SubscribeOptions { include_values, resume, max_frame }`.
The receiving bound is distinct from the request connection's ingress bound and
cannot exceed the advertised signal bound. A resume cursor is epoch scoped.
Registration is not deduplicated: an unknown creation outcome is an unresolved
cleanup obligation, never permission to automatically accumulate registrations.
The original `subscribe` still requests no values and no cursor.

`scan_range` pages an explicit immutable snapshot over a half-open key range.
The caller retains release, total-work bounds and reconciliation responsibility.

On Windows, `ChangeListener::bind` owns a caller-selected local first-instance
pipe and rejects remote clients. Create it before subscribing. `receive` checks
the token, namespace, prefix and cursor epoch. Quiet timeout preserves a clean
stream. Complete buffered frames remain readable after writer closure; this
requires full decoding and association, never success inferred from an OS error.
Partial-frame timeout, truncation or malformed/foreign input retires the
connection. A failed native reset closes the handle; subsequent receive is Closed
and the caller must explicitly bind another listener. Successful reset allows
another writer on the same endpoint. `cancel`/`close` release the endpoint after canceled I/O is drained.
These methods require exclusive mutable access; they are not an asynchronous
cross-thread cancellation channel. A listener does not cancel a server token.

`prepare_timer_set` and `prepare_timer_cancel` retain complete immutable typed
requests, original epoch and conservative retry horizon. Explicit `timer_set` or
`timer_cancel` never reconnects or retries. Set returns only generation, as the
server actually replies; cancel returns no payload and uses the service's fixed
OS persistence mode. `TimerExpectation` distinguishes unconditional, absent and
exact generation. Operation IDs must be globally unique across timer namespaces.
Retain an original uncertain result even if later status or an explicit retry
settles present state; a later refusal does not disprove the original effect.

`timer_status` returns the version-bound schedule and latest envelope;
`TimerEnvelope::decode` owns MBT1 decoding. Namespace comes from the surrounding
operation/change, not MBT1 itself. Timer change versions are configuration
generations, not KV revisions at signal keys. Wakes precede progress persistence,
can repeat after restart, can be lost after checkpoint, and stop on progress
refusal. Latest status and pipe writes are not receiver receipts. Canceled timer
identities retain capacity. See [TIMERS.md](TIMERS.md) for the resident contract.

The caller owns snapshot release, listening before subscription, bounded paging,
buffering hints during capture, periodic refresh and cancellation. Creation is
never retried automatically: a lost reply can leave an unknown remote token.
Known tokens require a continuing cleanup owner; tokens alone are not durable
cleanup across owner loss. Change hints establish no receipt or native authority.

KV included bytes belong to the indicated publication; a later GET can observe
a newer revision. Timer signal keys instead carry MBT1 envelopes and create no
public KV value at that key; use the envelope and timer-status reconciliation.
Neither publication, GET, status nor an attempted command establishes useful
duty completion. Connection admission carries the resident's shared local-principal
powers. HELLO identity, tokens, generations and frame association do not authenticate
application actors or peers. Endpoint authenticity, effective OS security and any
required separation between admitted actors remain caller/deployment concerns.

`connect_pipe` uses an explicit local Windows pipe. `connect_tcp` uses an explicit
socket address for foreground tests. A caller-supplied `Transport` must enforce
each supplied monotonic deadline on reads and writes; built-in transports reset
the remaining budget before every transfer. No operation multiplexes a
connection. HELLO and every reply validate exact version, opcode and request ID.
Transport, association or malformed payload failure makes the connection
unusable. Reconnect creates a new Client and negotiates its actual epoch.

## Stable mutations and reconciliation

```rust,no_run
use membus::client::{Ack, Client, Deadline, MutationRequest, Options};
use std::{path::Path, time::{Duration, Instant}};
# fn example() -> Result<(), Box<dyn std::error::Error>> {
# #[cfg(windows)] {
let deadline = Deadline {
    unix_ms: membus::now_ms() + 2000,
    local: Instant::now() + Duration::from_secs(2),
};
let mut client = Client::connect_pipe(
    Path::new(r"\\.\pipe\caller-selected-endpoint"),
    Options::new("caller-selected-identity", 65536), deadline.local,
)?;
let mut attempt = client.prepare(MutationRequest {
    operation_id: "caller-stable-operation-id".into(),
    namespace: "caller-namespace".into(), key: "deterministic-key".into(),
    value: b"value containing caller operation identity".to_vec(),
    ack: Ack::Flush, expected: Some(0), // absent; None selects unconditional PUT
})?;
let result = client.mutate(&mut attempt, deadline);
// Retain attempt on uncertainty. The caller decides whether to reconcile/retry.
# let _ = result;
# }
# Ok(()) }
```

`Deadline` keeps server Unix milliseconds and local monotonic expiry separate.
Lease clock bridging stays in the lease daemon. The client never invents an
operation identity or retries/reconnects automatically. An `Attempt` holds the
immutable complete mutation, negotiated epoch and a conservative retry horizon
starting before its first send. An explicit `mutate` on the same Attempt refuses
an epoch change or expired horizon before any wire send. An initial send remains
possible when the negotiated retry horizon is zero; a second send is refused.

`Error::Uncertain` carries the caller ID, original epoch and cause for transport,
association, payload or achieved-mode failure after send, and refusals16/17.
Refusals12 (committed conflict) and19 (pending conflict) remain distinct typed
refusals. A caller may restrict retry further, such as slots' single transport
retry in the original deadline/epoch. The generic client authorizes none.

Preparing again must not reset the safety boundary for an uncertain operation.
After losing an Attempt or crossing its epoch/horizon, reconcile a deterministic
key and its embedded operation identity, then make an explicit application
decision. A new Client, new identity or retryable refusal is not proof that the
old mutation did not commit. These in-memory tokens are not a second journal or
a permanent exactly-once guarantee.

## Adoption mappings

Registry backend GET maps to `get(namespace,key,caller_id,deadline)` and registry
CAS maps to `prepare` with `Ack::Os` and `expected: Some(version)`, then one
`mutate`. Registry request IDs, UTC conversion and publication policy remain in
registry. Its service protocol client/pipe transport are separate and retained.

Lease backend GET uses the same read and flush CAS selects `Ack::Flush` and
`Some(expected)`. Its daemon incarnation/counter IDs, suspend-inclusive clock
budget conversion and Engine error classification remain in leases. An unknown
mutation must map conservatively to its uncertain result rather than a committed
conflict or newly generated retry identity.

Slots uses explicit `Ack::Os` and `expected: None`; its caller mints and retains
the stable operation ID, optionally reconnects once on uncertain transport,
and invokes the same Attempt only within its original budget and epoch. Its
CURRENT/HISTORY/bus/STATE publication order stays in slots.

## Qualification

After supplying the Windows fixture inputs below, `cargo test --offline --locked`
runs the independent expected-frame controls in `tests/client.rs` and
`tests/client_forwarder.rs`, package-resident tests in `tests/client_native.rs`,
and original-resident tests in `tests/forwarder_native.rs` and the listener's
library regression module. The latter records a test-only count of actual native
error-232 acceptance, independently of unsubscribe timing. Complete-frame reads,
empty/partial input, clean idle receiving, terminal reset, closure and endpoint
reuse have distinct assertions. This does not claim that a timing-only test
observed a cancellation/completion race.

The original-resident controls require both inputs, including for library tests:

- `MEMBUS_FORWARDER_TEST_ROOT`: fresh private qualification storage, retained
  with the run's logs and state.
- `MEMBUS_FORWARDER_TEST_SERVER`: absolute path to the explicitly selected,
  source-pinned original resident executable. This is a qualification input,
  not installed or default operating configuration.

Select/build and preserve that original image before the run. Record its exact
source revision, compiler/target, build command and SHA-256. Also record the
candidate source/tree and lockfile, native fixture inputs, finite runner/resource
admission, results, and direct-wait/empty-Job evidence. The package-resident suite
uses this package's candidate binary; the externally selected original image has
a separate implementation identity. Neither image proves installed compatibility.

The package-resident suite checks legacy19/scan22, reads, OS/flush PUT/CAS,
duplicate outcomes, conflict12/pending19, pipe timeout, explicit same-epoch
reconciliation and restart refusal/replay. `MEMBUS_CLIENT_TEST_ROOT` remains
optional for that suite; otherwise it retains fixtures under the ignored
`target/client-fixtures`. Every resident child starts suspended with NoWindow,
enters an owned Job before resume, is directly waited and requires empty-Job
settlement. Run the suites under finite native custody and actual resource
admission.
