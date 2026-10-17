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

`Options::new(identity, max_frame)` supplies the caller identity and ingress
bound. Baseline negotiation validates all original 19 refusal names/classes,
epoch, frame/value/batch limits and retry horizon. Optional `Options.scans` uses
the existing `;scans=1` suffix and accepts the current scan22 list or an older
legacy19 server. `Negotiated.scans` reports actual availability. Snapshot open,
prefix paging and release use the current service cursor bound. Set
`Options.subscriptions` to request the existing subscribe24 vocabulary and its
change limits; zero history retention still permits live subscriptions.
Subscription creation, cancellation, status and change-frame decoding are public.
Timers and memory acknowledgment are not offered by this client.

The caller owns snapshot release, listening before subscription, bounded paging,
buffering hints during capture, periodic refresh and cancellation. Creation is
never retried automatically: a lost reply can leave an unknown remote token.
Known tokens require a continuing cleanup owner; tokens alone are not durable
cleanup across owner loss. Change hints establish no receipt or native authority.

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

`cargo test --offline --locked` includes independent expected-frame controls in
`tests/client.rs` and a Windows physical fixture in `tests/client_native.rs`.
The latter starts only this package's resident binary on fresh state and a unique
owned pipe, checks legacy19/scan22, reads, OS/flush PUT/CAS, duplicate outcomes,
conflict12/pending19, actual pipe timeout, explicit same-epoch reconciliation,
and restart epoch refusal/replay. Its children use NoWindow and are directly
stopped/waited/reaped; run it under the project's finite owned Job runner.
Set `MEMBUS_CLIENT_TEST_ROOT` to the qualification scratch directory to retain
fixture logs/state. Otherwise it uses the ignored `target/client-fixtures`.
