# Ordinary Python call retries

`Client.call` sends one encoded request and may resend it once after an
`OSError` or `ConnectionError`. PUT (4), conditional PUT (5), REMOVE (6), BATCH
(7), unsubscribe (17), and generic policy SET (24) retain their first attempt's
endpoint, epoch, complete payload, operation identity and negotiated `retry_ms`.
BATCH payload custody includes every item identity and requested Ack. Resends
use a new transport request ID and the identical original payload and deadline.

The client samples `monotonic_ns()` immediately before its first exchange. A
resend requires elapsed time strictly less than the original negotiated horizon,
both after transport loss and after reconnect. Reconnect's new horizon cannot
renew it. Zero retry horizon permits the first send only. A changed endpoint,
epoch, expired deadline or exhausted horizon prevents a resend. Calls are owned
by one foreground caller; concurrent mutation of Client state is unsupported.

After a transport failure of these default mutation calls, `CallUncertain`
retains `original` (`CallOriginal`), the first transport `cause`, and `last_error`
if no definitive successful reply arrives. A subsequent refusal, malformed
reply, reconnect failure or epoch change does not erase that first uncertainty.
`original.payload` includes the original absolute deadline, namespace, operation
ID and suffix; `endpoint`, `epoch`, `first_send_ns` and `retry_ms` are immutable
in-memory evidence. This is neither a durable receipt nor an executable retry
token. Retain the intended keys, values, Ack and embedded application identity;
reconcile those original intentions before deciding on any later call. Repeating
`call` with the same ID or a fresh deadline starts another call and does not
carry custody forward. No automatic cross-call replay is provided.

First-attempt protocol errors and definitive refusals keep their existing types;
codes 16 and 17 retain their protocol uncertainty meaning. `retry_transport=False`
still sends once and leaves errors to its explicit owner. Indexed publication
uses this opt-out and its own original tokens. Frozen atomic publication calls
`exchange` directly. Neither path acquires this ordinary-call retry policy.

GET/GET_MANY (2/3), health/metrics (8/9/19/20/27), subscription/timer/key status
(18/23/26), policy GET (25), same-token eligibility (31), known-token scans
(11/12/14/15), and token release (13) retain their existing one transport retry
without a mutation-horizon gate. Snapshot creation (10), subscribe (16), timer
set/cancel (21/22), and scans with an empty snapshot token remain single-send.
Unknown generic opcodes are single-send; an unrecognized operation cannot infer
replay safety from a method name or a negotiated refusal vocabulary.

These are local checks before exchange, not a bound on scheduler pauses,
transport delay, server admission time, clock divergence or identity retention.
In particular, elapsed time within the client horizon is not proof that a
dedup entry still exists. The resident may forget an expired ID after retired
ring eviction or restart; an unknown ID may then execute again. Equal numeric
epochs at different endpoints do not establish common custody. See [wire
uncertainty and retention](KV1.md) and [retained outcomes](RETRY_RETENTION.md).
No Store eviction history or operating service guarantee follows from a finite
byte-peer qualification.
