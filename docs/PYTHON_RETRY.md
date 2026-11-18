# Ordinary Python call custody

One foreground caller owns a Client connection. Ordinary mutation calls PUT,
conditional PUT, REMOVE, BATCH, unsubscribe and raw policy SET retain immutable
in-memory CallOriginal evidence before their first exchange: endpoint, epoch,
opcode, operation ID, namespace, exact payload, monotonic first-send time and
the first negotiated retry horizon. This evidence is not a durable receipt or
an authorization to replay. BATCH retains every embedded item identity and Ack.

A transport failure may lead to one resend of the identical payload, with a new
transport request ID. The elapsed monotonic time must be strictly less than the
first negotiated horizon both after loss and before resend. The endpoint and
epoch must still match, and the original deadline must still be in the future.
A successor may offer a different horizon. The original call keeps its first
horizon. Zero horizon allows only the first send. A subsequent refusal, protocol
error, reconnect failure or typed validation error retains the first transport
cause and original evidence in CallUncertain. Its last_error names the later
error.
Reconcile original intentions before deciding on another call. A separate call
does not inherit this custody. There is no cross-call automatic replay.

Client.call validates known successful mutation replies while custody remains
in scope. PUT/conditional PUT/REMOVE require an exact version and Ack in 0..2.
BATCH requires the original count, complete positional rows, negotiated refusal
codes or zero success, Ack in 0..2 and no trailing data. Unsubscribe requires an
empty success body. Valid public wrapper results and direct-call bytes remain
unchanged. These direct calls now reject malformed successes. Fixed mutation
length errors remain struct.error; invalid Ack/count/code/tail and unsubscribe
payloads raise ValueError without an earlier loss, or become last_error inside
CallUncertain after a transport loss. Validation never triggers another send.
No Ack is upgraded and BATCH remains non-atomic.

retry_transport=False explicitly sends once and leaves transport errors to the
caller without CallUncertain custody. This optional keyword is also available
on put; existing positional put calls and legacy call overrides keep their
signature. Other wrappers forward it through their existing keyword options.
Overrides that bypass Client.call retain their own settlement responsibility.

Client.call allows transport retries for the bounded mutations above and an
explicit list of reads and status operations. This includes GET and GET_MANY,
health and metrics, known-token scans and release, subscription and timer
status, policy and key status reads, and the default-profile snapshot
eligibility check (31). A read retry sends the original payload and deadline.
An associated refusal, including unsupported, ends the call.

Snapshot opens, scans with an empty snapshot token, subscription registration,
timer set and cancel, and unclassified raw commands are single-send. Opcodes
28–30 are single-send through Client.call. They carry atomic capture,
publication and bounded capture on the default profile, and create, renew and
cancel on the affirmative finite-hint profile. The Python SDK uses the default
HELLO profile. The Python atomic binding uses its own single-exchange path.

Outer refusal framing, durable application custody, native clients,
indexed/atomic publication, caller admission, service deployment and capacity
are separate contracts. Sampling and checking a retry horizon cannot make a
delayed send atomic with server admission. After retired-ID eviction or restart,
an expired ID can become unknown and execute again. The provider describes
these limits in [KV1](KV1.md) and [retry retention](RETRY_RETENTION.md).
