# Ordinary Python call custody

One foreground caller owns a Client connection. Ordinary mutation calls PUT,
conditional PUT, REMOVE, BATCH, unsubscribe and raw policy SET retain immutable
in-memory CallOriginal evidence before their first exchange: endpoint, epoch,
opcode, operation ID, namespace, exact payload, monotonic first-send time and
the first negotiated retry horizon. This evidence is not a durable receipt or
an authorization to replay. BATCH retains every embedded item identity and Ack.

A transport failure may lead to one resend of the identical payload, with a new
transport request ID. The original endpoint, epoch, deadline and retry horizon
must still match after reconnect. A new handshake cannot extend that horizon.
Zero horizon allows only the first send. A subsequent refusal, protocol error,
reconnect failure or typed validation error retains the first transport cause
and original evidence in CallUncertain; last_error names the later error.
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

Other raw/read/status calls retain baseline retry and decoding responsibilities.
Snapshot/subscription/timer creation remains single-send. Outer refusal framing,
durable application custody, native clients, indexed/atomic publication, caller
admission, service deployment and capacity are separate contracts. Sampling and
checking a retry horizon cannot make a delayed send atomic with server admission.
