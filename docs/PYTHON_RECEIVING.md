# Receiving the Python SDK

Add the source or Windows package's `python` directory to the import path. Ship
`membus.py`, `membus_atomic.py`, `membus_indexed.py`, `membus_signals.py` and
`membus_timers.py` together from the same source tree. The bindings use the
serialized `membus.Client` and a caller-selected provider endpoint.

- [Python atomic publication](PYTHON_ATOMIC.md) captures a bounded namespace,
  prepares a canonical original, and publishes the whole guarded change set.
- [Python indexed collections](PYTHON_INDEXED.md) declare and append MBIX1
  records with immutable rows and postings and conditional head publication.
  An indexed query uses one eligible snapshot token.

## Negotiated provider profile

| Default KV1 opcode | Contract |
| --- | --- |
| 28 | Unpaged MAV1 capture in MAW1 through the typed Rust client. |
| 29 | Publish an exact MAR1 or MAR2 original. The Python atomic binding can publish either format. |
| 30 | Bounded MAS2 capture used by Python atomic. Complete paging on the returned token uses the negotiated scans and bounds. The resulting view prepares a MAR2 guard. |
| 31 | Check complete-query eligibility on the exact snapshot token before indexed selection. Empty success permits selection. Refusals 16, 20 and 21 preserve ineligibility, expiry and invalid-token outcomes. |

The Python clients use the default profile for these operations. Connections
that select finite hints by affirmative `MSH2` discovery use 28–30 for finite
hint controls and refuse 31. Older providers can also refuse unsupported
operations. The indexed binding surfaces unsupported 31 as
`membus.NotAvailable`. See [atomic wire](ATOMIC-WIRE.md),
[bounded population](ATOMIC-POPULATION.md) and
[captured eligibility](SNAPSHOT-INTEGRITY.md).

## Publication and receiving bounds

Indexed preparation preflights the complete encoded row against the 4 MiB
value bound and the negotiated value and frame limits. It requires a positive
negotiated retry horizon. Its in-memory publication retains the endpoint,
epoch, original requests, requested acknowledgment and earliest first send.
Each indexed mutation uses `retry_transport=False`. Explicit reconciliation
retains the first uncertainty and original horizon.

The atomic binding uses one exchange per call and a separate original custody
format. Ordinary `Client.call` keeps its documented
[transport retry behavior](PYTHON_RETRY.md). Ordinary BATCH retains
per-item outcomes from the connection's negotiated refusal vocabulary.

After HELLO, a nonzero outer refusal code on an associated frame must belong
to that vocabulary and have an empty payload. An unknown code or trailing
payload invalidates the reply and closes the channel. If an atomic or indexed
mutation may already have been sent, its publication retains the original
and the resulting uncertainty. Valid outer refusals 16 and 17 also leave
these publications uncertain.

Qualify the chosen binary and the staged SDK together on an owned Windows
fixture. Exercise the indexed token check and bounded atomic capture using
their public entry points and the provider profile the application will use.
