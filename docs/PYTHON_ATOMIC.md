# Python canonical atomic binding

`python/membus_atomic.py` binds the canonical bounded population and original
publisher through the existing `membus.Client`. It does not change that client's
defaults, indexed collections or legacy BATCH behavior.

```python
from membus import Client, MAX_FRAME, deadline_ms
from membus_atomic import Ack, Change, Limits, Publication, capture

# endpoint, namespace and custody_path are explicitly selected by the caller.
client = Client(endpoint, transport="pipe", timeout=10)
limits = Limits(60_000, 96 * 1024 * 1024, 10_000, MAX_FRAME // 3)
view = capture(client, namespace, limits, read_id, deadline_ms(20))
# Compute the full related key set from view.rows. Domain IDs remain domain IDs.
original = view.prepare(operation_id, Ack.FLUSH, final_changes)
submission = Publication(client, original)
submission.persist_new(custody_path)  # before first send when restart matters
receipt = submission.publish(client, deadline_ms(20))
client.close()
```

The example bounds are an authored profile, not live population sufficiency.
`Row` retains key, revision, exact achieved OS/Flush Ack and opaque value bytes.
`Change(key, bytes)` puts; `Change(key, None)` removes. Input values must already
be immutable bytes. All final targets are unique; count/value/aggregate bounds
are checked before original encoding. The complete capture/fingerprint/charge
contract is [ATOMIC-POPULATION.md](ATOMIC-POPULATION.md).

`Original.from_bytes` strictly validates MAR1 or MAR2, complete lengths, order,
identities, acknowledgments, discriminators, duplicate targets and bounds.
`Publication.persist_new` creates a caller-selected new-only MPC2 custody file
containing original bytes and endpoint identity. It syncs the file and its
existing parent directory using the platform barrier. Failure retains output
and raises; no send follows automatically. This is original-operation custody,
not another queue or authoritative dataset. Preserve the file unchanged.

After caller restart, explicitly connect to the selected same endpoint and use
`Publication.restore(client, custody_path)`. Restoration begins with uncertainty
because the original might already have been sent. `publish` sends exactly those
bytes, even after a provider epoch change, solely for reconciliation. It never
replaces the original epoch, expiry, ID, Ack, guard or final set. A retained known
success may reconcile; absent old-epoch outcomes remain RestartUncertain.
Endpoint equality is an association check, not independent serving authority or
protection from endpoint/data-root rollback.

The binding uses one transport exchange for each call and never automatically
reconnects or retries a mutation. The caller may explicitly create a successor
Client and revisit the SAME Publication. It must not make a fresh original after
an uncertain result. Capturing/paging/releasing are likewise explicit bounded
read exchanges, without hidden legacy call retries.

`Refused` carries the exact canonical pre-admission status/resource code. Outer
wire `membus.Refusal` stays distinct. Unsupported old servers, quota, capacity,
conflict, expired original and retry expiry never become empty histories or
successful partial publication. Transport loss, invalid/malformed/unknown success,
wrong achieved Ack, and canonical Uncertain/RestartUncertain or outer 16/17 raise
`Uncertain` carrying the original and cause, and close the connection. A valid
success requires a nonzero version and EXACT requested achieved Ack.
`Publication.uncertainties` retains every uncertain observation across later
refusal and success. This list is process-local; durable restoration always
retains the conservative unknown-send state. It is not a persistent receipt log.

Deadlines are explicit Unix milliseconds and preflight before mutation entry.
The inherited Python transport uses wall-clock I/O deadlines; this binding does
not claim Rust's monotonic whole-call transport behavior. Cleanup uses a fresh
bounded read deadline, preserves the primary capture error and reports any
cleanup failure. Expiry releases server custody through the existing registry;
it is never converted into a partial complete view.
Capture uses the connection's per-value limit before accepting any page row.
After transport loss or a reply association failure the binding closes the
channel. An ordinary call requires a fresh stream, and the authentic server
token keeps its original expiry. A publication that exceeds the connected
value limit is refused locally and keeps the same original and uncertainty.

This source does not bind stopped-store recovery, issue grants, integrate an
application adapter, acquire an old dataset, change retention, or prove safe live
handover/rollback. Retain originals and reconcile all later outcomes through
the actual authority and data owners before any serving transition.
