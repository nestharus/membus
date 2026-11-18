# Python indexed collections

`python/membus_indexed.py` binds the existing public Rust
`membus::indexed::Collection` MBIX1 declaration, immutable row/posting publication,
conditional head and pinned queries. It uses `membus.Client`; it opens no backing
files and starts no worker. Add the repository's `python` directory to the
caller's import path. Python 3.10 or later and the standard library suffice.
The [Python SDK receiving guide](PYTHON_RECEIVING.md) describes the provider
profile shared with `membus_atomic`.

The caller supplies an already connected Client, namespace, collection,
immutable original operation identity, acknowledgment, deadline and query
bounds. Connections are single-owner and serialized; concurrent publishers use
separate Clients. Transport request counters are independent of logical operation
IDs. Repeated references in record fields or payload do not deduplicate records.
The owning application chooses distinct submission IDs, ordinals and revision
links; this generic API never interprets or rewrites those bytes.

```python
from membus import Client
from membus_indexed import (
    Ack, All, Collection, Deadline, Equal, Limits, Record, Schema, Uncertain,
)

def publish_and_read(private_pipe, namespace, submission_id, payload):
    client = Client(private_pipe, transport="pipe", timeout=10)
    try:
        facts = Collection("facts", Schema(("reference", "revises")))
        original = facts.prepare_append(
            client, namespace,
            Record(1, ((b"repeated-reference", b"repeated-reference"), ()), payload),
            submission_id, Ack.FLUSH,
        )
        receipt = facts.append(client, original, Deadline.after(10))
        with client.snapshot(namespace) as token:
            whole = facts.query(client, namespace, token, All(),
                                Limits(100, 4 * 1024 * 1024),
                                submission_id + ":read", Deadline.after(10))
            linked = facts.query(client, namespace, token,
                                 Equal("reference", b"repeated-reference"),
                                 Limits(100, 4 * 1024 * 1024),
                                 submission_id + ":links", Deadline.after(10))
        return receipt, whole, linked
    except Uncertain as error:
        # Preserve original AND error for the application's reconciliation owner.
        # This function does not persist custody or decide whether to retry.
        raise
    finally:
        client.close()
```

The example's ordinal 1 is for a new identified collection, not an allocator for
an existing history. Caller-supplied `submission_id` must have room for control
suffixes. A human request/reference string is not automatically that identity.
Opaque payloads preserve exact IDs, metadata, numbers, times and links; extracting
declared fields belongs to the application codec.

## Typed callable contract

- `Collection(name: str, schema: Schema)` validates a 1..64 byte ASCII collection
  name and at most 64 distinct bounded UTF-8 field names, preserving field order.
- `Record(ordinal, fields, payload)` freezes field lists to tuples. Values and
  payload must be immutable bytes. Ordinals are exact integers 1..u64::MAX-1;
  fields retain order and duplicate values, with at most 1024 values per field.
- `encoded_len(record)` validates and measures the canonical MBIX1 record before
  allocating its encoded representation. Encoded rows must fit the existing
  4 MiB value bound and negotiated value/frame limits. No truncation occurs.
- `prepare_declaration(client, namespace, operation, ack)` and
  `prepare_append(client, namespace, record, operation, ack)` return a frozen
  `Publication` containing the immutable original request, endpoint, epoch and
  conservative retry horizon. Preparation performs no I/O. It preflights each
  planned mutation's exact value and frame bounds before any publication effect.
  A negotiated zero retry horizon raises `Unsupported` during preparation or
  execution, before any publication request. Positive horizons bound how long
  execution can proceed after the first mutation send.
- `declare(client, publication, deadline)` establishes only the immutable schema
  and head. `append(client, publication, deadline)` reserves the immutable row,
  writes deduplicated CRC candidate postings, then CAS-publishes head. Both
  check the whole publication against the executing client's limits before the
  first request, including when the caller presents an original after reconnect.
  Append returns `AppendReceipt(through, already_committed)` using the existing
  participating writer protocol.
- `query(client, namespace, snapshot, query, limits, operation, deadline)` returns
  `Read(through, records, encoded_bytes_read)` from one explicit 16-byte snapshot.
  Queries are `All()`, inclusive `Ordinals(first,last)`, or `Equal(field,bytes)`.
  Internal pages keep the same token/cursor and return complete ordinal order.
  No current GET substitutes for a snapshot read. A caller can page a large
  ordinal history through separately bounded `Ordinals` calls on the same token.

All query variants first call the canonical provider's
`snapshot_require_complete(namespace, snapshot, op_id=..., deadline=...)`
contract on the exact supplied token, with transport retry disabled. The
default connection's opcode 31 returns an empty success payload only when
that captured view is eligible for a complete query. A known incomplete recovered population
raises `Refusal` code 16 before schema, head, row or posting selection. Missing
postings cannot become a successful empty or partial equality answer merely
because schema, head and matching rows survived. Providers without this
contract raise `membus.NotAvailable`; there is no fallback to unchecked reads
or a later live health observation. See the
[captured eligibility contract](SNAPSHOT-INTEGRITY.md).

The native snapshot owner captures eligibility with its immutable population.
Later health changes do not certify an ineligible token or revoke an eligible
captured view; selected payload failures still refuse normally. This covers
known storage uncertainty, including replay-detected missing closed chunks,
and conservatively may refuse an intact namespace after a global storage error.
It is not a physical integrity scan or a serving-authority grant. Ordinary raw
degraded snapshot inspection has a weaker contract and is not a complete
indexed query.

`Limits.max_rows` must be positive. `max_bytes` is the encoded-row work budget,
including collision candidates, not only returned matches. Equality counts all
examined postings, including staging; full original field bytes decide equality.
An equality operand is bounded to the existing value limit. A zero-byte budget
can read an actually empty validated collection. Schema, head, missing row,
corrupt encoding, expired snapshot or exhausted bounds refuse the whole query;
they never become a partial successful or empty history. Budgets do not claim a
total Python allocator/RSS limit. Negotiated snapshot/frame/cursor bounds apply.

## Ack, conflict and original uncertainty

Select `Ack.OS` or `Ack.FLUSH` explicitly. OS means complete append/publication;
Flush adds the provider's covering sync. Each successful mutation must return
exactly the requested achieved Ack and a nonzero opaque revision. Existing
immutable values require exact bytes and Ack equality. Reading a matching
committed row reconciles existing indexed publication; it does not perform a new
whole-collection Flush or certify physical power-loss behavior.

The additive `retry_transport=False` option on `Client.call` and `Client.put`
disables their legacy reconnect/retry. The binding always selects this option.
It requires an explicitly connected Client and never changes an uncertain send
into an implicit retry. Ordinary Client defaults remain unchanged.

`Deadline` carries explicit Unix admission milliseconds and a local monotonic
preflight boundary. `Deadline.after(seconds)` creates both. Each Client call is
bounded by the smaller remaining Unix/local budget at entry. The existing Python
transport enforces Unix-millisecond I/O deadlines; this binding does not upgrade
it to Rust's per-transfer monotonic transport contract. Clock jumps during an I/O
remain that transport's limitation.

`Conflict` describes immutable/schema/ordinal/publication disagreement.
Only a validated refusal for the current conditional PUT can settle it. Code 12
triggers reconciliation. Pending conflict 19, retry expiry 13, identity mismatch
14, quota 8 and other definitive codes retain their `Refusal` type and code.
Codes 16/17, refusals from outside that exchange, transport loss, malformed
mutation reply or wrong achieved Ack raise `Uncertain` with the original
Publication, MutationRequest, operation ID, epoch and cause.
The token's `uncertainties` tuple retains every such original observation even
when a later explicit reconciliation succeeds or refuses.

The binding records `Uncertain` on the same publication before detaching and
closing the channel. `Uncertain.cleanup_errors` retains close failures.
Keyboard interrupts, process and generator exits, and the standard `asyncio`
and `concurrent.futures` `CancelledError` classes propagate as the original
exception at the mutation receipt boundary. The same exceptions propagate
during cleanup if an ordinary mutation failure came first. An application can
use `with cancellation_exceptions(StopRequested):` around `declare` or `append`
to give a dedicated `Exception` class the same contract. The registration
matches exact classes in the current Python context and requires no signal
handler integration. Other unexpected exceptions at the receipt boundary
propagate after custody is recorded. Ordinary I/O, refusal and unregistered
application exceptions during close stay in `cleanup_errors`. When cancellation
propagates from cleanup, the retained uncertainty still names the original
mutation failure. The binding never reconnects or resends in that invocation.

An application may explicitly reconnect the same Client/endpoint and revisit
the SAME Publication with a fresh bounded deadline, within its original epoch
and conservative horizon from the first mutation send. No automatic loop does
so. Changed epoch, changed endpoint or elapsed horizon refuse before sending.
Each positive executing offer can shorten the retained bound from the first
mutation send. A later wider offer cannot lengthen it. Later sends, successful
reconciliation and reconnects retain the earliest timestamp and previous
uncertainties. A short positive horizon can expire with retained staging.
Expiration supplies no absence or replacement permission.
A matching epoch on another endpoint is not lineage. Even the same endpoint
can have been restored; external authority and application reconciliation are
still required. This API cannot establish recovery authority from copied epoch.

## Supported boundary and exact remainder

The module exposes bounded MBIX1 declaration/append/query only. Large records
above the encoded bound raise typed `Unsupported`, including before schema/head
effects during preparation. Chunked material publication is unavailable. Missing
schema/required data is `Corrupt`/unavailable, not an empty replacement history.

An interrupted row/posting stage remains retained outside committed head. A
different record cannot overwrite it; later ordinals cannot skip it. The original
in-memory Publication can reconcile/resume within its bounds. There is no staging
garbage collector, abandonment takeover, durable Python operation journal,
token import, cross-process custody recovery, complete dataset capture/restore,
writer fencing or live old-data migration/rollback API here. Those capabilities
remain separate generic-provider/application work. No cleanup is implicit.

The resident's bounded retry history and startup sync barrier are inherited,
with their exact exclusions in [KV1](KV1.md), [retry retention](RETRY_RETENTION.md)
and [recovery Flush](RECOVERY_FLUSH.md). Private process replay demonstrates only
that authored covered data reopens under that source; it grants no restored
serving authority and proves no complete backup or post-capture effect custody.
Namespace retention, raw-writer discipline and dataset coverage remain explicit
owner responsibilities as in [indexed collections](INDEXED_COLLECTIONS.md).

## Focused controls

`tests/indexed_python.py` contains authored byte/absence/refusal/uncertainty and
private IPC controls. Run its `--discover` mode to list all controls, or run
`python -m unittest discover -s tests -p indexed_python.py -k LocalControls -v`
for controls requiring no endpoint. The full caller controls require explicit
`MEMBUS_PYTHON_INDEXED_PIPE` and `MEMBUS_PYTHON_INDEXED_NAMESPACE` from an owned
private fixture; the script starts no server and discovers no installed service.

Use a fresh state directory and unique local named pipe for the private runs.
The script's `--integrity-complete`, `--integrity-fault` and `--integrity-empty`
controls expect a fixture with two rows, respectively two, one or zero
postings, and the corresponding captured eligibility. `--zero-retry` expects
a provider launched with `--retry-ms 0`.
