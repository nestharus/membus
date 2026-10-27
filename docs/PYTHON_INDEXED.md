# Python indexed collections

`python/membus_indexed.py` binds the existing public Rust
`membus::indexed::Collection` MBIX1 declaration, immutable row/posting publication,
conditional head and pinned queries. It uses `membus.Client`; it opens no backing
files and starts no worker. Add the repository's `python` directory to the
caller's import path. Python 3.10 or later and the standard library suffice.

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
  planned mutation's frame size before any publication effect. A negotiated
  zero retry horizon raises `Unsupported` during preparation, before any
  schema, head, row or posting effect. This multi-step binding cannot safely
  complete that configuration; positive horizons remain fixed bounds, not a
  promise that every admitted publication will finish within them.
- `declare(client, publication, deadline)` establishes only the immutable schema
  and head. `append(client, publication, deadline)` reserves the immutable row,
  writes deduplicated CRC candidate postings, then CAS-publishes head. It returns
  `AppendReceipt(through, already_committed)`. This is the existing participating
  writer protocol, not an atomic BATCH or a transaction with independent keys.
- `query(client, namespace, snapshot, query, limits, operation, deadline)` returns
  `Read(through, records, encoded_bytes_read)` from one explicit 16-byte snapshot.
  Queries are `All()`, inclusive `Ordinals(first,last)`, or `Equal(field,bytes)`.
  Internal pages keep the same token/cursor and return complete ordinal order.
  No current GET substitutes for a snapshot read. A caller can page a large
  ordinal history through separately bounded `Ordinals` calls on the same token.

All query variants first call the canonical provider's
`snapshot_require_complete(namespace, snapshot, op_id=..., deadline=...)`
contract on the exact supplied token, with transport retry disabled. The
additive opcode 31 returns an empty success payload only when that captured
view is eligible for a complete query. A known incomplete recovered population
raises `Refusal` code 16 before schema, head, row or posting selection. Missing
postings cannot become a successful empty or partial equality answer merely
because schema, head and matching rows survived. Providers without this
contract raise `membus.NotAvailable`; there is no fallback to unchecked reads
or a later live health observation. This source requires a provider implementing
that canonical contract, including the corresponding Rust indexed consumer.

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
`Refusal` preserves the Client's exact code: committed conflict 12, pending
conflict 19, retry expiry 13, identity mismatch 14, quota 8 and other refusals stay
distinct. Transient classification is not retry permission. Codes 16/17,
transport loss, malformed mutation reply or wrong achieved Ack raise `Uncertain`
with the original Publication, MutationRequest, operation ID, epoch and cause.
The token's `uncertainties` tuple retains every such original observation even
when a later explicit reconciliation succeeds or refuses.

An application may explicitly reconnect the same Client/endpoint and revisit
the SAME Publication with a fresh bounded deadline, within its original epoch
and conservative horizon from the first mutation send. No automatic loop does
so. Changed epoch, changed endpoint or elapsed horizon refuse before sending.
The earliest original send is retained incrementally as mutations accumulate;
later sends, successful reconciliation and reconnects never renew that bound
or clear previous uncertainties. A short positive horizon can expire with
retained staging; expiration supplies no absence or replacement permission.
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

Qualification also used a retained external native harness compiled against the
exact source-bound library, using the existing finite-child support unchanged.
It ran only unique local named pipes with explicit absolute fixture/output roots,
verified Rust/Python publication in both directions, restarted that same authored
root and reread its facts. All roots, raw/index/replay files and control source
remain retained for their owner's explicit disposition. This is a private
qualification result, not an installed-service or complete-recovery claim.
