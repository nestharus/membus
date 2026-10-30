# Bounded selected-view acquisition

`Store::snapshot_open_selected(namespace, selection, budget)` and
`Client::snapshot_open_selected(namespace, selection, budget, operation_id, deadline)`
acquire one immutable selected population. Public types live in
`membus::selected_view`:

```rust
use membus::selected_view::{AcquisitionBudget, Selection};
let selection = Selection::Range { lower: "events/2026-10-01/", upper: "events/2026-10-02/" };
let budget = AcquisitionBudget {
    max_candidates: 8192,
    max_metadata_bytes: 8 * 1024 * 1024,
    max_retained_bytes: 32 * 1024 * 1024,
};
```

`Selection::Prefix("")` selects every live key in the namespace. Ranges are
half-open and require `lower < upper`; both bounds must be explicit. Keys and
bounds use ordinary UTF-8 byte lexical order. Bounds are at most `MAX_ID` bytes.
Values remain opaque, including empty values, non-UTF-8 bytes and terminators.
Tombstones are examined but excluded from the selected live population.

Success returns `SelectedView { snapshot: [u8; 16], population: u64,
candidates: u64, metadata_bytes: u64, retained_bytes: u64 }`. Use the existing
`scan_prefix` or `scan_range` with this token and **exactly the captured bounds**.
Keys-only scans use the same selection and token. A changed selection, namespace,
cursor or token association is refused; a disjoint query does not become a
successful empty page. Ordinary namespace snapshot behavior is unchanged.
`Store::snapshot_release` is also public for native token owners; native page
requests use the existing Store dispatch interface.

## Exact acquisition work units

The acquisition uses the existing ordered committed BTree index. It walks from
the index's **first entry**, through the first entry beyond the requested
namespace/selection, or to index exhaustion. It does not perform an uncharged
range seek. Preceding namespaces, names below the lower bound, tombstones and
the first stopping entry all count, regardless of application relevance. Entries
after that stopping entry do not count. A late namespace can therefore refuse
even when its selected population is tiny or empty. No separate index is built.

There are two passes under one committed-index read lock: measure, then pin.
For every visited entry `(namespace, key)`, the first pass reserves **both**
passes' work before comparisons or pinning:

* `candidates += 2`;
* `metadata_bytes += 2 * (128 + namespace.len() + key.len())`.

The candidate and fixed metadata allowance are checked before advancing the
iterator. String lengths are read from that admitted entry's headers; their
allowance is checked before examining the strings. The second pass visits
exactly the measured number of entries and does not discover additional input.
If either ceiling would be exceeded, the whole acquisition fails. A budget of
one candidate cannot admit a two-pass entry visit. Exact equality is allowed.
Index exhaustion is known from the iterator's remaining length, without an
extra candidate. Empty-index acquisition can use zero candidate/metadata budget.

These are explicit entry/metadata-footprint units, not CPU instructions, every
individual string comparison byte, BTree structural pointer steps, disk reads,
or a hard elapsed-time/cancellation bound. Work is proportional to the charged
visits plus existing BTree traversal setup. Lock waits are not bounded by these
units. Page budgets still govern output and later value loads separately.
Existing registry expiry and cache trimming retain their Store-configured
count/byte bounds; these housekeeping operations are outside the selected-index
discovery counters.

## Retained admission and lifetime

The exact retained charge is:

```text
256 + namespace.len() + selection_bound_bytes
    + sum(128 + key.len() + indexed_value_length) over selected live rows
```

The base covers the view, selector and registry bookkeeping. Per-row allowance
covers key/handles/loaded-value bookkeeping; full selected payload capacity is
reserved even while payloads are cold or shared. Neither nonselected payload
bytes nor tombstone payload bytes are retained. Checked arithmetic refuses
overflow. The caller's byte ceiling is independent of both work ceilings.

Before allocating the rows, cloning keys/selector, or incrementing entry
references, admission serializes `committed -> scans -> state` and checks the
same Store snapshot count, aggregate snapshot bytes and remaining process
capacity as ordinary snapshots and atomic original reservations. Admission
trims the live cache against the new reservation. Concurrent acquisitions
cannot independently spend the same remaining capacity. No value is loaded
during acquisition or keys-only paging. Existing immutable entry locations,
first-load coordination and fallible payload validation are reused.

The instance monotonic clock starts the TTL before waiting for the committed
lock. Expiry is checked before traversal, before admission, and under the
registry lock before publication. A recoverable row-allocation refusal, expiry or storage refusal
drops the whole reservation and pinned population. No partial token is returned.
Infallible standard-library allocations still have the process's ordinary
allocator-abort behavior; this API does not turn an allocator abort into a refusal.
The token retains the existing epoch/id format and does not survive restart.
Tokens belong to their originating Store/endpoint. Epochs are not globally unique
across independent instances; callers must retain that origin association and
must not transplant tokens to another resident. Tokens are not authentication.
Explicit release, ordinary snapshot expiry and the existing reaper apply.
In-flight dispatchers keep their Arc and byte/count charge until their final
reference drops, even after registry removal. UTC jumps do not extend the TTL.

## Wire and compatibility

The additive `membus.kv/1` minor-zero opcode is **32**, exported as
`SNAPSHOT_OPEN_SELECTED`. HELLO, refusal vocabulary, `Options`, `Negotiated`,
old opcodes, scan pages and cursor formats are unchanged. The native client
requires the existing scans opt-in. That opt-in does **not** advertise selected
acquisition support. The operation itself discovers support: an older resident
returns existing refusal `2 unsupported`; there is no fallback to namespace
capture and no automatic retry. The Python binding is unchanged.

After the ordinary `deadline:u64, operation_id:lp, namespace:lp` prefix, send:

```text
kind:u8 (0 prefix, 1 range)
lower:lp UTF-8
upper:lp UTF-8                 # range only
max_candidates:u64
max_metadata_bytes:u64
max_retained_bytes:u64
```

The success payload is `snapshot:lp(16), population:u64, candidates:u64,
metadata_bytes:u64, retained_bytes:u64` (52 bytes; 67 including the reply
header, excluding its transport length). Reply capacity is checked before
opening. Native errors are `AcquisitionError`: invalid operands map to `3`,
caller work/retained ceilings to `4`, Store resource/allocation/identifier
exhaustion to `22`, acquisition expiry to `20`, read-only/warming to `10/11`,
and known storage failure to `16`. All refuse the entire acquisition. Wire
refusals preserve the existing names/transience; work and retained ceilings
share `too_large` on the wire. Later scan failures retain their existing codes.

Opening is not operation-ID deduplicated. A lost/malformed reply may leave an
unknown remote reservation until TTL; it is not proof that no token exists.
The client does not retry or reconnect. Malformed or wrong-epoch success closes
client usability. A fresh acquisition after expiry starts a different view;
callers must discard any partial previous result before using it.

## What completeness means here

Success pins the complete **live indexed key population for this selection**
and its revisions/lengths under one immutable observation. A zero population
means bounded discovery actually completed. Acquisition does not read or verify
payload storage. A later scan can still refuse corruption, missing data, expiry,
an oversized row, or transport failure. Callers must receive and validate every
page, require the captured population, and preserve errors as unavailable rather
than accepting partial input or treating failure as an empty collection.

This is not an `atomic::NamespaceView`, COMPLETE guard, original-budget grant,
publication/recovery receipt, storage-eligibility or durability certificate. It
does not establish that the selected keys contain all physical chunks/rows of
an application record. Record framing, UTF-8/JSON/numeric validation, interval
selection, counts and schema/partition qualification remain application duties.
Existing large-envelope cold-read work and scratch limits are unchanged; this
operation does not compose atomic publication, complete large records and
durable original/receipt recovery into an application store contract.
