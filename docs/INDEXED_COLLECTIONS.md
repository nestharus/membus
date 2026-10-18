# Indexed append-only collections

`membus::indexed` is a public library capability over the existing resident
`Client`. It uses the existing KV mutations and pinned namespace scans. It does
not open backing files, implement another wire client, parse caller documents,
or introduce a daemon. The caller explicitly supplies the namespace, collection,
operation identity, acknowledgment, deadline and query limits.

`Schema { fields }` declares up to 64 distinct exact UTF-8 field names. A `Record`
has an original ordinal, opaque payload bytes, and one list of byte values per
field. The owning codec extracts these values. Missing fields have empty lists;
multiple links do not clone a row. Original payload, value order and duplicate
values survive retrieval. Collection names are bounded ASCII identifiers.

The version-one typed query language is `Query::All`, inclusive
`Query::Ordinals { first, last }`, and `Query::Equal { field, value }`. Results
retain original ordinal order. Unknown fields and malformed intervals refuse.
Every result includes its captured committed `through` boundary. `All` means all
committed history; it does not apply revisions, retirement, recipient routing or
cursor policy. Those meanings stay with the consumer's existing implementation.

## Publication and snapshots

Within the selected namespace, `x/NAME/schema` holds the immutable declaration,
`x/NAME/r/ORDINAL` the immutable encoded record, and `x/NAME/i/FIELD/BUCKET/ORDINAL`
its postings. Decimal ordinals use 20 digits; valid row ordinals are 1 through
u64::MAX-1. Field indexes use the existing CRC32 as a candidate bucket only.
Equality always compares complete original field bytes, including collisions.

Append reserves the immutable row first, writes all declared postings, then
conditionally advances the existing keyed `head` control record. A conflicting
writer at the same ordinal cannot publish its postings for another row. The
head's condition protects publication, not mutation of ledger rows: rows and
postings are never updated by this API. Existing keyed KV updates remain valid
outside these immutable records. The control record is not an authored row.

Interruption before the head leaves staged data outside the committed boundary.
An explicit revisit with the same immutable record can reconcile it. The API
does not retry a mutation, invent a caller operation ID, change epochs, or label
an uncertain result absent. Shared-client uncertainty and definitive refusals
remain distinguishable. Already committed identical rows reconcile without a
write. Changed bytes or acknowledgment refuse; the API does not upgrade an old
OS acknowledgment by silently rewriting immutable data.

The caller opens one resident namespace snapshot and retains it across queries.
Schema, head, postings and row payloads are read from that same snapshot. No GET
of current state substitutes for a captured value. The caller owns snapshot
release, including error paths. Snapshot expiry, missing schema/head/row, gaps,
invalid encoding and exhausted query budgets refuse the whole query rather than
returning a partial successful account. Resident frame/value/snapshot bounds also
apply. Encoded records, including field values, must fit the existing value
bound. The library never truncates a large row.

This is an append/publication contract for participating callers in an explicitly
selected namespace, not an access-control boundary against arbitrary raw KV
writers. Unsupported overwrites can corrupt that namespace. Ordered query
validation detects missing or malformed committed rows, but no universal
malicious-writer, authenticity, power-loss or all-writer commitment claim is made.
The namespace's class/retention and authority remain its owner's responsibility.

## Verification

`cargo test --offline --locked --test indexed` exercises actual isolated resident
processes through the supported Client: pinned boundaries, complete ordinal and
field/link reads, original bytes, immutable duplicate/conflict behavior, schema
and budget refusals, a stopped publication after reservation, malformed committed
data, and a deliberate CRC bucket collision. Fixtures own and settle their
children through the repository's existing finite-child support. These controls
do not install a service, select a consumer backend, or establish a performance
improvement. A consumer must still demonstrate its actual query and semantics.
