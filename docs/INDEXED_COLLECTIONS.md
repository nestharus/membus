# Indexed append-only collections

`membus::indexed` is a public library capability over the existing resident
`Client`. It uses the existing KV mutations and pinned namespace scans. It does
not open backing files, implement another wire client, parse caller documents,
or introduce a daemon. The caller explicitly supplies the namespace, collection,
operation identity, acknowledgment, deadline and query limits.

`Schema { fields }` declares up to 64 distinct exact UTF-8 field names. A `Record`
has an original ordinal, opaque payload bytes, and one list of byte values per
field. The owning codec extracts these values. Missing fields have empty lists;
each field permits at most 1024 values. Multiple links do not clone a row.
Original payload, value order and duplicate values survive retrieval. Collection
names are ASCII alphanumeric/underscore/hyphen identifiers of 1..64 bytes.

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
head's condition protects publication, not mutation of immutable rows: rows and
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
An encoded-byte budget of zero can return an actually empty collection after
validating its declaration and committed boundary. A required record still
refuses; the row-count budget must remain positive. For equality queries,
`max_rows` bounds examined postings, including staged and collision candidates;
`max_bytes` charges encoded committed records examined, including collisions,
rather than only returned matches. These are query-work bounds, not a complete
allocator/response-memory limit. `encoded_len` validates and
measures an exact record before publication.

Measurement checks ordinal, field counts and cumulative byte bounds in encoding
order without allocating or copying the record. Publication uses that same
validated length to reserve its encoded output once. This leaves the wire bytes,
refusal precedence and append/reconciliation protocol unchanged. Complete
publication also allocates other protocol data.

Each `Collection` keeps the canonical encoded bytes of its own immutable schema.
Construction validates and encodes once; declaration reuses those bytes and query
compares them with the declaration read from the caller's resident snapshot.
Every query still reads and checks that resident declaration, including when the
same collection is used in another namespace. This caches expected local bytes,
not acceptance of a remote declaration. A cloned collection owns its own copy;
the bytes leave with the collection. The exact boxed encoding is at most
`8 + 64 * (4 + MAX_ID)` bytes per collection, separate from resident cache usage.

Immutable publication moves the encoded value into the client's prepared
mutation. Conflict reconciliation compares against that retained request, so no
second full value copy is kept just for the comparison. The request remains
owned until publication/reconciliation returns; acknowledgment matching and
immutable collision refusal still apply. Other transport and resident copies
remain.

For `All` and `Ordinals`, row pages also carry an acquisition budget: remaining
encoded-row bytes plus conservative protocol overhead, capped by the negotiated
frame. The overhead allows the fixed page fields, a maximum query-bound cursor,
and each page slot's maximum key and value envelope. Page slots are capped by the
requested committed interval and negotiated batch count. This positive slot cap
remains fixed across continuations so extra malformed rows still reach the
existing identity/completeness checks.

When the smaller query-derived budget refuses a first item as `too_large`, the
collection returns its ordinary `Limit`; an inherited transport-frame refusal
keeps its typed client error. Row-byte accounting and all validation remain in
place after reception. Conservative overhead can admit some excess encoded
bytes before `Limit`; this is not an exact storage-read cutoff or a total memory
bound. Declaration/boundary reads and equality row lookup keep their existing
acquisition behavior. The caller still owns snapshot release on every error.

This is an append/publication contract for participating callers in an explicitly
selected namespace, not an access-control boundary against arbitrary raw KV
writers. Unsupported overwrites can corrupt that namespace. Ordered query
validation detects missing or malformed committed rows, but no universal
malicious-writer, authenticity, power-loss or all-writer commitment claim is made.
The namespace's class/retention and authority remain its owner's responsibility.

Operation bases generate suffixed KV operation IDs (schema, row, posting and
commit), so leave room within the resident identifier limit. Revisit an interrupted
append with its original operation base, bytes, ordinal and acknowledgment. A
staged ordinal cannot be skipped by publishing a later row. The API does not clean
abandoned staging automatically.

## Available checks

`cargo test --offline --locked --test indexed` exercises actual isolated resident
processes through the supported Client: pinned boundaries, complete ordinal and
field/link reads, original bytes, immutable duplicate/conflict behavior, schema
and budget refusals, a stopped publication after reservation, malformed committed
data, and a deliberate CRC bucket collision. Fixtures own and settle their
children through the repository's existing finite-child support. These controls
do not install a service, select a consumer backend, or establish a performance
improvement. A consumer must still demonstrate its actual query and semantics.

`indexed_admission` covers refusal before loading an oversized required row or
evicting a useful cached value, retry with the same view, exact byte allowances,
transport refusals, saturated arithmetic, and unexpected continuation rows.
Cold-load counts describe the application cache, not physical-drive I/O.

## Bounded allocation and acquisition

Equality queries bound each posting page by the remaining candidate count plus
one overflow witness. Required candidate rows use the remaining encoded-byte
allowance, including CRC collisions; staged postings above the captured boundary
do not load rows. A query-derived `too_large` becomes `Limit`, while a binding
transport-frame refusal retains its client error. Range queries reject an
impossible remaining interval before acquiring a page, using the minimum record
encoding size after validating the captured declaration and boundary.

Collections retain their immutable schema encoding and control keys. Publication
borrows the schema until a missing key needs an owned mutation, skips temporary
bucket sets for empty or singleton fields, and formats row and posting keys into
their final buffers. Returned identities are checked without formatted copies.
Schema validation uses at most 64 borrowed name slots and preserves field order.

Decoded field lists reserve their validated counts; record slots are reserved
from a received page after its first row validates, never from the captured head.
A fully validated row can transfer its received encoding to a dominating payload
when capacity is at most twice the payload length. Smaller payloads copy only
their own bytes. Snapshot capture reserves its exact live-row count after
admission, under the same committed-index read lock as its counting pass.
