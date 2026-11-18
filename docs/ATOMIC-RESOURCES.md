# Complete atomic populations and allocation ownership

`Store::capture_namespace_funded(namespace, bytes)` requests a finite allocation
account from the existing Store. The integer is a requested reservation, not a
grant carried in an original. Capture measures the complete live namespace while
holding the admission gate and committed-index read lock. Before allocating any
growing row, key, value or guard vector, it reserves the entire account against
snapshot count, snapshot bytes and the Store's current memory allowance. Ordinary
snapshot reservation and this reservation serialize through the same scan lock.
Insufficient resources return `Resource(7)` without materializing a capture.
Checked overflow and the original frame limit also refuse explicitly.

The account funds captured rows, a shared complete key/revision guard population,
and each distinct original prepared from the view. `NamespaceView::prepare` checks
the original's representation and available account before retaining its body.
It compacts supplied buffers into funded backing rather than retaining arbitrary
caller capacities. Multiple originals share the immutable guard allocation and
reserve their own operation/body storage. Each `AtomicRequest::clone` shares the
entire immutable original; it does not duplicate keys, values or guard storage.

Dropping a view releases its logical row charge, while any retained original or
clone keeps the Store reservation alive. The entire requested Store allowance,
including unused headroom, is conservatively retained until its last owner drops.
It is never recycled merely because the view expired or disappeared. The existing
snapshot count is likewise retained; outstanding originals can therefore cause a
later capture to refuse. Choose an account appropriate to the complete population
and planned original bodies. A refusal supplies no permission to recapture or
replace a previously uncertain operation.

Caller context/command backing can share that same captured account through
`NamespaceView::reserve_backing`. Its complete declared layout and lifetime
obligations are separate from original preparation; see
[captured caller backing](ATOMIC-CAPTURED-BACKING.md). A payload count or a new
`OriginalBudget` is not a substitute for the actual captured-account charge.

The legacy `capture_namespace` keeps its conservative 4096-row profile. Its account
includes complete measured capture storage plus one frame of original headroom;
this can refuse earlier under a small memory allowance. Larger populations use
the explicit funded method. They have no replacement row-count constant: the
complete captured guards, finite original representation and actual account all
have to fit. No paging, truncated population or allocator-history inference is
performed. Publication compares the entire current live key/revision interval
under the existing gate, including insertions, removals, replacements and absent
targets. Four final changes still use the same envelope, version range and index
publication; BATCH remains independent.

`BorrowedOriginal::from_bytes` validates the complete immutable original with
constant auxiliary storage. It exposes borrowed metadata, guard and change
iterators; the input owner must retain the frame for the borrow's lifetime.
Counts must fit the literal remaining bytes before iteration. Keys, order,
revisions, duplicate targets, discriminators, lengths and trailing bytes are
validated, without first allocating a population. This is syntactic validation,
not authenticated domain intent or an allocation grant.

`OriginalBudget::new(bytes)` creates a finite caller-owned logical allocation
account. Its clones share one atomic counter. `allocation_bytes()` on a validated
borrow measures the managed backing needed by `to_owned(&budget)` without
allocation. `AtomicRequest::from_bytes_funded(bytes, &budget)` reserves both the
complete guard and original-body allocations before constructing either. Failure
releases partial reservations. A live original owns its reservation; shared clones
retain it until their last owner drops. The compatibility `from_bytes` convenience
uses a separate, exactly measured per-original account. It provides no aggregate
caller-memory guarantee; callers needing one must use a shared budget.

Startup envelope validation, routing and cold selection use this same borrowed
validator. Startup retains the complete borrowed original identity and materializes
one bounded mutation at a time through the existing replay path. Routing retains
only the bounded final-key projection. Cold selection validates every peer before
copying the selected subrecord. None relies on a future publication reservation
to fund an earlier owned population. Existing frame/selected-value, index, replay,
queue, retry, cache and disk contracts remain separate.

Managed charges conservatively cover owned Rust backing storage and fixed object
overheads. They do not measure allocator bookkeeping, RSS, physical RAM, caller
buffers or arbitrary external concurrent allocations. Original serialization
returns a bounded caller-owned buffer under the existing frame contract. No
process-survival, crash, serving-authority or operating-capacity guarantee follows.
Epoch, original horizon, identity, requested/achieved Ack and retained uncertainty
keep their existing meanings. Decoding or copying an original supplies no domain
provenance or deleted-ID allocator authority.

The pure library controls exercise complete 4161-key decoding, shared capture
materialization and preparation, a four-change and next-enqueue authored oracle,
resource refusal, ownership through view/clone drops, concurrent account admission,
malformed input, whole-guard conflicts, identity/uncertainty and borrowed codec
conservation. Authored index intervals and scan charges stand for those production
seams; no operating Store, physical crash, allocator/RSS measurement or real
application enqueue is qualified by these tests.
