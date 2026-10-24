# Unresolved catalogue appends

`Catalogue::open` admits one cooperative writer per canonical catalogue directory.
A persistent sibling `<directory>.catalogue-owner` file holds the lifetime lock,
including while import replaces the directory or compaction replaces the log.
The log handle also excludes writable hardlink aliases. `Catalogue::path()` exposes
the immutable path; direct filesystem mutation and older noncooperating writers
are outside this contract. Store acquires ownership before import and destructive
replay; read-only catalogue snapshots remain available.

Catalogue publication requires a flushed frame and resolution of its append marker. Before writing
the frame, the writer creates and flushes `catalogue.log.append-pending`, containing a versioned
boundary record, then synchronizes the containing directory before attempting the frame write.
An intent-directory failure leaves the catalogue boundary unchanged, retains the marker and fences
further appends; a known-live-state rewrite can retry the recovery. A failed append restores and
flushes its previous boundary before removing that marker. Successful publication, completed rollback
and recovery by rewrite each synchronize the containing directory after removing the marker.
Neither append accounting nor the successful-rewrite baseline advances before that barrier completes.
If boundary restoration or marker resolution fails, further appends are fenced.

Before removing a marker, the writer retains its exact bytes for error recovery. If the removal's
directory barrier fails, it attempts to recreate and flush those bytes and synchronize the directory
again, then returns the original failure. This preserves restart refusal even when the unaccepted
frame is complete. If marker restoration also fails, both errors are reported and the live writer
stays fenced. A failed restoration barrier does not establish power-loss persistence of the marker;
no automatic startup repair is inferred from such a failure.

Startup refuses catalogue replay while the marker exists, including when its contents are incomplete.
It preserves the catalogue and chunk files and attempts to record the refusal in the replay report.
Report publication can fail; an earlier ready report alone is not current availability evidence. A complete,
valid trailing frame therefore cannot turn an unresolved failed retirement into permission to delete
its chunk on restart.

An existing owner with known in-memory live state can resolve the fence through the ordinary live-list
rewrite, including with growth compaction disabled. `Store::maintain_catalogue()`
returns `Unchanged`, `Compacted`, or `Recovered { evidence }`, and propagates I/O
errors. Before replacing uncertain state, it copies and flushes the exact log and
any pending marker into a new `catalogue-recovery-*` directory, then persists that
directory and its parent. These evidence copies require disk space and an operator
retention policy; recovery does not delete them or clear sticky error flags.
The replacement is flushed, its handle is prepared and its path published before the marker
is removed. Startup has no such live-state witness and does not remove the marker or infer a safe
boundary. Recovery after that refusal needs an independently established retained-state account; it
is not an automatic repair or a reason to delete the marker manually.

The replacement's containing directory is synchronized after the rename and
before an unresolved marker is cleared. If that barrier fails, the replacement
may already be visible: the writer retains its handle on that image and fences
further appends. Any existing unresolved marker remains, so replay/open still
refuse it. A successful retry from the owner's known live list completes the
barrier and marker resolution before clearing the fence or advancing the
successful-rewrite baseline. A failure before replacement publication retains
the prior image and handle, as before.

The tests represent complete write followed by sync failure and boundary-recovery failure, partial
markers, live-list recovery, retained chunk bytes on restart refusal, and still-readable values after
a later retirement fails. They exercise process restart and injected I/O error branches. They do not
establish filesystem behavior after power loss, a recovery deadline, or compatibility with an older
binary that does not understand the marker. Do not open unresolved state with such an older binary.

Run the Rust test suite with `cargo test --offline --locked`. `MEMBUS_TEST_OUTPUT` can select an isolated
test output folder. The marker adds a separate file flush and directory barriers for creation and
removal to each catalogue append; no throughput claim is made without a measurement for the selected
storage and workload.

Local catalogue controls observe complete marker contents and the unchanged prior frame boundary
at each intent barrier, reject an injected intent-directory failure before any frame write, and
exercise partial-write rollback after a successful barrier. Replacement controls verify rename
ordering, retained unresolved markers, append fencing and handle alignment after a failed directory
barrier. Resolution controls in `src/chunks.rs` cover successful publication,
rollback, failed marker-removal barriers, exact marker restoration, repeated restoration failures
and the replacement handle/baseline across rewrite retry. A Windows held-marker control exercises a
real denied unlink before any resolution barrier. The per-catalogue observer exists only in
unit-test builds. These checks establish operation and failure ordering, rather than physical
power-loss behavior.

Replay records a flushed `truncation_pending` stage in the existing replay evidence
before removing a torn tail. That stage has `ready_ms=0`: it records what was found
and planned, rather than claiming a successful truncation or ready service. A later
ready report replaces the current JSON; the bounded replay log retains the earlier
stage if startup stopped after truncation. Failure to publish this evidence refuses
the destructive step. Report publication includes directory barriers after
snapshot and history-rotation renames, and after flushing the appended history
line, before recovery may truncate either input. The directory ordering and
error behavior are described in [recovery directories](RECOVERY-DIRECTORIES.md).
This ordering supports process-interruption evidence, without
adding a power-loss or unlimited-history guarantee.

Live-list rewrites retain original logical retirement times. Expiry summaries are
ordered by their timestamps and repeated older representations cannot refresh an
existing key's summary age. Successful compaction resets its growth baseline, so an
oversized unchanged live list does not trigger a rewrite on every idle tick. A fenced
running owner can still replace its known live list; sticky storage/read-only flags
are not cleared by that replacement.

The ticker releases durably retired chunk and closed-sink bookkeeping only after
the store's pending-operation relationship has discharged it. A reservation spans
append completion and any delayed flush notice/result. Merely closed or still
retiring sinks retain their covering extent, holders and retry state. The flush
owner drops coverage entries for discharged sinks on its next work-loop visit.
Lifetime counters remain separate from these released rows. This is bookkeeping
reclamation; it does not implement cache eviction or a total RSS budget.

See [chunk retirement](CHUNK-RETIREMENT.md) for the surrounding storage lifecycle
and [read-only inspection](RETIREMENT-INSPECTION.md) for stopped-catalogue observation.
The separate [recovered Flush prerequisite](RECOVERY_FLUSH.md) describes active
data-file synchronization before recovered values and retry outcomes are exposed;
catalogue synchronization alone does not establish that prerequisite.
