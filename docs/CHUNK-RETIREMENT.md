# Chunk retention, retirement and replay

Membus stores MBK1 records in class-specific chunk files. It deletes eligible
whole chunks; it does not compact surviving data records or evict resident values
into another storage tier. Policies govern subsequent writes. Existing records
keep their original chunk's class and close time.

## Classes and policy publication

A namespace can select a class by UTF-8 key prefix. Longest prefix wins; no
matching policy means `forever`.

| Class | Automatic retirement of a closed chunk |
|---|---|
| forever | Never |
| latest | After close + configured retry horizon, only when it holds no key's latest record |
| expire(retention_ms) | After close + retention, including keys whose latest records it holds |

Expire retention must be positive and at least the configured retry horizon,
otherwise policy admission refuses malformed (3). The internal timer namespace
uses latest; internal policy records use forever. Public operations cannot
access either internal namespace directly.

Automatic retirement waits until both the UTC age since close and, for chunks
closed in this process, the monotonic age satisfy retention and strictly exceed
the retry horizon. Retry outcomes remain valid at the exact retry boundary,
including a zero-length window at the close instant. Longer expire retention
keeps its existing inclusive retention boundary. Recovered chunks use the UTC
age without adding to a timestamp, so a close time near the integer limit cannot
shorten either window. Explicit forced retirement retains its clock override.

HEALTH2 counts latest-class bytes as pinned only after the same UTC and local
retry bounds have passed. An exact-boundary chunk or a UTC correction alone
does not raise the pinned-byte high-water signal. Recovered chunks use their
actual UTC age, including when close timestamps approach the integer limit.

The blocked-retirement count reflects observed delete failures. Its oldest age
uses local monotonic time; a UTC correction does not age or clear the one-hour
high-water signal. A restart starts a new observation when recovery again finds
the delete blocked. `ChunkInfo::blocked_since` is this in-memory monotonic start,
not a persisted UTC timestamp. Successful retirement clears the observation.
If a pass stops after a durable retiring intent but before attempting deletion,
the retry path records its own first denied delete. Repeated denials preserve
that start; the existing sticky storage-error observation remains unchanged.

The class cap is 64 distinct classes, including built-in forever/latest,
selected policies, nonretired instantiated classes, in-flight routing choices
and prospective policy admissions. A setter exceeding it refuses capacity (7).
Old policies remain selected during a setter's durable wait. At the cap, replacing
a class may require clearing its policy and waiting for outstanding obligations
to discharge. Retired historical names do not consume a permanent lifetime slot.

Policy setters persist with flush acknowledgment, then publish the routing
projection before completing. Their operation IDs use the common caller namespace
and bind prefix/class/clear content. Identical retained retries return the original
version without reapplying an older projection; a comparator mismatch refuses (14).
Comparison has the [retained retry encoding/collision limits](RETRY_RETENTION.md).
Pending duplicates join through routing completion with their own deadline.
Routing admission/publication is ordered without holding a routing lock over
append or sync. A failed reply remains subject to mutation uncertainty.

Windows CLI forms:

```text
membus policy LOCAL_PIPE get NAMESPACE
membus policy LOCAL_PIPE set NAMESPACE PREFIX forever
membus policy LOCAL_PIPE set NAMESPACE PREFIX latest
membus policy LOCAL_PIPE set NAMESPACE PREFIX expire:MILLISECONDS
membus policy LOCAL_PIPE clear NAMESPACE PREFIX
```

These are explicit administrative mutations, not a migration of old chunks.
Select policy according to the owning application's retention requirements.

## Rotation and accounting

A key's stable hash chooses one of four stripes. Each class has at most one open
chunk per stripe, at `chunks/CLASS/STRIPE-SEQUENCE.log` under the state directory.
Chunk sequence IDs are monotonic. A new record opens a chunk on demand; no empty
chunks are created just to satisfy a timer. Rotation occurs when another append
would exceed `chunk_bytes` (default 8 MiB). Finite classes also rotate at
`rotation_ms` (default one hour), including idle closure by the maintenance tick
(default one second). Forever rotates by size only. A single allowed large record
is still bounded by the protocol value/frame limits.

`chunks/catalogue.log` records Open, Close, Retiring and Retired transitions,
version high-water, horizons, expiry summaries and import provenance. Publication
requires the complete catalogue append, sync and marker resolution. Open is
published before creating the chunk file. Close syncs data and records the close
boundary. Catalogue rewrites retain the live metadata account, without copying
data records; growth beyond the 1 MiB default threshold triggers rewriting,
measured relative to the last compact image.

The library's `catalogue_rewrite_bytes = 0` disables growth-triggered compaction.
It does not disable repair of a fenced catalogue or discharge of incomplete
retirement records. Background maintenance still attempts the known-live
rewrite required before another catalogue append can succeed. A failed repair
retains the fence and pending requests and exposes its cause in HEALTH2; a
successful repair permits the ordinary operation to retry. It does not apply
the operation whose publication failed.

Replay chooses each key's greatest retained revision across files, including
policy moves between classes. Catalogue high-water prevents revision reuse when
the record holding the greatest issued revision has retired. Horizons suppress
older retained revisions that would otherwise resurrect an expired/deleted key.
A horizon is needed while another retained chunk holds a record it must hide;
retirement cannot proceed without publishing required metadata.

Live namespace quotas and global index slots differ:

- A put to a missing key needs both; a put to a tombstone needs a live slot again.
- A committed remove frees its live namespace slot, while its tombstone occupies
  a global index slot. Missing-key remove also persists a tombstone.
- A tombstone index entry can drop when no other retained chunk holds an older
  record of that key. Older records in its own chunk retire with it.
- Expiring a latest record removes its live value and replaces its index slot
  with an expiry summary, retained for 30 days by default.

GET/GET_MANY/current scans treat expired keys as absent, and expected=0 can
recreate one. GET_STATUS distinguishes live, expired and missing while the
summary exists; afterwards expired becomes missing. Existing immutable snapshots
retain captured values through later expiry. Expiry does not publish a new KV
mutation receipt to subscribers.

## Retirement and failure boundaries

The maintenance thread runs the retirement pass every ten seconds by default.
It selects closed eligible chunks, skips keys with pending operations, and:

1. Publishes Retiring with horizons, expired-key summaries and revision high-water.
2. Removes affected current index/value entries.
3. Deletes the file.
4. Publishes Retired, then releases retained-byte accounting.

Large Retiring transitions use indexed `RetiringPart` records (catalogue kind 10).
Each payload is at most the 16 MiB frame bound, measured from encoded UTF-8 bytes
including length prefixes. Every part carries the same chunk, revision high-water,
retirement timestamp and part count, and consecutive indices starting at zero.
The state/catalogue lock covers the whole append sequence. Each append must finish
its sync and marker resolution before proceeding; only after the final part succeeds
may horizons, summaries, index entries or chunk state change, or the file be deleted.
Replay applies a complete consecutive set as one transition. A fresh index zero
starts a new attempt; parts from different attempts are never combined. The old
single-record Retiring form still replays unchanged and is used when it fits.
Live-list rewrites also split retained Retiring transitions and preserve their age.
Older binaries that do not recognize kind 10 cannot read these new catalogues.

An incomplete set has no logical effect during replay, including on high-water
marks. Its resolved frames remain on disk until a known-live catalogue rewrite
omits them. Their physical cost is 57 bytes per part (16-byte frame header plus
41-byte fixed payload), plus 16 + namespace bytes + key bytes per horizon and
8 + namespace bytes + key bytes per expired key. These bytes count in catalogue
size/growth, separately from the chunk-data limit. Before another retirement
attempt the owner forces this rewrite, even with threshold rewrites disabled;
the periodic rewrite also discharges a detected prefix. A failed rewrite keeps
the prefix and its retry requirement, exposes the cause in HEALTH2, and admits
no replacement transition. After a successful rewrite, the next pass can retry
retirement from the unchanged live state. Unresolved append markers still refuse
startup as described below; ignoring resolved partial sets does not bypass that fence.

Selection/catalogue work holds the state and chunk locks; deletion holds the
chunk lock. This is not a separate retirement I/O pool. Current indexed entries
retain shared file handles. Snapshots capture record locations and file handles
without loading payloads; value pages load and pin selected payloads once per
view outside the committed-index lock. Views and active reads can retain those
handles independently of current-index retirement, delaying physical reclamation.
Deleting a name does not establish that every handle or returned copy is gone;
platform sharing rules determine when physical allocation is released. External
handles can also block Windows deletion. A failed delete remains Retiring,
keeps bytes charged,
and is retried on later passes/startup. `retire_blocked` and its age describe
observed failed deletions; an initiation record alone does not prove failure or
completion. A blocked chunk older than one hour sets
`retire_blocked_high_water`. See [read-only inspection](RETIREMENT-INSPECTION.md).

Logical removal and class retirement are distinct from tenant-wide copy cleanup;
see [tenant lifecycle boundaries](TENANT_LIFECYCLE.md) before making a hosted
deletion promise. Class/stripe chunks can contain multiple namespaces.

Eligibility uses UTC close time and, within a run, monotonic elapsed time; both
must allow retirement. Startup pauses new automatic retirement with
`expiry_degraded` if UTC precedes the catalogue's last record or is more than
24 hours beyond the retained heartbeat. Stable ticks for one rotation age, or
`membus expiry resume STATE_DIR`, resolve that pause through a catalogue Resume
record. Already Retiring deletions are retried separately. Clock checks and
maintenance scheduling do not provide a precise expiry deadline.

Recovery also retains each chunk's latest local retry cutoff among its
reconstructed outcomes. Automatic retirement and latest-class pinned pressure
wait strictly past that inclusive cutoff. A backward UTC correction between
admission and chunk close can otherwise make close-time age pass while the
record's recovered outcome remains retryable. The extra cutoff is in memory;
recorded close timestamps are unchanged. Forced retirement
keeps its explicit override, and already Retiring cleanup is unchanged.

A failed Resume publication keeps the pause and any explicit `expiry.resume`
request in place for a later tick. HEALTH2 sets the sticky `storage_error`
observation for either an explicit or automatic resume failure. A fenced
catalogue still requires a successful live-list rewrite before appending Resume;
the request does not bypass that fence. The request file is consumed after
successful publication, or when the clock is already unpaused. The CLI reports
that it requested a resume; it does not wait for the server to publish it.

Replay skips retired data and resumes published retirement. An Open record with
no file can represent a crash before creation and is closed as empty. A missing
closed file without Retiring, or an unaccounted chunk file, sets storage_error
and read-only state. Complete CRC, magic, length/complement or payload corruption
refuses startup, including a complete corrupt final frame. Only short final
headers/bodies in chunks open at the crash may be truncated; a torn closed chunk
is corruption. Empty open chunks can be retired during startup.

After allowed truncation and before exposing recovered values/retry outcomes,
startup synchronizes present, nonempty active chunks, including closed chunks,
and active legacy logs. Recovered Close publication follows that barrier.
A required sync failure refuses startup without publishing a successful Store
or a new Close; it attempts a non-ready report including storage errors already
found. Report publication can fail and an older ready report cannot establish
current availability. [RECOVERY_FLUSH.md](RECOVERY_FLUSH.md) retains the exact
barrier and historical-backup limits; a requested Flush byte alone is not proof
of achieved synchronization.

Before truncation, a flushed replay report records `truncation_pending` with
ready_ms=0. Failure to publish that report refuses the destructive step. Later
readiness replaces the current report; the bounded replay log retains prior
stages within its retention. This establishes process-interruption evidence,
not unlimited history or power-loss durability.

A catalogue append-pending marker refuses startup even when the last frame looks
complete. Recovery requires known retained-state evidence, not deleting the
marker or guessing an append boundary. A running owner can rewrite its known
live list; storage/read-only flags remain sticky. See
[CATALOGUE-APPEND-RECOVERY.md](CATALOGUE-APPEND-RECOVERY.md).

The executable's default 8 GiB data limit reserves 1 GiB from ordinary writes;
smaller limits reserve an eighth (up to 1 GiB), overridable explicitly. Retired
bytes leave accounting only at Retired publication. Tombstones and timer progress
also append. Import backups, catalogue/report files and other sidecars require
separate disk provision. A full store can still read retained values; manually
deleting chunks destroys the recovery account.

## Legacy import

For a legacy-only store, startup reads the old `stripe-N.log` inputs and constructs
`chunks.import`. Before a new import switches or moves backups read-only, it
synchronizes the original legacy logs, covering original retry history as well
as copied latest values. It copies latest records at original versions/timestamps into
selected classes. Optional `import-policies.txt` contains tab-separated
`NAMESPACE`, `PREFIX`, `forever|latest|expire:MILLISECONDS` fields; those policies
are persisted before copying data. Without it, ordinary imported keys use the
forever default. It is a one-time import input, never reapplied on restart.

The import independently replays the replacement and compares latest bytes,
revision, ack and tombstone, namespace live counts, version high-water and retry
projection (including retained legacy dedup). Only a successful comparison permits
the flushed Import record and directory switch. A difference refuses startup,
leaves legacy inputs in place and discards the incomplete replacement. Existing
unaccounted chunk evidence refuses import rather than being assumed empty.

Input sizes and SHA-256 hashes, including the exact optional policy bytes, are
recorded. On Windows the policy input denies write/delete sharing through the
switch. Inputs move unmodified to `legacy-EPOCH` and become read-only; interrupted
moves resume with conflict checks. Startup continues verifying them: changed or
missing backups cause storage_error/read-only. Older import records without a
policy-input entry remain readable with that provenance unknown. Arbitrary state
writers and non-Windows input locking are outside this ownership guarantee.

Within the configured retry window after import (default 24 hours), legacy
records supplement dedup only, never values. The window includes its exact
boundary, including the import instant when the configured window is zero.
Each record must also satisfy its own UTC age limit; recovery gives it only the
remaining local monotonic window. A superseded put or remove retried at the
boundary returns its original outcome without changing the latest value.
Later starts stop that dedup replay
but still verify the retained inputs. These already imported read-only backups
receive no new data-file sync witness; on Windows a read-only handle cannot flush
them. Their existing bytes, permissions and retry grafting rules remain unchanged,
so this barrier is not universal historical recovery-durability evidence. See
[RECOVERY_FLUSH.md](RECOVERY_FLUSH.md). A store already containing catalogued
chunks alongside old logs treats the old logs as a retained forever chunk rather
than importing them again. There is no automatic backup-removal command.

Restoring pre-switch legacy files loses all post-switch writes. Retaining a
backup does not establish a lossless downgrade or compatibility with old timer
and catalogue readers. Stop/wait the writer and qualify any recovery or binary
replacement against the actual retained state.

## Wire diagnostics and available controls

All suffixes below follow the [KV1 common request](KV1.md) fields.

| Opcode | Request suffix | Successful payload |
|---|---|---|
| 24 POLICY_SET | bound prefix; u8 kind (0 forever, 1 latest, 2 expire, 255 clear); u64 retention_ms | u64 version; u8 ack=2 |
| 25 POLICY_GET | empty | u16 count; (bound prefix; u8 kind; u64 retention_ms)* for the namespace |
| 26 GET_STATUS | text key | u8 status (0 live, 1 expired, 2 missing); u64 version (live only, otherwise 0) |
| 27 HEALTH2 | empty | u16 count; (bytes UTF-8 key=value)* |

HEALTH2 exposes state_bytes/limit/high_water, read_only, storage_error,
expiry_degraded, dedup_entries/cap/high_water, keys/index_cap/quota,
namespace_high_water, namespace.N counts, chunks_open/closed, retained_bytes.C
per class and aggregate retained_bytes, pinned_bytes/high_water,
retired_chunks/bytes, retire_blocked,
retire_blocked_oldest_ms and retire_blocked_high_water. State pressure is 75%;
dedup/namespace pressure is 70%; pinned pressure defaults to 64 MiB. Pinned bytes
name latest-class closed chunks past the retry window still holding current
records, not failed deletes.
Lifetime retirement and I/O counters differ from current retained populations.

`replay.json` and bounded `replay.log` record stages, epoch/start/ready/duration,
per-stripe replay counts/bytes/truncations, finished retirement, skipped retired
chunks, horizons, dedup, keys/counts, import, storage errors and refusal.
Heartbeat exposes a smaller diagnostic projection; see [RESIDENT.md](RESIDENT.md).
Health inspection grants no application lease or receiver authority.

Rust controls live in `tests/chunks.rs`, `tests/retirement.rs`, `tests/import.rs`,
`tests/retirement_inspection.rs` and library catalogue/replay tests. They cover
rotation, class routing, expiry/anti-resurrection, key accounting, blocked deletion,
import provenance/comparison and injected recovery failures. These controls do
not establish physical power-loss behavior, a retirement deadline, service
installation compatibility or an application-specific retention policy.

The retirement measurements, kept with the other performance measurements outside this
repository, refer to numbered rationale in the
[earlier retirement document](https://github.com/nestharus/membus/blob/d7ec2b79bb48cc1f5edfa5e6cab8549c8e5d57d5/docs/CHUNK-RETIREMENT.md).
That immutable version preserves the historical comparisons; the behavior and
limits described above correspond to the current implementation.

Chunk IDs retain a representable successor: the last assignable ID is
`u64::MAX - 1`. Exhaustion refuses before publishing an Open, creating a file or
exposing a route, including during import. Runtime allocation failure uses the
existing I/O refusal (16) and write fence; prior Flush values and retry outcomes
remain available across restart. An Open at `u64::MAX` refuses replay before
cleanup or epoch publication. ID zero remains reserved for the legacy aggregate.

Replay validates catalogue identities and lifecycle records before repairing
tails, deleting files, applying retirement suppression or publishing an epoch:

- Every Open has a nonzero, unique ID and a stripe from 0 through 3. A second
  Open is invalid even when identical or separated by Close or Retired records.
- Every Close, committed Retiring and Retired references an earlier Open in the
  retained image. A later Open cannot authorize an earlier transition.
- Close cannot revive a Retiring or Retired chunk or change an established byte
  boundary. An identical repeated boundary retains its next-version high water.
- Committed Retiring requires Closed or Retiring state. A repeated transition
  preserves its original timestamp, effective per-key horizon maxima and expiry
  key set. Reordering and redundant members are allowed; next-version high water
  may advance. Complete multipart sets are compared after folding; incomplete
  prefixes remain unapplied.
- Retired for a Closed chunk requires committed Retiring intent, including for
  zero-byte chunks. An incomplete multipart prefix does not authorize completion.
- Direct Open-to-Retired cleanup requires an absent path or a zero-length regular
  file. Populated files, torn bytes and non-file paths refuse; metadata errors
  other than NotFound propagate. Repeated completions remain valid.

Invalid metadata returns InvalidData and preserves source bytes and the prior
epoch for correction. Atomic live-list rewrites retain each Open before its
lifecycle and preserve retirement/summary ages. Normal recovery deletes an empty
Open file before completing retirement; a leftover zero-length retired file
retains the existing storage-error diagnostic.


New expiry resumes use catalogue kind 11, `ResumeWithBasis`: an explicit request
records `Operator`; automatic recovery records its observed stable-clock start UTC,
local start/observed monotonic readings, required duration and drift tolerance.
The latest resume survives replay and compaction and is available through
`Store::expiry_resume_record()`. Historical kind 8 remains basis unknown; kind 10
continues to encode multipart retirement. Older readers refuse kind 11. Resume
basis describes the initiating route, not operator authentication or a portable
monotonic epoch. Failed publication retains the pause and explicit request.
