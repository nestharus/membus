# Recovery directory durability

These are filesystem publication prerequisites. The proposed
[storage capability contract](STORAGE_CAPABILITIES.md) distinguishes them from
object-manifest publication; arbitrary remote storage is not an existing adapter.

## State directory creation

Before opening the owner lock or starting replay, startup finds the nearest
existing directory in the requested state path. It synchronizes that directory's
parent, then creates each missing descendant in order, synchronizing its parent
before creating the next descendant. The existing-name barrier also runs when
the state directory already exists: a previous failed attempt may have created
that name without completing its parent barrier. A filesystem root has no parent
entry to synchronize. Relative paths are resolved against the current directory.

A failed barrier returns the original I/O error before ownership, migration,
repair, epoch publication or worker creation. Already created directories remain;
the next attempt repeats the last uncovered name's barrier before continuing.
There is no refusal-report write before ownership. An existing report remains a
historical observation, not evidence that this attempt opened a Store. Startup
requires access to synchronize the containing directory, including for an
already-existing state directory. This does not synchronize every ancestor of a
preexisting caller-provided directory tree.

## Recovery publication

Recovery synchronizes each rename's containing directory before performing the
next dependent step. Synchronizing a file alone is not a directory publication
barrier.

Catalogue marker removal has the same namespace prerequisite. Successful append,
completed boundary rollback and live-list rewrite synchronize the containing
directory after unlinking `catalogue.log.append-pending`, before publishing
success or clearing the live fence. On directory failure the writer attempts to
restore and flush the exact original marker and its directory name, then returns
the failure. Further appends stay fenced; append accounting and the successful
rewrite baseline do not advance. A second restoration failure is reported alongside
the original; it does not establish marker durability after power loss. A later known-live-list
rewrite can retry. See [catalogue append recovery](CATALOGUE-APPEND-RECOVERY.md)
for the refusal and qualification limits.

Retirement completes its namespace work before publishing reclaimed bytes.
After removing a chunk file, the live pass synchronizes the containing class
directory before appending the Retired record or releasing that chunk's byte
charge. Retry follows the same order even when the file is already absent:
absence alone does not establish that an earlier delete barrier completed.
A held or denied delete stays Retiring without reaching the directory barrier.

A directory-sync failure returns the error, retains Retiring responsibility
and sets the existing storage-error observation. It does not publish that
chunk's Retired record or reclaim its byte charge. Successful chunks earlier
in the pass keep their completed publication and reclamation. A later retry
can complete the absent file's barrier, with the storage-error observation
remaining sticky. Already durable retirement intent, horizons and expiry
effects are not rolled back by a later deletion failure.

Startup completion follows the same deletion-before-barrier-before-Retired
order for interrupted Retiring chunks and empty Open cleanup. An already
absent file still requires the barrier. When an interrupted Open never created
its class directory, startup synchronizes the existing `chunks` directory to
cover that absence; it does not try to open a nonexistent class directory as
the barrier. Required directory failures refuse startup before the failed
chunk's Retired record and before epoch/ready publication, retaining collected
observations. A completed earlier retirement remains completed; a later start
can finish the remainder. Held-delete handling retains its existing behavior.

Live chunk creation has a matching namespace prerequisite. The router first
flushes the Open catalogue record, reserves its sequence, creates the class
directory and prepares the empty file's writer and reader handles. It then
synchronizes the class directory, covering the new chunk name, and `chunks`,
covering a newly created class directory. Both barriers precede publication of
the route or any first-frame write. Every new chunk, including rotation into
an existing class directory, follows this order. A later Flush acknowledgement
still requires the ordinary data-file synchronization; directory synchronization
does not replace it.

Either name-barrier failure returns the original OS error from route creation.
The ordinary append-error path fences the stripe and refuses the mutation;
it does not publish a value, a successful receipt or a route for that file.
The flushed Open record and empty file remain as restart evidence, and the
reserved catalogue sequence is not reused. Startup handles that interrupted
Open normally; a later mutation can retry in a new chunk. After a failed
rotation, the earlier flushed value remains readable and recoverable.

Startup also establishes file-name durability for state written by an older
writer. After synchronizing all required data files, it synchronizes each
distinct parent class directory of the present, nonempty Open or Closed
chunks. This precedes catalogue open/name publication, recovered Close records
and the epoch. The later `chunks` barrier covers the class-directory names.
A failure refuses startup with collected observations and no new Close or
epoch; a later open can retry. It does not undo earlier reported tail truncation.
Missing, retiring and retired chunks are outside this active-file prerequisite;
retirement completion has the separate deletion barriers above. Historical
read-only import backups remain outside the active-chunk prerequisite.

Before the import's directory switch, startup also synchronizes the constructed
replacement tree. After all data files and their Close records are synced, the
replacement is independently replayed and checked for equivalence, and the
Import catalogue record is synced. Startup then synchronizes each distinct
class directory, followed by `chunks.import` itself. This covers the chunk file
names, class directory names and catalogue name before either swap rename or any
legacy input move. A catalogue-only import still synchronizes `chunks.import`.

Failure at one of these construction barriers discards the unpublished
replacement and refuses startup. The original chunks directory, legacy input
bytes/permissions and epoch remain unchanged, so another startup can rebuild
and retry. The directory operations use the same helper and OS error handling
as the rename barriers below.

Ordinary startup also synchronizes `chunks` after opening its catalogue, which
may create `catalogue.log`. This precedes recovered Close/retirement records,
epoch publication and runtime assembly. A failure refuses startup with collected
replay observations and `ready_ms=0`. A fresh refusal consumes no epoch; after
the barrier succeeds, the normal epoch/root barrier still covers the `chunks`
directory entry in the state root.

Replay evidence has its own publication barriers. After flushing and renaming
`replay.json.tmp` to `replay.json`, the writer synchronizes the state directory
before changing history. When history exceeds its size threshold, the retained
`replay.log.tmp` image is flushed and renamed, then the state directory is
synchronized before the new report line is appended. After that line is flushed,
the directory is synchronized again, covering a newly created `replay.log` name.
The public report writer and startup's pending, refused and ready reports use
the same ordering. Original OS errors propagate; a refusal report remains a
best-effort diagnostic and cannot replace the startup failure that caused it.

A pending-report barrier failure stops recovery before either data or catalogue
tail truncation and before epoch publication. Already completed report renames
or appends are retained, so a visible report is not proof that all its barriers
completed. A later startup can retry; history may contain a repeated report
after a late failure. These barriers do not flush the parent of the state root
or establish physical power-loss behavior for a particular storage device.

Before publishing the ready report, startup places all running workers in their
Store owner. A report error therefore runs the normal stop-and-join sequence
before returning the error and releasing the state-directory lock. The epoch
may already have been published; a subsequent successful open uses the next
epoch. A visible ready JSON after a late publication failure does not establish
that a Store was returned or that a resident is serving requests.

| Rename | Barrier before continuing |
| --- | --- |
| `chunks` to `chunks.pre-import-<epoch>` | State root, before publishing the replacement `chunks`. |
| `chunks.import` to `chunks` | State root, before releasing the policy input guard and moving legacy inputs. |
| Each legacy input into `legacy-<epoch>` | Backup directory, then state root, before setting the input read-only or moving the next one. |
| `epoch.tmp` to `epoch` | State root, after the epoch file's own sync and before exposing reconstructed state or a ready report. |

An unsuccessful directory barrier returns an error. Startup refuses; it does not
continue to a ready Store or undo an already completed rename. An epoch barrier
failure also writes a refused replay report with `ready_ms=0`. A later start
retries backup directory barriers even if every original input name is already
absent: absence alone cannot establish that an earlier attempt completed its
barriers. Existing backup names are rechecked and made read-only after the
barriers, while the catalogue continues to identify the exact imported inputs.

On Unix, `log_io::sync_dir` opens the directory and calls `sync_all`. On Windows,
it opens the directory with `FILE_FLAG_BACKUP_SEMANTICS`, `GENERIC_WRITE`, and
read/write/delete sharing, then calls `FlushFileBuffers` on that handle.
Microsoft's [CreateFileW directory-handle documentation](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew)
requires the backup-semantics flag for opening directories. Its caching section
also identifies rename operations as metadata changes subject to flushing.
[FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)
requires a handle with write access and reports failure through its return value
and `GetLastError`. These requirements motivate an actual barrier, rather than
assuming that NTFS journaling makes directory flushing unnecessary. Unsupported
directory flushes and access denials propagate; they are never treated as a
successful no-op. Storage hardware and filesystems still determine actual media
persistence. These barriers cover live chunk creation and retirement, active class
directories and retirement completion at restart, replacement-tree creation during
import, the startup catalogue and report namespaces, and the named recovery renames.
They do not cover every creation, removal or rename elsewhere in the store.

Tests in `src/replay/durability_tests.rs` observe filesystem state at each barrier,
inject failures, and verify both refusal and a subsequent successful retry.
The hook is per-Config and compiled only for unit tests. Normal builds contain
neither a global hook nor an environment-controlled bypass.
The construction controls check complete catalogue records and unchanged inputs
at the class/root barriers, fail each barrier before the switch, verify refusal
and successful retry, and cover a catalogue-only replacement. An equivalence
refusal must not reach any of those publication barriers. These checks establish
call/failure ordering, not physical power-loss behavior.
Catalogue controls refuse both a fresh startup and a recovered open chunk before
Close/epoch publication, preserve an already collected storage observation, and
retry successfully with the same next epoch.

`src/routing/directory_tests.rs` observes live creation barriers with the new
file still empty and no published route, rejects either failure, checks new
class creation and repeated rotation, retains the prior flushed value on failed
rotation and tests restart retry without sequence reuse. Its observer is
per-Config and compiled only for unit tests. The recovery controls also observe
both open and already-closed active classes, retain collected file observations
on either class-directory failure and stop before any name barrier when data
sync fails.

The synthetic policy operation remains caller-scoped: `policy_key` embeds the
caller namespace in the key, and `dedup_identity` projects a policy mutation back
to that caller. Local tests force collisions with the unsalted and first salted
import IDs and require the next salt, while preserving both original outcomes.
The policy and timer control namespaces are refused as policy callers. No
production identity rule changed.
