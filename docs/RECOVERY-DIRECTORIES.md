# Recovery directory durability

Recovery synchronizes each rename's containing directory before performing the
next dependent step. Synchronizing a file alone is not a directory publication
barrier.

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
persistence. These barriers cover replacement-tree creation during import, the
startup catalogue namespace and the named recovery renames, not every creation
or rename elsewhere in the store.

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

The synthetic policy operation remains caller-scoped: `policy_key` embeds the
caller namespace in the key, and `dedup_identity` projects a policy mutation back
to that caller. Local tests force collisions with the unsalted and first salted
import IDs and require the next salt, while preserving both original outcomes.
The policy and timer control namespaces are refused as policy callers. No
production identity rule changed.
