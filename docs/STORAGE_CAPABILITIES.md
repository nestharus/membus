# Storage capabilities for remote volumes

This is a proposed adapter contract, not an implemented volume API. Current
`Store::open` selects a filesystem state directory. Separating payload locations
from the memory index/cache does not make arbitrary remote storage interchangeable.
The private `Route` carries append/sink/extent information, but still exposes
file-backed logs. Indexed reads use `Arc<Mutex<File>>`; catalogue, replay,
import, epoch/report publication and retirement also call filesystem APIs.
Replacing Route alone would leave those dependencies behind.

Remote operation access and remote storage are independent choices. A client
can use a remote authenticated service whose resident keeps local storage.
It does not need direct credentials or paths for that service's volumes.

## Separate the storage roles

| Role | Current dependency | Required future contract |
|---|---|---|
| Active append | One append owner per stripe; complete record writes, partial-write fencing, class routing and rotation. | Stable sink/generation identity, ordered append completion and end extent, bounded admission, explicit partial/uncertain outcomes and writer fencing. An object PUT is not this append interface. |
| Durability coverage | Flush waits carry sink identity and complete-record end offset; successful sync/close supplies coverage for that sink. | State exactly which extent, generation and failure domain a completion covers. Unrelated file/object success cannot cover the waiter. Preserve local Os/Flush meaning; negotiate a distinct remote/quorum promise if offered. |
| Indexed range read | A location is class, shared file handle, offset and record length; reads validate MBK1 framing/CRC and expected value metadata. | Immutable source/volume/object generation plus bounded offset/length and declared integrity/visibility. Distinguish absent application state from an unavailable, missing required or corrupt backing object. |
| Catalogue and namespace publication | Framed catalogue appends/markers, file/directory syncs, replacement and import renames, epoch/report ordering. | Atomic/versioned root publication and durable dependency ordering, implemented for the selected backend. Filesystem rename/directory sync and conditional object-manifest publication are different mechanisms. |
| Ownership | State-directory owner lock and local stripe fences. | A single fenced writer for each mutable authority generation, including after timeout, partition, restore and promotion. A mount path or expiring lease alone is insufficient remote fencing. |
| Recovery and reclamation | Catalogue replay, tail repair, retained inputs, retirement/horizons and held-delete retry. | Recover one consistent root and its dependencies; delete only objects no required reader, recovery point, replica or retained backup still needs. Listing or local cache eviction cannot authorize deletion. |

Keep these roles explicit in a future capability record: format/version,
logical volume and generation, read/range limits and consistency, append/partial
completion, extent-sync durability scope, root publication/conditional-update
semantics, fencing enforcement, integrity verification, deletion visibility and
resource limits. Refuse an unsupported required capability before admitting
work. A successful storage SDK call is not a capability negotiation.

Persist logical volume/object identities and generations separately from their
configured location and credentials. Current process-local handles and sink
indices are not portable remote references. A URL or credential rotation must
not change content identity; a reused object name must not silently change the
bytes an old offset denotes. Bind manifests/index entries to the selected format,
generation, extent and integrity evidence. Whole-object digests require the
whole bytes or a qualified block/range verification scheme; a short range cannot
be certified merely by naming an unread whole-object digest.

## Acknowledgment and publication order

Current Os publishes after complete append without deliberately waiting for a
covering data sync. Rotation and metadata work can still synchronize. Current
Flush waits for successful covering sync on the actual sink; a closed sink can
satisfy an older waiter only through its recorded synced extent. Catalogue
barriers, a sync on another file, a cached read or a completed upload queue entry
cannot substitute for that coverage.

A remote filesystem must qualify these operations at the selected storage and
failure boundary, including directory/name behavior and read visibility. A mount
that accepts the API calls does not establish equivalent semantics. Preserve
partial/uncertain outcomes: after timeout, the remote write may still complete.
Fencing and recovery must prevent an old writer's delayed completion from
becoming authoritative after cutover. Keep original mutation identity and
uncertainty; do not blindly retry append or publish under a new identity.

For a first immutable object tier, keep a qualified active journal, then seal
and upload closed immutable chunks outside ordinary KV completion. Verify the
referenced objects at the required durability/visibility, then conditionally
publish the new manifest/root, and only later release superseded dependencies.
Retain the old root until the declared recovery/reader/backup obligations permit
reclamation. Remote-durable acknowledgment must wait for its actual dependency
and root boundary. Local Flush need not wait for off-path export when it promises
only its existing local durability; it must not be advertised as remote survival.
A manifest of earlier closed chunks cannot acknowledge a newer mutation still
present only in the active journal.

Do not translate an unavailable or not-yet-visible required object into an absent
key or discard its index entry. Current wire GET returns refusal 16 on a backing
read error; `Store::try_get` preserves the error. The convenience `Store::get`
collapses it to absence. An adapter should use the fallible path. Likewise,
recovery's missing-file interpretations require the backend's qualified visibility
and publication account, rather than a transient remote not-found observation.

## Preserve the local read path and bound remote work

| Path | Work and latency boundary | Volume consequence |
|---|---|---|
| Warm value-cache hit | Current index read lock, cache-shard lock and required owned payload copy; no volume read, seek, append lock or fsync. A resident client additionally pays local transport/framing. | Keep this path independent of remote metadata lookups, upload progress and cold-request queues. Cache memory and coherence remain separately bounded. |
| Cold filesystem/range read | Current code takes one of two Store-wide cold-read permits, then the shared file-handle mutex for seek/frame reading, followed by decode/copy/cache admission. A remote range adds its request/response and provider buffering. | Remote latency can exhaust the existing permit budget; reads for the same handle also serialize. Declare concurrency, bytes and whole-operation deadlines instead of replacing a local seek with unlimited remote work. |
| Active append and Flush | Serialization/checksums, admission and append work remain; remote active storage adds transfer and remote durability/metadata coordination. | Batching can amortize calls but adds waiting; do not hide that cost behind local receipt names. Separate queued bytes from durable bytes. |
| Seal/upload/manifest/GC | Reads, hashes, transfer, root updates and retained-object bookkeeping. | Batch off-path with bounded backlog, bandwidth, memory and tenant work. Apply explicit admission pressure when full, retain required active data, and keep cached reads independent of the upload queue. |

Concurrency illustrates the design constraint: with two occupied cold-read
permits and a hypothetical 10 ms miss duration, the idealized ceiling is
`2 / 0.010 = 200` misses/s, or 6.25 MiB/s of 32 KiB payloads before framing.
This is arithmetic, not a remote benchmark; serialization, same-handle locking,
CPU, bandwidth and contention can lower it. Increasing concurrency needs matching
memory/byte admission and fairness. A future adapter can separate bounded remote
I/O from bounded decode work and use an immutable chunk/block cache or measured
prefetch, rather than stretch the current two-permit local design unchanged.

Current namespace snapshots capture keys and record locations under a full-payload
reservation, loading and pinning selected values lazily as pages request them.
Startup rebuilds indexes by replaying retained records. A remote tier must budget
snapshot/replay bytes, requests and recovery time as well as individual GETs.
There is no replay index checkpoint or transparent object tier. Cache keys and restored authority
still need the [client-cache](CLIENT_CACHE.md) and
[restore-lineage](BACKUP_RECOVERY.md) contracts.

## Qualification and selection

Start with remote service access over qualified local storage when that meets
placement needs. Qualify a remote filesystem only against its complete file and
ownership contract. Add immutable remote chunks when measured cold-read,
recovery, retained-byte and request/egress economics justify the work. Keep
hot data/immutable blocks local where locality pays. Exact backend traits and
formats can wait until implementation; logical identities and distinct durability
promises should be preserved now.

Qualification needs partial/late writes, extent coverage across rotation,
missing/stale reads, corrupt ranges, root-update conflicts, file/name barrier
failure, stale-writer completion, interrupted import/recovery, held deletion,
reader/backup pins, outage/reconnect and bounded-backlog recovery. Measure hot
and miss p50/p99, accepted operations/payload bytes, remote request counts,
buffer/queue peaks, upload lag and restart/capture costs at the offered durability.
Current grouped/chunk/cache controls exercise local primitives, not these remote
contracts, a provider's power-loss behavior or a sustained cloud capacity claim.

See [grouped Flush](GROUPED-FLUSH.md), [directory barriers](RECOVERY-DIRECTORIES.md)
and [recovered Flush](RECOVERY_FLUSH.md) for the current ordering that a storage
change must preserve or explicitly replace with a qualified contract.
