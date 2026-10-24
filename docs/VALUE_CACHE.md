# Value cache and disk reads

Committed values live in the existing immutable chunk records. The resident index
holds each key's version, acknowledgement, length, retention class and record
location. Entries in one chunk share a read handle. A cache miss reads and checks
the original MBK1 frame, including its CRC, exact indexed frame length,
namespace, key, revision, acknowledgement and payload length. Eviction never appends, flushes,
rewrites, compacts or retires a record.

`membus.kv/1` and the chunk formats are unchanged. GET and GET_MANY synchronously
resolve cache misses; a failed disk read returns the existing error 16 and raises
`storage_error`. Library callers can use `Store::try_get` to distinguish an I/O
error from absence; the compatibility `get` method returns `None` on either.
GET_STATUS consults the index and expiry summaries without loading a value.
Hot reads take an index read lock and a cache shard lock, with no append or stripe
lock. Cold reads use separate read handles and at most two concurrent decoding
operations. Each chunk's cursor lock covers only seek and frame capture; checksum
validation runs after releasing it. The filesystem may satisfy a cold application
read from its own cache.

Eligible cold misses take one of 64 fixed revision load gates and recheck the
resident cache before decoding. Hot hits bypass those gates. Disabled caching,
never-cache, zero active TTL and values whose indexed payload plus label/entry
charge exceeds the configured maximum bypass load coordination. A preceding
loader cannot retain such a value for a waiter. Actual admission likewise returns
before its mutex when the allocated charge exceeds a fixed caller or configured
ceiling. Potentially retainable values still coordinate and recheck the current
allowance under that mutex, including during temporary pressure.

Captured revisions already invalidated by replacement, deletion or retirement
also bypass final admission coordination. Their successful payloads remain
pinned under the snapshot's existing reservation. Eligibility is still rechecked
under the destination shard lock before publishing, so invalidation during
admission cannot restore an obsolete revision to the live cache. Other cold-read
locks and pending allowance recalculation can still delay a snapshot load.

GET and GET_MANY preflight complete reply sizes from immutable index entries
before payload reads or cache changes. Fitting committed reads and snapshot pages
encode directly into their final response buffer. Request and cold-record
identifiers are borrowed during validation; retained operations take owned copies.

Snapshot scans also borrow validated UTF-8 bounds and their length-prefixed request
bytes during dispatch. Continuations bind the opcode and those exact encoded
bounds, and are written and authenticated directly in the final reply buffer.
This avoids temporary bound, query and continuation buffers while preserving
the wire layout, byte budgets and cursor validation. Request slices do not escape
dispatch.

Replay checks records one at a time and rebuilds locations and retry comparators.
It starts with an empty value cache. Timer schedules and retention policies are
decoded into their bounded control structures. The one-time legacy import also
retains locations, comparing the imported state one value at a time before
publishing the imported chunks.

## Lease policy

Every eligible OS-acknowledged or Flush-acknowledged write receives an initial
cache lease. Mem-only writes remain speculative and never enter the committed
cache. An entry larger than the shared allowance bypasses admission; the durable
write still succeeds. Tombstones do not retain payloads in the cache.

TTL values are milliseconds on the monotonic clock:

| Namespace default | Initial TTL | Active TTL |
|---|---:|---:|
| ledger | 1,000 | 3,600,000 |
| metrics | 500 | 300,000 |
| logs | 50 | 10,000 |
| evals | 50 | 10,000 |
| other | 1,000 | 60,000 |

The first read strictly before the initial deadline promotes the entry once,
setting its deadline to that read's time plus `active_ttl`. Further reads update
LRU recency without moving the deadline. An untouched initial lease expires to
disk. A cold load enters with the active TTL. `refresh_on_read=false` disables
initial promotion. `never_cache=true` preserves the initial readback window,
disables promotion, and prevents subsequent cold loads from being retained.
A zero TTL means immediate expiry/bypass for that lease.

`Config::cache_policy` supplies the fallback. `Config::cache_rules` overrides it
by namespace, key prefix and/or retention class; the last matching rule wins.
Each rule has a reporting label. A record's immutable chunk class determines its
class selector even after a retention policy change. Supply up to 64 rules.

The resident accepts `--cache-bytes N` and repeatable rules:

```text
--cache-policy NS,PREFIX,CLASS,LABEL,INITIAL_MS,ACTIVE_MS,REFRESH,NEVER
--cache-policy logs,,*,logs,10,5000,true,false
--cache-policy archive,,*,archive,20,0,false,true
--cache-policy *,event/,expire:86400000,events,25,10000,true,false
```

`*` selects any namespace or retention class. Classes are `forever`, `latest`,
or `expire:MS`. Boolean fields are `true` or `false`. These CLI fields cannot
contain commas; the library configuration supports arbitrary valid identifiers.
Policies are supplied on each startup and do not change durable retention.

## Bounds and observation

`Config::capacity` / `--capacity` defaults to a 512 MiB admission budget.
`cache_bytes` defaults to 128 MiB. Sixteen independently locked LRU/expiry shards
share that allowance, so a value can use idle space in other shards. Admissions
and allowance reductions serialize; hot hits and invalidations keep their shard
locks. Under pressure, expired entries across shards yield first, then live LRU
entries in the destination and successive shards. This is per-shard recency.

Each shard has a 256-slot expiry wheel. Bucket deadline hints skip future leases;
promotion replaces its link. The maintenance tick sweeps expiry independently of
traffic, and reads enforce exact deadlines between ticks. No background cache
thread is created. Per-label residency totals update on admission/removal, so
health reads enumerate labels rather than every resident value.

Cache charges include payload capacity, labels and conservative entry/map/LRU/
wheel overhead. Admission also charges latest-key indexes, retained chunk key
references, retry identities and comparators (including expired retry IDs),
pending serialization buffers, signal retention/delivery, and pinned snapshots.
A separate margin reserves the smaller of one eighth of capacity and 32 MiB for
fixed structures, transient reads and allocator overhead. The cache yields space
before new work is admitted. Full requests may return capacity (7) or snapshot
full (22). Existing queue item/byte limits remain independently enforced.
Change history's byte ceiling is the smaller of its configured limit and one
eighth of process capacity; the effective value is advertised by subscription
negotiation. Library-returned values are caller-owned allocations.

The charges are conservative managed-resource estimates, not an operating-system
RSS limit. Allocator retention, thread stacks and caller allocations still need
measurement for a deployment, especially when scaling test budgets below the
runtime's fixed overhead. The benchmark example reports Windows process private
bytes alongside read latency.

Snapshots seek their namespace in the ordered latest-key index and capture keys
and immutable record locations under the existing full-payload reservation.
Opening a view, reading keys and refusing an oversized page load no payloads.
Selected values load through the revision cache and are pinned once per view;
repeated pages reuse those Arcs, including with caching disabled or after a key
changes. A per-row gate coordinates first loads without retaining errors. A
selected load failure returns error 16 and raises `storage_error`.

Capturing another view releases the registry mutex before waiting on the index,
so admitted views can still page or release. Reservation limits are rechecked
before admission. Release/TTL expiry removes registry retention; the last reader
returns the charge and file handles. Old snapshot revisions survive replacement
and retirement but cannot repopulate an invalidated cache entry. Their handles
can delay physical reclamation of retired chunk files.

Returning a snapshot charge marks the live cache allowance for recalculation.
The next successful cold load recomputes it under state admission before cache
insertion, including newer reservations. Hot hits add no lock. Cache eviction
does not return snapshot reservations, and release while a reader retains the
view does not return capacity yet.

`Store::cache_stats`, `Health2::cache` and HEALTH2's additive fields report
`cache_bytes`, `cache_budget`, the current `cache_limit`, entries, hits, misses,
cold reads, coalesced reads, evictions by budget/expiry/replacement/retirement, and
`cache_class.LABEL.bytes` / `.entries`. HEALTH2 also reports `memory_bytes` and
`memory_budget`. Per-class totals include only current cache entries; snapshots
and change history are separately charged retained consumers. The compact
heartbeat and legacy HEALTH layouts stay unchanged.

Hits/misses count one initial resident-cache observation per read attempt.
`coalesced_reads` / HEALTH2 `cache_coalesced_reads` counts initial misses satisfied
by the gated recheck; it does not inflate initial hits. A page using an already
pinned snapshot value does not consult the resident cache. `cold_reads` counts
application frame loads, including failures, rather than physical-device reads.

## Reproduce the measurements

`cargo test --offline --locked --test value_cache` exercises budgets, leases,
expiry-wheel rotations, class selection, never-cache, concurrent eviction,
retirement, failed writes, replay and disk errors. Tests use authored values and
the existing manual monotonic clock.

`cargo run --release --offline --locked --example cache_measure -- FIXTURE_DIR`
creates an authored 128 MiB working set, advances its manual clock past initial
expiry, warms a 1 MiB subset before timing hot reads, reopens the store, and times
first reads and warmed reads. Use a fresh owned fixture directory each time and
remove it after retaining the numeric output. The example uses APIs available
before cache implementation so both versions can run the same source and work
set. It reports application-cold reads after replay, with no claim of cold OS or
device caches.
