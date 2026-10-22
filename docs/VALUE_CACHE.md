# Value cache and disk reads

Committed values live in the existing immutable chunk records. The resident index
holds each key's version, acknowledgement, length, retention class and record
location. Entries in one chunk share a read handle. A cache miss reads and checks
the original MBK1 frame, including its CRC. Eviction never appends, flushes,
rewrites, compacts or retires a record.

`membus.kv/1` and the chunk formats are unchanged. GET and GET_MANY synchronously
resolve cache misses; a failed disk read returns the existing error 16 and raises
`storage_error`. Library callers can use `Store::try_get` to distinguish an I/O
error from absence; the compatibility `get` method returns `None` on either.
GET_STATUS consults the index and expiry summaries without loading a value.
Hot reads take an index read lock and a cache shard lock, with no append or stripe
lock. Cold reads use separate read handles and at most two concurrent decoding
operations. The filesystem may satisfy a cold application read from its own cache.

Replay checks records one at a time and rebuilds locations and retry comparators.
It starts with an empty value cache. Timer schedules and retention policies are
decoded into their bounded control structures. The one-time legacy import also
retains locations, comparing the imported state one value at a time before
publishing the imported chunks.

## Lease policy

Every eligible OS-acknowledged or Flush-acknowledged write receives an initial
cache lease. Mem-only writes remain speculative and never enter the committed
cache. An entry larger than its shard allowance bypasses admission; the durable
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
`cache_bytes` defaults to 128 MiB. Sixteen cache shards divide that allowance
equally, with an independent LRU and 256-slot expiry wheel per shard. Expiry
links are replaced on promotion, so repeated reads do not accumulate wheel
entries. The maintenance tick sweeps expiry independently of traffic; reads
enforce the exact deadline even between ticks. No background cache thread is
created.

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

Snapshots preserve the established immutable value semantics: capture explicitly
loads missing values under its full-payload byte reservation and retains their
Arcs until release/expiry and the last reader. Cache eviction does not release a
snapshot's reservation or remove its values. New reads after chunk retirement
consult the updated index and return the existing expired/missing status.

`Store::cache_stats`, `Health2::cache` and HEALTH2's additive fields report
`cache_bytes`, `cache_budget`, the current `cache_limit`, entries, hits, misses,
cold reads, evictions by budget/expiry/replacement/retirement, and
`cache_class.LABEL.bytes` / `.entries`. HEALTH2 also reports `memory_bytes` and
`memory_budget`. Per-class totals include only current cache entries; snapshots
and change history are separately charged retained consumers. The compact
heartbeat and legacy HEALTH layouts stay unchanged.

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
