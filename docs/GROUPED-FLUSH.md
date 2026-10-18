# Grouped append and flush

Four append workers drain bounded key-hash stripe queues. Current storage routes
each stripe's writes into chunk files by retention class. One flush worker
coordinates covering extents per sink. [KV1.md](KV1.md) describes admission,
visibility and uncertainty; [CHUNK-RETIREMENT.md](CHUNK-RETIREMENT.md) describes
rotation and retained-byte accounting.

Admission charges `fingerprint length + value length + 128` bytes per queued
mutation, at most 1024 outstanding items and `queue_bytes / 4` charged bytes per
stripe, within `queue_bytes` globally. Saturation returns busy (6) before pending
key, version or retry identity insertion. Empty values still consume resources.
A record larger than the configured stripe queue can repeatedly refuse busy;
retry alone cannot fix the configuration. Durable writes reserve their exact
encoded data length under the ordinary state ceiling before acceptance.

OS writes publish after complete append. Flush writes publish after a sync
covers their complete-record end offset. OS appends do not deliberately wait for
a flush gather window, although storage, routing, rotation and scheduling can
still delay them. The append lock is released before gathering and syncing.

A flush notice arriving with no other outstanding flush waiter is classified
idle-immediate and bypasses gathering and interval spacing. A notice arriving
while another waiter is queued or syncing retains the gather policy. Its first
worker receipt opens a `flush_ms` window; retained groups use interval spacing
after the previous sync attempt. Selection initially takes one sink and up to
64 waiters or 144192 record bytes, permitting one larger record. Sync covers all
complete appends before its captured extent, so more than 64 known covered
waiters can finish together. The actual cohort remains bounded by admitted
stripe items and bytes. A late notice for an already covered extent reuses the
successful coverage; a newer extent needs another sync.

The default `flush_ms=50` is configurable, without an optimum or throughput
claim. It imposes no global 20-sync/second ceiling: idle serial writes can sync
once per uncovered row. Concurrent low-rate writers can overlap and enter the
full gather path. Window time is additional to scheduling and device latency;
neither logical record bytes nor a selection budget bounds physical flush bytes.

Batch validates the full request before item admission and remains non-atomic.
Distinct keys overlap through at most 32 scoped helpers; same-key operations and
reply entries retain input order. Only one batch executes per Store; another
batch refuses busy before admitting its items. Individual mutations and reads
do not take the batch gate. Cross-key revision assignment is not ordered by
input position.

A partial append fences its stripe so later records cannot follow a torn tail.
Sync failure fences that stripe's flush work; late waiters receive uncertain
I/O failure (16), and new durable admissions become read-only (10). A complete
record left by failed I/O can still replay. Preserve uncertainty and stable
operation identity. Committed GET reads use memory without append/flush locks
or file I/O; HEALTH and metrics can wait for a log lock. Store teardown joins
its durable workers.

## Diagnostics and test controls

Opcode 9 IO_METRICS has the common deadline/operation ID/namespace prefix and no
suffix. Its reply contains three little-endian u64 values: current-process
logical data-log append bytes, actual data-log `File::write` calls (including
interrupted attempts), and accounted data-log syncs. The sync counter includes
flush-worker attempts and syncs of successfully closed and published chunks.
It omits failed close syncs and data syncs followed by failed catalogue Close
publication. Partial append bytes count even on uncertain outcomes. Epoch,
heartbeat, catalogue marker/frame/rewrite I/O, replay reporting, replay reads and
transport I/O are excluded. These counters cannot establish all-storage sync
cost or physical-device utilization. Older servers return unsupported (2).

`tests/grouped.rs` covers covering groups, hot reads, queue/disk bounds, batch
ordering and exclusion, delayed notices and sync failure. Library tests cover
idle-immediate and overlapping notice classification and reuse of covered
extents. These are available controls, not a performance certification.

`MEMBUS_TEST_PAUSE_PARTIAL` and `MEMBUS_TEST_PAUSE_SYNC` are path-valued pause
seams; leave them unset during operation. The latter marks worker entry before
sync. The optional Cargo feature `sync-witness` brackets actual Windows
FlushFileBuffers with entering/returned markers selected by
`MEMBUS_SYNC_WITNESS`. Ordinary releases omit that feature's marker code.
User-space brackets do not prove kernel interruption, dirty-page state or
power-loss survival. Historical measurements and policy comparisons are
available through Git history; they are not current latency guarantees.
