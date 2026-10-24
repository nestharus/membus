# Resident operation

The executable serves one explicitly selected state directory and endpoint:

```text
membus STATE_DIR pipe LOCAL_PIPE [OPTIONS]
membus STATE_DIR tcp LOOPBACK_ADDRESS [OPTIONS]
```

`pipe` is available on Windows. `tcp` accepts loopback addresses only and is a
foreground test transport without cross-user authentication. The named-pipe
server uses first-instance protection, rejects remote connections, and sets an
ACL for its current user and LocalSystem. Run with the intended account and
logon/session access; changing the service account changes the current-user SID.
HELLO identity is correlation, not application authentication. Admitted clients
share the resident's local-principal powers.

Shared hosting needs a separate [tenant admission boundary](TENANCY_BOUNDARY.md)
covering every operation and shared resource. The guide describes a proposed
service contract; namespace naming and feature negotiation are not authorization.

Choose persistent storage independently of source checkouts and build output.
`owner.lock` fences concurrent owners: Windows holds a share-denied handle for
the resident lifetime, released on exit. On non-Windows the create-new lock
requires operator cleanup after confirming the previous owner has stopped.
The executable starts timer ownership after successful Store startup, including
its recovered data-file sync barrier, and writes its readiness
message before entering the selected transport. Transport/heartbeat checks are
needed to establish that it can actually serve.

## Options and resource bounds

Byte options use bytes; time options use milliseconds.

| Option | Default | Meaning |
|---|---:|---|
| `--idle-ms` | 60000 | Positive timeout per transport I/O, below u32::MAX |
| `--heartbeat-ms` | 30000 | Replacement interval; zero disables, otherwise at least 100 |
| `--state-limit-bytes` | 8589934592 | Positive retained data-byte admission limit |
| `--maintenance-reserve-bytes` | min(1 GiB, state limit / 8) | Ordinary writes stop below this reserve; must be below the limit |
| `--flush-ms` | 50 | Gather window and spacing for gathered flush work |
| `--retry-ms` | 86400000 | Bounded mutation outcome retention horizon |
| `--quota` | 500000 | Live keys per namespace, including admission reservations |
| `--index-cap` | 917000 | Global index slots, including tombstones and expiry summaries |
| `--capacity` | 536870912 | Managed memory admission budget; not a measured RSS ceiling |
| `--cache-bytes` | 134217728 | Bounded committed-value cache allowance |
| `--queue-bytes` | 67108864 | Charged queued representations, divided across four stripes |
| `--dedup-cap` | 262144 | Active outcomes and bounded retired-ID ring capacity |
| `--snapshot-bytes` | 268435456 | Aggregate conservative retained snapshot charge |
| `--snapshot-max` | 8 | Concurrent retained/in-flight snapshots |
| `--snapshot-ttl-ms` | 30000 | Fixed lifetime from capture entry; reads do not renew it |
| `--signal-history` | 4096 | Retained changes; zero disables history |
| `--signal-bytes` | 16777216 | History representation budget; zero disables history |
| `--chunk-bytes` | 8388608 | Positive chunk rotation size |
| `--rotation-ms` | 3600000 | Age rotation for finite classes |
| `--retire-ms` | 10000 | Retirement pass interval |
| `--allow-mem` | off | Permit explicitly negotiated disposable memory acknowledgments |
| `--delay-ms` | 0 | Artificial append delay used by tests |

The library's `Config::default()` has a zero maintenance reserve; the executable
sets the reserve shown above. At the default state limit, ordinary persistent
admissions stop at 7 GiB. Retired chunk bytes leave accounting only after
`Retired` publication. Failed appends are charged conservatively until replay.
The limit is data accounting, not an all-files disk quota: catalogue, import
backups, epoch, heartbeat and replay reports need separate storage provision.
A tombstone appends and can also refuse at capacity. Raising a limit requires
sufficient disk and memory; deleting data files is not a capacity remedy.

Both transports cap active connections at 64 and reserve an aggregate 64 MiB
of incoming frame bodies before allocation. The reservation lasts through
dispatch. Saturation can return busy and close a connection without a usable
request association. A partial trickling frame can retain a connection and its
reservation: the server idle option is not a whole-frame or request deadline.
Windows overlapped I/O cancellation drains completion before releasing buffers.

Committed payloads remain in chunk files, with a bounded memory cache. Hot reads
use the committed index and a cache shard; misses synchronously read and validate
the indexed record through a separate file handle. Replay rebuilds the index with
an empty cache. Configure cache leases with repeatable `--cache-policy` rules;
[VALUE_CACHE.md](VALUE_CACHE.md) describes their syntax and admission behavior.

Managed memory charges cover indexes, retained retry metadata, pending mutation
buffers, chunk references, signals and pinned snapshots, plus a fixed margin.
The cache yields space before new work is admitted. Queue, snapshot and signal
bounds remain independently enforced. These are conservative resource estimates,
not a calibrated process RSS limit: response encoding, allocator retention, thread
stacks, caller allocations and OS pipe buffers also matter. Snapshot value pages
can load and pin cold payloads beyond the cache allowance under the view's
full-payload reservation; opening a view does not load them.
Comparator compaction does not shrink complete queued mutation representations,
disk records or identifiers. See [RETRY_RETENTION.md](RETRY_RETENTION.md) for
encoding/collision limits and [VALUE_CACHE.md](VALUE_CACHE.md) for memory charges.

## Health and bounded reports

`heartbeat.json` is replaced via `heartbeat.tmp` each interval, without a sync
request. It includes pid, epoch, UTC time, successful/attempted heartbeat writes
and bytes, failures, state bytes/limit, pressure flags and retirement diagnostics.
Failed replacements retry once per interval; the next successful object reports
failures. Logical requested bytes do not measure physical storage I/O.

On Windows:

```text
membus health LOCAL_PIPE --state-dir STATE_DIR
membus retirement-status LOCAL_PIPE
```

Pass `--state-dir` explicitly to select the instance's heartbeat. Health performs
HELLO and HEALTH, checks the Windows pipe server PID against the heartbeat PID,
requires matching epoch and heartbeat age below 90 seconds, and uses two-second
I/O bounds. A disabled heartbeat cannot pass this command. Its success covers
those association and freshness checks; it does not evaluate HEALTH2 degradation
flags or individual timer errors. Freshness alone does not establish transport
health. `retirement-status` projects selected HEALTH2
fields; see [inspection](RETIREMENT-INSPECTION.md).

`state_high_water` is 75% of the full state limit. Dedup and namespace pressure
flags use 70%. Other flags include `read_only`, `storage_error`,
`expiry_degraded`, `pinned_high_water` and `retire_blocked_high_water`.
[Chunk retirement](CHUNK-RETIREMENT.md) describes their meaning and HEALTH2.
The heartbeat does not aggregate timer errors; reconcile individual timer status.

For hosted routing, separate process/endpoint health, write eligibility, pressure
and workflow settlement. See [service readiness](SERVICE_HEALTH.md); the native
health command does not implement that proposed supervisor policy.

Startup writes `replay.json` and `replay.log`, including refusal/truncation
stages. The log is compacted to its last 256 lines when it exceeds 1 MiB.
Startup/fatal diagnostics go to stderr; there is no per-request access log.
The supervisor must bound and rotate its own captures.

## Restart and replacement

Stop and wait for the existing resident before replacing its executable or
inspecting stopped state. Verify the replacement's identity and qualify it
against a retained state copy. Preserve backups independently of the checkout.
A changed epoch invalidates snapshots, subscriptions and resume history; reconcile
uncertain writes and application state after reconnecting.

Restoring or cloning older state can reuse an epoch from an abandoned history.
See [backup and isolated recovery](BACKUP_RECOVERY.md) for complete-state capture,
caller reconciliation, restore generations and cutover boundaries.

Replay reconstructs committed state before service. Before exposing recovered
values or retry outcomes, startup synchronizes present, nonempty active chunks
(including closed ones) and active legacy logs. An on-disk requested Flush byte
alone cannot establish achieved Flush. A required barrier failure refuses startup
and attempts a non-ready report that retains storage observations already found;
report publication itself can fail, so an older ready report is not current
availability evidence. [RECOVERY_FLUSH.md](RECOVERY_FLUSH.md) describes this
ordering and the unchanged historical imported-backup boundary.

Complete corruption refuses
startup; only allowed incomplete open tails are truncated, with evidence written
first. Missing/unaccounted chunk files can start read-only with storage_error.
Unresolved catalogue appends refuse replay and require an independently
established recovery account. See [retirement/replay](CHUNK-RETIREMENT.md) and
[catalogue recovery](CATALOGUE-APPEND-RECOVERY.md). Legacy import is automatic for
a legacy-only store and retains its inputs; restoring those inputs loses every
post-import write. An older executable must not be assumed compatible with
current catalogue markers or timer state.
