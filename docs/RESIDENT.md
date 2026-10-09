# Resident membus (kv/1)

F-E6-21 supplies the executable and checks; lane 88 owns service registration.
The first client is lane 83's optional shadow mirror: its fsynced file ledger
stays primary, conditional_put(row/<file seq>, absent) uses explicit os ack,
and its own cursor catches up after outages. No primary adoption is implied.

## Installation and command

Install a verified release binary at `D:/projects/membus/bin/membus.exe`, outside
worktrees. Stage a replacement as `membus.next.exe`, verify its sha256 against
the build record, stop through `fleet services`, replace only while stopped,
then restart and check a fresh heartbeat and kv/1 hello. Keep the previous
binary as `membus.previous.exe` until replay and readback pass; rollback uses
the same stopped replacement. Do not replace a running executable or state.

Working directory: `D:/projects/membus/`. State: `D:/projects/membus/state/`.
Exact intended command (the pipe argument is one literal argument):

```
D:/projects/membus/bin/membus.exe D:/projects/membus/state pipe \\.\pipe\LOCAL\membus-kv1 --idle-ms 60000 --heartbeat-ms 30000 --state-limit-bytes 8589934592
```

No third-party crate is added. Build and tests use host cargo offline/locked
through lineup, cpu/memory and disk:d slots. The new CLI controls leave kv/1.0
framing and refusal codes intact. `--idle-ms` is a positive per-I/O timeout
(default 60000): a pipe or TCP client stalled in a read or write is closed,
including a client that sends only part of a frame. A continuing trickle can
keep a connection live; this is an idle bound, not a whole-request deadline.
Pipe I/O uses overlapped operations, cancellation and completion drainage.
An incomplete trickling frame keeps its ingress reservation as well as its
connection slot; the aggregate64MiB frame budget remains bounded and other
clients can receive busy6. A total-frame deadline is a later policy choice.
The current-user/LocalSystem ACL, local-only endpoint and 64-client cap remain.
The service must run as xteam in the same user/logon session as its clients;
running as SYSTEM changes the current-user SID and would exclude user clients.
Python's existing reconnect and epoch reconciliation require no wire change.

## Startup, health and bounded output

The existing share-denied owner.lock fences another live owner. A killed
predecessor releases handles (including its pipe); startup replay truncates
only a short final header/incomplete final body, rejects complete corruption,
then creates a new epoch. Clients reconcile deterministic keys on epoch change
before retrying an uncertain send. os and flush acknowledgements survive a
server process kill; os is not a power-loss promise. mem requires explicit opt-in.

`heartbeat.json` is a single bounded JSON object in state, replaced every
`--heartbeat-ms` milliseconds (default30000, minimum100; zero disables it).
It contains pid, epoch, unix_ms, heartbeat_writes and heartbeat_bytes counters,
plus heartbeat_attempts, heartbeat_bytes_attempted and heartbeat_failures.
`heartbeat.tmp` is bounded to one object too. Each successful replacement
counts one successful replacement; no sync is requested. Each attempted temp
write is counted too, and failures appear in the next successful object. Bytes
attempted bound the requested temp-write bytes, including failed/partial writes;
they do not claim physical I/O. A persistent error retries once per interval,
then becomes stale health. With an object below512bytes this costs below1024
logical requested bytes/minute at the default interval during successful operation,
plus filesystem metadata; physical-device bytes are not inferred. There is
no per-request log. stdout/stderr have just startup/fatal messages; lane88's
launcher must rotate/cap captures at1MiB with one prior file. Health check:
fresh heartbeat (age<90s), matching live pid and successful kv/1 hello/health;
heartbeat freshness alone is not a proof the transport can serve.
Native `membus.exe health PIPE [--state-dir PATH]` (state defaults to the resident
state path above) exits0 only after kv/1 hello+health, with the pipe server pid
queried through Windows, matching heartbeat pid and hello epoch, age<90s.
It uses2s per-I/O timeouts and emits a single success line; failures exit nonzero.
Busy pipe instance creation retries for at most2s. Native health requires the
heartbeat to be enabled; `--heartbeat-ms 0` is for foreground tests only.

## State growth and upgrades

Until rotation/tiering exists, the four stripe logs only grow. Default
`--state-limit-bytes 8589934592` caps their combined logical record bytes;
replay counts existing lengths, and exact new record reservations refuse
capacity(code7) before acceptance at the cap. Reads and heartbeat continue;
do not delete stripe files to regain space. Raise the provisioned cap only
after disk accounting or migrate an instance; zero is invalid. This bound
excludes fixed owner.lock and heartbeat files, filesystem metadata and logs
owned by the launcher. Existing state above the cap remains readable and
refuses further persistent appends. Memory caps remain independent.
Heartbeat also reads the in-memory replay+append counter `state_bytes`, names
`state_limit_bytes`, and warns `state_high_water` at75% (6GiB of the default8GiB).
This causes no drive reads. These fields are diagnostic local health readings;
the existing kv/1 health packet is unchanged and its lifetime write counter
still excludes pre-restart bytes. Operators poll the heartbeat before the cap.
`state_bytes` conservatively charges the complete attempted frame after an
uncertain append until replay recounts actual valid bytes; pending reservations
are excluded from the diagnostic value. At the cap, persistent removes also
refuse since tombstones append and cannot reclaim log space. The8GiB cap applies
to this unrotated stripe-log slice. PLAN's later1GiB maintenance reserve/7GiB
ordinary ceiling must be provisioned before a maintenance/rotation rollout;
this slice supplies neither snapshots nor a reserved maintenance tier.
No automatic capacity-pressure alert consumer is wired yet; the heartbeat
field is available to local operators and its reporting policy is an owner
question recorded in the Act evidence. The cap itself is enforced immediately.

A wire minor/major change needs hello negotiation and documented compatibility;
an incompatible client receives incompatible_version. An on-disk format change
requires an explicit migration/backup plan before replacing the binary. This
slice retains MBK1 and kv/1.0, so replay uses existing state without migration.
Lane88's eco seq452 answer selects task `Oulipoly membus`, xteam logon plus
1-minute repeats, IgnoreNew, below-normal priority, unlimited duration. The
minute repeat restarts after exit; Task Scheduler RestartOnFailure concerns
start failures and is deliberately omitted. Lane88's pythonw resident launcher
owns the1MiB+one-prior capture. Registration waits for verified installed binary
hash+commit and the native health command; the final Act report records whether
that registration and fleet restart test happened.

## Plan basis and verification

Read against KV1 sections6,8,9, PLAN section12 O19 and lane83 review
`membus-interface-review/review.md:145-149`. Design record
`D:/dev/handoff/memory-ecosystem.md` sha256
`074d22267522e57ad4ed32e1d95aef64833358a41fc9908e1bf958d99d2ffcb6`:
Rust throughout; drive pressure is the biggest risk; cache eviction never
rewrites data; avoid hard coupling. Survey
`D:/dev/handoff/fast-local-store-survey.md` sha256
`b128d2588930d54819fceae4047299e4591535763f7048d89c4cf31a9fcfd10f`.

Pass conditions, stated before runs: host cargo tests pass; named-pipe partial
frame idle client is closed within the configured timeout plus scheduling
tolerance; os/flush-acknowledged keys replay equally after kill and a torn tail;
epoch changes; heartbeat counter and bytes agree with bounded-file updates;
state cap refuses before acceptance and preserves old reads. Raw small outputs
and co-tenant lineup/process context go in the e4 act-work/F-E6-21 directory.
The first91-byte-object witness predates the additive failure counters; the
final report locates the current object's measured byte count.
An observe-class reviewer reads the diff against the commission and named
basis; every observation gets a disposition before merge and private push.
