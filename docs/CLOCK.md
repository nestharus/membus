# Instance time

Each Store owns a `chunks::Clock`. Recovery and runtime owners share its clones;
`Config::clock` optionally supplies an `Arc<ManualClock>`. `Store::clock()` exposes
the same instance clock to resident code such as heartbeat serialization.

`ManualClock::advance` saturates UTC and elapsed counters individually at
`u64::MAX`. A large advance therefore cannot wrap elapsed time backward and
reopen a retry window or snapshot lease. A saturated elapsed counter has no
further representable time; use a fresh ManualClock for a subsequent scenario.
`jump_utc` continues to adjust UTC alone, with its existing zero/maximum bounds.
Counter updates use an atomic read/modify/write: concurrent corrections and
advances preserve intervening UTC changes. The UTC and elapsed counters remain
independent, and reading both does not form an atomic snapshot.

| Site | Clock | Meaning |
| --- | --- | --- |
| Replay `replay_into`, including chunk records and backup retry grafts | `utc_ms()` | Compare persisted record timestamps and import timestamps with current UTC. |
| Legacy `import_legacy` / `read_legacy` | `utc_ms()` | Select the retry horizon using original persisted UTC timestamps. |
| Recovery report `start_ms`, ready report `ready_ms` | `utc_ms()` | Report wall timestamps in the same domain as the store. |
| Recovery report `duration_ms`, including pending/refused snapshots and the ready summary | `mono_ms()` | Local elapsed recovery time must not change when UTC is corrected. |
| Recovery expiry-summary cleanup | `utc_ms()` after epoch publication | Compare persisted retirement timestamps with UTC at cleanup, including time spent in publication I/O. |
| Store policy-generated operation ID and mutation record timestamp | `utc_ms()` | Persist wall timestamps; epoch and sequence disambiguate generated IDs. |
| Live mutation and policy retry cutoffs, including expiry queue cleanup | `mono_ms()` | Elapsed retry bounds cannot change when UTC is corrected. |
| Store policy waits/checks and mutation waits/admission checks | `utc_ms()` | These deadlines arrive from the caller as absolute UTC, including duplicate wait deadlines. |
| Codec non-mutation deadline check | `utc_ms()` | Compare client wire deadlines with UTC; mutation/policy paths retain their retry semantics. |
| Timer admission, schedule due times, missed occurrence selection and checkpoint Mutation deadline | `utc_ms()` | Schedule anchors and Mutation deadlines use UTC on disk and on the wire. |
| Snapshot creation, lookup, capture/page expiry and reaper | `mono_ms()` | Snapshot lifetime measures local elapsed time. |
| Maintenance pass interval | `mono_ms()` | Local elapsed interval between passes. |
| Automatic chunk retirement | UTC age and local monotonic age since close | Both must satisfy retention and pass the inclusive retry horizon; recovered closed chunks have only their persisted UTC basis. |
| HEALTH2 latest-class pinned bytes | UTC age and local monotonic age since close | Use the same passed retry bounds as automatic retirement before reporting retention pressure. |
| HEALTH2 blocked-retirement duration | `mono_ms()` | Measure time since this process observed a denied delete; UTC corrections cannot raise or clear the one-hour signal. |
| Flush gathering, spacing and previous-sync timestamp | `mono_ms()` | Local elapsed intervals, shared with the Store clock. |
| Change delivery connection/write budget | `mono_ms()` | One local elapsed budget covers connection and partial writes. |
| Resident heartbeat timestamp | `utc_ms()` | Recorded wall timestamp, including when a manual clock is supplied. |
| Recovery heartbeat forward-gap check | `utc_ms()` | Compare the nonnegative age with 24 hours, without adding to a persisted timestamp that can be near the integer limit. |
| Snapshot capture diagnostics | Clock monotonic nanoseconds | Preserve nanosecond telemetry resolution; manual milliseconds convert to nanoseconds. |

Ending a clock pause also requires successful catalogue publication. A failed
Resume sets `storage_error`, leaves expiry paused and retains an explicit
`expiry.resume` request for retry. See [retirement controls](CHUNK-RETIREMENT.md).
Necessary fence repair still runs when size-triggered catalogue compaction is
disabled with `catalogue_rewrite_bytes = 0`.

On restart, monotonic timestamps from a previous process have no meaning. Replay
filters retry records by their UTC age. Recovery converts each surviving record's
remaining window into a cutoff in the new instance's monotonic domain. Live
admission uses monotonic cutoffs thereafter. The retry boundary is inclusive;
an age equal to `retry_ms` remains retryable. UTC corrections after recovery
neither close nor reopen that local window. Restart still necessarily uses
persisted UTC to account for time outside the process.

Each replayed chunk also keeps the latest inclusive local cutoff of its
reconstructed retry outcomes. Automatic retirement and pinned-pressure reporting
wait past it even when a UTC correction made the recorded close earlier than
an admission. Recovery computes this once; hot health and retirement checks do
not scan the retry table. This in-memory bound changes no persisted timestamps.

Recovery captures a monotonic start alongside its UTC start timestamp. Pending
and refused reports capture elapsed time at that stage; successful startup
captures it just before publishing the ready report. JSON, replay history and
the one-line CLI summary use that captured duration. UTC may jump forward,
backward or return to the original value while recovery runs: the wall
timestamps reflect the correction, while duration reflects only elapsed time.
A nonzero refusal duration does not indicate readiness; `stage` and `ready_ms`
retain their existing meanings. Each restart establishes a new elapsed bound,
including when it reuses a ManualClock whose monotonic count is already nonzero.

`ReplayReport::elapsed_ms` is an optional in-memory duration, not a new JSON
field. Startup supplies it; standalone reports constructed from UTC timestamps
may leave it `None` to retain the historical saturating UTC-difference fallback.
The serialized report keys and numeric value types are unchanged.

Pending and refused reports carry the validated attempted epoch, even when
the epoch file has not yet been published. A failure to read or validate the
previous epoch, including exhaustion, leaves the report epoch at zero. This
identifies the attempt rather than proving that it consumed an epoch: a retry
before publication can use the same number. Use `stage` and `ready_ms` to assess
readiness; successful reports retain their existing epoch meaning and JSON shape.

OS timed waits remain the blocking mechanism. Wakeups recheck the instance clock;
advancing a ManualClock does not itself notify a condition variable. Tests can
notify the owning condition variable or run the relevant tick explicitly.
Client `Instant` deadlines, CLI payload deadlines in `main.rs`, qualification
timestamps, and test temporary names retain their process clocks. The optional
`sync-witness` instrumentation measures the physical OS call with `Instant`;
it is test evidence rather than a store admission or recovery decision.

`cargo test --offline --locked` includes deterministic manual-clock recovery and
runtime witnesses in `src/replay/clock_tests.rs` and a source guard in
`tests/clock_guard.rs`. The guard reports file and line for server `now_ms()`
calls, excluding the clock implementation, definition, tests and named client/CLI
files. See the README for native test fixture inputs.
Elapsed-report controls cover UTC jumps in both directions, advancing time with
unchanged UTC, nonzero pre-start elapsed time, restart, refused/pending stages,
both output formats and the standalone-report fallback.

Non-ready reports retain the population observed at their stage: keys (including
tombstones and charged expiry summaries), retry identities, and live namespace
counts. Replay failure describes only the valid prefix. Pending repair and early
refusals precede cleanup; late control refusals and ready reports describe the
cleaned projection. Collecting counts requires no additional value reads.

Summary cleanup samples instance UTC after epoch publication and recomputes the
chunk index charge before serving. Expired summaries spend neither index slots
nor serving memory. The final admission gate counts live entries, tombstones,
current summaries, retries and chunk metadata plus the process margin; exact
budget equality is admitted. Early preflight checks exclude summary charges so
obsolete summaries cannot prevent an empty restart. These checks bound serving
state, not temporary replay memory. A late refusal retains completed repairs and
the published epoch; its retry advances from that epoch.

Recovery rechecks clock doubt after epoch publication using current UTC, the
catalogue high-water timestamp including recovery's Close records, and heartbeat
UTC. Backward corrections or heartbeat gaps greater than 24 hours pause automatic
retirement; an exactly 24-hour gap remains allowed. A pause already observed is
preserved until explicit Resume or the existing stable-clock procedure succeeds.
Elapsed time without net UTC age creates no persisted-clock gap. The heartbeat
reader accepts whitespace around `unix_ms`'s colon and integer; an unavailable
integer retains the existing fallback.

A failed final ready-report publication stops and joins workers while retaining
ownership, then attempts a refused report with zero ready time, the published
epoch, recovered observations and elapsed shutdown duration. A failed pending
report likewise attempts refusal before repair or epoch publication. Secondary
report errors preserve the original error. See [recovery publication](RECOVERY_FLUSH.md)
for the remaining refusal paths and the limits of best-effort diagnostics.
