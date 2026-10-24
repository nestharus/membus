# Instance time

Each Store owns a `chunks::Clock`. Recovery and runtime owners share its clones;
`Config::clock` optionally supplies an `Arc<ManualClock>`. `Store::clock()` exposes
the same instance clock to resident code such as heartbeat serialization.

| Site | Clock | Meaning |
| --- | --- | --- |
| Replay `replay_into`, including chunk records and backup retry grafts | `utc_ms()` | Compare persisted record timestamps and import timestamps with current UTC. |
| Legacy `import_legacy` / `read_legacy` | `utc_ms()` | Select the retry horizon using original persisted UTC timestamps. |
| Recovery report `start_ms`, ready report `ready_ms` | `utc_ms()` | Report wall timestamps in the same domain as the store. |
| Recovery report `duration_ms`, including pending/refused snapshots and the ready summary | `mono_ms()` | Local elapsed recovery time must not change when UTC is corrected. |
| Store policy-generated operation ID and mutation record timestamp | `utc_ms()` | Persist wall timestamps; epoch and sequence disambiguate generated IDs. |
| Live mutation and policy retry cutoffs, including expiry queue cleanup | `mono_ms()` | Elapsed retry bounds cannot change when UTC is corrected. |
| Store policy waits/checks and mutation waits/admission checks | `utc_ms()` | These deadlines arrive from the caller as absolute UTC, including duplicate wait deadlines. |
| Codec non-mutation deadline check | `utc_ms()` | Compare client wire deadlines with UTC; mutation/policy paths retain their retry semantics. |
| Timer admission, schedule due times, missed occurrence selection and checkpoint Mutation deadline | `utc_ms()` | Schedule anchors and Mutation deadlines use UTC on disk and on the wire. |
| Snapshot creation, lookup, capture/page expiry and reaper | `mono_ms()` | Snapshot lifetime measures local elapsed time. |
| Maintenance pass interval | `mono_ms()` | Local elapsed interval between passes. |
| Flush gathering, spacing and previous-sync timestamp | `mono_ms()` | Local elapsed intervals, shared with the Store clock. |
| Change delivery connection/write budget | `mono_ms()` | One local elapsed budget covers connection and partial writes. |
| Resident heartbeat timestamp | `utc_ms()` | Recorded wall timestamp, including when a manual clock is supplied. |
| Snapshot capture diagnostics | Clock monotonic nanoseconds | Preserve nanosecond telemetry resolution; manual milliseconds convert to nanoseconds. |

On restart, monotonic timestamps from a previous process have no meaning. Replay
filters retry records by their UTC age. Recovery converts each surviving record's
remaining window into a cutoff in the new instance's monotonic domain. Live
admission uses monotonic cutoffs thereafter. The retry boundary is inclusive;
an age equal to `retry_ms` remains retryable. UTC corrections after recovery
neither close nor reopen that local window. Restart still necessarily uses
persisted UTC to account for time outside the process.

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
