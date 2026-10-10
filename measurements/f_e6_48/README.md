# F-E6-48 resident growth measurement

Purpose: read-only resident window and private mutation/replay experiment for
F-E6-48. Account: `D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/ledger.json`.
Commission and small evidence: that loop's `runs/e9/act-work/F-E6-48/`.
`REPORT.md` there carries the final result id, hashes, readings and limitations.

The native private executable is built from main **11c312c**; these scripts do
not change membus source or install a resident. Build in the measurement
worktree with `cargo build --release --offline --locked -j2` and native tests
with `cargo test --offline --locked -j2 -- --nocapture`, through lineup
`cpu=4,mem=2,disk:d`. `run_recorded.py OUTPUT.json COMMAND ...` retains context
and direct-process consumption (compiler descendants remain unmeasured).

`window.py` performs only health/scans on `\\.\pipe\LOCAL\membus-kv1` and
reads resident logs/heartbeat. It writes the start marker, samples identity,
and captures the endpoint after 3600 seconds. `disk.ps1` samples raw and
formatted physical disk **2 D:** counters. This is this machine's mapping,
not a portable assumption. `--resume` preserves the original endpoint and
records an interrupted instrumentation interval. Use a tracked execution
session; runtime observation E-ba0cb2bddcd744a08565a14a2ba215d3 (seq1103)
records that its judge ended the detached original monitor because it could
not link it to the seat or locate its outputs. The tracked resume preserved
the original start/end and the continuously running disk collector.
Same resident PID/epoch at endpoints and sampled heartbeats demonstrate the
retained service identity; queue samples do not fill unobserved intervals.

`private.py` launches only UUID private pipes, first a 1000-key put/overwrite/
remove test, then 200000 synthetic records (268400000 bytes). State is ignored
in `.bench/F-E6-48-synthetic`; the small state path is the frontier's
`private-state`. Stop owned servers before removing both with
`PYTHONPATH=D:/dev/runtime-pins/430c52c/cognition-workflows python -m cognition.remove ABSOLUTE_PATH...`.
Generation uses old record times to exclude expired retry fingerprints.
Startup-to-ready includes process startup, replay, hello and health; Windows
cache is uncontrolled. Synthetic values and key cardinality differ from the
resident's repeated quota/state writes.

`ledger_rates.py` reads each found operational ledger once and emits only
counts/times/path hashes. `--retry-errors` retries only unresolved reads.
`project.py` parses the endpoint records into namespace/family rates, applies
the hypothetical publisher shape to trailing24h row rates, and exposes all
capacity/replay arithmetic. The physical device is shared and its bytes
cannot be attributed to membus. Two-point replay extrapolation is an estimate,
not a test at6GiB/8GiB. `account.py` retains sends/results and merges late use
records into the parent run's file.

`metric_scenario.py` provides explicit unknown-rate three-day-retirement
thresholds, including whole-chunk granularity/pins/metadata limitations.
`review_supplement.py` recomputes token-rate counterevidence, earlier key-slot
limits and ledger deployment sensitivity from the saved evidence only.
Actual File::write/sync-attempt counters were not captured at the endpoints.
The retained monitor is commission-specific: it hardcodes this resident PID,
and its prospective collector-reuse path has not been exercised. A future
run needs new start files/identity and coverage validation if a collector
dies; selecting the earliest collector alone could under-cover that run.
The actual completed window has continuous primary disk coverage; its
135.324-second gap is only in heartbeat instrumentation.

No third-party dependency is added. Python is measurement instrumentation,
not a production ecosystem tool. Output, target and synthetic data stay out
of git; only the experiment source is retained here.
