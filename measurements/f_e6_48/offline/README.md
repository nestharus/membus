# Offline F48 numerical re-entry

This Rust command regenerates the historical growth projection, review supplement
and metric-retention scenario from saved artifacts. It opens no resident connection,
queries no device, acquires no ledger and executes no original producer. Python is
not required. Original Python producers, participants, reports and inputs remain
retained for their distinct capture, experiment and provenance responsibilities.

Build with `cargo build --offline --locked --release -j2 --manifest-path Cargo.toml`
from this directory, after dependency admission. Run the resulting
`membus-offline-f48` executable:

```text
membus-offline-f48 --inputs SAVED_DIRECTORY --change-at 2026-10-10T01:35:00+00:00 --output NEW_DIRECTORY
```

The timestamp above is the reported effective publisher change in the historical
experiment. Supply the appropriate explicit timestamp when using a different
fixture. It must fall strictly inside the saved endpoint window. Inputs are:

| File | Required saved content |
| --- | --- |
| resident-start.json, resident-end.json | Endpoint unix_time, hello_epoch, binary_sha256, heartbeat identity/counters, logs, health and scans |
| WINDOW-COMPLETE.json | started_at, completed_at, duration_seconds, resident_pid, resident_epoch and same_identity |
| disk-counters.jsonl | at, collector_pid, physical_device and raw cumulative disk counters; raw:null is an unsuccessful sample |
| private-small.json | restart.total_log_bytes and startup_to_ready_seconds |
| private-big.json | synthetic.bytes and both labeled readiness runs |
| ledger-rates.json | measured_at, trailing-24-hour totals and per-loop row/shape/rate summaries |

The schema is the retained F48 producer schema. Each log contains complete_bytes
and records with offset, end, bytes, namespace, key, family, at_ms, removed and
value_bytes. Stripe prefixes must remain identical, records contiguous, and each
key belong to one stripe. Opaque versions never order records across stripes.
Health includes quota bytes_written/flush_calls/keys/index_cap and slots
namespace_keys/namespace_quota; scans supply live and per-family counts. Publisher
summaries must reconcile with per-loop rows/24 and the assumed record shape
58 fixed + 9 namespace + resolved-key bytes + 36 UUID + 199 value bytes.

Each input is bounded to 32 MiB and each JSONL file to 100000 nonempty rows.
Numeric record coordinates must be exact nonnegative integers no larger than
2^53. UTF-8 with an optional BOM is accepted. Invalid or inconsistent required
inputs fail before creating the output directory. An existing output location is
refused. A write failure can leave a partial new directory; use a different fresh
location after diagnosing it. Do not treat a directory without manifest.json as a
completed result. A present manifest can itself be partial after interruption;
successful completion requires the command's zero exit and a valid manifest with
the complete output set and matching byte counts/hashes. Crash durability is not
established by these checks.

The library's public `regenerate` function follows the same file-loading and
verification path as the executable. Input and report construction are internal;
callers cannot replace verified host rows or their recorded identities. A supplied
host without an expected matching hash is rejected; it cannot produce a verified
report. There is no public in-memory verification bypass.

Outputs are projection.json/PROJECTION.md, review-supplement.json/
REVIEW-SUPPLEMENT.md, metric-retention-scenario.json/METRIC-RETENTION.md,
host-metrics-supplement.json and manifest.json. The three calculations run in that
order. The manifest identifies exact input bytes and every output except itself
by SHA256 and byte count. JSON serializes represented f64 results without display
rounding; this is not arbitrary-precision arithmetic. Markdown tables
render those same values. Zero growth produces null capacity dates. Signed
negative key dates retain the meaning "threshold already exceeded".

Required scans must contain namespace observations, integer live counts and
family-count objects whose counts sum to the live count. A family absent from an otherwise complete scan means zero;
a missing scan means missing evidence and is rejected. Impossible value/record
sizes, checked-integer overflow and nonfinite derived arithmetic are errors before
output creation; they never become unknown/null results.

Rate denominators use endpoint unix_time differences. The marker duration has a
separate producer anchor, is required positive and is reported separately; it is
not reconciled with rate duration. Marker timestamps must be within five seconds
of endpoint times. Stripe offsets select the appended tail. Record timestamps then
split that tail without an extra endpoint-time filter, preserving original
population semantics. These are explicit admission limits, not atomic capture.

An optional host JSONL supplement requires **both** `--host FILE` and
`--host-sha256 HEX`. Its rows use epoch-second `t` and `v` fields. The command
selects rows inclusively between primary disk sample endpoints and preserves
disk.D.queue/disk.D.busy_pct without interpreting busy scaling. The supplied hash
identifies present bytes; historical read-version correspondence remains
unqualified. Omitting this supplement is an explicit sub-result and leaves core
arithmetic available. No current host file is silently substituted.

The historical experiment was conducted on 2026-10-10 at baseline 11c312c.
Its heartbeat sampling gap, continuous primary disk collection, unattributed
shared-device traffic, missing file-write/sync-attempt counters and unexplained
token-rate discrepancy remain material limits. The command computes disk gaps
and collector separation from the supplied series; a new fixture need not have
the historical coverage. Full-window mixture and post-change rates are separate
populations. New-key accounting retains previously consumed slot identities,
including removes and resurrection.

Capacity dates assume constant rates and the hypothetical publisher shape. Both
warm readiness fits extrapolate different state shapes and do not measure 6/8 GiB
replay or bootability. Metric byte and key thresholds preserve the original
720-hour horizon, ideal 72-hour retention, fixed historical caps and 26 publisher
keys. Future metric serialized rate and key shape remain unknown. Chunk span,
metadata, pins/backlog, expiry-summary retention and slot reclamation can invalidate
the ideal bound. Statements about then-unimplemented retirement are dated to that
experiment and say nothing about current implementation or installed behavior.
Saved ledger summaries reproduce downstream arithmetic only. Capture reconstruction,
historical private state, original participants, raw ledger acquisition, recurring
qualification, current source acceptance and research claims are separate duties.

Tests use fresh bounded files. Set `MEMBUS_OFFLINE_FIXTURE_ROOT` to a nonexistent
directory below your own scratch location, then run
`cargo test --offline --locked -j2 -- --nocapture --test-threads=1`.
Fixtures and reports remain there for inspection; each run needs a new location.
