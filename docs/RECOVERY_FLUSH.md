# Recovered Flush prerequisite

A complete MBK1 record contains the requested acknowledgement. It can survive a
failed or interrupted live write without having received a covering data-file
sync. Replaying its acknowledgement byte alone does not establish achieved Flush.

Before publishing recovered values or retry outcomes, startup synchronizes every
present, nonempty active chunk, including closed chunks. This also covers chunks
closed by an older startup. Truncation precedes the barrier; publishing recovered
Close records follows it. A required sync failure refuses startup and attempts to
record a non-ready replay report, including storage errors already found by
replay. Report publication can itself fail; startup still refuses, and an earlier
ready report alone cannot establish current service availability. It does not
publish a successful Store or a new Close.
Missing-file storage errors and retiring/retired chunks retain their existing
handling. Original mutation IDs, versions, contents, receipt clocks and retry
outcomes are preserved; successful recovery can resolve an earlier uncertainty.

Active legacy stripe logs receive the same barrier. A new legacy import also
synchronizes its original logs before the switch and read-only backup movement,
so the original retry history is covered as well as the copied latest values.
Already imported read-only backups are preserved without changing their bytes,
permissions or grafting rules. This change does not establish a new data-file
sync witness for those historical backups. On Windows a read-only file handle
cannot flush them; this historical boundary must not be represented as qualified
universal recovery durability.

`tests/recovery_flush.rs` uses the public Store API to produce a complete append
followed by an injected failure before a covering live sync. It distinguishes a
refused recovery barrier from successful value and original retry publication,
and checks that refusal changes neither the data nor the catalogue.
It separately exercises a real Windows write-open denial on a fresh owned active
data file and preserves already located missing-file evidence when another
required sync fails. These controls do not represent every startup side effect
or a physical-device sync failure.

The explicit `sync-witness` feature records the actual file handle's target,
extent and FlushFileBuffers result in an append-only `.calls` diagnostic when
`MEMBUS_SYNC_WITNESS` names an owned fixture. Existing `.entering` and `.returned`
markers retain their format. Ordinary builds contain no witness lookup or code.
The owned Request compatibility fixture can require this evidence, binding a
successful recovery sync to the actual complete uncovered data extent before it
consumes Flush. Readable bytes, a catalogue sync or a green test count alone are
insufficient evidence of that prerequisite.

`MEMBUS_TEST_FAIL_AFTER_APPEND` and `MEMBUS_TEST_FAIL_RECOVERY_SYNC` explicitly
select the existing append fault and the separate startup fault in an owned
resident fixture. Ordinary launches select neither. They introduce no protocol
or caller-authority change. These tests simulate failure ordering; they do not
demonstrate power-loss behavior or an observed production incident.

## Startup epoch admission

Startup reads and validates the persisted epoch before importing legacy logs or
repairing replay tails. Only a missing epoch file starts at zero. A malformed
file, a read error, or an exhausted `u64` epoch refuses startup before moving
legacy inputs, truncating data, or appending recovered Close records. Refusal
attempts to replace the replay report with stage `refused` and `ready_ms=0`;
report-write failure cannot turn the refusal into success.

The next epoch is checked once and shared by the import's backup name and the
eventual epoch publication. A failed epoch admission consumes no epoch. Correcting
the persisted input permits the normal import/recovery attempt; an exhausted
epoch requires explicit state remediation and never wraps to zero.

`tests/recovery_epoch.rs` covers malformed lengths, a real epoch read error,
exhaustion without a panic, preservation of a torn tail and catalogue, replacement
of a prior ready report, normal restart increments, and import/publication at the
last available epoch. Its owned fixtures are synthetic and removed after use.
