# Recovered Flush prerequisite

This startup barrier is one prerequisite of recovery, not a backup protocol.
[Backup and isolated recovery](BACKUP_RECOVERY.md) describes complete-state
capture, mutable validation and the rollback/clone limits of resident epochs.

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

After the data-file syncs, startup synchronizes each distinct class directory
holding a present nonempty Open or Closed chunk, before the catalogue directory
barrier, recovered Close records and epoch publication. This establishes the
active chunk file-name prerequisite for older writers as well as the current
writer's live creation barriers. A failed class-directory sync refuses startup
and retains already collected observations. File contents and directory names
remain separate prerequisites; neither synchronization substitutes for the other.

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

Errors while creating, writing, or synchronizing `epoch.tmp`, or replacing
`epoch`, also attempt a refused report. That report retains observations already
collected by replay; the original I/O error, including its OS code, is returned.
The file-operation order and successful publication behavior stay the same.

`tests/recovery_epoch.rs` covers malformed lengths, a real epoch read error,
exhaustion without a panic, preservation of a torn tail and catalogue, replacement
of a prior ready report, normal restart increments, and import/publication at the
last available epoch. Its owned fixtures are synthetic and removed after use.
The publication controls block the temporary file with a directory and, on
Windows, deny replacement through a real file handle. They check refusal,
preserved replay observations and successful retry without skipping an epoch;
they do not simulate a physical write or sync failure.

## Owned recovery refusals

Owned startup attempts a refused snapshot/history report for failed catalogue
write-open or recovered Close/Retired appends, empty-Open deletion, tail repair,
pending-report publication, and late timer/policy projection. The report retains
the original error and observed replay evidence, identifies the validated
attempted epoch, and sets `stage=refused` and `ready_ms=0`. A secondary reporting
error cannot replace the underlying failure; earlier diagnostics may remain.

Pending tail evidence still precedes destructive repair. Completed repairs or
deletions are not undone when a later step fails, and no failed append is treated
as committed. Pre-publication failures retain the prior epoch. Control projection
and final serving-budget checks follow durable epoch publication, so their
refusals consume an epoch and corrected-input retry starts a later one.

If final ready-report publication fails, startup joins every worker while holding
directory ownership before attempting refusal. A blocked history append can
leave a refused snapshot with older history; snapshot replacement failure can
leave older diagnostics too. A report alone never proves a resident is serving.
Pre-ownership directory failures retain the separate startup-directory contract.

The focused refusal modules cover catalogue and tail I/O denials, cold control
reads, pending/ready publication failures, preserved evidence and retry. They use
authored fixtures and keep the original OS error when reporting also fails.
