# Mutation retry retention

The scalar store retains a 32-byte SHA-256 comparator per admitted retry identity,
using the same SHA-256 implementation as storage verification. The comparator
covers the complete existing canonical mutation representation with its generated
version and admission time set to zero. Namespace, key, operation ID, expected
revision, acknowledgement level, removal flag and value participate; the caller's
deadline does not. Equality relies on SHA-256 collision resistance. This is not a
mathematical guarantee of byte equality for arbitrary colliding inputs.
The existing encoding also uses the same expected-revision sentinel for `None`
and `Some(u64::MAX)`; comparator compaction does not change that encoding alias.

The existing namespace-scoped operation identity, pending-operation join, original
outcome and retry horizon remain in force. Replay and legacy import derive the
same comparator from complete retained mutation records. No digest is written to
the log and no protocol or persistence format changes. Existing data can be read
by either implementation, subject to each resident's own resource limits.

Recovery retains a source-chunk tag with each outcome and derives one local retry
cutoff per replayed chunk. Retirement cannot discard that chunk while a recovered
outcome is still retryable, even if a UTC correction made the close timestamp
earlier than admission. The conservative retry charge includes eight additional
bytes per identity for the tag. This bookkeeping is in memory and does not alter
the record format. See [instance time](CLOCK.md) for the clock domains and bounds.

Only the retained comparator is compacted. Identifiers, outcomes and bookkeeping
still consume memory per operation. Full values and mutation records still incur
serialization, hashing, queue, disk and recovery work. Queue admission continues
to charge the complete mutation representation and value, independently of the
comparator size; comparator compaction leaves disk-byte accounting and configured
retention classes unchanged. A consumer
that repeatedly publishes growing state still has growing transfer and CAS work.

This representation supplies no workload-rate, retention-suitability or resident
headroom guarantee. Callers retain their existing identity and uncertainty
reconciliation responsibilities.

Every allocated revision leaves a representable next-revision high water.
The final assignable revision is `u64::MAX - 1`. Once its successor is the
exhausted counter, fresh mutations and policy updates refuse with capacity (7)
before allocating a revision or appending. Reads and known retry outcomes
continue, including after restart; the counter never wraps to zero.

A stored record at `u64::MAX` cannot supply that successor. Recovery refuses
it as invalid data, before tail repair or epoch publication, rather than
panicking or silently resetting the high water. The error identifies its file
and offset. Legacy import also refuses a synthetic policy revision when the
counter is exhausted, preserving the source and policy inputs. An import
needing no new policy revision can retain the existing final outcomes.

Revision zero is reserved for absence and is never a stored mutation revision.
Recovery refuses a complete zero-version record, including one below an
anti-resurrection horizon, as invalid data with its file and offset. This
precedes tail repair, import publication and epoch publication, leaving the
inputs available for correction. Ordinary first revision one, the zero
expected-version comparator, and an empty catalogue high-water hint retain
their existing meanings.
