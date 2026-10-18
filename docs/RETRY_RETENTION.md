# Mutation retry retention

The scalar store retains a 32-byte SHA-256 comparator per admitted retry identity,
using the same SHA-256 implementation as storage verification. The comparator
covers the complete existing canonical mutation representation with its generated
version and admission time set to zero. Namespace, key, operation ID, expected
revision, acknowledgement level, removal flag and value participate; the caller's
deadline does not. Equality relies on SHA-256 collision resistance. This is not a
mathematical guarantee of byte equality for arbitrary colliding inputs.

The existing namespace-scoped operation identity, pending-operation join, original
outcome and retry horizon remain in force. Replay and legacy import derive the
same comparator from complete retained mutation records. No digest is written to
the log and no protocol or persistence format changes. Existing data can be read
by either implementation, subject to each resident's own resource limits.

Only the retained comparator is compacted. Identifiers, outcomes and bookkeeping
still consume memory per operation. Full values and mutation records still incur
serialization, hashing, queue, disk and recovery work. Queue admission continues
to charge the complete mutation representation and value, independently of the
comparator size; disk accounting and record retirement are unchanged. A consumer
that repeatedly publishes growing state still has growing transfer and CAS work.

This representation supplies no workload-rate, retention-suitability or resident
headroom guarantee. Callers retain their existing identity and uncertainty
reconciliation responsibilities.
