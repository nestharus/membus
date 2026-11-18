# Complete-query eligibility of a captured namespace

`Client::snapshot_require_complete(namespace, &token, operation, deadline)`
requires the eligibility of the **same immutable snapshot token** before an
indexed query selects its schema, boundary, rows or postings. All Rust
`Collection::query` variants call it. The caller still owns capture, deadlines,
paging and release. It performs no automatic retry or new capture.

On the default KV1 connection, opcode **31 SNAPSHOT_REQUIRE_COMPLETE** uses the
existing request prefix followed by a length-prefixed 16-byte snapshot token.
Success has an empty payload. Refusal 16 means the captured population is
ineligible for a complete query. Refusal 20 means the token is absent, expired,
released or from another epoch. Refusal 21 means the token has the wrong length
or belongs to a different namespace.
Older providers and connections that selected finite hints through affirmative
`MSH2` discovery refuse with unsupported (2). The typed Rust method requires
the default profile and negotiated scans before sending. A caller must retain
the unsupported result and must not substitute live HEALTH2. The operation
uses the existing refusal vocabulary and limits. See [atomic wire](ATOMIC-WIRE.md)
and [finite hints](FINITE-HINTS.md).

At capture, the canonical snapshot owner records eligibility while holding the
same committed-index read lock and state lock used to select and reserve its
whole population. The existing authoritative `storage_error`, `read_only` and
`warming` observations conservatively make that snapshot ineligible. In
particular, replay's known missing closed chunk cannot become a successful empty
or partial indexed result merely because its postings are absent while schema,
head and matching rows survived. Ordinary degraded snapshot inspection remains
available; complete-query consumers explicitly require the stronger contract.

The eligibility field is immutable. Later health changes neither certify an
ineligible old token nor revoke an eligible captured population. Actual selected
payload failures still refuse through the existing fallible snapshot loader and
sticky storage-error owner. This is a known-storage-state contract, not an eager
physical integrity scan, a proof against undetected corruption, a durability
barrier, a transaction or a serving-authority grant. An unrelated global storage
error may conservatively refuse an otherwise intact namespace. Deployment policy
and unknown storage faults remain outside this bounded contract.

The [Python indexed binding](PYTHON_INDEXED.md) calls
`snapshot_require_complete(namespace, token, op_id=..., deadline=...,
retry_transport=False)` before selection. It reports unsupported as
`membus.NotAvailable`, preserves the other wire refusals, and validates field
bytes, candidate budgets and snapshot identity on the captured view.
