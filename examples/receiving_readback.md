# Native receiving receipt readback

`receiving_readback` validates two native storage-helper receipts against a
separately retained input. It decodes the stage acknowledgement, all four complete
GET values, their versions and durable acknowledgements, and the singleton
namespace's Latest-prefix policy reply. It independently decodes retained
catalogue snapshots, then creates one new recovered-value file.

Build with `cargo build --release --offline --locked --example receiving_readback`.
Invoke with `INPUT STAGE_RECEIPT RECEIVE_RECEIPT CATALOGUE_STAGE CATALOGUE_RECEIVE
SOURCE_HEAD INPUT_SHA256 NEW_RECOVERED`. Supply the independently observed input
hash and selected source. The expected version comes from the actual stage
receipt, rather than a fixed fixture version. Retain the original input and raw
receipts separately.

To retain matching catalogue bytes with the current `storage-receive` helper,
append `--catalogue-snapshot NEW_FILE` to each stage or receive command. For
example, using caller-selected paths and identifiers:

```text
storage-receive stage FRESH_STATE NS KEY INPUT SOURCE_HEAD --catalogue-snapshot CATALOGUE_STAGE
storage-receive receive STATE NS KEY INPUT SOURCE_HEAD STAGED_VERSION --catalogue-snapshot CATALOGUE_RECEIVE
receiving_readback INPUT STAGE_RECEIPT RECEIVE_RECEIPT CATALOGUE_STAGE CATALOGUE_RECEIVE SOURCE_HEAD INPUT_SHA256 NEW_RECOVERED
```

Capture each storage command's stdout as its receipt; use the stage receipt's
version for `STAGED_VERSION`. Snapshot paths and recovered output must not exist.
A snapshot failure can occur after storage effects; retain the state and failure
instead of assuming no write occurred. Ordinary stage/receive opens and maintains
a Store; the standalone readback reads only retained files.

Current receipts label their catalogue phase `before_orderly_reopen`. Both count
and digest describe the saved snapshot, and readback checks both. A catalogue
copied after the command returns can contain later orderly-close records and
must not be substituted for this snapshot. Legacy receipts without a phase are
also supported: their hash describes a post-return snapshot while their count
was taken earlier. Readback validates that snapshot's hash and records, reports
`legacy_post_return_hash_only`, and does not compare the legacy count.

This helper never opens or mutates a Store. It reads bounded receipt JSON and
input bytes, reuses the separate read-only catalogue decoder, and writes only a
fresh recovered file. Digests and decoded values use the same retained bytes.
The local JSON reader preserves exact numbers and rejects duplicate keys; there
are no external package dependencies. Ordinary stage/receive correspondence is
supplementary handoff evidence, distinct from recovery replacement, cold
restoration, general policy validation, sole-authority storage or semantic
correctness of the recovered text.
