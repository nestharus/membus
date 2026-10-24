# Policy admission and native receiving

The public Rust policy setters canonicalize kinds 0/1 to Forever/Latest with zero retention,
as wire conversion does. Unsupported kinds and zero Expire refuse before policy mutation.
Positive Expire still requires the retry window; `None` clears a prefix. Canonicalization
precedes retry fingerprinting, capacity admission, durable encoding and effective publication.

The retained-policy fixture checks acknowledged policy bytes/version/ack, effective Latest
selection and a subsequent matching value's independently decoded new chunk route after
resident recovery and orderly reopen. The acknowledged catalogue prefix supplies surviving
chunk obligations; newly destructive retirement records for those survivors are rejected.
Retirement already acknowledged in that prefix is permitted. This is a living-owner fixture:
neither retained RAM nor the independent decoder supplies cold recovery authority. It does
not qualify abrupt OS failure, power loss, installed readers or downgrade safety.

## Native stage and receive

Build the `storage-receive` binary with `MEMBUS_BUILD_HEAD` set to the selected source commit.
Its embedded source label is a selection check, not an executable provenance attestation.

```
storage-receive stage FRESH_STATE NS KEY INPUT EXPECTED_HEAD
storage-receive receive STATE NS KEY INPUT EXPECTED_HEAD EXPECTED_VERSION
```

Stage refuses an existing directory, sets Latest with KEY as a prefix, stores INPUT with
flushed acknowledgement 2, and receives the exact bytes/version before and after orderly
reopen. Use the actual returned stage version for receive. Receive requires existing state
and independent input/version and requests no value mutation. Both use the native public
library wire dispatcher, without an installed server or network transport.

KEY selects neighboring keys beginning with it unless a longer prefix wins. Both commands
require exactly one policy row in NS: that KEY prefix with Latest. An unrelated row in NS
refuses even when the requested key still routes Latest. This deliberately scoped helper is
not a general existing-state policy resolver. Normal startup may advance epoch, append closure
records or finish resolved replay before a later policy, value or version refusal. Retain
partial effects and both output streams. Unresolved marker recovery is not supported here.

Successful stdout is JSON with source/transport, version, ack, input/catalogue hashes, value
hex and raw acknowledgement/policy/read/reopen replies. Refusals exit nonzero with stderr
context. `input_sha256` hashes the supplied bytes actually compared. `catalogue_sha256` and
`decoded_records` describe the same captured bytes at `catalogue_phase: "before_orderly_reopen"`;
normal reopen may append Close records afterward. `policy_scope: "singleton_namespace_latest_prefix"`
and `policy_prefix` identify the selection and namespace restriction. The independent decoder
recognizes multipart retirement kind 10 and expiry resume basis kind 11. Input remains subject
to the normal value/identifier limits and a 64 MiB receiving catalogue bound.

These authored fixtures express bounded regression obligations. They are separate from actual
configured receiving use and supply no general unattended recovery or migration qualification.
