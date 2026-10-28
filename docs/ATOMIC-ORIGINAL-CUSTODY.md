# Atomic original inspection and domain custody

`AtomicRequest::inspect()` borrows every original field: namespace, stable
operation ID, provider epoch and monotonic horizon, the complete ordered
key/revision population, requested Ack, and every ordered final put/remove with
its exact value bytes. The projection cannot mutate the original. MAR1 encoding,
decoding, persistence and publication remain the existing provider operations.

Inspection and decoding supply no proof that the domain's typed planner produced
an original. A request could be syntactically valid while violating the domain's
creation preconditions. `atomic::original::DomainOriginalCustody` is the explicit
read-only port to that existing domain authority. It has no default or storage
implementation. An adapter must authenticate the original producer at its actual
decision boundary and retain the exact request before its first send. Its record
binds the dataset/domain, independently expected generation, domain operation
kind and stable operation identity. These fields must not be inferred from the
offered bytes or a later recapture. The domain retains its own application
conditions, IDs, read values, lifecycle and provenance policy.

```rust,no_run
use membus::atomic::{AtomicRequest, original::{
    recover_original, CustodyError, DomainOriginalCustody, OriginalScope,
}};
fn inspect_recovered(
    authority: &impl DomainOriginalCustody,
    expected: OriginalScope<'_>,
    offered: &AtomicRequest,
) -> Result<(), CustodyError> {
    let recovered = recover_original(authority, expected, offered)?;
    let complete = recovered.original().inspect();
    // The domain validates its exact record set and meaning using this projection.
    // This function neither publishes nor declares an earlier outcome settled.
    assert!(!complete.changes.is_empty());
    Ok(())
}
```

`recover_original` validates inputs, calls the selected custodian once, checks
the independently expected scope, validates its returned original, and compares
the entire canonical original with the offered request. A wrong body, guard,
namespace, operation, epoch, horizon or requested Ack refuses as OriginalMismatch.
It returns the custodian's exact original without changing or renewing it.
UnknownOriginal, ProvenanceUnavailable and Unavailable remain explicit failures;
they do not authorize a new decision or establish that no previous effect exists.

The port is a trust boundary. A caller-selected implementation that simply echoes
offered bytes is not an authenticated custodian. Rust type checking and this
comparison cannot establish that an external adapter fulfilled its contract.
This source supplies no authentication transport, journal, issuer, key manager,
or durable production adapter. A same-process authored keeper qualifies only the
pure comparison and failure contract. It does not survive loss of its process.
Caller-process-loss support requires independently surviving authenticated custody
from the actual domain owner and separate qualification of that adapter. Until
then typed cross-process recovery is unavailable.

Recovery is separate from reconciliation. The existing `Store::publish_atomic`
still owns retained outcome lookup, exact achieved Ack, retry/epoch/horizon
refusals and uncertainty. A recovered original is no outcome receipt or serving
grant. In particular, preserve earlier uncertain observations across any later
refusal; no later refusal proves the absence of an earlier effect. After provider
restart, an unknown old original still returns RestartUncertain under the existing
rules. No caller restart, provider restart, OS effect or live handover is executed
or qualified by this port. Domain receiving and provider adoption remain required
before a consumer changes its supported boundary.
