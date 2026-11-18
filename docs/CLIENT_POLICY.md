# Typed namespace policies

This additive projection uses the existing Client and kv/1 POLICY_GET (25) and
POLICY_SET (24). It opens no Store, discovers no endpoint and supplies no
namespace, authority or retention defaults. No core policy algorithm changes.

## Typed source API

```rust,ignore
Client<T>::policy_get(&mut self, namespace: &str, operation_id: &str,
                     deadline: Deadline) -> Result<PolicyTable, client::Error>
Client<T>::prepare_policy_set(&self, request: PolicySetRequest)
    -> Result<PolicySetAttempt, client::Error>
Client<T>::policy_set(&mut self, original: &mut PolicySetAttempt,
                     deadline: Deadline) -> Result<MutationReply, client::Error>
```

`PolicySetRequest` owns `operation_id`, `namespace`, `prefix` and the existing
`membus::Class`. `Class::LATEST`, `Class::FOREVER` and `Class::expire(ms)` retain
their defining scalar forms. Expire must be positive and at least the actual
negotiated retry horizon. Noncanonical class fields refuse; no clear method is
added. The setter's achieved Ack is fixed by the existing wire to Flush.

`PolicySetAttempt` is `Attempt<PolicySetRequest>`; the original request is private
and inspected through `request()`, with `epoch()` retaining its negotiated epoch.
The existing spelling `Attempt` remains `Attempt<MutationRequest>` through a
default type argument. Existing prepare/mutate/remove signatures remain source
compatible. The exact same original epoch/first-send/retry-window check now lives
once in `Attempt::check_horizon`. No horizon renewal, automatic retry, reconnect,
backend mutation or new retry policy is introduced. These tokens remain in-memory
custody, not durable caller-crash recovery. The caller preserves its fixed original
Deadline across an explicit revisit; Deadline is not fingerprinted by kv/1.

`PolicyTable::{namespace,epoch,rows,exact}` expose the selected request context,
complete bounded rows and an exact configured-prefix lookup. A missing prefix
returns None: neither FOREVER nor a longest-prefix substitution is inferred.
Namespace and epoch are request/connection association, not new echoed wire
fields. They cannot authenticate a dataset or independently prove a provider
selected the correct namespace. Actual endpoint and authorization are owner inputs.

## Wire ABI and bounds

Frame version1/minor0, header/request association, HELLO/refusal vocabulary,
transport deadlines and legacy defaults are unchanged. GET payload is the existing
u64 UTC deadline, lp operation ID, lp namespace. Its reply is u16 count followed
by (lp UTF8 prefix, u8 class kind, u64 retention). The complete reply must fit the
negotiated frame; count must be possible given minimum13 bytes per row; every
prefix and combined policy identity must fit the existing MAX_ID bound; duplicate
prefixes, invalid/noncanonical classes, invalid UTF8, short/extra bytes refuse.
The whole table is validated before owning prefix strings. Unknown/malformed
replies never become a successful empty table and invalidate the connection.

SET adds lp exact prefix, u8 class kind and u64 retention to the same prefix.
Success must contain only nonzero u64 version and u8 Ack2. The existing provider
publishes its durable policy projection before completing. Namespace/prefix/class/
operation bind the original provider identity; they are not echoed in the SET
reply. GET has no per-policy revision or operation-ID echo. Consequently readback
cannot prove that an uncertain SET never happened, nor attribute a current equal
value to that SET. Preserve its original and known receipts; reconcile only that
original within its epoch and fixed horizon, then check current policy.

Definitive negotiated refusals retain their code/name/transience. Codes16/17,
lost/malformed/misassociated reply or wrong achieved Ack remain Error::Uncertain
with original operation/epoch, leaving the connection unusable. A later refusal
or successful equal readback does not erase prior uncertainty. Wrong epoch or
expired original horizon refuses before another send. All this uses the existing
Client, not a standalone policy transport.

## Explicit configuration and readback

This helper leaves original custody and any known SET receipt with its caller:

```rust,no_run
use membus::client::{Client, Deadline, Error, MutationReply, PolicySetAttempt, Transport};
fn configure_one<T: Transport>(client: &mut Client<T>, original: &mut PolicySetAttempt,
    known_set: &mut Option<MutationReply>, read_id: &str, fixed: Deadline)
    -> Result<(), Error>
{
    if client.negotiated().retry_window_ms > 86_400_000 {
        return Err(Error::Invalid("completed recovery needs retry horizon at most 24h"));
    }
    *known_set = Some(client.policy_set(original, fixed)?);
    let request = original.request();
    let table = client.policy_get(&request.namespace, read_id, fixed)?;
    if table.epoch() != original.epoch()
        || table.namespace() != request.namespace
        || table.exact(&request.prefix) != Some(request.class) {
        return Err(Error::Protocol("required policy readback mismatch"));
    }
    Ok(())
}
```

The caller first prepares each intended exact namespace/prefix/class with a fresh
logical SET operation ID, and provides a distinct read ID and fixed Deadline.
For the scoped completed-recovery workflow, actually configure and read back
LATEST for every approved current/pending prefix and expire(86400000) for the
completed-recovery prefix BEFORE data writes, with actual retry horizon at most
24h. These are consumer requirements, not hardcoded library limits. Verify the
entire table's overlapping-prefix effects and protect policy ownership against
intervening writers. This helper alone supplies neither protection nor that
configuration evidence. Never replace an uncertain original with a fresh attempt.

Completed consumer processing must precede same-original conditional recovery
copy + Flush, confirmed association, and only then owed payload reduction.
Pending/running/unresolved source and Required remain complete on uncertainty.
Chunk retirement is whole-chunk from CHUNK CLOSE, hence at least24h after that
completed recovery write, not exact-item/session/physical deletion. MBC1 hints
remain volatile: durable reconciliation and explicitly owned unsubscribe remain
required. Existing CLI policy admin generates an internal time-derived operation
ID; this API does not invent a CLI caller-ID option or authorize repeating an
uncertain admin command.

Authored tests use inert scripted bytes only. Runtime/compiler/native and live
policy/readback acceptance remain separate; no selected image, endpoint, namespace,
prefix, permission, grant or operational source/data is supplied by this document.
