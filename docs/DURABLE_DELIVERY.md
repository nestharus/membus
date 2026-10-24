# Bridging committed intent to durable delivery

This is a proposed consumer contract for forwarding mandatory work/events to a
queue or another service. Membus supplies state operations and bounded volatile
[change hints](SUBSCRIPTIONS.md), not a durable queue bridge or universal
exactly-once effects. Choose recoverable intent, source enumeration and checkpoint
meaning before implementing a forwarder. Ordinary local reads and writes must
not acquire a broker dependency just because another traffic class uses one.

## Establish what commits with the fact

| Construction | Conditions and costs |
|---|---|
| One conditional record contains the domain fact and retained pending intents | One successful mutation can commit both at the selected Ack. Every writer must preserve unresolved intents. Bound the pending set and refuse further mandatory work when it cannot fit; repeated full-record updates add serialization, transfer and log bytes. A latest event field overwritten by the next update loses history. |
| A committed immutable application event contains fact and delivery intent | Participating callers can use an indexed collection's immutable row/postings and conditional publication head, then derive state from committed events. The collection does not atomically update an independent domain-state key. Publication uses multiple native mutations, and a staged row is not published until the head includes it. Retain records and reconcile interrupted publication. |
| An existing authoritative journal/transaction owns fact and outbox | Export from its actual recoverable commit boundary. Keep that owner authoritative instead of adding an independent membus write as supposed proof of atomicity. A new native commit-log export interface would need its own ordering, completeness, retention and failure qualification. |

Two independent KV mutations, including BATCH items, do not create an atomic
fact/outbox transaction. A lost hint or current-value scan cannot reconstruct
overwritten intermediate transitions. If mandatory intent was never recoverably
committed, no bridge retry policy can recover it. Explicitly best-effort telemetry
can instead tolerate/report loss under its declared contract.

Persist immutable message identity, schema/version, original event identity,
tenant/logical stream, intended destinations, payload or durable content identity,
and the required completion boundary. Authenticate tenant/routing scope rather
than trusting an arbitrary producer-supplied topic or receiver. Keep event and
effect IDs stable across transport reconnect, broker retry and restore. Current
writer authority/placement generation is separate from an already committed
event's original identity; do not mint new message IDs for its replay.

## Choose a source boundary that cannot skip late publication

Native revisions are allocated before append/completion. Distinct stripes can
publish in a different order. Seeing revision 102 does not prove revision 101
has published, failed or become irrelevant. A bridge that scans current keys,
saves the largest revision seen, and later asks only for greater revisions can
skip a still-pending lower revision. This is a source-derived possible ordering,
not a witnessed bridge incident. Wall-clock timestamps have the same completeness
problem. MBC1 cursors order publications but their ring is bounded and volatile;
they are not a durable log position.

Use either complete enumeration of retained pending intents with per-intent
settlement, or a qualified ordered committed stream with a contiguous published
boundary. For an indexed collection, use its application ordinal/head and one
captured namespace view: include rows only through that captured head. Native
row/head revisions and the signal cursor are different values. Do not skip a
staged ordinal, merge pages from different snapshots or use current GETs to fill
an allegedly common view. Enumeration must fit snapshot/query budgets today;
a scalable incremental export protocol is not supplied by this guide.
The current collection contract expects complete committed history; deleting
published rows can make its queries refuse. It supplies no consumer-offset-aware
pruning. A bounded/prunable event source needs a separately declared retention
and query contract, not just an exporter checkpoint.

Scope a checkpoint by source lineage, logical stream/partition, destination and
its routing/filter contract. A route update, restored root or recreated broker
stream must not make an unrelated checkpoint applicable. Preserve the original
event IDs and reconcile retained progress through those changes.

## Checkpoint the declared downstream boundary

Distinguish source intent committed, export pending, downstream accepted and
receiver/effect settled. Socket write, local queueing, source subscription status
and broker durable acceptance are different observations. A broker acknowledgment
does not prove that an application completed useful processing.

Advance source progress only after the required downstream acceptance is proven
and the corresponding progress is durable. If the broker accepted a message but
the reply or checkpoint is lost, resend/reconcile with the original identity.
This deliberately permits duplicates. If progress is recorded before acceptance,
a crash can permanently skip work. For native source/progress mutations, retain
SDK uncertainty and original Attempt/horizon. A fresh connection or definitive
later refusal cannot erase an earlier possible send.

For concurrent delivery of an ordered source, advancing directly to the highest
accepted ordinal can skip a lower failed/unaccepted message. Advance only through
the prefix meeting the chosen acceptance boundary, or retain a durable, bounded sparse
settlement account with a proven recovery rule. Nonmatching source records may
advance a filtered scan only after their classification is established under
the checkpoint's filter contract. Legitimate filtered cursor gaps are not proof
that missing required work succeeded. Isolate partitions/destinations so one
blocked tenant need not stall unrelated streams.

Multiple workers need explicit ownership/fencing and conditional checkpoint
publication. A successful CAS on progress does not retract a stale worker's
external send; downstream idempotency/effect reconciliation is still required.
Native retry retention and a broker's finite dedup window are not permanent
application effect uniqueness. Size receiver identity retention for the promised
replay/restore horizon, or reject/reconcile older replay explicitly.

## Retention, pressure and latency

Do not retire unaccepted mandatory intent merely because a time limit elapsed.
After transfer, source reclamation depends on the declared handoff boundary,
durable progress, every required destination and the selected downstream failure
model. Some contracts can transfer responsibility at broker acceptance; others
must retain intent until receiver/effect settlement. Poison or unavailable
destinations need explicit outstanding/failure handling; quarantine does not
silently satisfy a required delivery. Preserve [backup lineage](BACKUP_RECOVERY.md)
and [tenant lifecycle](TENANT_LIFECYCLE.md) obligations for retained copies.

Use small hints for promptness and periodic durable reconstruction after loss.
Batch by explicit item, byte and total-time bounds, with tenant/destination
fairness. Waiting only for silence can postpone handling forever. Bound in-flight
copies, retries, sparse settlements and backlog bytes/age; apply a declared
admission/refusal policy before mandatory intent exceeds its safe retention or
storage budget. Sustainable downstream bytes/s and fan-out determine whether
backlog shrinks. A queue outage cannot be made free by moving its work off-path.

A local fact/intent commit retains its chosen Os/Flush append, locks and sync
cost; a multi-record publication adds its own mutations/CAS. Bridging adds
serialization, payload copies, a downstream RPC and selected persistence/replica
coordination per batch, plus durable source progress. Keep these outside ordinary
KV completion. Batching amortizes costs and increases visibility delay. Only a
caller explicitly asking for downstream acceptance must wait for that receipt.
An enqueue-only receipt must report the pending boundary truthfully.

Qualify lost hint/restart, late lower-revision publication, sparse failure,
duplicate acceptance, lost broker replies/checkpoints, poisoned messages, expired
retry/dedup windows, stale workers, source retention pressure and restore replay.
Measure source commit, queue acceptance, receiver arrival and useful settlement
separately at equal durability and fan-out. Existing signal and indexed tests
check local hint/publication behavior; they do not qualify this bridge or a broker.
