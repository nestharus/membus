# Python indexed receiving contract

Add this source tree's `python` directory to the import path. The callable
`membus_indexed.Collection` and `Publication` contract is described in
[Python indexed collections](PYTHON_INDEXED.md). It uses the existing serialized
`membus.Client`; it starts no provider and owns no backing store.

This composition preserves the Python source from
`b35c7d1ffb6747b6e8dbc2ccfdda68674dcd9961`, including the indexed corrections
from `d9da9cb2dc062076812662d6391155fc00c9c343`, on published-main source
`6d323bba709e205d3c85b188c0f4a3cca720c374`. Python source availability does not
establish an installed backend or application capability.

## Provider dependency

Complete indexed queries require the provider's opcode 31 on the same supplied
snapshot token, before any schema, head, row or posting selection. The defining
native source is `06158d36986cf7d74d1b2ec25212450a18e4ec5c`; its fresh-main
receiving candidate is `5003a2c21d540bbd0267ad33e400b6117e611aa1`. Those are
separate source candidates. This Python branch preserves its main Rust tree
and does not include those native changes.

| Selected backend | Complete indexed query |
| --- | --- |
| Published main `6d323bba` | Opcode 31 unavailable; `NotAvailable` from unsupported code 2, without fallback or query selection. |
| Native `06158d3` or fresh-main candidate `5003a2c` | Captured-token eligibility API exists in source. Empty success permits selection; ineligible 16, expired 20 and invalid 21 refuse. Actual operating selection and configuration require separate qualification. |

Captured eligibility is immutable; later HEALTH2 is not a substitute. It covers
known storage uncertainty, not undetected corruption, physical integrity,
authenticated dataset identity or restored serving authority. Raw snapshot
inspection has a weaker contract. No live provider was probed for this source
composition.

## Exact supported bounds

Indexed records retain at most 64 fields and 1024 values per field. The complete
encoded row must fit 4 MiB and the negotiated value/frame limits; preparation
refuses oversized representations before mutation, without truncation. The
maximum protocol frame is 16 MiB. Page limits are at most 128 and remain subject
to negotiation, token lifetime, process and snapshot admission. Query row and
encoded-byte budgets are explicit. These are bounds, not available capacity or
a guarantee that a particular application population fits.

Indexed preparation requires a positive negotiated retry horizon. Publication
retains endpoint, epoch, original requests, requested Ack and the earliest first
send. Mutations use `retry_transport=False`. Explicit reconciliation cannot
renew the horizon or discard uncertainty. Publication custody is in memory;
there is no durable whole-append token or abandoned-stage takeover API.

The shared outer decoder requires a negotiated refusal member and an empty
refusal payload. Malformed associated refusals close the channel with
`ValueError`; the indexed owner retains the original uncertain mutation. Valid
16/17 remain uncertain, while valid definitive refusals keep their codes.
Ordinary `Client.call` retains its existing default transport retry behavior;
its ordinary mutation retry/dedup-horizon debt is not repaired by these checks.
Signals, timers, HELLO and explicit connection/snapshot cleanup retain their
existing contracts.

## Atomic receiving gap

This tree supplies no `membus_atomic` Python module. The separately qualified
module at `a0608eb956cf70eea98ac56353e89310a8299ad6` requires native opcode 30
for `capture(client, namespace, Limits, read_id, deadline)`: MAS2 population
guard, exact limits/charge, retained scan token, complete paging and MAR2
`View.prepare`. Native `06158d3` and `5003a2c` do not implement that operation.
Importing that module alone would not establish its capture capability.

The native candidates instead supply Rust `Client::capture_atomic_namespace`
on opcode 28 with one MAV1/MAR1 population and `Client::publish_atomic` on
opcode 29. Their limits remain 4096 total live namespace keys, 128 final
changes, FOREVER-only policies, canonical original at most MAX_FRAME/3 and
complete negotiated-frame/admission limits. No supported Python MAR1
capture/prepare/custody/publication binding is added here. Neither numeric
`Client.call`, scalar writes nor per-item BATCH substitutes for it.

Large-population MAR2, complete large-record representation, read-cohort
accounting, durable Python custody, application retention/history/restore,
independent serving authority and live handover remain separate capabilities.
