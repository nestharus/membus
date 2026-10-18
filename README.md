# Membus

Read-only [retirement inspection](docs/RETIREMENT-INSPECTION.md) exposes selected
HEALTH2 counters and exact chunk states from a stopped store's catalogue for
qualification. These commands grant no lease or receiver authority.

A memory and IPC store project. This repository begins empty apart from this scope record; implementation remains a separate selected act.

Ledger: D:\projects\oulipoly\trunk\research\87_operations_representation\claude6\ledger.json

Frontier: F-W6-6 (store work); repository setup: F-W6-7.

Layout: D:/projects/membus/trunk and D:/projects/membus/worktrees. Private repository; each work branch records purpose, ledger and frontier in its description.

Current owner after the 2026-10-09 loop split: claude6-eco. Ledger: D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/ledger.json; frontier F-E6-1. The F-W6-6 reference above retains the original repository-setup commission.

The first-client design and candidate sheet are in `docs/PLAN.md`, with observation dispositions in `docs/REVIEW-DISPOSITION.md` and measured selection rules in `docs/MEASUREMENT-DECISION.md`. They are plans for a later server build, not implemented behaviour. Runtime state is planned at `D:/projects/membus/state/`, outside source checkouts/worktrees so source cleanup cannot remove canonical data. Build and probe outputs stay ignored.

The current server implements kv/1 durable writes, grouped flushes, bounded namespace snapshots and Windows change subscriptions. F-E6-33's interface and limits are in docs/SUBSCRIPTIONS.md; its report, review returns, measurements, install identity and message receipts are in D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e8/act-work/F-E6-33/. That folder records the purpose of this slice's artifacts. Signal history is bounded and epoch scoped; a pipe write is transport delivery, not receiver acknowledgement.

F-E6-41 adds persisted one-off/repeating timer wakes with anchored jitter, compact per-fire progress, catch-up counts and generation-fenced cancel/replace. The protocol, crash boundaries and byte rules are in [TIMERS.md](docs/TIMERS.md). Its verified outputs, reviews/dispositions, release source/hash binding and install/cleanup record belong to D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e9/act-work/F-E6-41/. Timers deliver generic wakes; the consumer's due-id/lease governs payload execution.

The public resident Rust client is documented in [RUST_CLIENT.md](docs/RUST_CLIENT.md). It connects to an explicit existing endpoint and supports negotiated HELLO, committed GET/GET_MANY, OS/flush PUT/CAS and caller-owned uncertainty/reconciliation tokens; it never opens backing state. Consumer-specific retry, operation identity and clock policy remain above this API.
