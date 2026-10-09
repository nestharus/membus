# Measurement decisions for the first membus client

Act e2, F-E6-1, 2026-10-09. These are probe results and proposed operating rules, not a qualified server. Raw base: `D:\projects\oulipoly\trunk\research\87_operations_representation\claude6-eco\runs\e1\act-work\measure`.

Design record `D:/dev/handoff/memory-ecosystem.md` SHA256 `074d22267522e57ad4ed32e1d95aef64833358a41fc9908e1bf958d99d2ffcb6`; survey `D:/dev/handoff/fast-local-store-survey.md` SHA256 `b128d2588930d54819fceae4047299e4591535763f7048d89c4cf31a9fcfd10f`.

## Transport rule (PLAN section 2e)

At 2,253 bytes (the approximately 2.2 KB client row), select pipe when repeated final-build Python p99 improves >10%, with no >15% loss at 8/32 clients. The following raw rows give the decision:

| File, raw line (TCP/pipe) | Clients | TCP p99 us | Pipe p99 us | Pipe improvement % |
|---|---:|---:|---:|---:|
| ipc-1.jsonl:8/26 | 1 | 69.6 | 29.7 | 57.33 |
| ipc-1.jsonl:10/28 | 8 | 271.7 | 209.4 | 22.93 |
| ipc-1.jsonl:12/30 | 32 | 1545.4 | 1529.8 | 1.01 |
| ipc-2.jsonl:8/26 | 1 | 120.0 | 57.7 | 51.92 |
| ipc-2.jsonl:10/28 | 8 | 291.2 | 322.1 | -10.61 |
| ipc-2.jsonl:12/30 | 32 | 1717.1 | 1436.2 | 16.36 |
| ipc-reviewed-1.jsonl:8/26 | 1 | 97.8 | 56.3 | 42.43 |
| ipc-reviewed-1.jsonl:9/27 | 8 | 143.5 | 56.6 | 60.56 |
| ipc-reviewed-1.jsonl:12/30 | 32 | 212.1 | 131.1 | 38.19 |
| ipc-final.jsonl:8/26 | 1 | 79.1 | 56.2 | 28.95 |
| ipc-final.jsonl:9/27 | 8 | 96.3 | 83.7 | 13.08 |
| ipc-final.jsonl:12/30 | 32 | 216.3 | 202.9 | 6.20 |

All four runs meet the relative rule, but ipc-1/2 and reviewed/final use different client concurrency harnesses and are not pooled as capacity evidence. The final-build IPC repeat at the end of this document confirms the repeated rule. Named pipe is selected for the first Windows client; TCP remains the fallback and shares framing. ACL/reconnect bounds and integrated store performance still need their build tests.

## Flush tickets, batch cap and bounded wait (PLAN sections 2a, 4, 12, 13.4)

| File:line | Kind | Payload (bytes, or records/batch) | p50 us | p99 us | Bytes/batch |
|---|---|---:|---:|---:|---:|
| flush-D-1.jsonl:7 | flush | 4096 | 779.3 | 1461.5 | 4096 |
| flush-D-1.jsonl:8 | flush | 16384 | 780.1 | 12926.8 | 16384 |
| flush-D-1.jsonl:9 | flush | 65536 | 782.8 | 12817.6 | 65536 |
| flush-D-1.jsonl:10 | write_through | 4096 | 64.1 | 183.3 | 4096 |
| flush-D-1.jsonl:11 | write_through | 16384 | 68 | 166 | 16384 |
| flush-D-1.jsonl:12 | write_through | 65536 | 104.8 | 217.1 | 65536 |
| flush-D-1.jsonl:16 | group_commit | 1 | 769 | 1265.4 | 2253 |
| flush-D-1.jsonl:17 | group_commit | 8 | 827.3 | 1512.1 | 18024 |
| flush-D-1.jsonl:18 | group_commit | 64 | 1155.1 | 2699.8 | 144192 |
| flush-D-1.jsonl:19 | group_commit | 512 | 2504.5 | 3036 | 1153536 |
| flush-D-2.jsonl:7 | flush | 4096 | 758.4 | 12771.4 | 4096 |
| flush-D-2.jsonl:8 | flush | 16384 | 806.4 | 14435.5 | 16384 |
| flush-D-2.jsonl:9 | flush | 65536 | 809.4 | 24680.1 | 65536 |
| flush-D-2.jsonl:10 | write_through | 4096 | 99.6 | 318.9 | 4096 |
| flush-D-2.jsonl:11 | write_through | 16384 | 101.5 | 12607 | 16384 |
| flush-D-2.jsonl:12 | write_through | 65536 | 119.1 | 232.5 | 65536 |
| flush-D-2.jsonl:16 | group_commit | 1 | 758.1 | 1373.7 | 2253 |
| flush-D-2.jsonl:17 | group_commit | 8 | 809 | 1496.7 | 18024 |
| flush-D-2.jsonl:18 | group_commit | 64 | 969.6 | 1211.8 | 144192 |
| flush-D-2.jsonl:19 | group_commit | 512 | 2891.3 | 14552.5 | 1153536 |

**Historical e1 rule evaluation, superseded by the e2 fixed-spacing rule below:** ticket calibration used **group_commit at 1/8/64 records**, each 2,253 bytes, because these are the measured ledger append shapes. Upper observed p99 is 2699.8 us; max(50 ms, 10 × 2.6998 ms) = **50 ms**, initially at most 20 tickets/s across every handle and internal flush. Group512 is outside this calibrated envelope. Run1 predates 599f33d; run2 is the single final-build run at this issue. Recalibrate from F-E6-2 final-build repeat before a production trial.

Batch64 bytes = 64 × 2,253 = 144,192. clamp(8 × 144,192, 65,536, 1,048,576) = **1,048,576 bytes (1 MiB)** as an admission/append cap. This does not claim a 1 MiB flush has the latency of batch64. A ledger ticket gathers at most 64 ordinary rows or 144,192 bytes; larger work uses separately measured shapes and maintenance stays behind urgent client flushes.

**Historical estimate, superseded below:** a lone small ledger row on an otherwise empty scheduler waits at most **50 ms for a ticket**, plus its append and flush. Using the observed group p99 upper gives an estimated 52.6998 ms before IPC, rather than a 247 ms gather delay. This is a scheduler bound and observed latency estimate, not a hard end-to-end guarantee. Competing handles, stalled devices and maintenance add queueing; the client supplies a deadline, and a missed deadline returns uncertain acceptance or pre-admission refusal explicitly. Deadline expiry does not silently weaken durability. Queue depth and measured integrated p99 are required in the first server trial.

Single-flush 4/16/64 KiB p99 reaches 12.8176–24.6801 ms and would yield 128.176–246.801 ms spacing if used indiscriminately. Write-through without an explicit flush also has a 12.607 ms tail, so this is evidence consistent with a device stall, not proof that every stall is caused by flush. Larger flush workloads retain that tail and are tested separately; limiting ticket frequency alone does not prove device utilization <=10%.

## Device and acknowledgement rules (PLAN sections 2b and 8)

D: remains the first log device: flushed 4/16/64 KiB p50 = 758.4–809.4 us in run2 and 779.3–782.8 us in run1 (raw rows above). C: is retained as a loaded-device observation, not a capacity estimate. Raw flush-C-2 rows below document the per-shape cost:

| File:line | Kind / bytes | p50 us | p99 us |
|---|---|---:|---:|
| flush-C-2.jsonl:7 | flush / 4096 | 24593.9 | 108082.4 |
| flush-C-2.jsonl:8 | flush / 16384 | 23826.4 | 120639.9 |
| flush-C-2.jsonl:9 | flush / 65536 | 23917.9 | 83998.6 |

Co-tenant evidence is in each .context.jsonl plus baseline-2.jsonl.tenants.jsonl. C: run2 had concurrent users and durable writes cost approximately 13–25 ms p50. No C: remeasurement is required for selecting D:.

| Ack form | First-client fit and failure scope | Selection rule |
|---|---|---|
| mem | Disposable signals; loss on server kill possible | Explicit opt-in only |
| os | Append complete, process-kill recovery; OS/power crash unqualified | Rebuildable views/logs |
| flush | Append plus FlushFileBuffers before ack; authoritative rows; hardware policy limits power-loss claim | Ledger default; qualify replay and kill tests |
| write_through (proposed fourth form) | FILE_FLAG_WRITE_THROUGH without explicit FlushFileBuffers, 64.1–119.1 us p50 in std probe above; affects its own writes rather than requesting a general flush. Windows cache policy reports no declared power protection. Process recovery requires replay tests; T700 power-loss survival is **unknown**. | Keep as a candidate, outside kv/1 negotiated modes, until official device/filesystem durability basis and equivalent kill/recovery tests establish the required guarantee; then benchmark same framed group workload and version the capability. It cannot replace ledger flush on latency alone. |

The published hardware facts are in write-cache-policy.jsonl (disk1/2 cache enabled, flush supported, no user-declared power protection). Neither latency nor that policy establishes actual NAND persistence after power loss. Crucial specification lookup belongs to F-E6-2 and may remain not located. Diskspd numbers provide corroboration only: its admission/scan/review correction is recorded in F-E6-2; this design runs no further diskspd.

## Remaining measurement-named rules

Section2c/d alternatives remain deferred until actual index/cache pressure and comparable maintenance load; no probe qualifies a database/cache implementation. Section4 gather window is superseded by the section13 ticket rule above; OS appends drain immediately. Section5 compression requires >=10% savings and actual read-reuse data for tier ages; neither was measured by IPC/flush probes. Section9 and section13 require integrated concurrent-reader, kill/recovery, cache working-set, memory, restart and maintenance trials; these are future build milestones. Current measurements decide transport, ticket calibration, batch admission and log device only.

## e2 same-build repeat and explicit rule revision

Raw `D:/projects/oulipoly/trunk/research/87_operations_representation/claude6-eco/runs/e2/act-work/F-E6-2/flush-D-repeat.jsonl` on clean599f33d at12:34:59Z:

| Line | Records/batch | p99 us |
|---|---:|---:|
| 16 | 1 | 13256.7 |
| 17 | 8 | 12573.7 |
| 18 | 64 | 1585.5 |
| 19 | 512 | 2813.4 |

The upper repeated final-build group1/8/64 p99 is **13256.7 us**. Applying the old formula literally yields **132.567 ms**, so the earlier50ms formula outcome above is superseded. It reacts to device completion stalls also seen without explicit flush, and would impose an additional wait on every row. The first-build rule is now **fixed50ms device ticket spacing, at most20 calls/s**, while the per-client deadline bounds admission/queueing and actual flush completion is measured separately. This deliberately replaces the unbounded10×p99 spacing heuristic; it is not a claim of <=10%device utilization. Integrated trial must report actual completion latency and co-tenant pressure before production adoption. A lone row with no queued competitors waits <=50ms for its ticket plus append/flush/IPC; using the new observed group p99 upper estimates63.2567ms before IPC, not a hard end-to-end bound. All flush shapes remain subject to device stalls. Batch cap and D: selection stand.

## Final-build IPC repeat qualification

Raw `D:\projects\oulipoly\trunk\research\87_operations_representation\claude6-eco\runs\e2\act-work\F-E6-2\ipc-repeat.jsonl`, matching clean599f33d,500requests/client:

| TCP/pipe raw line | Clients | TCP p99 us | Pipe p99 us | Improvement % |
|---|---:|---:|---:|---:|
| 8/26 | 1 | 69.5 | 42.4 | 38.99 |
| 9/27 | 8 | 73.1 | 40.1 | 45.14 |
| 12/30 | 32 | 246.4 | 199.7 | 18.95 |

Both final-build runs meet section2e (>10%single-client improvement and no>15%concurrent loss); pipe default is now repeated-build qualified for this relative probe rule. Earlier single-build pending wording above records initial issue and is superseded here. Echo measurements still exclude the implemented store.

## Observation qualification details

ipc-reviewed-1 carries the same executable/client hashes as ipc-final, but lacks its pinned final-build commit provenance. ipc-final's only dirty file was measurements/summarize.py, the table renderer; the measured executable and client match final599f33d. ipc-repeat records that final commit and clean tree. Identification here is the existing provenance, not an added hashing obligation. Earlier IPC variants used different concurrency methods; only the same-build final/repeat pair qualifies the final client rule.

Flush samples are200 per small shape and64 per group64/512; the wrapper's provenance field timed_requests_per_client=2000 is an IPC default and not the flush sample count. Row samples and the probe source define the flush sample count. Sparse observed p99 is not a stability/confidence guarantee. Repeat write_through p50 is47.0–66.2us, below the earlier64.1–119.1us range; Windows raw policy includes WriteCacheType=2, WriteCacheEnabled=2, WriteThroughSupported=2, FlushCacheSupported=true, UserDefinedPowerProtection=false, NVCacheEnabled=false. Enum-valued properties are not interpreted as booleans; no power survival follows from them. The candidate remains unqualified for authoritative ledger acknowledgement.

Ledger record sizes and fan-out, the two-ticket claim grant, and the transition to leases are covered in PLAN's e2 qualification section. Fixed50ms spacing replaces the old device-wall-time heuristic; it bounds isolated ticket scheduling only, and leaves integrated contention/completion evidence open.
