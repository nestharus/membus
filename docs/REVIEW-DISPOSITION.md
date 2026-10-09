# F-E6-1 observe dispositions

First cold observation: e1/act-work/membus-review.json, O1–O23, original design-read SHA256 010b65c4118e32e3b39c198f5c59bbe4afebef617715ee7a1a2a1dc4f1f9e40a. It read the initial plan before seq90 client answer and seq100 tiers were incorporated. Review adds located observations, not an approval. Plan section12 carries authoritative clarifications for older provisional clauses.

| Observation | Fix or answer |
|---|---|
| O1 | Named speculative immediate visibility versus recoverable committed-read delay; OS append has no flush gather wait. |
| O2 | Explicit server-file versus per-writer-file interpretation and departure; unacknowledged queues can be lost, retries named. |
| O3 | Finite leases24h/maxTTL5min; forever data quota; tombstone does not promise physical reclamation. |
| O4 | Metadata index checkpoint plus quota-bound fallback replay; extra snapshot writes stated. |
| O5 | Device-wide flush call budget and measured p99 rule, write-only backpressure. |
| O6 | Correct one flush per touched handle; class/namespace admission bound64. |
| O7 | Age rotation finite-retention only, skip empty chunks; forever size rotation. |
| O8 | Persist anti-resurrection horizon independent of expired/missing display summaries; policy/tombstone test. |
| O9 | Immutable stripe count/hash seed per data directory; changed setting refuses startup. |
| O10 | Explicit fleet authority loss on restart/outage; no priority, router fencing and supervision external. |
| O11 | remaining_ms + client pre-send monotonic safety deadline; validate failure refuses effect, latency unmeasured. |
| O12 | Seq90 client accepts CAS; extra RTT/conflicts/write cost explicit, increment reconsidered on demand. |
| O13 | High-level Python convenience fetch with deadline, low-level status preserves no-drive hot claim. |
| O14 | Outcome/summary/overlay bytes assigned to caps; rough index estimate labelled proposal, measure overhead. |
| O15 | Subscription persistence explicit, first service same logon; LOCAL-pipe service test deferred packaging. |
| O16 | Complete TCP fallback for all mixed cases; explicit flush-budget heuristic, client durability now located. |
| O17 | Byte-sum batch admission, bounded reply pages, per-item partial outcome semantics. |
| O18 | Trusted signed-in user model explicit; self identity not authentication, fencing cooperating processes. |
| O19 | Durable data outside checkout in project state directory; .gitignore now present for generated output. |
| O20 | F1 design-only versus Act F2 suite wording clarified; real measurement appendix before send; both evolving basis hashes retained. |
| O21 | Proposed slice branch purposes named; future frontier IDs remain Decide's, branches created only once commissioned. |
| O22 | Clean original whole-chunk coverage retained; founder tiers added and scoped local cold codec, cloud external facts. |
| O23 | RwLock publication identified and short contention explicitly stated; no disk/write waits while holding it. |

Followup observation of changed section12, landed client facts and tiers: e1/act-work/membus-review-followup.*; its findings are appended here before final delivery.

Second cold observe a4f977e4-e58e-4a34-993b-18df789ff90c completed, findings F1–F19 (e1/act-work/membus-review-followup.json). Plan section13 resolves them as contracts and explicit limits; subsequent consolidation of stale paths/wording is editorial, not another implemented claim.

| Findings | Final fix/answer |
|---|---|
| F1/F12/F13 | Project-state shadow paths, record in README/commit; statuses and all-mode speculative overlay explicit; landed seq90/tier coverage and terminology consolidated. |
| F2/F19 | All retained/cold-key metadata stays bounded in index; cold does not relieve key count; reserve headroom, no absent-on-cold CAS; pressure coupling explicit. |
| F3 | Sparse full snapshots after10×byte threshold and30min, not60s; amplification/readiness tradeoff and all added I/O named. |
| F4/F18 | Every internal flush budgeted, nonzero ticket spacing from repeated p99, priority/deadline loss, os drain and concrete flush batch cap rule. |
| F5/F6 | Total/logical disk quota and maintenance reserve; refusal/fallback is client-owned unresolved protocol question; optional reconciler lease correctly labelled and not adoption prerequisite. |
| F7 | Forever epoch/claim-id control storage, never reused tokens; stronger flush/reclaim versus client os/renew remains explicitly delivered proposal. |
| F8/F9 | Named tier sub-slices/tests and deterministic archive/catalogue/shadow restart actions/status interruption. |
| F10/F14/F17 | Compression-effectiveness rule and literal fallback, no fictional lineup mutex;1h finite/tier rotation, file cap, full load/capacity consequence. |
| F11/F16 | status/catalogue/change queries, policy fields/default modes, canonical transition event schema and catch-up/gap contract. |
| F15 | Warm rereads/OS-cache uncertainty added to drive cost and hot-age selection. |

No third review is asserted; the commission requires cold observation and each finding fixed or answered, which these two returns/dispositions provide. Build correctness and native performance remain future tests.

## e2 independent changed-section observation

Observe5 returned27located observations in e2/act-work/F-E6-1-observations.json; initialclaude2 launch ended with API429 and no observations. Observe5 completed successfully; no verdict requested. Current design SHA074d2226 and survey SHA b128d258 match the cited full hashes.

| Observation | Fix or answer |
|---|---|
| O-01 | Fixed: final IPC repeat raw table and same-build qualification added; PLAN2e updated. |
| O-02 | Fixed: repeated-build qualifier and exact>10%/no>15% thresholds restored in PLAN2e and MD. |
| O-03 | Answered: recorded exe/client hashes match; summarize-only dirty renderer differs from measured implementation; reviewed-1 lacks pinned commit provenance and is labelled accordingly. |
| O-04 | Fixed: earlier concurrency variants are directional only; final/repeat pair supplies rule qualification. |
| O-05 | Fixed: sizes/p95/max and>64row bursts mapped to per-handle splitting/deadlines; no uniform row-size latency claim. |
| O-06 | Fixed: one-thread/handle shape and percentile/sample limits explicit; concurrent service tests remain open. |
| O-07 | Answered: approximately13ms p95 stalls and variable frequency stated; device completion tail retained, no causal flush-only claim. |
| O-08 | Fixed:4KiB stall included in range wording. |
| O-09 | Fixed: original section12O5 now uses final fixed-spacing rule; original appendix calculations marked historical; wall-time inference not a utilization guarantee. |
| O-10 | Fixed: section4 gather window directly updated; OS drain versus flush tickets distinguished. |
| O-11 | Answered:4handle/256handle fan-out yields200ms/12.8s ticket spacing;50ms applies only isolated row, client deadlines apply under load. |
| O-12 | Fixed: claim control high-water then diagnostic grant both budgeted;2tickets possible; only return after both durable, unused ids retained. |
| O-13 | Answered: stronger flush/epoch/fencing remains an explicit proposal versus clientOSrequest, not founder-owned or adopted. |
| O-14 | Answered: exact operations match; transfer unsupported is explained release/reacquire gap, not silent transfer promise. |
| O-15 | Fixed: registry authority_host explicitly matches leaseProcessIdentity, presentation if present else base; supervised base move versus authority-host move distinguished. |
| O-16 | Fixed: overlapping rollout requires same mutex plus lease on new paths; lease-only starts only after legacy hosts verified stopped and deployment revision requires leases. |
| O-17 | Fixed: OS row says rebuildable/log default; ledger flush namespace default explicit. |
| O-18 | Fixed: raw enum-valued cache fields and repeat47–66us included without inferring power safety. |
| O-19 | Answered: device mapping matches devices/context witness; no alteration needed. |
| O-20 | Fixed: prepare slotcpu10,mem4,disk:c specified; separate std targetdisk:d route explained. |
| O-21 | Answered: RLE/literal departure from ZIP retained and optional/cloud scope stated. |
| O-22 | Fixed: disk:d applies to explicit maintenance command; bounded in-server I/O pool remains separate. |
| O-23 | Fixed: versioned admin transition_chunk signature/rules added for owner-triggered tier formB. |
| O-24 | Fixed: rough ordinary key capacity917,504 at224MiB plus reserve. |
| O-25 | Answered: speculative immediate visibility and committed recoverable acceptance remain explicit departure from survey, not hidden compatibility claim. |
| O-26 | Answered: retention values remain identified proposals, founder optional tier relation and claude2 examples preserved. |
| O-27 | Fixed: row/probe sample count200/64 distinguished from wrapperIPCdefault2000; no new executable hash required bycommission. |

## F-E6-11 protocol/replay review dispositions

Observe return: e3/act-work/F-E6-11/observe-findings.md and observe-return.json (session 2d1ab387-e13e-4f36-8699-7059b98c0ae5). This is located observation, not approval. The candidate evolved during the read; final code and KV1.md below supersede the initial staged line numbers. Verification: cargo-acceptance.txt and integration-acceptance/integration-test.json, retained in the commissioned project evidence folder.

| Observation | Act disposition and location |
|---|---|
| O1 post-admission read_only overload | Fixed: lib.rs returns io_failed_uncertain (16) after admitted I/O failure; read_only (10) is before admission. CODES/Python/KV1 §4 agree; injected post-append failure tested through dispatcher. A partial failed append additionally fences its stripe under the append-owner lock before already-admitted followers can append behind a torn tail. Deterministic two-writer partial-failure test replays the earlier acknowledged key and discards only the torn tail. A complete uncertain record may replay, which is why the caller reconciles a deterministic key. |
| O2 pending deadline overload | Fixed: pending_timeout_uncertain (17) for a waiting duplicate; deadline (5) remains pre-admission. Concurrency test produces 17 and then the original outcome. |
| O3 dedup capacity class/rate | Fixed: transient dedup_full (18), default 262,144 slots, --dedup-cap configurable; FIFO lazy expiry and explicit 24 h horizon. KV1 §6 retains a bounded guarantee, not perpetual exactly once. |
| O4 pending conflict persistence | Fixed classification, amended for lane83 eco seq402: pending_conflict (19) is transient against reservations, while conflict (12) is committed expected-state mismatch. KV1 §§3/4 state re-read/reselect and client-owned hole filling if a pending winner fails. A committed wrong expected state still needs client action. |
| O5 CRC dedup identity | Fixed: exact mutation-byte equality replaces CRC identity; CRC is only log corruption detection. Exact retained payloads are charged to capacity. |
| O6 past-window coverage | Fixed: protocol test covers expired known id, retired-ring id and unknown-after-restart re-execution. Ring eviction has the same unknown-id rule; its finite cap remains documented. |
| O7 ingress/connection memory bounds | Fixed: 64 active connections, aggregate 64 MiB frame admission before allocation, released after dispatch; queue admission remains separate. Foreground server pipe slots can remain occupied until disconnect; finite resource bound and client cancellation are explicit. |
| O8 pipe exhaustion/accept errors | Fixed: bounded active slots stop pipe instance creation at 64, with 255 kernel instance ceiling; excess callers reach their deadline without ending the listener. ERROR_NO_DATA retries accept; TCP accept errors continue. Integration opens 64 pipe connections and checks listener survival. Persistent unrecoverable creation errors remain fatal with stderr. |
| O9 negotiated frame / pipe allocation | Fixed: serve enforces negotiated request body/reply cap; Python enforces stored cap. Pipe read/write buffers are at most 64 KiB each, avoiding remainder-sized repeated allocation. Minimum hello offer is 1,024 bytes for the refusal list. |
| O10 admission scan blocks reads | Fixed: separate metadata, scalar key/namespace/value/dedup counters, bounded retired-id hash membership and ordered expiry queue. Admission never holds the committed read lock or scans its key map. Publication remains a short map update; get takes no write/flush/admission mutex. |
| O11 os blocked by flush sleep | Fixed: append mutex released before ticket sleep and sync on a cloned file handle. OS appends take no deliberate ticket wait. Answered performance reach: slice 1 does not group flushes; 3,065 serial flush rows may take ~153 s at 50 ms, disclosed in KV1 §9. Grouping remains the next write-path slice, not an adoption claim. |
| O12 interior length mistaken for tear | Fixed: log header contains length and its bitwise complement. Complement mismatch reports corruption before considering a final incomplete body. Dedicated test flips interior length and proves the file is unmodified. |
| O13 replay modifications/stripe ownership | Fixed: stage torn-tail truncations until all stripes validate and enforce key-to-stripe ownership. Complete corrupt frames remain untouched and block startup. No automatic repair or power-loss availability claim. |
| O14 pipe timeout/first-instance/ACL | Fixed: first-instance guard and creator-user/LocalSystem ACL; remote clients rejected, active/frame bounds enforced. Answered: hard server pipe idle timeout belongs to later overlapped/service transport; client deadline cancellation is implemented and tested. Foreground endpoint is bounded, not service adoption. |
| O15 TCP idle close | Answered in KV1 §6: 60 s per-I/O idle bound; stable-op-id reconnect handles an idle socket close. Absolute server connection lifetime cutoff is not claimed. |
| O16 Python reconnect/epoch/association | Fixed: calls reconnect from None, close on malformed/association failures, and surface EpochChanged before uncertain replay across a restart. Reuse after timeout tested. Batch supports remove and returns code name/class; remove has the same version/ack dict as put; health uses requested namespace. |
| O17 framing diagnostics | Fixed: short body is malformed (3), large is too_large (4); version error closes; ready prints store.epoch. |
| O18 namespace quota / warnings | Fixed: initial 500k namespace keys covers located 270–350k; requested-namespace count and 70% namespace/global high-water flags on wire. Quota/cap remain configurable; value/dedup limits are distinct. |
| O19 raw seq keys incompatible UTF-8 | Fixed wording: KV1 §1 specifies fixed-width UTF-8 decimal/hex encodings; raw be64 is refused by this version. No covert binary-key claim. Client can preserve lexical order without a server stream. |
| O20 copied replay witness | Fixed: replayed list and counts are collected from actual successful get_many values, compared to acknowledged list, and missing ids computed. |
| O21 contention/flush evidence | Fixed measurement: four configured-50ms flush latencies and elapsed time recorded; API and CLI baselines both stated. Answered scope: main burst is single-writer OS-ack framing/allocation; pending conflict and duplicate use separate concurrent-client tests. This does not establish concurrent dense-seq or ledger flush-burst capacity. |
| O22 idle kill / synthetic tear | Answered: commissioned acceptance is every acknowledged OS/flush write after a killed server; test witnesses all 3,070. Torn header and length corruption are synthesized independently. Kill-during-partial-append stress belongs with queue/grouped-write slice; no natural torn-body or power-failure coverage claim. |
| O23 old in-place plan | Fixed: PLAN §1/§3 and §13 item5 replace the outage story with client-owned unsequenced spool; KV1 §9 supersedes fixed flush spacing with configurable initial value and current comparison. Historical preliminary measurements and future slice designs retain their scope. |
| O24 smaller notes | Answered/fixed: tombstones count as keys and peak-memory overwrite reservation can refuse early (KV1 §7); persisted incrementing epoch replaces wall clock; non-Windows lock cleanup is disclosed; put/hello/get reply now have Rust/Python shared golden fixtures; the closed code list is exact for major1/minor0 and changes require version negotiation. |

Late scoped input eco seq402: lane83 found six of eight statements met, asked which conflict is pending/committed, and requested a timed killed-server flush-acked burst. Pending conflict now has code19; tests/flush_burst.py verifies 3,070 fully flush-acked conditional rows at configured50ms, kill/replay and actual id sets, with output in e3/act-work/F-E6-11/flush-burst/integration-flush.json. Item5 remains a stated later scan scope; item7 now names 54–70% namespace occupancy at adoption and unmeasured per-key allocator memory. KV1 §8 records the client's own shadow invariants rather than treating shadow as selected or primary migration. Grouped commit remains later work.
